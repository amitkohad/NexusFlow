"""Closed Temporal chains cannot accept late human-task outbox signals."""

from __future__ import annotations

import asyncio
import os
import shutil
from collections.abc import AsyncIterator
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio
from human_task_service.dispatch import (
    TaskWorkflowClosed,
    TaskWorkflowMissing,
    TemporalTaskDispatcher,
)
from temporalio import workflow
from temporalio.client import WorkflowExecutionStatus, WorkflowFailureError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="module")]


@workflow.defn(name="HumanTaskDispatchWait")
class WaitingWorkflow:
    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: False)


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def temporal_environment() -> AsyncIterator[WorkflowEnvironment]:
    cli_path = os.environ.get("TEMPORAL_CLI_PATH") or shutil.which("temporal")
    if not cli_path or not Path(cli_path).is_file():
        pytest.fail("Human-task dispatch test requires a local Temporal CLI")
    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=cli_path,
        dev_server_log_level="error",
        ui=False,
    ) as environment:
        yield environment


@pytest.mark.parametrize("close", ["cancel", "terminate"])
async def test_late_signal_to_closed_matching_chain_is_blocked(
    temporal_environment: WorkflowEnvironment, close: str
) -> None:
    client = temporal_environment.client
    queue = "closed-task-dispatch-" + uuid4().hex
    async with Worker(client, task_queue=queue, workflows=[WaitingWorkflow]):
        handle = await client.start_workflow(
            WaitingWorkflow.run,
            id="closed-task-" + uuid4().hex,
            task_queue=queue,
        )
        first_run_id = handle.first_execution_run_id
        assert first_run_id is not None
        if close == "cancel":
            await handle.cancel()
            expected = WorkflowExecutionStatus.CANCELED
        else:
            await handle.terminate(reason="operator closed test workflow")
            expected = WorkflowExecutionStatus.TERMINATED
        with pytest.raises(WorkflowFailureError):
            await asyncio.wait_for(handle.result(), 15)
        assert (await handle.describe()).status == expected
        dispatcher = TemporalTaskDispatcher(client)
        with pytest.raises(TaskWorkflowClosed):
            await dispatcher.signal(
                handle.id,
                first_run_id,
                "approve",
                {
                    "task_id": "old-task",
                    "event_id": uuid4().hex,
                    "idempotency_key": "old-approval-key",
                    "approved": True,
                    "approver": "alice",
                    "comment": "",
                },
            )


async def test_missing_temporal_workflow_is_classified_permanent(
    temporal_environment: WorkflowEnvironment,
) -> None:
    dispatcher = TemporalTaskDispatcher(temporal_environment.client)
    with pytest.raises(TaskWorkflowMissing):
        await dispatcher.signal(
            "missing-human-task-" + uuid4().hex,
            "missing-first-run",
            "approve",
            {"task_id": "missing-task", "event_id": uuid4().hex},
        )
