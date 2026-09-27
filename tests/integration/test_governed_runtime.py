"""Real Temporal execution across independently owned Activity workers."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
from collections.abc import AsyncIterator, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
import pytest_asyncio
from contracts import (
    ActivityRequest,
    ActivityResponse,
    DefinitionDocument,
    RuntimeContext,
    RuntimeStartRequest,
)
from temporalio import activity
from temporalio.client import Client, WorkflowFailureError, WorkflowHandle
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import ActivityError, RetryState, TimeoutError, TimeoutType
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker
from workflow_runtime.workflows import GovernedWorkflowV1

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="module")]
ROOT = Path(__file__).resolve().parents[2]
ORCHESTRATION_QUEUE = "workflow-orchestration-tq"
VALIDATION_QUEUE = "validation-tq"
NOTIFICATION_QUEUE = "notification-tq"
INTEGRATION_QUEUE = "integration-tq"
HUMAN_TASK_QUEUE = "human-task-tq"
SAMPLE_BUSINESS_QUEUE = "sample-business-tq"
GovernedHandle = WorkflowHandle[GovernedWorkflowV1, dict[str, Any]]


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def governed_environment() -> AsyncIterator[WorkflowEnvironment]:
    cli_path = os.environ.get("TEMPORAL_CLI_PATH") or shutil.which("temporal")
    if not cli_path or not Path(cli_path).is_file():
        pytest.fail("Governed runtime tests require a local Temporal CLI or TEMPORAL_CLI_PATH")
    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=cli_path,
        dev_server_log_level="error",
        ui=False,
        data_converter=pydantic_data_converter,
    ) as environment:
        yield environment


def worker_activities() -> dict[str, list[Callable[..., Any]]]:
    from human_task_worker import ACTIVITIES as HUMAN_ACTIVITIES
    from integration_worker import ACTIVITIES as INTEGRATION_ACTIVITIES
    from notification_worker import ACTIVITIES as NOTIFICATION_ACTIVITIES
    from sample_business_worker import ACTIVITIES as SAMPLE_ACTIVITIES
    from validation_worker import ACTIVITIES as VALIDATION_ACTIVITIES

    return {
        VALIDATION_QUEUE: list(VALIDATION_ACTIVITIES),
        NOTIFICATION_QUEUE: list(NOTIFICATION_ACTIVITIES),
        INTEGRATION_QUEUE: list(INTEGRATION_ACTIVITIES),
        HUMAN_TASK_QUEUE: list(HUMAN_ACTIVITIES),
        SAMPLE_BUSINESS_QUEUE: list(SAMPLE_ACTIVITIES),
    }


@asynccontextmanager
async def running_workers(
    client: Client,
    *,
    include_runtime: bool = True,
    exclude_queues: frozenset[str] = frozenset(),
    overrides: dict[str, list[Callable[..., Any]]] | None = None,
) -> AsyncIterator[None]:
    async with AsyncExitStack() as stack:
        if include_runtime:
            await stack.enter_async_context(
                Worker(
                    client,
                    task_queue=ORCHESTRATION_QUEUE,
                    workflows=[GovernedWorkflowV1],
                )
            )
        owned_activities = worker_activities()
        owned_activities.update(overrides or {})
        for queue, activities in owned_activities.items():
            if queue not in exclude_queues:
                await stack.enter_async_context(
                    Worker(
                        client,
                        task_queue=queue,
                        activities=activities,
                        graceful_shutdown_timeout=timedelta(seconds=0),
                    )
                )
        yield


def sample_document(amount: int = 7500, *, transient_failure: bool = False) -> dict[str, Any]:
    document: dict[str, Any] = json.loads(
        (ROOT / "examples/customer_adjustment.json").read_text(encoding="utf-8")
    )
    document["request"]["amount"] = amount
    document["request"]["simulate_transient_failure"] = transient_failure
    return document


def validation_document(*, timeout: int = 10, attempts: int = 3) -> dict[str, Any]:
    return {
        "start_at": "validate",
        "steps": {
            "validate": {
                "type": "activity",
                "capability": "validate_request",
                "next": "done",
                "timeout_seconds": timeout,
                "retry": {
                    "maximum_attempts": attempts,
                    "initial_interval_seconds": 0.1,
                    "maximum_interval_seconds": 0.2,
                },
            },
            "done": {"type": "end"},
        },
    }


async def start_governed(
    client: Client,
    document: dict[str, Any],
    *,
    amount: int = 1000,
) -> GovernedHandle:
    payload = runtime_payload(document, amount)
    return await client.start_workflow(
        GovernedWorkflowV1.run,
        payload,
        id=f"governed-{uuid4().hex}",
        task_queue=ORCHESTRATION_QUEUE,
        execution_timeout=timedelta(seconds=30),
    )


def runtime_payload(document: dict[str, Any], amount: int) -> dict[str, Any]:
    return RuntimeStartRequest(
        context=RuntimeContext(
            tenant="integration-tenant",
            business_domain="finance",
            application="adjustments",
            workflow_type="customer-adjustment",
            definition_id="customer-adjustment-definition",
            definition_version="1.0",
            business_reference="adjustment-123",
            correlation_id="runtime-trace",
            actor="integration-manager",
        ),
        definition_document=DefinitionDocument.model_validate(document),
        request={**document.get("request", {}), "amount": amount},
        variables=document.get("variables", {}),
    ).model_dump(mode="json")


async def wait_for_status(
    handle: GovernedHandle,
    *,
    state: str | None = None,
    step: str | None = None,
) -> dict[str, Any]:
    async with asyncio.timeout(15):
        while True:
            status: dict[str, Any] = await handle.query("status")
            if (state is None or status["state"] == state) and (
                step is None or status["current_step"] == step
            ):
                return status
            await asyncio.sleep(0.025)


async def scheduled_activities(handle: GovernedHandle) -> list[tuple[str, str]]:
    history = await handle.fetch_history()
    return [
        (
            event.activity_task_scheduled_event_attributes.activity_type.name,
            event.activity_task_scheduled_event_attributes.task_queue.name,
        )
        for event in history.events
        if event.HasField("activity_task_scheduled_event_attributes")
    ]


async def test_sample_approval_uses_all_five_owned_queues(
    governed_environment: WorkflowEnvironment,
) -> None:
    async with running_workers(governed_environment.client):
        handle = await start_governed(governed_environment.client, sample_document(), amount=7500)
        waiting = await wait_for_status(handle, state="WAITING_FOR_APPROVAL")
        assert waiting["current_step"] == "manager_approval"
        await handle.signal(
            "approve", {"approved": True, "approver": "manager", "comment": "Verified"}
        )
        result = await asyncio.wait_for(handle.result(), 15)
        assert result["state"] == "COMPLETED"
        assert result["results"]["post"]["posted"] is True
        assert result["results"]["notify"]["sent"] is True
        assert set(await scheduled_activities(handle)) == {
            ("validate_request.v1", VALIDATION_QUEUE),
            ("risk_check.v1", SAMPLE_BUSINESS_QUEUE),
            ("create_approval_task.v1", HUMAN_TASK_QUEUE),
            ("post_adjustment.v1", INTEGRATION_QUEUE),
            ("send_notification.v1", NOTIFICATION_QUEUE),
        }


async def test_low_amount_routes_without_human_task_and_substitutes_template(
    governed_environment: WorkflowEnvironment,
) -> None:
    document = sample_document(1000)
    document["steps"]["notify"]["input"]["channel"] = "${variables.channel}"
    document["variables"]["channel"] = "email"
    async with running_workers(governed_environment.client):
        handle = await start_governed(governed_environment.client, document)
        result = await asyncio.wait_for(handle.result(), 15)
        assert result["state"] == "COMPLETED"
        assert result["results"]["notify"]["channel"] == "email"
        assert "manager_approval" not in result["results"]
        assert ("create_approval_task.v1", HUMAN_TASK_QUEUE) not in await scheduled_activities(
            handle
        )


@pytest.mark.parametrize("decision,outcome", [(False, "REJECTED"), (None, "TIMED_OUT")])
async def test_rejection_and_approval_timeout_follow_configured_branch(
    governed_environment: WorkflowEnvironment,
    decision: bool | None,
    outcome: str,
) -> None:
    document = sample_document()
    if decision is None:
        document["steps"]["manager_approval"]["timeout_seconds"] = 1
    async with running_workers(governed_environment.client):
        handle = await start_governed(governed_environment.client, document, amount=7500)
        await wait_for_status(handle, state="WAITING_FOR_APPROVAL")
        if decision is not None:
            await handle.signal(
                "approve", {"approved": decision, "approver": "manager", "comment": "Denied"}
            )
        result = await asyncio.wait_for(handle.result(), 15)
        assert result["state"] == outcome
        assert "post" not in result["results"]
        if decision is False:
            assert result["results"]["record_rejection"]["recorded"] is True
            assert ("record_rejection.v1", SAMPLE_BUSINESS_QUEUE) in await scheduled_activities(
                handle
            )


async def test_transient_technical_failure_retries_the_owned_activity(
    governed_environment: WorkflowEnvironment,
) -> None:
    document = sample_document(1000, transient_failure=True)
    async with running_workers(governed_environment.client):
        handle = await start_governed(governed_environment.client, document)
        assert (await asyncio.wait_for(handle.result(), 15))["state"] == "COMPLETED"
        history = await handle.fetch_history()
        attempts = [
            event.activity_task_started_event_attributes.attempt
            for event in history.events
            if event.HasField("activity_task_started_event_attributes")
        ]
        assert 2 in attempts


async def test_validation_failure_is_non_retryable(
    governed_environment: WorkflowEnvironment,
) -> None:
    async with running_workers(governed_environment.client):
        handle = await start_governed(governed_environment.client, validation_document(), amount=0)
        with pytest.raises(WorkflowFailureError) as caught:
            await asyncio.wait_for(handle.result(), 15)
        failure = caught.value.cause
        assert isinstance(failure, ActivityError)
        assert failure.retry_state == RetryState.NON_RETRYABLE_FAILURE
        history = await handle.fetch_history()
        attempts = [
            event.activity_task_started_event_attributes.attempt
            for event in history.events
            if event.HasField("activity_task_started_event_attributes")
        ]
        assert attempts == [1]


@activity.defn(name="validate_request.v1")
async def slow_validation(request: ActivityRequest) -> ActivityResponse:
    await asyncio.sleep(30)
    return ActivityResponse(capability="validate_request", output={"valid": True})


async def test_activity_timeout_is_recorded_by_temporal(
    governed_environment: WorkflowEnvironment,
) -> None:
    async with running_workers(
        governed_environment.client, overrides={VALIDATION_QUEUE: [slow_validation]}
    ):
        handle = await start_governed(
            governed_environment.client, validation_document(timeout=1, attempts=1)
        )
        with pytest.raises(WorkflowFailureError) as caught:
            await asyncio.wait_for(handle.result(), 15)
        failure = caught.value.cause
        assert isinstance(failure, ActivityError)
        assert isinstance(failure.cause, TimeoutError)
        assert failure.cause.type == TimeoutType.START_TO_CLOSE


async def test_runtime_timer_cancellation_is_cooperative(
    governed_environment: WorkflowEnvironment,
) -> None:
    document = {
        "start_at": "wait",
        "steps": {
            "wait": {"type": "timer", "seconds": 300, "next": "done"},
            "done": {"type": "end"},
        },
    }
    async with running_workers(governed_environment.client):
        handle = await start_governed(governed_environment.client, document)
        await wait_for_status(handle, step="wait")
        await handle.cancel()
        with pytest.raises(WorkflowFailureError):
            await asyncio.wait_for(handle.result(), 15)
        history = await handle.fetch_history()
        assert any(event.HasField("timer_started_event_attributes") for event in history.events)
        assert any(
            event.HasField("workflow_execution_canceled_event_attributes")
            for event in history.events
        )


async def test_notification_worker_outage_does_not_block_validation_queue(
    governed_environment: WorkflowEnvironment,
) -> None:
    async with running_workers(
        governed_environment.client, exclude_queues=frozenset({NOTIFICATION_QUEUE})
    ):
        pending = await start_governed(governed_environment.client, sample_document(1000))
        await wait_for_status(pending, step="notify")
        independent = await start_governed(governed_environment.client, validation_document())
        assert (await asyncio.wait_for(independent.result(), 10))["state"] == "COMPLETED"
        assert ("send_notification.v1", NOTIFICATION_QUEUE) in await scheduled_activities(pending)
        async with Worker(
            governed_environment.client,
            task_queue=NOTIFICATION_QUEUE,
            activities=worker_activities()[NOTIFICATION_QUEUE],
        ):
            assert (await asyncio.wait_for(pending.result(), 15))["state"] == "COMPLETED"


async def test_runtime_restart_during_approval_and_history_replay(
    governed_environment: WorkflowEnvironment,
) -> None:
    client = governed_environment.client
    async with running_workers(client, include_runtime=False):
        async with Worker(client, task_queue=ORCHESTRATION_QUEUE, workflows=[GovernedWorkflowV1]):
            handle = await start_governed(client, sample_document(), amount=7500)
            await wait_for_status(handle, state="WAITING_FOR_APPROVAL")
        async with Worker(client, task_queue=ORCHESTRATION_QUEUE, workflows=[GovernedWorkflowV1]):
            await handle.signal(
                "approve", {"approved": True, "approver": "manager", "comment": "After restart"}
            )
            assert (await asyncio.wait_for(handle.result(), 15))["state"] == "COMPLETED"
    history = await handle.fetch_history()
    replay = await Replayer(
        workflows=[GovernedWorkflowV1], data_converter=pydantic_data_converter
    ).replay_workflow(history)
    assert replay.replay_failure is None
