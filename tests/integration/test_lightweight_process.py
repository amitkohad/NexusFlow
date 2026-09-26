from __future__ import annotations

import asyncio
import json
import os
import shutil
from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
import pytest_asyncio
from temporalio.client import Client, WorkflowHandle
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from app.activities import execute_capability
from app.workflows import LightweightProcess

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="module")]

ProcessHandle = WorkflowHandle[LightweightProcess, dict[str, Any]]
WorkerClient = tuple[Client, str]


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def temporal_environment() -> AsyncIterator[WorkflowEnvironment]:
    cli_path = os.environ.get("TEMPORAL_CLI_PATH") or shutil.which("temporal")
    if not cli_path or not Path(cli_path).is_file():
        pytest.fail(
            "Temporal integration tests need a local Temporal CLI. Add temporal to "
            "PATH or set TEMPORAL_CLI_PATH to the executable's absolute path. "
            "These tests do not download binaries or use a shared Temporal server."
        )

    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=cli_path,
        dev_server_log_level="error",
        ui=False,
    ) as environment:
        yield environment


@pytest_asyncio.fixture(loop_scope="module")
async def worker_client(
    temporal_environment: WorkflowEnvironment,
) -> AsyncIterator[WorkerClient]:
    task_queue = f"baseline-{uuid4().hex}"
    async with Worker(
        temporal_environment.client,
        task_queue=task_queue,
        workflows=[LightweightProcess],
        activities=[execute_capability],
    ):
        yield temporal_environment.client, task_queue


@pytest.fixture
def process_spec() -> dict[str, Any]:
    example = Path(__file__).resolve().parents[2] / "examples" / "customer_adjustment.json"
    spec: dict[str, Any] = json.loads(example.read_text(encoding="utf-8"))
    spec["process_id"] = f"baseline-{uuid4().hex}"
    spec["request"]["simulate_transient_failure"] = False
    return spec


async def start_process(worker_client: WorkerClient, spec: dict[str, Any]) -> ProcessHandle:
    client, task_queue = worker_client
    return await client.start_workflow(
        LightweightProcess.run,
        spec,
        id=f"baseline-{uuid4().hex}",
        task_queue=task_queue,
        execution_timeout=timedelta(seconds=20),
    )


async def wait_for_approval(handle: ProcessHandle) -> dict[str, Any]:
    async with asyncio.timeout(10):
        while True:
            status = await handle.query(LightweightProcess.status)
            if status["state"] == "WAITING_FOR_APPROVAL":
                return status
            await asyncio.sleep(0.05)


async def completed_result(handle: ProcessHandle) -> dict[str, Any]:
    return await asyncio.wait_for(handle.result(), timeout=15)


def transition_states(result: dict[str, Any]) -> list[tuple[str, str]]:
    return [(item["step"], item["state"]) for item in result["transitions"]]


async def test_low_amount_completes_without_approval(
    worker_client: WorkerClient, process_spec: dict[str, Any]
) -> None:
    process_spec["request"]["amount"] = 1250
    handle = await start_process(worker_client, process_spec)

    result = await completed_result(handle)
    assert result["state"] == "COMPLETED"
    assert result["process_id"] == process_spec["process_id"]
    assert result["results"] == {
        "validate": {"valid": True, "amount": 1250.0},
        "risk": {"risk_score": 25, "risk_band": "LOW"},
        "post": {"posted": True, "reference": "ADJ-POST-1"},
        "notify": {"sent": True, "channel": "email"},
    }
    assert transition_states(result) == [
        ("validate", "STARTED"),
        ("validate", "COMPLETED"),
        ("risk", "STARTED"),
        ("risk", "COMPLETED"),
        ("route", "STARTED"),
        ("route", "ROUTED"),
        ("post", "STARTED"),
        ("post", "COMPLETED"),
        ("notify", "STARTED"),
        ("notify", "COMPLETED"),
        ("completed", "STARTED"),
        ("completed", "COMPLETED"),
    ]
    times = [datetime.fromisoformat(item["workflow_time"]) for item in result["transitions"]]
    assert times == sorted(times)
    status = await handle.query(LightweightProcess.status)
    assert status["state"] == "COMPLETED"
    assert status["current_step"] is None
    assert status["workflow_name"] == "customer-adjustment"
    assert status["results"] == result["results"]
    assert status["transitions"] == result["transitions"]


async def test_high_amount_waits_for_approval_and_records_actor(
    worker_client: WorkerClient, process_spec: dict[str, Any]
) -> None:
    handle = await start_process(worker_client, process_spec)
    status = await wait_for_approval(handle)

    assert status["current_step"] == "manager_approval"
    assert status["approval"] == {
        "step_id": "manager_approval",
        "assignee_group": "operations-managers",
    }
    assert set(status["results"]) == {"validate", "risk"}
    assert status["results"]["risk"] == {"risk_score": 65, "risk_band": "HIGH"}

    decision = {"approved": True, "approver": "manager.alex", "comment": "Verified"}
    await handle.signal(LightweightProcess.approve, decision)
    result = await completed_result(handle)

    assert result["state"] == "COMPLETED"
    assert result["results"]["manager_approval"] == decision
    assert result["results"]["post"]["posted"] is True
    assert result["results"]["notify"]["sent"] is True
    approvals = [item for item in result["transitions"] if item["state"] == "APPROVED"]
    assert len(approvals) == 1
    assert approvals[0]["step"] == "manager_approval"
    assert approvals[0]["detail"] == "manager.alex"


async def test_rejection_records_actor_and_does_not_post_or_notify(
    worker_client: WorkerClient, process_spec: dict[str, Any]
) -> None:
    handle = await start_process(worker_client, process_spec)
    await wait_for_approval(handle)
    decision = {"approved": False, "approver": "manager.sam", "comment": "Denied"}
    await handle.signal(LightweightProcess.approve, decision)

    result = await completed_result(handle)
    assert result["state"] == "REJECTED"
    assert result["results"]["manager_approval"] == decision
    assert result["results"]["record_rejection"] == {
        "recorded": True,
        "reason": "business approval rejected",
    }
    assert "post" not in result["results"]
    assert "notify" not in result["results"]
    assert ("manager_approval", "REJECTED") in transition_states(result)
    rejection = next(
        item
        for item in result["transitions"]
        if item["step"] == "manager_approval" and item["state"] == "REJECTED"
    )
    assert rejection["detail"] == "manager.sam"
    assert transition_states(result)[-2:] == [("rejected", "STARTED"), ("rejected", "REJECTED")]
    status = await handle.query(LightweightProcess.status)
    assert status["state"] == "REJECTED"
    assert status["current_step"] is None


async def test_approval_timeout_follows_configured_route(
    worker_client: WorkerClient, process_spec: dict[str, Any]
) -> None:
    process_spec["steps"]["manager_approval"]["timeout_seconds"] = 1
    handle = await start_process(worker_client, process_spec)
    await wait_for_approval(handle)

    result = await completed_result(handle)
    assert result["state"] == "TIMED_OUT"
    assert set(result["results"]) == {"validate", "risk"}
    assert transition_states(result)[-3:] == [
        ("manager_approval", "TIMED_OUT"),
        ("timed_out", "STARTED"),
        ("timed_out", "TIMED_OUT"),
    ]
    status = await handle.query(LightweightProcess.status)
    assert status["state"] == "TIMED_OUT"
    assert status["current_step"] is None


async def test_transient_risk_failure_is_retried_by_temporal(
    worker_client: WorkerClient, process_spec: dict[str, Any]
) -> None:
    process_spec["request"]["amount"] = 1250
    process_spec["request"]["simulate_transient_failure"] = True
    process_spec["steps"]["risk"]["retry"]["initial_interval_seconds"] = 0.1
    handle = await start_process(worker_client, process_spec)

    result = await completed_result(handle)
    assert result["state"] == "COMPLETED"
    assert result["results"]["risk"] == {"risk_score": 25, "risk_band": "LOW"}
    assert transition_states(result).count(("risk", "COMPLETED")) == 1

    history = await handle.fetch_history()
    client, _ = worker_client
    scheduled_steps: dict[int, str] = {}
    started_attempts: dict[str, int] = {}
    for event in history.events:
        if event.HasField("activity_task_scheduled_event_attributes"):
            scheduled = event.activity_task_scheduled_event_attributes
            arguments = await client.data_converter.decode(scheduled.input.payloads)
            scheduled_steps[event.event_id] = arguments[0]["step_id"]
        elif event.HasField("activity_task_started_event_attributes"):
            started = event.activity_task_started_event_attributes
            started_attempts[scheduled_steps[started.scheduled_event_id]] = started.attempt

    # Temporal folds retried attempts into the final ActivityStarted event; the
    # attempt field is stronger evidence than merely observing a successful run.
    assert started_attempts == {"validate": 1, "risk": 2, "post": 1, "notify": 1}


async def test_timer_uses_a_durable_temporal_timer(worker_client: WorkerClient) -> None:
    spec = {
        "process_id": "baseline-timer",
        "start_at": "pause",
        "steps": {
            "pause": {"type": "timer", "seconds": 1, "next": "done"},
            "done": {"type": "end", "outcome": "COMPLETED"},
        },
    }
    handle = await start_process(worker_client, spec)

    result = await completed_result(handle)
    assert result["state"] == "COMPLETED"
    assert result["results"] == {}
    assert transition_states(result) == [
        ("pause", "STARTED"),
        ("pause", "COMPLETED"),
        ("done", "STARTED"),
        ("done", "COMPLETED"),
    ]
    started, completed = result["transitions"][:2]
    elapsed = datetime.fromisoformat(completed["workflow_time"]) - datetime.fromisoformat(
        started["workflow_time"]
    )
    assert elapsed >= timedelta(seconds=1)
    history = await handle.fetch_history()
    assert sum(event.HasField("timer_started_event_attributes") for event in history.events) == 1
    assert sum(event.HasField("timer_fired_event_attributes") for event in history.events) == 1
