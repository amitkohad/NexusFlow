"""Temporal boundary checks independent of the HTTP layer and worker code."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from contracts import DefinitionDocument, ExecutionState
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode
from workflow_api.backend import BackendNotFound, BackendUnavailable, TemporalBackend


@pytest.fixture
def temporal_client() -> Any:
    handle = SimpleNamespace(
        result_run_id="started-run",
        describe=AsyncMock(
            return_value=SimpleNamespace(
                status=WorkflowExecutionStatus.RUNNING, run_id="stored-run"
            )
        ),
        query=AsyncMock(
            return_value={
                "state": "WAITING_FOR_APPROVAL",
                "current_step": "approve",
                "transitions": [{"step": "approve", "state": "STARTED"}],
            }
        ),
        result=AsyncMock(
            return_value={
                "state": "COMPLETED",
                "transitions": [{"step": "done", "state": "COMPLETED"}],
            }
        ),
        signal=AsyncMock(),
        cancel=AsyncMock(),
    )
    return SimpleNamespace(
        start_workflow=AsyncMock(return_value=handle),
        get_workflow_handle=MagicMock(return_value=handle),
        service_client=SimpleNamespace(check_health=AsyncMock(return_value=True)),
    )


@pytest.fixture
def backend(temporal_client: Any) -> TemporalBackend:
    return TemporalBackend(
        cast(Client, temporal_client), task_queue="owned-queue", runtime_profile="legacy"
    )


@pytest.fixture
def definition() -> DefinitionDocument:
    return DefinitionDocument.model_validate(
        {
            "process_id": "untrusted-process-id",
            "workflow_name": "untrusted-name",
            "request": {"amount": 100},
            "variables": {"actor": "default"},
            "start_at": "done",
            "steps": {"done": {"type": "end"}},
        }
    )


async def test_start_binds_business_context_and_prevents_run_reuse(
    temporal_client: Any, backend: TemporalBackend, definition: DefinitionDocument
) -> None:
    assert (
        await backend.start(
            "execution", "adjustment", definition, {"amount": 10}, {"actor": "user"}
        )
        == "started-run"
    )
    args, kwargs = temporal_client.start_workflow.await_args
    assert args[0] == "LightweightProcess"
    assert args[1]["process_id"] == "execution"
    assert args[1]["workflow_name"] == "adjustment"
    assert args[1]["request"] == {"amount": 10}
    assert args[1]["variables"] == {"actor": "user"}
    assert kwargs["task_queue"] == "owned-queue"
    assert kwargs["id"] == "execution"
    assert kwargs["id_reuse_policy"] == WorkflowIDReusePolicy.REJECT_DUPLICATE
    assert kwargs["id_conflict_policy"] == WorkflowIDConflictPolicy.FAIL
    assert definition.process_id == "untrusted-process-id"
    assert definition.request == {"amount": 100}


@pytest.mark.parametrize("closed", [False, True])
async def test_duplicate_start_resolves_existing_run_including_closed_execution(
    temporal_client: Any, backend: TemporalBackend, definition: DefinitionDocument, closed: bool
) -> None:
    temporal_client.start_workflow.side_effect = WorkflowAlreadyStartedError(
        "execution", "LightweightProcess", run_id="stored-run"
    )
    handle = temporal_client.get_workflow_handle.return_value
    handle.describe.return_value = SimpleNamespace(
        run_id="stored-run",
        status=WorkflowExecutionStatus.COMPLETED if closed else WorkflowExecutionStatus.RUNNING,
    )
    assert await backend.start("execution", "adjustment", definition, {}, {}) == "stored-run"
    assert temporal_client.start_workflow.await_count == 1
    temporal_client.get_workflow_handle.assert_called_once_with("execution")
    handle.result.assert_not_awaited()


async def test_running_business_status_is_queried_on_the_pinned_run(
    temporal_client: Any, backend: TemporalBackend
) -> None:
    snapshot = await backend.status("execution", "stored-run")
    assert snapshot.state == ExecutionState.WAITING_FOR_APPROVAL
    assert snapshot.current_step == "approve"
    assert snapshot.transitions == ({"step": "approve", "state": "STARTED"},)
    temporal_client.get_workflow_handle.assert_called_once_with("execution", run_id="stored-run")
    handle = temporal_client.get_workflow_handle.return_value
    assert handle.query.await_args.args == ("status",)
    handle.result.assert_not_awaited()
    handle.query.return_value["transitions"][0]["state"] = "changed"
    assert snapshot.transitions[0]["state"] == "STARTED"


@pytest.mark.parametrize("state", ["COMPLETED", "REJECTED", "TIMED_OUT", "CANCELLED", "FAILED"])
async def test_running_execution_does_not_freeze_a_transient_terminal_query_state(
    temporal_client: Any, backend: TemporalBackend, state: str
) -> None:
    temporal_client.get_workflow_handle.return_value.query.return_value = {
        "state": state,
        "current_step": "handler",
        "transitions": [],
    }
    snapshot = await backend.status("execution", "stored-run")
    assert snapshot.state == ExecutionState.RUNNING
    assert snapshot.current_step == "handler"


async def test_completed_business_status_uses_result_and_never_queries_closed_worker(
    temporal_client: Any, backend: TemporalBackend
) -> None:
    handle = temporal_client.get_workflow_handle.return_value
    handle.describe.return_value.status = WorkflowExecutionStatus.COMPLETED
    snapshot = await backend.status("execution", "stored-run")
    assert snapshot.state == ExecutionState.COMPLETED
    assert snapshot.current_step is None
    assert snapshot.transitions == ({"step": "done", "state": "COMPLETED"},)
    handle.query.assert_not_awaited()
    assert handle.result.await_args.kwargs["follow_runs"] is False


async def test_completed_workflow_cannot_be_reported_as_still_running(
    temporal_client: Any, backend: TemporalBackend
) -> None:
    handle = temporal_client.get_workflow_handle.return_value
    handle.describe.return_value.status = WorkflowExecutionStatus.COMPLETED
    handle.result.return_value = {"state": "RUNNING", "transitions": []}
    with pytest.raises(BackendUnavailable):
        await backend.status("execution", "stored-run")


@pytest.mark.parametrize(
    ("temporal_state", "business_state", "failure_code"),
    [
        (WorkflowExecutionStatus.FAILED, ExecutionState.FAILED, "workflow_failed"),
        (WorkflowExecutionStatus.CANCELED, ExecutionState.CANCELLED, None),
        (WorkflowExecutionStatus.TERMINATED, ExecutionState.CANCELLED, "workflow_terminated"),
        (WorkflowExecutionStatus.TIMED_OUT, ExecutionState.TIMED_OUT, "workflow_timed_out"),
        (
            WorkflowExecutionStatus.CONTINUED_AS_NEW,
            ExecutionState.MANUAL_INTERVENTION,
            "runtime_run_changed",
        ),
    ],
)
async def test_closed_runtime_states_map_to_sanitized_business_outcomes(
    temporal_client: Any,
    backend: TemporalBackend,
    temporal_state: WorkflowExecutionStatus,
    business_state: ExecutionState,
    failure_code: str | None,
) -> None:
    handle = temporal_client.get_workflow_handle.return_value
    handle.describe.return_value.status = temporal_state
    snapshot = await backend.status("execution", "stored-run")
    assert snapshot.state == business_state
    assert snapshot.failure_code == failure_code
    handle.query.assert_not_awaited()
    handle.result.assert_not_awaited()


@pytest.mark.parametrize("operation", ["start", "status", "signal", "cancel"])
@pytest.mark.parametrize("not_found", [False, True])
async def test_runtime_errors_have_sanitized_categories(
    temporal_client: Any,
    backend: TemporalBackend,
    definition: DefinitionDocument,
    operation: str,
    not_found: bool,
) -> None:
    error = RPCError(
        "SECRET internal routing data",
        RPCStatusCode.NOT_FOUND if not_found else RPCStatusCode.UNAVAILABLE,
        b"",
    )
    handle = temporal_client.get_workflow_handle.return_value
    target = (
        temporal_client.start_workflow
        if operation == "start"
        else {"status": handle.describe, "signal": handle.signal, "cancel": handle.cancel}[
            operation
        ]
    )
    target.side_effect = error
    with pytest.raises(BackendNotFound if not_found else BackendUnavailable) as caught:
        if operation == "start":
            await backend.start("execution", "type", definition, {}, {})
        elif operation == "status":
            await backend.status("execution", "run")
        elif operation == "signal":
            await backend.signal("execution", "run", "approve", {"approved": True})
        else:
            await backend.cancel("execution", "run")
    assert "SECRET" not in str(caught.value)


@pytest.mark.parametrize(
    "payload",
    [
        None,
        {},
        {"state": "unknown"},
        {"state": "RUNNING", "transitions": ["raw-event"]},
        {"state": "RUNNING", "current_step": 2},
    ],
)
async def test_invalid_worker_business_payloads_do_not_escape_as_api_data(
    temporal_client: Any, backend: TemporalBackend, payload: object
) -> None:
    temporal_client.get_workflow_handle.return_value.query.return_value = payload
    with pytest.raises(BackendUnavailable):
        await backend.status("execution", "run")


async def test_signals_and_cancellation_target_the_stored_run(
    temporal_client: Any, backend: TemporalBackend
) -> None:
    await backend.signal(
        "execution", "stored-run", "approve", {"approved": True, "approver": "actor"}
    )
    await backend.cancel("execution", "stored-run")
    assert all(
        call.kwargs["run_id"] == "stored-run"
        for call in temporal_client.get_workflow_handle.call_args_list
    )
    handle = temporal_client.get_workflow_handle.return_value
    assert handle.signal.await_args.args == ("approve", {"approved": True, "approver": "actor"})
    handle.cancel.assert_awaited_once()


async def test_readiness_checks_temporal_health_without_starting_workflows(
    temporal_client: Any, backend: TemporalBackend
) -> None:
    assert await backend.ready()
    temporal_client.service_client.check_health.side_effect = RPCError(
        "SECRET", RPCStatusCode.UNAVAILABLE, b""
    )
    assert await backend.ready() is False
    temporal_client.start_workflow.assert_not_awaited()
