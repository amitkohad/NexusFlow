"""Temporal adapter exposing business status without importing a worker.

The Phase 3 API uses the existing LightweightProcess workflow until the separate
runtime lands. Handles remain pinned to the stored run, and duplicate workflow
IDs are never allowed to create another run, including after completion.
"""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Protocol

from contracts import DefinitionDocument, ExecutionState, JsonObject
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode

_TERMINAL_STATES = {
    ExecutionState.COMPLETED,
    ExecutionState.REJECTED,
    ExecutionState.TIMED_OUT,
    ExecutionState.CANCELLED,
    ExecutionState.FAILED,
}


class BackendUnavailable(Exception):
    """Sanitized runtime failure; the HTTP layer can return a retryable problem."""

    def __init__(self) -> None:
        super().__init__("Workflow runtime is unavailable")


class BackendNotFound(Exception):
    """The requested runtime execution does not exist."""

    def __init__(self) -> None:
        super().__init__("Workflow execution was not found")


@dataclass(frozen=True)
class BusinessSnapshot:
    state: ExecutionState
    current_step: str | None = None
    transitions: tuple[dict[str, Any], ...] = ()
    failure_code: str | None = None
    failure_summary: str | None = None


class WorkflowBackend(Protocol):
    async def start(
        self,
        workflow_id: str,
        workflow_type: str,
        definition: DefinitionDocument,
        request: JsonObject,
        variables: JsonObject,
    ) -> str: ...

    async def status(self, workflow_id: str, run_id: str) -> BusinessSnapshot: ...

    async def signal(
        self, workflow_id: str, run_id: str, signalname: str, payload: dict[str, Any]
    ) -> None: ...

    async def cancel(self, workflow_id: str, run_id: str) -> None: ...

    async def ready(self) -> bool: ...


def _runtime_error(error: Exception) -> BackendUnavailable | BackendNotFound:
    if isinstance(error, RPCError) and error.status == RPCStatusCode.NOT_FOUND:
        return BackendNotFound()
    return BackendUnavailable()


def _business_snapshot(payload: object, *, completed: bool) -> BusinessSnapshot:
    if not isinstance(payload, Mapping):
        raise BackendUnavailable()
    try:
        state = ExecutionState(payload["state"])
        if completed and state not in _TERMINAL_STATES:
            raise ValueError("Completed workflow returned a nonterminal business state")
        if not completed and state in _TERMINAL_STATES:
            # Legacy rejection/timeout routes set state before their subsequent
            # handler finishes. Only a closed Temporal execution is terminal.
            state = ExecutionState.RUNNING
        current_step = None if completed else payload.get("current_step")
        if current_step is not None and (
            not isinstance(current_step, str) or not current_step.strip()
        ):
            raise ValueError("Invalid current step")
        transitions = payload.get("transitions", [])
        if not isinstance(transitions, (list, tuple)) or any(
            not isinstance(item, dict) for item in transitions
        ):
            raise ValueError("Invalid business transitions")
        return BusinessSnapshot(
            state=state, current_step=current_step, transitions=tuple(deepcopy(transitions))
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise BackendUnavailable() from exc


class TemporalBackend:
    def __init__(
        self,
        client: Client,
        task_queue: str = "lightweight-workflows",
        *,
        rpc_timeout: timedelta = timedelta(seconds=5),
    ) -> None:
        self.client = client
        self.task_queue = task_queue
        self.rpc_timeout = rpc_timeout

    async def start(
        self,
        workflow_id: str,
        workflow_type: str,
        definition: DefinitionDocument,
        request: JsonObject,
        variables: JsonObject,
    ) -> str:
        payload = definition.model_dump(mode="json", exclude_none=True)
        payload.update(
            process_id=workflow_id,
            workflow_name=workflow_type,
            request=deepcopy(request),
            variables=deepcopy(variables),
        )
        try:
            try:
                handle = await self.client.start_workflow(
                    "LightweightProcess",
                    payload,
                    id=workflow_id,
                    task_queue=self.task_queue,
                    id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                    id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
                    rpc_timeout=self.rpc_timeout,
                )
                run_id = handle.result_run_id
            except WorkflowAlreadyStartedError:
                # The server may have accepted a previous timed-out start. Reuse
                # its run regardless of whether it is still running or closed.
                description = await self.client.get_workflow_handle(workflow_id).describe(
                    rpc_timeout=self.rpc_timeout
                )
                run_id = description.run_id
            if not run_id:
                raise BackendUnavailable()
            return run_id
        except Exception as exc:
            raise _runtime_error(exc) from exc

    async def status(self, workflow_id: str, run_id: str) -> BusinessSnapshot:
        handle = self.client.get_workflow_handle(workflow_id, run_id=run_id)
        try:
            description = await handle.describe(rpc_timeout=self.rpc_timeout)
            if description.status == WorkflowExecutionStatus.RUNNING:
                payload = await handle.query("status", rpc_timeout=self.rpc_timeout)
                return _business_snapshot(payload, completed=False)
            if description.status == WorkflowExecutionStatus.COMPLETED:
                result = await handle.result(follow_runs=False, rpc_timeout=self.rpc_timeout)
                return _business_snapshot(result, completed=True)
            if description.status == WorkflowExecutionStatus.FAILED:
                return BusinessSnapshot(
                    state=ExecutionState.FAILED,
                    failure_code="workflow_failed",
                    failure_summary="Workflow execution failed",
                )
            if description.status == WorkflowExecutionStatus.CANCELED:
                return BusinessSnapshot(state=ExecutionState.CANCELLED)
            if description.status == WorkflowExecutionStatus.TERMINATED:
                return BusinessSnapshot(
                    state=ExecutionState.CANCELLED,
                    failure_code="workflow_terminated",
                    failure_summary="Workflow execution was terminated",
                )
            if description.status == WorkflowExecutionStatus.TIMED_OUT:
                return BusinessSnapshot(
                    state=ExecutionState.TIMED_OUT,
                    failure_code="workflow_timed_out",
                    failure_summary="Workflow execution timed out",
                )
            if description.status == WorkflowExecutionStatus.CONTINUED_AS_NEW:
                return BusinessSnapshot(
                    state=ExecutionState.MANUAL_INTERVENTION,
                    failure_code="runtime_run_changed",
                    failure_summary="Workflow advanced to another execution run",
                )
            raise BackendUnavailable()
        except Exception as exc:
            raise _runtime_error(exc) from exc

    async def signal(
        self, workflow_id: str, run_id: str, signalname: str, payload: dict[str, Any]
    ) -> None:
        try:
            await self.client.get_workflow_handle(workflow_id, run_id=run_id).signal(
                signalname, payload, rpc_timeout=self.rpc_timeout
            )
        except Exception as exc:
            raise _runtime_error(exc) from exc

    async def cancel(self, workflow_id: str, run_id: str) -> None:
        try:
            await self.client.get_workflow_handle(workflow_id, run_id=run_id).cancel(
                rpc_timeout=self.rpc_timeout
            )
        except Exception as exc:
            raise _runtime_error(exc) from exc

    async def ready(self) -> bool:
        try:
            return await self.client.service_client.check_health(timeout=self.rpc_timeout)
        except Exception:
            return False
