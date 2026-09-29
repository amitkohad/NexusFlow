"""Temporal signal delivery outside task database transactions."""

from __future__ import annotations

from typing import Any, Protocol

from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.service import RPCError, RPCStatusCode


class TaskSignalDispatcher(Protocol):
    async def signal(
        self,
        workflow_id: str,
        first_execution_run_id: str,
        signal_name: str,
        payload: dict[str, Any],
    ) -> None: ...


class TaskChainMismatch(Exception):
    """The task's original execution chain is no longer the workflow ID owner."""


class TaskWorkflowClosed(Exception):
    """The original execution chain has definitively closed."""


class TaskWorkflowMissing(Exception):
    """Temporal no longer has the task's workflow execution ID."""


class TemporalTaskDispatcher:
    def __init__(self, client: Client) -> None:
        self.client = client

    async def ready(self) -> bool:
        try:
            return await self.client.service_client.check_health()
        except Exception:
            return False

    async def signal(
        self,
        workflow_id: str,
        first_execution_run_id: str,
        signal_name: str,
        payload: dict[str, Any],
    ) -> None:
        # The SDK's first_execution_run_id handle property is NOT passed to the
        # SignalWorkflowExecution RPC. Describe the current ID owner, verify its
        # chain, then signal that exact run ID. If Continue-As-New or ID reuse
        # races after Describe, the run-specific signal fails safely and the
        # outbox retries. It can never reach a newly reused workflow ID.
        try:
            description = await self.client.get_workflow_handle(workflow_id).describe()
        except RPCError as exc:
            if exc.status == RPCStatusCode.NOT_FOUND:
                raise TaskWorkflowMissing("Workflow execution is no longer available") from exc
            raise
        current_chain = description.raw_description.workflow_execution_info.first_run_id
        if not current_chain:
            raise RuntimeError("Temporal did not expose workflow chain provenance")
        if current_chain != first_execution_run_id:
            raise TaskChainMismatch("Workflow ID belongs to another execution chain")
        if description.status in {
            WorkflowExecutionStatus.COMPLETED,
            WorkflowExecutionStatus.FAILED,
            WorkflowExecutionStatus.CANCELED,
            WorkflowExecutionStatus.TERMINATED,
            WorkflowExecutionStatus.TIMED_OUT,
        }:
            raise TaskWorkflowClosed("Workflow execution chain is closed")
        # CONTINUED_AS_NEW is not final: the successor may become visible on
        # the next attempt. Do not block an event during that handoff.
        await self.client.get_workflow_handle(workflow_id, run_id=description.run_id).signal(
            signal_name, payload
        )
