"""Governed operations with durable reservations before runtime submission."""

from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from contracts import (
    ActivityStep,
    BusinessAuditEvent,
    ExecutionState,
    RegisterDefinitionRequest,
    SignalWorkflowRequest,
    StartWorkflowRequest,
    WorkflowDefinition,
    WorkflowExecutionResponse,
    WorkflowLinks,
)
from workflow_sdk.definitions import DefinitionValidationError, validate_definition

from .backend import BackendNotFound, BackendUnavailable, WorkflowBackend
from .errors import ApiError
from .repository import ExecutionRecord, WorkflowRepository
from .security import Principal

LEGACY_CAPABILITIES = frozenset(
    {"validate_request", "risk_check", "post_adjustment", "send_notification", "record_rejection"}
)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def execution_response(
    record: ExecutionRecord, replayed: bool = False
) -> WorkflowExecutionResponse:
    path = f"/api/v1/workflows/{record.workflow_id}"
    return WorkflowExecutionResponse(
        **record.scope.as_dict(),
        workflow_id=record.workflow_id,
        workflow_type=record.workflow_type,
        definition_id=record.definition_id,
        definition_version=record.definition_version,
        state=record.state,
        current_step=record.current_step,
        business_reference=record.business_reference,
        correlation_id=record.correlation_id,
        started_at=record.started_at,
        updated_at=record.updated_at,
        completed_at=record.completed_at,
        failure_code=record.failure_code,
        failure_summary=record.failure_summary,
        links=WorkflowLinks(self=path, status=f"{path}/status", history=f"{path}/history"),
        replayed=replayed,
    )


def _contains_template(value: object) -> bool:
    if isinstance(value, str):
        return "${" in value
    if isinstance(value, dict):
        return any(_contains_template(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_template(item) for item in value)
    return False


class WorkflowService:
    def __init__(
        self,
        repository: WorkflowRepository,
        backend: WorkflowBackend,
        environment: str,
        max_definition_steps: int = 500,
    ) -> None:
        self.repository = repository
        self.backend = backend
        self.environment = environment
        self.max_definition_steps = max_definition_steps

    async def register(self, request: RegisterDefinitionRequest, actor: str) -> WorkflowDefinition:
        try:
            document = validate_definition(
                request.definition_document,
                allowed_capabilities=LEGACY_CAPABILITIES,
                max_steps=self.max_definition_steps,
            )
        except DefinitionValidationError as exc:
            raise ApiError(
                422, "definition_invalid", "Definition failed validation", exc.issues
            ) from exc
        for step in document.steps.values():
            if isinstance(step, ActivityStep) and (
                step.task_queue is not None
                or step.contract_version is not None
                or step.compensation is not None
                or _contains_template(step.input)
            ):
                raise ApiError(
                    422,
                    "runtime_feature_unavailable",
                    "Queue routing, contract versions, compensation and input templates require the dedicated runtime",
                )
        if request.dependencies:
            raise ApiError(
                422,
                "runtime_feature_unavailable",
                "Versioned worker dependencies require the dedicated runtime",
            )
        return await asyncio.to_thread(self.repository.register, request, actor, utcnow())

    async def start(
        self, workflow_type: str, request: StartWorkflowRequest, principal: Principal
    ) -> tuple[WorkflowExecutionResponse, bool]:
        canonical = {"workflow_type": workflow_type, **request.model_dump(mode="json")}
        fingerprint = hashlib.sha256(
            json.dumps(
                canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False
            ).encode()
        ).hexdigest()
        record = await asyncio.to_thread(
            self.repository.get_by_idempotency,
            principal.scope,
            workflow_type,
            request.idempotency_key,
        )
        created = False
        if record is not None:
            if record.request_fingerprint != fingerprint:
                raise ApiError(
                    409, "idempotency_conflict", "Idempotency key has a different request"
                )
        else:
            definition = await asyncio.to_thread(
                self.repository.resolve_promoted,
                principal.scope,
                workflow_type,
                self.environment,
                request.definition_version,
            )
            now = utcnow()
            record, created = await asyncio.to_thread(
                self.repository.reserve_execution,
                ExecutionRecord(
                    workflow_id=f"nf-{uuid4().hex}",
                    scope=principal.scope,
                    workflow_type=workflow_type,
                    definition_id=definition.definition_id,
                    definition_version=definition.version,
                    definition_document=definition.definition_document,
                    request=request.request,
                    variables=request.variables,
                    business_reference=request.business_reference,
                    correlation_id=request.correlation_id,
                    idempotency_key=request.idempotency_key,
                    request_fingerprint=fingerprint,
                    created_by=principal.subject,
                    started_at=now,
                    updated_at=now,
                ),
            )
        assert record is not None
        submitted = record.run_id is None
        if submitted:
            # Never remove this reservation on a timeout. A retry submits the
            # same frozen revision and ID, and the runtime rejects duplicate IDs.
            run_id = await self.backend.start(
                record.workflow_id,
                record.workflow_type,
                record.definition_document,
                record.request,
                record.variables,
            )
            record = await asyncio.to_thread(
                self.repository.mark_started, record.scope, record.workflow_id, run_id, utcnow()
            )
        return execution_response(record, replayed=not created), created or submitted

    async def refresh(self, principal: Principal, workflow_id: str) -> ExecutionRecord:
        record = await asyncio.to_thread(
            self.repository.get_execution, principal.scope, workflow_id
        )
        if record.run_id is None:
            return record
        # Progress in the runtime's append-only transition prefix orders polls.
        # Request observation time breaks ties between equal progress snapshots.
        observed_at = utcnow()
        snapshot = await self.backend.status(record.workflow_id, record.run_id)
        return await asyncio.to_thread(
            self.repository.update_snapshot,
            record.scope,
            workflow_id,
            snapshot.state,
            snapshot.current_step,
            list(snapshot.transitions),
            observed_at,
            snapshot.failure_code,
            snapshot.failure_summary,
        )

    async def signal(
        self, principal: Principal, workflow_id: str, request: SignalWorkflowRequest
    ) -> ExecutionRecord:
        record = await self.refresh(principal, workflow_id)
        if record.state != ExecutionState.WAITING_FOR_APPROVAL or record.run_id is None:
            raise ApiError(409, "workflow_not_waiting", "Workflow is not waiting for an approval")
        await self.backend.signal(
            workflow_id,
            record.run_id,
            request.signal,
            {
                "approved": request.approved,
                "approver": principal.subject,
                "comment": request.comment,
            },
        )
        await asyncio.to_thread(
            self.repository.append_execution_event,
            principal.scope,
            workflow_id,
            "execution.signal_accepted",
            principal.subject,
            utcnow(),
            {"approved": request.approved},
        )
        return record

    async def cancel(self, principal: Principal, workflow_id: str, reason: str) -> ExecutionRecord:
        record = await self.refresh(principal, workflow_id)
        if record.run_id is None:
            raise ApiError(409, "workflow_not_started", "Workflow has not reached the runtime")
        if record.state in {
            ExecutionState.COMPLETED,
            ExecutionState.REJECTED,
            ExecutionState.TIMED_OUT,
            ExecutionState.CANCELLED,
            ExecutionState.FAILED,
        }:
            raise ApiError(409, "workflow_closed", "Workflow execution is already closed")
        await self.backend.cancel(workflow_id, record.run_id)
        await asyncio.to_thread(
            self.repository.append_execution_event,
            principal.scope,
            workflow_id,
            "execution.cancel_requested",
            principal.subject,
            utcnow(),
            {"reason": reason},
        )
        return record

    async def history(
        self, principal: Principal, workflow_id: str, limit: int, offset: int
    ) -> tuple[list[BusinessAuditEvent], bool]:
        refreshed = True
        try:
            record = await self.refresh(principal, workflow_id)
            refreshed = record.run_id is not None
        except (BackendUnavailable, BackendNotFound):
            # Business evidence survives runtime outages and history retention.
            # Scoped repository lookup below still rejects unknown executions.
            refreshed = False
        events = await asyncio.to_thread(
            self.repository.list_history, principal.scope, workflow_id, limit, offset
        )
        projected = []
        for event in events:
            data: dict[str, Any] = event.model_dump(
                mode="json",
                exclude={
                    "run_id",
                    "task_queue",
                    "worker",
                    "activity",
                    "retention_class",
                },
            )
            # Runtime transition detail can contain capability names or decision
            # values. The business projection exposes the ordinal and step/state.
            if event.event_type == "execution.transition":
                data["metadata"] = {"ordinal": event.metadata["ordinal"]}
            projected.append(BusinessAuditEvent.model_validate(data))
        return projected, refreshed
