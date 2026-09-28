"""Governed operations with durable reservations before runtime submission."""

from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any, cast
from uuid import uuid4

from contracts import (
    ActivityStep,
    ApprovalStep,
    BusinessAuditEvent,
    DefinitionDependency,
    DefinitionDocument,
    ExecutionState,
    PackageRuntimeStartRequest,
    RegisterDefinitionRequest,
    RuntimeContext,
    RuntimeProfile,
    SignalWorkflowRequest,
    StartWorkflowRequest,
    WorkflowDefinition,
    WorkflowExecutionResponse,
    WorkflowLinks,
)
from workflow_sdk.definitions import DefinitionValidationError, validate_definition

from workflows.common.catalog import (
    CAPABILITY_ROUTES,
    LEGACY_TASK_QUEUE,
    RUNTIME_TASK_QUEUE,
    resolve_capability,
)

from .backend import BackendNotFound, BackendUnavailable, WorkflowBackend
from .errors import ApiError
from .package_backend import PackageBackend
from .package_repository import PackageRepository
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
        runtime_profile: RuntimeProfile = "governed",
        runtime_task_queue: str | None = None,
    ) -> None:
        self.repository = repository
        self.backend = backend
        self.environment = environment
        self.max_definition_steps = max_definition_steps
        self.runtime_profile = runtime_profile
        self.runtime_task_queue = runtime_task_queue or (
            LEGACY_TASK_QUEUE if runtime_profile == "legacy" else RUNTIME_TASK_QUEUE
        )

    async def register(self, request: RegisterDefinitionRequest, actor: str) -> WorkflowDefinition:
        try:
            document = validate_definition(
                request.definition_document,
                allowed_capabilities=None
                if self.runtime_profile == "package"
                else LEGACY_CAPABILITIES,
                max_steps=self.max_definition_steps,
            )
        except DefinitionValidationError as exc:
            raise ApiError(
                422, "definition_invalid", "Definition failed validation", exc.issues
            ) from exc
        for step in document.steps.values():
            if isinstance(step, ActivityStep) and (
                step.compensation is not None
                or (
                    self.runtime_profile == "legacy"
                    and (
                        step.task_queue is not None
                        or step.contract_version is not None
                        or _contains_template(step.input)
                    )
                )
            ):
                raise ApiError(
                    422,
                    "runtime_feature_unavailable",
                    "Definition requires features unavailable in the selected runtime profile",
                )
        if self.runtime_profile == "legacy" and request.dependencies:
            raise ApiError(
                422,
                "runtime_feature_unavailable",
                "Versioned worker dependencies require the dedicated runtime",
            )
        if self.runtime_profile == "governed":
            normalized = document.model_dump(mode="json")
            capabilities: set[str] = set()
            for step_id, step in document.steps.items():
                if isinstance(step, ActivityStep):
                    try:
                        route = resolve_capability(
                            step.capability, step.contract_version, step.task_queue
                        )
                    except ValueError:
                        raise ApiError(
                            422,
                            "activity_contract_unavailable",
                            "Activity version or queue violates its owned contract",
                        ) from None
                    normalized["steps"][step_id].update(
                        task_queue=route.task_queue, contract_version=route.contract_version
                    )
                    capabilities.add(step.capability)
                elif isinstance(step, ApprovalStep):
                    capabilities.add("create_approval_task")
            expected = tuple(
                DefinitionDependency(
                    capability=capability,
                    contract_version=CAPABILITY_ROUTES[capability].contract_version,
                    task_queue=CAPABILITY_ROUTES[capability].task_queue,
                )
                for capability in sorted(capabilities)
            )
            if request.dependencies and set(request.dependencies) != set(expected):
                raise ApiError(
                    422,
                    "dependency_mismatch",
                    "Dependencies must match the definition's versioned capability contracts",
                )
            request = request.model_copy(
                update={
                    "definition_document": DefinitionDocument.model_validate(normalized),
                    "dependencies": expected,
                }
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
            if self.runtime_profile == "legacy" and (
                definition.dependencies
                or any(
                    isinstance(step, ActivityStep)
                    and (
                        step.task_queue is not None
                        or step.contract_version is not None
                        or step.compensation is not None
                        or _contains_template(step.input)
                    )
                    for step in definition.definition_document.steps.values()
                )
            ):
                raise ApiError(
                    409,
                    "runtime_profile_mismatch",
                    "Promoted revision requires the governed runtime",
                )
            binding = None
            if self.runtime_profile == "package":
                packages = PackageRepository(self.repository)
                binding = await asyncio.to_thread(
                    packages.select_binding, principal.scope, definition, self.environment
                )
                actual = await cast(PackageBackend, self.backend).routing(
                    binding.worker_deployment_name, binding.temporal_namespace
                )
                confirmed = await asyncio.to_thread(
                    packages.confirm_routing,
                    principal.scope,
                    binding.package_id,
                    self.environment,
                    actual,
                )
                if not confirmed:
                    raise ApiError(
                        409,
                        "routing_not_confirmed",
                        "Reconcile Temporal routing before starting this workflow",
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
                    runtime_profile=self.runtime_profile,
                    runtime_task_queue=binding.workflow_task_queue
                    if binding
                    else self.runtime_task_queue,
                    package_binding=binding,
                ),
            )
        assert record is not None
        submitted = record.run_id is None
        if submitted:
            # Never remove this reservation on a timeout. A retry submits the
            # same frozen revision and ID, and the runtime rejects duplicate IDs.
            context = RuntimeContext(
                **record.scope.as_dict(),
                workflow_type=record.workflow_type,
                definition_id=record.definition_id,
                definition_version=record.definition_version,
                business_reference=record.business_reference,
                correlation_id=record.correlation_id,
                actor=record.created_by,
            )
            if record.runtime_profile == "package":
                if record.package_binding is None:
                    raise ApiError(
                        503, "runtime_binding_invalid", "Stored package provenance is unavailable"
                    )
                run_id = await cast(PackageBackend, self.backend).start_package(
                    record.workflow_id,
                    PackageRuntimeStartRequest(
                        context=context,
                        definition_document=record.definition_document,
                        request=record.request,
                        variables=record.variables,
                        release_binding=record.package_binding,
                    ),
                )
            else:
                run_id = await self.backend.start(
                    record.workflow_id,
                    record.workflow_type,
                    record.definition_document,
                    record.request,
                    record.variables,
                    context=context,
                    runtime_profile=record.runtime_profile,
                    task_queue=record.runtime_task_queue,
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
        current_run_id = record.run_id
        if record.runtime_profile == "package":
            if record.package_binding is None:
                raise ApiError(
                    503, "runtime_binding_invalid", "Stored package provenance is unavailable"
                )
            runtime = cast(PackageBackend, self.backend)
            current_run_id, snapshot = await runtime.status_package(
                record.workflow_id, record.run_id
            )
            initial, current = await runtime.observe_package(
                record.workflow_id, record.run_id, record.package_binding
            )
            packages = PackageRepository(self.repository)
            await asyncio.to_thread(
                packages.validate_observed_builds,
                record.scope,
                record.package_binding,
                initial,
                current,
                record.observed_initial_build_id,
            )
            await asyncio.to_thread(
                packages.observe_execution, record.scope, workflow_id, initial, current, observed_at
            )
        else:
            snapshot = await self.backend.status(record.workflow_id, record.run_id)
        updated = await asyncio.to_thread(
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
        return replace(updated, run_id=current_run_id)

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
