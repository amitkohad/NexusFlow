"""Business records shared by API, workers, and audit services.

Timestamps are supplied explicitly by the caller and must have a UTC offset.
These models validate record shape; registry/task lifecycle enforcement belongs
to the corresponding service rather than these transport contracts.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import Field, StrictBool, model_validator

from .base import ContractModel, Identifier, JsonObject, NonEmptyString, OffsetDatetime, SHA256Hash
from .definitions import DefinitionDocument
from .policies import (
    CancellationPolicyModel,
    FailurePolicyModel,
    HeartbeatPolicyModel,
    IdempotencyPolicyModel,
    RetryPolicyModel,
    TimeoutPolicyModel,
)


class BusinessContext(ContractModel):
    tenant: NonEmptyString
    business_domain: NonEmptyString
    application: NonEmptyString


class TenantContext(BusinessContext):
    correlation_id: NonEmptyString
    business_reference: NonEmptyString | None = None
    actor: NonEmptyString | None = None
    authorization_claims: JsonObject = Field(default_factory=dict)


class DefinitionStatus(StrEnum):
    DRAFT = "draft"
    VALIDATED = "validated"
    APPROVED = "approved"
    PROMOTED = "promoted"
    DEPRECATED = "deprecated"
    RETIRED = "retired"


class DefinitionDependency(ContractModel):
    capability: Identifier
    contract_version: NonEmptyString
    task_queue: NonEmptyString


class WorkflowDefinition(BusinessContext):
    definition_id: NonEmptyString
    workflow_type: NonEmptyString
    version: NonEmptyString
    status: DefinitionStatus = DefinitionStatus.DRAFT
    content_hash: SHA256Hash
    definition_document: DefinitionDocument
    owner: NonEmptyString
    dependencies: tuple[DefinitionDependency, ...] = ()
    created_at: OffsetDatetime
    created_by: NonEmptyString
    approved_at: OffsetDatetime | None = None
    approved_by: NonEmptyString | None = None
    promoted_at: OffsetDatetime | None = None
    promoted_by: NonEmptyString | None = None


class ExecutionState(StrEnum):
    CREATED = "CREATED"
    RUNNING = "RUNNING"
    WAITING_FOR_APPROVAL = "WAITING_FOR_APPROVAL"
    COMPLETED = "COMPLETED"
    REJECTED = "REJECTED"
    TIMED_OUT = "TIMED_OUT"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"
    MANUAL_INTERVENTION = "MANUAL_INTERVENTION"


class WorkflowExecution(BusinessContext):
    workflow_id: NonEmptyString
    run_id: NonEmptyString
    workflow_type: NonEmptyString
    definition_id: NonEmptyString
    definition_version: NonEmptyString
    business_reference: NonEmptyString
    correlation_id: NonEmptyString
    idempotency_key: NonEmptyString
    state: ExecutionState = ExecutionState.CREATED
    current_step: Identifier | None = None
    started_at: OffsetDatetime
    updated_at: OffsetDatetime
    completed_at: OffsetDatetime | None = None
    failure_code: NonEmptyString | None = None
    failure_summary: NonEmptyString | None = None


class HumanTaskStatus(StrEnum):
    CREATED = "CREATED"
    ASSIGNED = "ASSIGNED"
    CLAIMED = "CLAIMED"
    COMPLETED = "COMPLETED"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    CANCELLED = "CANCELLED"


class HumanTask(BusinessContext):
    task_id: NonEmptyString
    workflow_id: NonEmptyString
    run_id: NonEmptyString
    step_id: Identifier
    definition_version: NonEmptyString
    correlation_id: NonEmptyString
    business_reference: NonEmptyString | None = None
    assignee: NonEmptyString | None = None
    assignee_group: NonEmptyString | None = None
    status: HumanTaskStatus = HumanTaskStatus.CREATED
    form_schema_version: NonEmptyString | None = None
    payload_reference: NonEmptyString | None = None
    due_at: OffsetDatetime | None = None
    sla_deadline: OffsetDatetime | None = None
    escalation_policy: JsonObject = Field(default_factory=dict)
    outcome: NonEmptyString | None = None
    actor: NonEmptyString | None = None
    evidence_reference: NonEmptyString | None = None
    created_at: OffsetDatetime
    claimed_at: OffsetDatetime | None = None
    completed_at: OffsetDatetime | None = None
    expired_at: OffsetDatetime | None = None


class ActivityContract(ContractModel):
    capability: Identifier
    action: Identifier
    contract_version: NonEmptyString
    task_queue: NonEmptyString
    input_schema: JsonObject
    output_schema: JsonObject
    retry_policy: RetryPolicyModel = Field(default_factory=RetryPolicyModel)
    timeout_policy: TimeoutPolicyModel = Field(default_factory=TimeoutPolicyModel)
    heartbeat_policy: HeartbeatPolicyModel | None = None
    cancellation_policy: CancellationPolicyModel = Field(default_factory=CancellationPolicyModel)
    idempotency_required: StrictBool = True
    idempotency_policy: IdempotencyPolicyModel = Field(default_factory=IdempotencyPolicyModel)
    failure_policy: FailurePolicyModel = Field(default_factory=FailurePolicyModel)
    worker_compatibility: tuple[NonEmptyString, ...] = ()

    @model_validator(mode="after")
    def validate_idempotency(self) -> ActivityContract:
        if self.idempotency_required != self.idempotency_policy.required:
            raise ValueError("idempotency_required must match idempotency_policy.required")
        return self


class AuditEvent(BusinessContext):
    event_id: NonEmptyString
    event_type: NonEmptyString
    workflow_id: NonEmptyString | None = None
    run_id: NonEmptyString | None = None
    workflow_type: NonEmptyString | None = None
    definition_version: NonEmptyString | None = None
    activity: Identifier | None = None
    worker: NonEmptyString | None = None
    task_queue: NonEmptyString | None = None
    business_reference: NonEmptyString | None = None
    step: Identifier | None = None
    previous_state: NonEmptyString | None = None
    new_state: NonEmptyString | None = None
    actor: NonEmptyString
    correlation_id: NonEmptyString
    timestamp: OffsetDatetime
    metadata: JsonObject = Field(default_factory=dict)
    retention_class: NonEmptyString = "standard"


class DeploymentRelease(ContractModel):
    service: NonEmptyString
    release_version: NonEmptyString
    image_digest: NonEmptyString
    worker_version: NonEmptyString | None = None
    activity_contract_versions: tuple[NonEmptyString, ...] = ()
    environment: NonEmptyString
    definition_dependencies: tuple[NonEmptyString, ...] = ()
    promoted_by: NonEmptyString
    promoted_at: OffsetDatetime
    rollback_reference: NonEmptyString | None = None
