"""Versioned, closed contracts for the shared durable human-task service."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import Field, StrictBool, StrictInt, StrictStr, StringConstraints, model_validator

from .api import ApiBusinessContext, CorrelationId, PageLimit, PageOffset, PublicIdentifier
from .base import ContractModel, Identifier, OffsetDatetime
from .domain import HumanTask, HumanTaskStatus

TaskText = Annotated[StrictStr, StringConstraints(min_length=1, max_length=256, pattern=r"\S")]
TaskComment = Annotated[StrictStr, StringConstraints(max_length=2000)]
TaskVersion = Annotated[StrictInt, Field(ge=0)]
TaskTimeout = Annotated[StrictInt, Field(ge=1, le=31_536_000)]


class EscalationPolicy(ContractModel):
    action: Literal["escalate", "expire"] = "expire"
    target_group: TaskText | None = None
    extend_seconds: Annotated[StrictInt, Field(ge=0, le=31_536_000)] = 0

    @model_validator(mode="after")
    def valid_escalation(self) -> EscalationPolicy:
        if self.action == "escalate" and not self.target_group:
            raise ValueError("Escalation requires a target group")
        return self


class CreateTaskRequest(ApiBusinessContext):
    workflow_id: PublicIdentifier
    run_id: PublicIdentifier
    first_execution_run_id: PublicIdentifier | None = None
    step_id: Identifier
    definition_version: TaskText
    correlation_id: CorrelationId
    business_reference: TaskText
    idempotency_key: TaskText
    task_type: Literal["approval"] = "approval"
    assignee: TaskText | None = None
    assignee_group: TaskText | None = None
    form_schema_version: TaskText | None = None
    payload_reference: TaskText | None = None
    timeout_seconds: TaskTimeout
    sla_deadline: OffsetDatetime | None = None
    escalation_policy: EscalationPolicy = Field(default_factory=EscalationPolicy)
    package_id: TaskText
    package_release_id: TaskText
    build_id: TaskText

    @model_validator(mode="after")
    def assigned(self) -> CreateTaskRequest:
        if not self.assignee and not self.assignee_group:
            raise ValueError("Task requires an assignee or assignee group")
        return self


class TaskResponse(HumanTask):
    first_execution_run_id: PublicIdentifier
    task_type: Literal["approval"] = "approval"
    version: TaskVersion
    idempotency_key: TaskText
    package_id: TaskText
    package_release_id: TaskText
    build_id: TaskText
    escalation_level: TaskVersion = 0
    delegated_by: TaskText | None = None
    comment: TaskComment = ""
    updated_at: OffsetDatetime
    replayed: StrictBool = False


class TaskPage(ContractModel):
    items: tuple[TaskResponse, ...]
    limit: PageLimit
    offset: PageOffset
    next_offset: PageOffset | None = None


class TaskMutationRequest(ContractModel):
    expected_version: TaskVersion | None = None


class TaskDecisionRequest(TaskMutationRequest):
    evidence_reference: TaskText | None = None
    comment: TaskComment = ""


class CompleteTaskRequest(TaskDecisionRequest):
    outcome: Literal["approved", "rejected"]


class ReassignTaskRequest(TaskMutationRequest):
    assignee: TaskText | None = None
    assignee_group: TaskText | None = None
    reason: TaskText

    @model_validator(mode="after")
    def destination(self) -> ReassignTaskRequest:
        if not self.assignee and not self.assignee_group:
            raise ValueError("Reassignment requires an assignee or group")
        return self


class DelegateTaskRequest(TaskMutationRequest):
    assignee: TaskText
    reason: TaskText


class EscalateTaskRequest(TaskMutationRequest):
    assignee_group: TaskText | None = None
    reason: TaskText


class ExpireTaskRequest(TaskMutationRequest):
    reason: TaskText = "Task expired"


TaskStatus = HumanTaskStatus
