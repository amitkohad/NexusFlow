"""Closed HTTP contracts exposing governed business data to clients.

Execution and history responses deliberately contain only business identifiers.
Runtime handles remain internal records in :mod:`contracts.domain`.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import Field, StrictBool, StrictInt, StrictStr, StringConstraints

from .base import ContractModel, JsonObject, NonEmptyString, OffsetDatetime, ValidationIssue
from .definitions import DefinitionDocument
from .domain import BusinessContext, DefinitionDependency, ExecutionState, WorkflowDefinition

ScopeName = Annotated[NonEmptyString, StringConstraints(max_length=128)]
PublicIdentifier = Annotated[NonEmptyString, StringConstraints(max_length=256)]
WorkflowType = Annotated[
    NonEmptyString,
    StringConstraints(max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$"),
]
DefinitionVersion = Annotated[
    NonEmptyString,
    StringConstraints(max_length=64, pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$"),
]
CorrelationId = Annotated[
    NonEmptyString,
    StringConstraints(max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$"),
]
PageLimit = Annotated[StrictInt, Field(ge=1, le=100)]
PageOffset = Annotated[StrictInt, Field(ge=0)]


class ApiBusinessContext(BusinessContext):
    """Bounded request scope, checked against the authenticated principal."""

    tenant: ScopeName
    business_domain: ScopeName
    application: ScopeName


class RegisterDefinitionRequest(ApiBusinessContext):
    definition_id: PublicIdentifier
    workflow_type: WorkflowType
    version: DefinitionVersion
    owner: PublicIdentifier
    definition_document: DefinitionDocument
    dependencies: tuple[DefinitionDependency, ...] = ()


class ApproveDefinitionRequest(ApiBusinessContext):
    pass


class PromoteDefinitionRequest(ApiBusinessContext):
    environment: Literal["local", "dev", "test", "prod"] = "local"


class StartWorkflowRequest(ApiBusinessContext):
    business_reference: PublicIdentifier
    correlation_id: CorrelationId
    idempotency_key: PublicIdentifier
    definition_version: DefinitionVersion | None = None
    request: JsonObject = Field(default_factory=dict)
    variables: JsonObject = Field(default_factory=dict)


class WorkflowLinks(ContractModel):
    status: NonEmptyString
    history: NonEmptyString
    self: NonEmptyString


class WorkflowStatusResponse(ApiBusinessContext):
    workflow_id: PublicIdentifier
    workflow_type: WorkflowType
    definition_id: PublicIdentifier
    definition_version: DefinitionVersion
    state: ExecutionState
    current_step: PublicIdentifier | None = None
    business_reference: PublicIdentifier
    correlation_id: CorrelationId
    started_at: OffsetDatetime
    updated_at: OffsetDatetime
    completed_at: OffsetDatetime | None = None
    failure_code: PublicIdentifier | None = None
    failure_summary: Annotated[StrictStr, StringConstraints(max_length=2000)] | None = None
    links: WorkflowLinks


class WorkflowExecutionResponse(WorkflowStatusResponse):
    replayed: StrictBool = False


class BusinessAuditEvent(ApiBusinessContext):
    """Business projection of an internal audit event, excluding runtime routing."""

    event_id: PublicIdentifier
    event_type: PublicIdentifier
    workflow_id: PublicIdentifier | None = None
    workflow_type: WorkflowType | None = None
    definition_version: DefinitionVersion | None = None
    business_reference: PublicIdentifier | None = None
    step: PublicIdentifier | None = None
    previous_state: PublicIdentifier | None = None
    new_state: PublicIdentifier | None = None
    actor: PublicIdentifier
    correlation_id: CorrelationId
    timestamp: OffsetDatetime
    metadata: JsonObject = Field(default_factory=dict)


class HistoryPage(ContractModel):
    items: tuple[BusinessAuditEvent, ...]
    limit: PageLimit
    offset: PageOffset
    next_offset: PageOffset | None = None
    refreshed: StrictBool = True


class DefinitionPage(ContractModel):
    items: tuple[WorkflowDefinition, ...]
    limit: PageLimit
    offset: PageOffset
    next_offset: PageOffset | None = None


class ExecutionPage(ContractModel):
    items: tuple[WorkflowExecutionResponse, ...]
    limit: PageLimit
    offset: PageOffset
    next_offset: PageOffset | None = None


class SignalWorkflowRequest(ContractModel):
    signal: Literal["approve"] = "approve"
    approved: StrictBool
    comment: Annotated[StrictStr, StringConstraints(max_length=2000)] = ""


class SignalResponse(ContractModel):
    workflow_id: PublicIdentifier
    accepted: StrictBool = True
    correlation_id: CorrelationId


class CancelWorkflowRequest(ContractModel):
    reason: Annotated[NonEmptyString, StringConstraints(max_length=2000)] = "Client cancellation"


class CancelResponse(ContractModel):
    workflow_id: PublicIdentifier
    accepted: StrictBool = True
    correlation_id: CorrelationId


class ProblemDetails(ContractModel):
    type: NonEmptyString = "about:blank"
    title: Annotated[NonEmptyString, StringConstraints(max_length=256)]
    status: Annotated[StrictInt, Field(ge=400, le=599)]
    detail: Annotated[StrictStr, StringConstraints(max_length=2000)]
    instance: NonEmptyString
    code: PublicIdentifier
    correlation_id: CorrelationId
    issues: tuple[ValidationIssue, ...] = ()
