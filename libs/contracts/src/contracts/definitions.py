"""Governed workflow definition v1.0 syntax, independent of runtime code."""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Literal

from pydantic import Field

from .base import (
    ContractModel,
    Identifier,
    JsonObject,
    JsonValue,
    NonEmptyString,
    NonNegativeInt,
    Path,
    PositiveInt,
)
from .policies import CompensationPolicyModel, RetryPolicyModel


class TerminalOutcome(StrEnum):
    COMPLETED = "COMPLETED"
    REJECTED = "REJECTED"
    TIMED_OUT = "TIMED_OUT"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


class ActivityStep(ContractModel):
    type: Literal["activity"] = "activity"
    capability: Identifier
    input: JsonObject = Field(default_factory=dict)
    timeout_seconds: PositiveInt = 30
    retry: RetryPolicyModel = Field(default_factory=RetryPolicyModel)
    compensation: CompensationPolicyModel | None = None
    task_queue: NonEmptyString | None = None
    contract_version: NonEmptyString | None = None
    next: Identifier


class DecisionStep(ContractModel):
    type: Literal["decision"] = "decision"
    field: Path
    operator: Literal["==", "!=", ">", ">=", "<", "<=", "in"] = "=="
    value: JsonValue
    on_true: Identifier
    on_false: Identifier


class ApprovalStep(ContractModel):
    type: Literal["approval"] = "approval"
    assignee_group: NonEmptyString = "approvers"
    timeout_seconds: PositiveInt = 300
    on_approved: Identifier
    on_rejected: Identifier
    on_timeout: Identifier


class TimerStep(ContractModel):
    type: Literal["timer"] = "timer"
    seconds: NonNegativeInt = 1
    next: Identifier


class EndStep(ContractModel):
    type: Literal["end"] = "end"
    outcome: TerminalOutcome = TerminalOutcome.COMPLETED


Step = Annotated[
    ActivityStep | DecisionStep | ApprovalStep | TimerStep | EndStep, Field(discriminator="type")
]


class DefinitionDocument(ContractModel):
    schema_version: Literal["1.0"] = "1.0"
    process_id: NonEmptyString | None = None
    workflow_name: NonEmptyString | None = None
    request: JsonObject = Field(default_factory=dict)
    variables: JsonObject = Field(default_factory=dict)
    start_at: Identifier
    steps: dict[Identifier, Step] = Field(min_length=1)
