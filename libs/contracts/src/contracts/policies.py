"""Declarative policies; execution and persistence belong to later services."""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated

from pydantic import Field, StrictBool, StrictFloat, StrictInt, model_validator

from .base import (
    ContractModel,
    ErrorCategory,
    Identifier,
    JsonObject,
    NonEmptyString,
    Path,
    PositiveInt,
    PositiveNumber,
)


class RetryPolicyModel(ContractModel):
    maximum_attempts: PositiveInt = 3
    initial_interval_seconds: PositiveNumber = 1
    maximum_interval_seconds: PositiveNumber = 10
    backoff_coefficient: Annotated[StrictInt | StrictFloat, Field(ge=1, allow_inf_nan=False)] = 2
    non_retryable_error_types: tuple[NonEmptyString, ...] = (
        "ValidationError",
        "BusinessError",
        "AuthorizationError",
    )

    @model_validator(mode="after")
    def validate_intervals(self) -> RetryPolicyModel:
        if self.maximum_interval_seconds < self.initial_interval_seconds:
            raise ValueError("maximum_interval_seconds must be at least initial_interval_seconds")
        return self


class TimeoutPolicyModel(ContractModel):
    start_to_close_seconds: PositiveNumber = 30
    schedule_to_close_seconds: PositiveNumber | None = None
    schedule_to_start_seconds: PositiveNumber | None = None


class HeartbeatPolicyModel(ContractModel):
    timeout_seconds: PositiveNumber = 30


class CompensationPolicyModel(ContractModel):
    capability: Identifier
    input: JsonObject = Field(default_factory=dict)
    timeout_seconds: PositiveInt = 30
    retry: RetryPolicyModel = Field(default_factory=RetryPolicyModel)
    task_queue: NonEmptyString | None = None
    contract_version: NonEmptyString | None = None


class CancellationMode(StrEnum):
    TRY_CANCEL = "try_cancel"
    WAIT_CANCELLATION_COMPLETED = "wait_cancellation_completed"
    ABANDON = "abandon"


class CancellationPolicyModel(ContractModel):
    mode: CancellationMode = CancellationMode.TRY_CANCEL
    compensate_completed: StrictBool = False


class IdempotencyPolicyModel(ContractModel):
    required: StrictBool = True
    key_path: Path | None = None
    retention_seconds: PositiveInt = 86400


class FailureAction(StrEnum):
    FAIL = "fail"
    COMPENSATE = "compensate"
    MANUAL_INTERVENTION = "manual_intervention"


class FailurePolicyModel(ContractModel):
    action: FailureAction = FailureAction.FAIL
    non_retryable_categories: tuple[ErrorCategory, ...] = (
        ErrorCategory.VALIDATION,
        ErrorCategory.BUSINESS,
        ErrorCategory.AUTHORIZATION,
        ErrorCategory.CONFIGURATION,
    )
