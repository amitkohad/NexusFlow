"""Shared validation primitives for transport-neutral platform contracts."""

from __future__ import annotations

import math
from datetime import datetime
from enum import StrEnum
from typing import Annotated

from pydantic import (
    AwareDatetime,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    StrictFloat,
    StrictInt,
    StrictStr,
    StringConstraints,
    model_validator,
)
from pydantic import JsonValue as PydanticJsonValue

NonEmptyString = Annotated[StrictStr, StringConstraints(min_length=1, pattern=r"\S")]
Identifier = Annotated[
    StrictStr, StringConstraints(min_length=1, pattern=r"^[A-Za-z_][A-Za-z0-9_-]*$")
]
Path = Annotated[
    StrictStr,
    StringConstraints(
        pattern=r"^(?:[A-Za-z_][A-Za-z0-9_-]*|0|[1-9][0-9]*)(\.(?:[A-Za-z_][A-Za-z0-9_-]*|0|[1-9][0-9]*))*$"
    ),
]
PositiveInt = Annotated[StrictInt, Field(gt=0)]
NonNegativeInt = Annotated[StrictInt, Field(ge=0)]
PositiveNumber = Annotated[StrictInt | StrictFloat, Field(gt=0, allow_inf_nan=False)]
SHA256Hash = Annotated[StrictStr, StringConstraints(pattern=r"^[a-f0-9]{64}$")]


def _validate_timestamp(value: object) -> object:
    if not isinstance(value, (str, datetime)):
        raise ValueError("Timestamps must be offset-aware datetimes or ISO 8601 strings")
    return value


OffsetDatetime = Annotated[AwareDatetime, BeforeValidator(_validate_timestamp)]
MAX_JSON_DEPTH = 64


def _validate_json(value: object) -> object:
    """Validate and detach bounded JSON payloads without scalar coercion."""
    active_containers: set[int] = set()

    def visit(item: object, depth: int) -> object:
        if depth > MAX_JSON_DEPTH or (depth == MAX_JSON_DEPTH and isinstance(item, (list, dict))):
            raise ValueError(f"JSON payload nesting must not exceed {MAX_JSON_DEPTH} levels")
        if item is None or type(item) in (str, bool, int):
            return item
        if type(item) is float:
            if not math.isfinite(item):
                raise ValueError("JSON numbers must be finite")
            return item
        if not isinstance(item, (list, dict)):
            raise ValueError("Payloads must contain only JSON values")
        container_id = id(item)
        if container_id in active_containers:
            raise ValueError("JSON payloads must not contain cycles")
        active_containers.add(container_id)
        try:
            if isinstance(item, dict):
                copied: dict[str, object] = {}
                for key, child in item.items():
                    if not isinstance(key, str):
                        raise ValueError("JSON object keys must be strings")
                    copied[key] = visit(child, depth + 1)
                return copied
            return [visit(child, depth + 1) for child in item]
        finally:
            active_containers.remove(container_id)

    return visit(value, 0)


JsonValue = Annotated[PydanticJsonValue, BeforeValidator(_validate_json)]
JsonObject = Annotated[dict[str, JsonValue], BeforeValidator(_validate_json)]


class ContractModel(BaseModel):
    """Closed contracts with frozen fields; nested payloads remain mutable."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, validate_default=True, allow_inf_nan=False
    )


class ErrorCategory(StrEnum):
    VALIDATION = "validation"
    BUSINESS = "business"
    AUTHORIZATION = "authorization"
    TECHNICAL = "technical"
    TIMEOUT = "timeout"
    CANCELLATION = "cancellation"
    CONFIGURATION = "configuration"


class ValidationIssue(ContractModel):
    code: NonEmptyString
    path: NonEmptyString
    message: NonEmptyString


class ErrorDetail(ContractModel):
    code: NonEmptyString
    category: ErrorCategory
    message: NonEmptyString
    retryable: bool = Field(default=False, strict=True)
    metadata: JsonObject = Field(default_factory=dict)
    issues: tuple[ValidationIssue, ...] = ()

    @model_validator(mode="after")
    def validate_retryability(self) -> ErrorDetail:
        if self.retryable and self.category in {
            ErrorCategory.VALIDATION,
            ErrorCategory.BUSINESS,
            ErrorCategory.AUTHORIZATION,
            ErrorCategory.CONFIGURATION,
            ErrorCategory.CANCELLATION,
        }:
            raise ValueError(f"{self.category.value} errors must not be retryable")
        return self
