"""Validated environment configuration, loaded explicitly outside workflow code."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from typing import Annotated, Literal, Self
from urllib.parse import urlsplit

from contracts import ValidationIssue
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    StrictStr,
    field_validator,
    model_validator,
)
from pydantic import (
    ValidationError as ModelValidationError,
)

from .errors import ConfigurationError, model_issues


class WorkerSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, validate_default=True)

    environment: Literal["local", "dev", "test", "prod"] = "local"
    temporal_address: StrictStr = "localhost:7233"
    temporal_namespace: Annotated[StrictStr, Field(min_length=1, max_length=255)] = "default"
    temporal_task_queue: Annotated[StrictStr, Field(min_length=1, max_length=255)] = (
        "workflow-orchestration-tq"
    )
    temporal_tls: StrictBool = False
    max_definition_steps: Annotated[StrictInt, Field(ge=1, le=10_000)] = 500
    max_payload_bytes: Annotated[StrictInt, Field(ge=1024, le=16_777_216)] = 1_048_576
    shutdown_grace_seconds: Annotated[StrictInt, Field(ge=1)] = 30

    @field_validator("temporal_address")
    @classmethod
    def validate_address(cls, address: str) -> str:
        if any(
            character.isspace() or ord(character) < 32 or ord(character) == 127
            for character in address
        ):
            raise ValueError("Address must not contain whitespace or control characters")
        parsed = urlsplit(f"//{address}")
        if (
            not parsed.hostname
            or parsed.port is None
            or not 1 <= parsed.port <= 65535
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path
            or parsed.query
            or parsed.fragment
            or address != address.strip()
        ):
            raise ValueError("Expected host:port without URL scheme or credentials")
        return address

    @field_validator("temporal_namespace", "temporal_task_queue")
    @classmethod
    def validate_identifier(cls, value: str) -> str:
        if value != value.strip() or not value.strip():
            raise ValueError("Expected a nonblank identifier without outer whitespace")
        return value

    @model_validator(mode="after")
    def validate_production(self) -> Self:
        if self.environment == "prod" and not self.temporal_tls:
            raise ValueError("Production Temporal connections require TLS")
        return self


_ENV_FIELDS = {
    "NEXUSFLOW_ENVIRONMENT": "environment",
    "TEMPORAL_ADDRESS": "temporal_address",
    "TEMPORAL_NAMESPACE": "temporal_namespace",
    "TEMPORAL_TASK_QUEUE": "temporal_task_queue",
    "TEMPORAL_TLS": "temporal_tls",
    "NEXUSFLOW_MAX_DEFINITION_STEPS": "max_definition_steps",
    "NEXUSFLOW_MAX_PAYLOAD_BYTES": "max_payload_bytes",
    "NEXUSFLOW_SHUTDOWN_GRACE_SECONDS": "shutdown_grace_seconds",
}
_INTEGER_FIELDS = {"max_definition_steps", "max_payload_bytes", "shutdown_grace_seconds"}


def load_settings(environment: Mapping[str, str] | None = None) -> WorkerSettings:
    """Read only supported variables. This function is never called at import time."""
    values: dict[str, object] = {}
    for variable, field in _ENV_FIELDS.items():
        value = (os.environ if environment is None else environment).get(variable)
        if value is None:
            continue
        if field in _INTEGER_FIELDS:
            if not re.fullmatch(r"[0-9]+", value):
                raise ConfigurationError(
                    "Invalid service configuration",
                    issues=(
                        ValidationIssue(
                            code="integer_required",
                            path=variable,
                            message="Expected a decimal integer",
                        ),
                    ),
                )
            try:
                values[field] = int(value)
            except ValueError:
                raise ConfigurationError(
                    "Invalid service configuration",
                    issues=(
                        ValidationIssue(
                            code="integer_out_of_range",
                            path=variable,
                            message="Integer exceeds supported limits",
                        ),
                    ),
                ) from None
        elif field == "temporal_tls":
            if value.lower() not in {"true", "false", "1", "0"}:
                raise ConfigurationError(
                    "Invalid service configuration",
                    issues=(
                        ValidationIssue(
                            code="boolean_required",
                            path=variable,
                            message="Expected true, false, 1, or 0",
                        ),
                    ),
                )
            values[field] = value.lower() in {"true", "1"}
        else:
            values[field] = value
    try:
        return WorkerSettings.model_validate(values)
    except ModelValidationError as error:
        raise ConfigurationError(
            "Invalid service configuration", issues=model_issues(error)
        ) from None
