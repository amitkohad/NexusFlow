"""Explicit API configuration; URLs and local tokens are never printed."""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Self

from contracts import RuntimeProfile
from nexusflow_common.config import WorkerSettings, load_settings
from nexusflow_common.errors import ConfigurationError
from pydantic import Field, SecretStr, ValidationError, model_validator
from sqlalchemy.engine import make_url


class ApiSettings(WorkerSettings):
    runtime_profile: RuntimeProfile = "governed"
    database_url: SecretStr
    local_token: SecretStr | None = Field(default=None)
    local_actor: str = "local-developer"
    local_tenant: str = "demo"
    local_business_domain: str = "customer-services"
    local_application: str = "adjustments"

    @model_validator(mode="after")
    def validate_api(self) -> Self:
        try:
            driver = make_url(self.database_url.get_secret_value()).drivername
        except Exception:
            raise ValueError("Invalid business database URL") from None
        if driver not in {"sqlite", "postgresql+psycopg"}:
            raise ValueError("Use sqlite or postgresql+psycopg for the business database")
        if self.runtime_profile in {"governed", "package"} and self.max_definition_steps > 500:
            raise ValueError("Version 1 runtime supports at most 500 steps")
        if self.environment == "prod" and driver != "postgresql+psycopg":
            raise ValueError("Production requires PostgreSQL")
        if self.local_token is not None:
            if self.environment not in {"local", "test"}:
                raise ValueError("Static bearer tokens are limited to local/test environments")
            if len(self.local_token.get_secret_value()) < 32:
                raise ValueError("Local bearer token must have at least 32 characters")
        if any(
            not value.strip() or len(value) > 128
            for value in (
                self.local_actor,
                self.local_tenant,
                self.local_business_domain,
                self.local_application,
            )
        ):
            raise ValueError("Local identity fields must be nonblank and at most 128 characters")
        return self


def load_api_settings(environment: Mapping[str, str] | None = None) -> ApiSettings:
    source = dict(os.environ if environment is None else environment)
    profile = source.get("NEXUSFLOW_RUNTIME_PROFILE", "governed")
    source.setdefault(
        "TEMPORAL_TASK_QUEUE",
        "lightweight-workflows" if profile == "legacy" else "workflow-orchestration-tq",
    )
    values = load_settings(source).model_dump()
    mapping = {
        "NEXUSFLOW_DATABASE_URL": "database_url",
        "NEXUSFLOW_RUNTIME_PROFILE": "runtime_profile",
        "NEXUSFLOW_API_TOKEN": "local_token",
        "NEXUSFLOW_API_ACTOR": "local_actor",
        "NEXUSFLOW_API_TENANT": "local_tenant",
        "NEXUSFLOW_API_DOMAIN": "local_business_domain",
        "NEXUSFLOW_API_APPLICATION": "local_application",
    }
    values.update({field: source[name] for name, field in mapping.items() if name in source})
    try:
        return ApiSettings.model_validate(values)
    except ValidationError:
        raise ConfigurationError("Invalid workflow API configuration") from None
