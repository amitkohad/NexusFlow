"""Explicit configuration, safe startup errors, and the local identity boundary."""

from __future__ import annotations

import importlib.util
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from contracts import DefinitionDocument
from fastapi import FastAPI
from nexusflow_common.errors import ConfigurationError
from temporalio.contrib.pydantic import pydantic_data_converter
from workflow_api import config, main
from workflow_api import repository as repository_module
from workflow_api.api import create_app
from workflow_api.backend import BackendUnavailable
from workflow_api.config import load_api_settings
from workflow_api.main import ConnectedBackend
from workflow_api.repository import WorkflowRepository
from workflow_api.security import Principal, StaticTokenAuthenticator

DATABASE_SECRET = "SECRET_DB_PASSWORD"
TOKEN = "development-test-token-0123456789-abcd"
DATABASE_URL = f"postgresql+psycopg://api:{DATABASE_SECRET}@db.internal/workflows"


def test_explicit_settings_mapping_does_not_read_or_mutate_process_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("NEXUSFLOW_DATABASE_URL", DATABASE_URL)
    monkeypatch.setenv("NEXUSFLOW_API_TOKEN", TOKEN)
    environment = {
        "NEXUSFLOW_DATABASE_URL": "sqlite:///:memory:",
        "TEMPORAL_ADDRESS": "temporal.test:7233",
        "TEMPORAL_NAMESPACE": "finance",
        "NEXUSFLOW_API_ACTOR": "verified-user",
        "NEXUSFLOW_API_TENANT": "tenant-one",
        "NEXUSFLOW_API_DOMAIN": "finance",
        "NEXUSFLOW_API_APPLICATION": "adjustments",
        "NEXUSFLOW_MAX_DEFINITION_STEPS": "250",
    }
    settings = load_api_settings(environment)
    assert settings.database_url.get_secret_value() == "sqlite:///:memory:"
    assert settings.local_token is None
    assert settings.temporal_address == "temporal.test:7233"
    assert settings.temporal_namespace == "finance"
    assert settings.temporal_task_queue == "workflow-orchestration-tq"
    assert settings.runtime_profile == "governed"
    assert settings.max_definition_steps == 250
    assert settings.local_actor == "verified-user"
    assert settings.local_tenant == "tenant-one"
    assert settings.local_business_domain == "finance"
    assert settings.local_application == "adjustments"
    assert "TEMPORAL_TASK_QUEUE" not in environment


def test_api_queue_can_be_explicitly_overridden() -> None:
    settings = load_api_settings(
        {
            "NEXUSFLOW_DATABASE_URL": "sqlite:///:memory:",
            "TEMPORAL_TASK_QUEUE": "owned-runtime",
        }
    )
    assert settings.temporal_task_queue == "owned-runtime"


def test_settings_repr_and_json_redact_database_credentials_and_local_token() -> None:
    settings = load_api_settings(
        {"NEXUSFLOW_DATABASE_URL": DATABASE_URL, "NEXUSFLOW_API_TOKEN": TOKEN}
    )
    for serialized in (repr(settings), str(settings), settings.model_dump_json()):
        assert DATABASE_SECRET not in serialized
        assert TOKEN not in serialized
    assert settings.database_url.get_secret_value() == DATABASE_URL
    assert settings.local_token is not None
    assert settings.local_token.get_secret_value() == TOKEN


@pytest.mark.parametrize(
    "environment",
    [
        {},
        {"NEXUSFLOW_DATABASE_URL": DATABASE_SECRET},
        {"NEXUSFLOW_DATABASE_URL": f"mysql://api:{DATABASE_SECRET}@db/workflows"},
        {"NEXUSFLOW_DATABASE_URL": DATABASE_URL, "NEXUSFLOW_API_TOKEN": "SECRET-short"},
        {"NEXUSFLOW_DATABASE_URL": DATABASE_URL, "NEXUSFLOW_API_ACTOR": " "},
        {"NEXUSFLOW_DATABASE_URL": DATABASE_URL, "NEXUSFLOW_API_TENANT": "x" * 129},
        {
            "NEXUSFLOW_DATABASE_URL": DATABASE_URL,
            "NEXUSFLOW_ENVIRONMENT": "dev",
            "NEXUSFLOW_API_TOKEN": TOKEN,
        },
        {
            "NEXUSFLOW_DATABASE_URL": DATABASE_URL,
            "NEXUSFLOW_ENVIRONMENT": "prod",
            "TEMPORAL_TLS": "true",
            "NEXUSFLOW_API_TOKEN": TOKEN,
        },
        {
            "NEXUSFLOW_DATABASE_URL": "sqlite:///:memory:",
            "NEXUSFLOW_ENVIRONMENT": "prod",
            "TEMPORAL_TLS": "true",
        },
    ],
)
def test_invalid_api_settings_have_sanitized_configuration_errors(
    environment: dict[str, str],
) -> None:
    with pytest.raises(ConfigurationError) as caught:
        load_api_settings(environment)
    assert caught.value.detail.code == "configuration_invalid"
    assert DATABASE_SECRET not in str(caught.value)
    assert "SECRET-short" not in str(caught.value)
    assert TOKEN not in str(caught.value)
    assert DATABASE_URL not in str(caught.value)


def test_production_requires_postgres_and_accepts_it_with_temporal_tls() -> None:
    settings = load_api_settings(
        {
            "NEXUSFLOW_DATABASE_URL": DATABASE_URL,
            "NEXUSFLOW_ENVIRONMENT": "prod",
            "TEMPORAL_TLS": "true",
        }
    )
    assert settings.environment == "prod"
    assert settings.temporal_tls is True
    assert settings.local_token is None


def test_factory_module_import_performs_no_environment_or_connection_operations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    read_settings = MagicMock(side_effect=AssertionError("Settings must be read explicitly"))
    construct_repository = MagicMock(
        side_effect=AssertionError("Database must be initialized explicitly")
    )
    connect_runtime = AsyncMock(side_effect=AssertionError("Runtime must be connected at startup"))
    monkeypatch.setattr(config, "load_api_settings", read_settings)
    monkeypatch.setattr(repository_module, "WorkflowRepository", construct_repository)
    monkeypatch.setattr(main.Client, "connect", connect_runtime)
    spec = importlib.util.spec_from_file_location("workflow_api._import_probe", Path(main.__file__))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    read_settings.assert_not_called()
    construct_repository.assert_not_called()
    connect_runtime.assert_not_called()


@dataclass(frozen=True)
class FactoryFixture:
    app: FastAPI
    repository: MagicMock
    connect: AsyncMock


@pytest.fixture
def factory_fixture(monkeypatch: pytest.MonkeyPatch) -> FactoryFixture:
    settings = load_api_settings({"NEXUSFLOW_DATABASE_URL": "sqlite:///:memory:"})
    repository = MagicMock(spec=WorkflowRepository)
    repository.ping.return_value = True
    runtime = SimpleNamespace(
        service_client=SimpleNamespace(check_health=AsyncMock(return_value=True))
    )
    connect = AsyncMock(return_value=runtime)
    monkeypatch.setattr(main, "load_api_settings", lambda: settings)
    monkeypatch.setattr(main, "WorkflowRepository", MagicMock(return_value=repository))
    monkeypatch.setattr(main.Client, "connect", connect)
    application = main.create_app_from_env()
    return FactoryFixture(application, repository, connect)


async def test_local_factory_defaults_to_denied_identity_and_connects_only_during_lifespan(
    factory_fixture: FactoryFixture,
) -> None:
    fixture = factory_fixture
    fixture.connect.assert_not_called()
    backend = cast(ConnectedBackend, fixture.app.state.service.backend)
    assert backend.delegate is None
    async with fixture.app.router.lifespan_context(fixture.app):
        fixture.connect.assert_awaited_once_with(
            "localhost:7233", namespace="default", tls=False, data_converter=pydantic_data_converter
        )
        assert backend.delegate is not None
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=fixture.app), base_url="http://workflow-api.test"
        ) as http:
            assert (await http.get("/health")).status_code == 200
            assert (await http.get("/ready")).status_code == 200
            denied = await http.get(
                "/api/v1/definitions", headers={"Authorization": "Bearer caller-token"}
            )
            assert denied.status_code == 401
            assert denied.headers["content-type"].startswith("application/problem+json")
            fixture.repository.list_definitions.assert_not_called()
            fixture.repository.close.assert_not_called()
    assert backend.delegate is None
    fixture.repository.close.assert_called_once()


async def test_runtime_startup_failure_is_sanitized_and_always_closes_repository(
    factory_fixture: FactoryFixture,
) -> None:
    fixture = factory_fixture
    fixture.connect.side_effect = RuntimeError(f"Cannot connect to {DATABASE_URL}; token={TOKEN}")
    with pytest.raises(RuntimeError, match="^Workflow runtime connection failed$") as caught:
        async with fixture.app.router.lifespan_context(fixture.app):
            pytest.fail("Startup must fail before serving requests")
    assert DATABASE_SECRET not in str(caught.value)
    assert TOKEN not in str(caught.value)
    assert caught.value.__suppress_context__ is True
    fixture.repository.close.assert_called_once()
    assert fixture.app.state.service.backend.delegate is None


def test_repository_construction_failure_does_not_expose_the_database_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = load_api_settings({"NEXUSFLOW_DATABASE_URL": DATABASE_URL})
    connect = AsyncMock()
    monkeypatch.setattr(main, "load_api_settings", lambda: settings)
    monkeypatch.setattr(
        main, "WorkflowRepository", MagicMock(side_effect=RuntimeError(DATABASE_URL))
    )
    monkeypatch.setattr(main.Client, "connect", connect)
    with pytest.raises(RuntimeError, match="^Business database configuration failed$") as caught:
        main.create_app_from_env()
    assert DATABASE_SECRET not in str(caught.value)
    assert caught.value.__suppress_context__ is True
    connect.assert_not_called()


@pytest.mark.parametrize("environment", ["dev", "prod"])
def test_local_factory_requires_enterprise_authenticator_in_deployment_environments(
    monkeypatch: pytest.MonkeyPatch, environment: str
) -> None:
    settings = load_api_settings(
        {
            "NEXUSFLOW_DATABASE_URL": DATABASE_URL,
            "NEXUSFLOW_ENVIRONMENT": environment,
            "TEMPORAL_TLS": "true",
        }
    )
    construct_repository = MagicMock()
    monkeypatch.setattr(main, "load_api_settings", lambda: settings)
    monkeypatch.setattr(main, "WorkflowRepository", construct_repository)
    with pytest.raises(RuntimeError, match="Configure an enterprise Authenticator"):
        main.create_app_from_env()
    construct_repository.assert_not_called()


@pytest.mark.parametrize("environment", ["dev", "prod"])
def test_injected_static_authenticator_is_also_rejected_outside_local_and_test(
    environment: str,
) -> None:
    authenticator = StaticTokenAuthenticator(
        {TOKEN: Principal("user", "tenant", "finance", "app", frozenset())}
    )
    with pytest.raises(ValueError, match="Static token authentication is limited to local/test"):
        create_app(
            MagicMock(spec=WorkflowRepository),
            ConnectedBackend(),
            authenticator=authenticator,
            environment=environment,
        )


@pytest.mark.parametrize("operation", ["start", "status", "signal", "cancel"])
async def test_connected_backend_rejects_operations_before_startup(operation: str) -> None:
    backend = ConnectedBackend()
    definition = DefinitionDocument.model_validate(
        {"start_at": "done", "steps": {"done": {"type": "end"}}}
    )
    assert await backend.ready() is False
    with pytest.raises(BackendUnavailable):
        if operation == "start":
            await backend.start("workflow", "type", definition, {}, {})
        elif operation == "status":
            await backend.status("workflow", "run")
        elif operation == "signal":
            await backend.signal("workflow", "run", "approve", {"approved": True})
        else:
            await backend.cancel("workflow", "run")
