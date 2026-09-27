"""Governed registration pins executable ownership and trusted business context."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from contracts import (
    DefinitionDependency,
    RegisterDefinitionRequest,
    StartWorkflowRequest,
)
from temporalio.client import Client
from workflow_api.backend import BackendUnavailable, TemporalBackend
from workflow_api.config import load_api_settings
from workflow_api.errors import ApiError
from workflow_api.repository import Scope, WorkflowRepository
from workflow_api.security import Principal
from workflow_api.service import WorkflowService

from workflows.common.catalog import RUNTIME_TASK_QUEUE

CONTEXT = {"tenant": "tenant", "business_domain": "domain", "application": "app"}


def registration(**step_changes: Any) -> RegisterDefinitionRequest:
    return RegisterDefinitionRequest.model_validate(
        {
            **CONTEXT,
            "definition_id": "definition",
            "workflow_type": "flow",
            "version": "1",
            "owner": "owner",
            "definition_document": {
                "start_at": "validate",
                "steps": {
                    "validate": {
                        "type": "activity",
                        "capability": "validate_request",
                        "next": "done",
                        **step_changes,
                    },
                    "done": {"type": "end"},
                },
            },
        }
    )


@pytest.fixture
def repository(tmp_path: Path) -> Any:
    repository = WorkflowRepository(f"sqlite:///{(tmp_path / 'registry.sqlite').as_posix()}")
    repository.create_schema()
    yield repository
    repository.close()


async def test_registration_resolves_and_freezes_capability_ownership(
    repository: WorkflowRepository,
) -> None:
    backend = TemporalBackend(cast(Client, SimpleNamespace()))
    service = WorkflowService(repository, backend, "local")
    original = registration(input={"amount": "${request.amount}"})
    definition = await service.register(original, "trusted-actor")
    step = definition.definition_document.steps["validate"].model_dump()
    assert step["task_queue"] == "validation-tq"
    assert step["contract_version"] == "1.0"
    assert definition.dependencies == (
        DefinitionDependency(
            capability="validate_request", contract_version="1.0", task_queue="validation-tq"
        ),
    )
    assert original.definition_document.steps["validate"].model_dump()["task_queue"] is None
    assert repository.get_definition(Scope(**CONTEXT), "flow", "1") == definition


@pytest.mark.parametrize(
    "changes", [{"task_queue": "notification-tq"}, {"contract_version": "2.0"}]
)
async def test_registration_rejects_unowned_queues_and_unsupported_versions(
    repository: WorkflowRepository, changes: dict[str, str]
) -> None:
    service = WorkflowService(repository, TemporalBackend(cast(Client, SimpleNamespace())), "local")
    with pytest.raises(ApiError) as caught:
        await service.register(registration(**changes), "actor")
    assert caught.value.status_code == 422
    assert caught.value.code == "activity_contract_unavailable"
    assert repository.list_definitions(Scope(**CONTEXT), 10, 0) == []


async def test_registration_rejects_dependencies_that_disagree_with_executable_routes(
    repository: WorkflowRepository,
) -> None:
    service = WorkflowService(repository, TemporalBackend(cast(Client, SimpleNamespace())), "local")
    request = registration().model_copy(
        update={
            "dependencies": (
                DefinitionDependency(
                    capability="validate_request",
                    contract_version="1.0",
                    task_queue="notification-tq",
                ),
            )
        }
    )
    with pytest.raises(ApiError) as caught:
        await service.register(request, "actor")
    assert caught.value.code == "dependency_mismatch"


async def test_start_uses_versioned_runtime_and_authenticated_context(
    repository: WorkflowRepository,
) -> None:
    client = SimpleNamespace(
        start_workflow=AsyncMock(return_value=SimpleNamespace(result_run_id="internal-run"))
    )
    backend = TemporalBackend(cast(Client, client))
    service = WorkflowService(repository, backend, "local")
    await service.register(registration(), "creator")
    scope = Scope(**CONTEXT)
    now = datetime.now(timezone.utc)
    repository.approve_definition(scope, "flow", "1", "reviewer", now)
    repository.promote_definition(scope, "flow", "1", "local", "promoter", now)
    request = StartWorkflowRequest(
        **CONTEXT,
        business_reference="case",
        correlation_id="trace",
        idempotency_key="case",
        request={"amount": 100},
        variables={"actor": "untrusted"},
    )
    response, accepted = await service.start(
        "flow", request, Principal("verified-actor", **CONTEXT, permissions=frozenset())
    )
    assert accepted is True
    call = client.start_workflow.await_args
    assert call.args[0] == "GovernedWorkflowV1"
    assert call.kwargs["task_queue"] == RUNTIME_TASK_QUEUE
    assert call.args[1]["context"] == {
        **CONTEXT,
        "workflow_type": "flow",
        "definition_id": "definition",
        "definition_version": "1",
        "business_reference": "case",
        "correlation_id": "trace",
        "actor": "verified-actor",
    }
    record = repository.get_execution(scope, response.workflow_id)
    assert (record.runtime_profile, record.runtime_task_queue) == ("governed", RUNTIME_TASK_QUEUE)
    assert "internal-run" not in response.model_dump_json()


async def test_governed_backend_cannot_start_without_business_context() -> None:
    client = SimpleNamespace(start_workflow=AsyncMock())
    backend = TemporalBackend(cast(Client, client))
    with pytest.raises(BackendUnavailable):
        await backend.start("id", "flow", registration().definition_document, {}, {})
    client.start_workflow.assert_not_awaited()


def test_explicit_legacy_configuration_preserves_its_queue() -> None:
    settings = load_api_settings(
        {"NEXUSFLOW_DATABASE_URL": "sqlite:///local.sqlite", "NEXUSFLOW_RUNTIME_PROFILE": "legacy"}
    )
    assert settings.runtime_profile == "legacy"
    assert settings.temporal_task_queue == "lightweight-workflows"


def test_governed_configuration_rejects_a_limit_above_the_v1_runtime_cap() -> None:
    from nexusflow_common.errors import ConfigurationError

    with pytest.raises(ConfigurationError):
        load_api_settings(
            {
                "NEXUSFLOW_DATABASE_URL": "sqlite:///local.sqlite",
                "NEXUSFLOW_MAX_DEFINITION_STEPS": "501",
            }
        )
