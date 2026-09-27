"""Pending starts retain their selected runtime across rollout and rollback."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import pytest
from alembic import command
from alembic.config import Config
from contracts import DefinitionDocument, ExecutionState, JsonObject, RuntimeContext, RuntimeProfile
from sqlalchemy import MetaData, Table, create_engine, inspect
from workflow_api.api import PERMISSIONS, create_app
from workflow_api.backend import BackendUnavailable, BusinessSnapshot
from workflow_api.repository import Scope, WorkflowRepository
from workflow_api.security import Principal, StaticTokenAuthenticator

ROOT = Path(__file__).resolve().parents[2]
SCOPE = Scope("rollout-tenant", "finance", "adjustments")
NOW = datetime(2026, 9, 27, tzinfo=timezone.utc)
DOCUMENT = DefinitionDocument.model_validate(
    {"start_at": "done", "steps": {"done": {"type": "end"}}}
)


@dataclass
class RecordedStart:
    workflow_id: str
    profile: RuntimeProfile | None
    queue: str | None
    definition: DefinitionDocument
    context: RuntimeContext | None


@dataclass
class SubmissionBackend:
    fail_once: bool = True
    starts: list[RecordedStart] = field(default_factory=list)

    async def start(
        self,
        workflow_id: str,
        workflow_type: str,
        definition: DefinitionDocument,
        request: JsonObject,
        variables: JsonObject,
        *,
        context: RuntimeContext | None = None,
        runtime_profile: RuntimeProfile | None = None,
        task_queue: str | None = None,
    ) -> str:
        self.starts.append(
            RecordedStart(workflow_id, runtime_profile, task_queue, definition, context)
        )
        if self.fail_once:
            self.fail_once = False
            raise BackendUnavailable()
        return f"run-{workflow_id}"

    async def status(self, workflow_id: str, run_id: str) -> BusinessSnapshot:
        return BusinessSnapshot(ExecutionState.RUNNING)

    async def signal(
        self, workflow_id: str, run_id: str, signalname: str, payload: dict[str, Any]
    ) -> None:
        pass

    async def cancel(self, workflow_id: str, run_id: str) -> None:
        pass

    async def ready(self) -> bool:
        return True


def test_migration_backfills_phase_three_pending_execution_binding(tmp_path: Path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / 'historical.db'}")
    config = Config(str(ROOT / "migrations/alembic.ini"))
    with engine.begin() as connection:
        config.attributes["connection"] = connection
        command.upgrade(config, "0001_governed_workflow_api")
        old_execution = Table("workflow_executions", MetaData(), autoload_with=connection)
        assert "runtime_profile" not in old_execution.c
        connection.execute(
            old_execution.insert().values(
                **SCOPE.as_dict(),
                workflow_id="historical-pending",
                workflow_type="adjustment",
                definition_id="historical-definition",
                definition_version="1.0",
                definition_document=DOCUMENT.model_dump(mode="json"),
                request={"amount": 1000},
                variables={},
                business_reference="case-123",
                correlation_id="trace-123",
                idempotency_key="pending-key",
                request_fingerprint="a" * 64,
                created_by="historical-starter",
                state="CREATED",
                started_at=NOW,
                updated_at=NOW,
            )
        )
    repository = WorkflowRepository(engine)
    try:
        assert not repository.ping()
        with engine.begin() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, "head")
        assert repository.ping()
        persisted = repository.get_execution(SCOPE, "historical-pending")
        assert persisted.runtime_profile == "legacy"
        assert persisted.runtime_task_queue == "lightweight-workflows"
        assert persisted.run_id is None
        assert persisted.definition_version == "1.0"
        assert persisted.definition_document == DOCUMENT
        assert persisted.request == {"amount": 1000}
        assert persisted.created_by == "historical-starter"
        with engine.begin() as connection:
            config.attributes["connection"] = connection
            command.downgrade(config, "0001_governed_workflow_api")
            old_execution = Table("workflow_executions", MetaData(), autoload_with=connection)
            assert "runtime_profile" not in old_execution.c
            assert (
                connection.scalar(
                    old_execution.select().with_only_columns(old_execution.c.workflow_id)
                )
                == "historical-pending"
            )
            command.upgrade(config, "head")
            assert "runtime_profile" in {
                column["name"] for column in inspect(connection).get_columns("workflow_executions")
            }
        assert repository.get_execution(SCOPE, "historical-pending").runtime_profile == "legacy"
    finally:
        repository.close()


@pytest.mark.asyncio
async def test_fresh_legacy_start_rejects_governed_promoted_revision(tmp_path: Path) -> None:
    repository = WorkflowRepository(f"sqlite:///{tmp_path / 'incompatible-rollout.db'}")
    repository.create_schema()
    backend = SubmissionBackend(fail_once=False)
    authenticator = StaticTokenAuthenticator(
        {
            "rollout-token": Principal(
                subject="original-starter", **SCOPE.as_dict(), permissions=PERMISSIONS
            )
        }
    )
    headers = {"Authorization": "Bearer rollout-token"}
    governed_app = create_app(repository, backend, authenticator=authenticator)
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=governed_app),
            base_url="http://rollout.local",
            headers=headers,
        ) as http:
            registered = await http.post(
                "/api/v1/workflows",
                json={
                    **SCOPE.as_dict(),
                    "definition_id": "governed-definition",
                    "workflow_type": "adjustment",
                    "version": "1.0",
                    "owner": "team",
                    "definition_document": {
                        "start_at": "validate",
                        "steps": {
                            "validate": {
                                "type": "activity",
                                "capability": "validate_request",
                                "next": "done",
                            },
                            "done": {"type": "end"},
                        },
                    },
                },
            )
            assert registered.status_code == 201, registered.text
            assert (
                registered.json()["definition_document"]["steps"]["validate"]["contract_version"]
                == "1.0"
            )
            assert registered.json()["dependencies"]
            assert (
                await http.post("/api/v1/definitions/adjustment/1.0/approve", json=SCOPE.as_dict())
            ).status_code == 200
            assert (
                await http.post(
                    "/api/v1/definitions/adjustment/1.0/promote",
                    json={**SCOPE.as_dict(), "environment": "local"},
                )
            ).status_code == 200
        legacy_app = create_app(
            repository,
            backend,
            authenticator=authenticator,
            runtime_profile="legacy",
            runtime_task_queue="lightweight-workflows",
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=legacy_app),
            base_url="http://rollout.local",
            headers=headers,
        ) as http:
            refused = await http.post(
                "/api/v1/workflows/adjustment/start",
                json={
                    **SCOPE.as_dict(),
                    "business_reference": "case-123",
                    "correlation_id": "trace-123",
                    "idempotency_key": "incompatible-key",
                    "request": {"amount": 1000},
                },
            )
            assert refused.status_code == 409, refused.text
            assert refused.json()["code"] == "runtime_profile_mismatch"
        assert backend.starts == []
        assert repository.get_by_idempotency(SCOPE, "adjustment", "incompatible-key") is None
        assert repository.list_executions(SCOPE, 100, 0) == []
    finally:
        repository.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "original,new,original_queue,new_queue",
    [
        ("legacy", "governed", "historic-lightweight-queue", "workflow-orchestration-tq"),
        ("governed", "legacy", "workflow-orchestration-tq", "rollback-lightweight-queue"),
    ],
)
async def test_pending_start_recovery_keeps_runtime_and_queue_after_configuration_change(
    tmp_path: Path,
    original: RuntimeProfile,
    new: RuntimeProfile,
    original_queue: str,
    new_queue: str,
) -> None:
    repository = WorkflowRepository(f"sqlite:///{tmp_path / 'rollout.db'}")
    repository.create_schema()
    backend = SubmissionBackend()
    authenticator = StaticTokenAuthenticator(
        {
            "rollout-token": Principal(
                subject="original-starter",
                **SCOPE.as_dict(),
                permissions=PERMISSIONS,
            )
        }
    )
    headers = {"Authorization": "Bearer rollout-token"}
    start_request = {
        **SCOPE.as_dict(),
        "business_reference": "case-123",
        "correlation_id": "trace-123",
        "idempotency_key": "pending-key",
        "request": {"amount": 1000},
    }
    original_app = create_app(
        repository,
        backend,
        authenticator=authenticator,
        runtime_profile=original,
        runtime_task_queue=original_queue,
    )
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=original_app),
            base_url="http://rollout.local",
            headers=headers,
        ) as http:
            registered = await http.post(
                "/api/v1/workflows",
                json={
                    **SCOPE.as_dict(),
                    "definition_id": "definition",
                    "workflow_type": "adjustment",
                    "version": "1.0",
                    "owner": "team",
                    "definition_document": DOCUMENT.model_dump(mode="json"),
                },
            )
            assert registered.status_code == 201, registered.text
            assert (
                await http.post("/api/v1/definitions/adjustment/1.0/approve", json=SCOPE.as_dict())
            ).status_code == 200
            assert (
                await http.post(
                    "/api/v1/definitions/adjustment/1.0/promote",
                    json={**SCOPE.as_dict(), "environment": "local"},
                )
            ).status_code == 200
            uncertain = await http.post("/api/v1/workflows/adjustment/start", json=start_request)
            assert uncertain.status_code == 503, uncertain.text
        pending = repository.get_by_idempotency(SCOPE, "adjustment", "pending-key")
        assert pending is not None and pending.run_id is None
        assert pending.runtime_profile == original
        assert pending.runtime_task_queue == original_queue

        changed_app = create_app(
            repository,
            backend,
            authenticator=authenticator,
            runtime_profile=new,
            runtime_task_queue=new_queue,
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=changed_app),
            base_url="http://rollout.local",
            headers=headers,
        ) as http:
            recovered = await http.post("/api/v1/workflows/adjustment/start", json=start_request)
            assert recovered.status_code == 202, recovered.text
            assert recovered.json()["workflow_id"] == pending.workflow_id
            assert recovered.json()["replayed"] is True
            confirmed = await http.post("/api/v1/workflows/adjustment/start", json=start_request)
            assert confirmed.status_code == 200, confirmed.text
            assert confirmed.json()["workflow_id"] == pending.workflow_id
            fresh_request = {**start_request, "idempotency_key": "fresh-key"}
            fresh = await http.post("/api/v1/workflows/adjustment/start", json=fresh_request)
            assert fresh.status_code == 202, fresh.text
        assert [attempt.profile for attempt in backend.starts[:2]] == [original, original]
        assert [attempt.queue for attempt in backend.starts[:2]] == [original_queue, original_queue]
        assert backend.starts[0].workflow_id == backend.starts[1].workflow_id
        assert backend.starts[0].definition == backend.starts[1].definition
        assert backend.starts[1].context is not None
        assert backend.starts[1].context.actor == "original-starter"
        assert backend.starts[2].profile == new
        assert backend.starts[2].queue == new_queue
        assert len(repository.list_executions(SCOPE, 100, 0)) == 2
        persisted = repository.get_execution(SCOPE, pending.workflow_id)
        assert persisted.run_id == f"run-{pending.workflow_id}"
        assert persisted.runtime_profile == original
        assert persisted.runtime_task_queue == original_queue
    finally:
        repository.close()
