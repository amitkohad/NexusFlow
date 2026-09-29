"""Real PostgreSQL migration and transactional concurrency checks.

Each test uses a uniquely named schema on the explicitly provided test database.
The suite never recreates or drops the database or any existing application schema.
"""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator
from uuid import uuid4

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from contracts import DefinitionDocument, EndStep, ExecutionState
from contracts.api import RegisterDefinitionRequest
from human_task_service.models import TaskBase
from sqlalchemy import MetaData, create_engine, inspect, text
from workflow_api.errors import ApiError
from workflow_api.models import Base
from workflow_api.repository import ExecutionRecord, Scope, WorkflowRepository

pytestmark = pytest.mark.postgres
NOW = datetime(2026, 9, 27, tzinfo=timezone.utc)
SCOPE = Scope("acme", "finance", "adjustments")
ROOT = Path(__file__).resolve().parents[2]
METADATA = MetaData()
for source in (Base.metadata, TaskBase.metadata):
    for table in source.tables.values():
        table.to_metadata(METADATA)


def _request(version: str = "1", scope: Scope = SCOPE) -> RegisterDefinitionRequest:
    return RegisterDefinitionRequest(
        tenant=scope.tenant,
        business_domain=scope.business_domain,
        application=scope.application,
        definition_id="adjustment",
        workflow_type="adjustment",
        version=version,
        owner="team",
        definition_document=DefinitionDocument(start_at="done", steps={"done": EndStep()}),
    )


def _record() -> ExecutionRecord:
    return ExecutionRecord(
        workflow_id=str(uuid4()),
        scope=SCOPE,
        workflow_type="adjustment",
        definition_id="adjustment",
        definition_version="1",
        definition_document=_request().definition_document,
        request={"amount": 100},
        variables={},
        business_reference="case-123",
        correlation_id="trace-123",
        idempotency_key="shared",
        request_fingerprint="a" * 64,
        created_by="user",
        started_at=NOW,
        updated_at=NOW,
    )


@pytest.fixture
def postgres_repository() -> Iterator[WorkflowRepository]:
    url = os.environ.get("NEXUSFLOW_TEST_DATABASE_URL")
    if not url:
        pytest.skip("Set NEXUSFLOW_TEST_DATABASE_URL to an isolated PostgreSQL test database")
    admin_engine = create_engine(url)
    if admin_engine.dialect.name != "postgresql":
        admin_engine.dispose()
        pytest.fail("NEXUSFLOW_TEST_DATABASE_URL must use PostgreSQL")
    schema = "nexusflow_test_" + uuid4().hex
    # Identifier is generated solely from a fixed prefix and UUID hex, and is
    # confined to this fixture. Cleanup touches exactly the created schema.
    with admin_engine.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(
        url, connect_args={"options": f"-csearch_path={schema}"}, pool_pre_ping=True
    )
    repository = WorkflowRepository(engine)
    try:
        config = Config(str(ROOT / "migrations/alembic.ini"))
        with engine.begin() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, "head")
        yield repository
    finally:
        repository.close()
        with admin_engine.begin() as connection:
            connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin_engine.dispose()


def test_postgres_migration_matches_models_and_roundtrips(
    postgres_repository: WorkflowRepository,
) -> None:
    assert postgres_repository.ping()
    config = Config(str(ROOT / "migrations/alembic.ini"))
    with postgres_repository.engine.begin() as connection:
        config.attributes["connection"] = connection
        assert compare_metadata(MigrationContext.configure(connection), METADATA) == []
        command.downgrade(config, "base")
        assert inspect(connection).get_table_names() == ["alembic_version"]
        command.upgrade(config, "head")
        assert compare_metadata(MigrationContext.configure(connection), METADATA) == []


def test_postgres_unique_reservation_is_atomic(postgres_repository: WorkflowRepository) -> None:
    postgres_repository.register(_request(), "author", NOW)
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(
            pool.map(postgres_repository.reserve_execution, [_record() for _ in range(16)])
        )
    assert sum(created for _, created in results) == 1
    assert len({record.workflow_id for record, _ in results}) == 1
    workflow_id = results[0][0].workflow_id
    assert len(postgres_repository.list_history(SCOPE, workflow_id, 100, 0)) == 1


def test_postgres_conflicting_idempotency_does_not_overwrite_winner(
    postgres_repository: WorkflowRepository,
) -> None:
    postgres_repository.register(_request(), "author", NOW)
    original, _ = postgres_repository.reserve_execution(_record())
    conflicting = replace(_record(), request={"amount": 200}, request_fingerprint="b" * 64)
    with pytest.raises(ApiError) as exc:
        postgres_repository.reserve_execution(conflicting)
    assert exc.value.code == "idempotency_conflict"
    assert postgres_repository.get_execution(SCOPE, original.workflow_id).request == {"amount": 100}


def test_postgres_promotion_slot_remains_unique_under_concurrency(
    postgres_repository: WorkflowRepository,
) -> None:
    for version in ("1", "2"):
        postgres_repository.register(_request(version), "author", NOW)
        postgres_repository.approve_definition(SCOPE, "adjustment", version, "reviewer", NOW)

    def promote(version: str) -> str:
        return postgres_repository.promote_definition(
            SCOPE, "adjustment", version, "local", "promoter", NOW
        ).version

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert set(pool.map(promote, ["1", "2"])) == {"1", "2"}
    promoted = postgres_repository.resolve_promoted(SCOPE, "adjustment", "local")
    assert promoted.version in {"1", "2"}
    with postgres_repository.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM workflow_promotions")) == 1


def test_postgres_concurrent_history_projection_is_deduplicated(
    postgres_repository: WorkflowRepository,
) -> None:
    postgres_repository.register(_request(), "author", NOW)
    record, _ = postgres_repository.reserve_execution(_record())
    postgres_repository.mark_started(SCOPE, record.workflow_id, "temporal-run", NOW)
    transitions = [
        {"step": "done", "state": "STARTED", "detail": "end", "workflow_time": NOW.isoformat()},
        {"step": "done", "state": "COMPLETED", "detail": "end", "workflow_time": NOW.isoformat()},
    ]

    def refresh(_: int) -> ExecutionRecord:
        return postgres_repository.update_snapshot(
            SCOPE, record.workflow_id, ExecutionState.COMPLETED, None, transitions, NOW
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        assert all(
            result.state == ExecutionState.COMPLETED for result in pool.map(refresh, range(16))
        )
    history = postgres_repository.list_history(SCOPE, record.workflow_id, 100, 0)
    assert len(history) == 5
    assert len({event.event_id for event in history}) == 5
    assert history == postgres_repository.list_history(
        SCOPE, record.workflow_id, 2, 0
    ) + postgres_repository.list_history(SCOPE, record.workflow_id, 3, 2)


def test_postgres_scope_and_sessions_preserve_definition_binding(
    postgres_repository: WorkflowRepository,
) -> None:
    postgres_repository.register(_request(), "author", NOW)
    record, _ = postgres_repository.reserve_execution(_record())
    second_repository = WorkflowRepository(postgres_repository.engine)
    persisted = second_repository.get_execution(SCOPE, record.workflow_id)
    assert persisted == record
    other_scope = Scope("other", SCOPE.business_domain, SCOPE.application)
    assert second_repository.list_executions(other_scope, 100, 0) == []
    with pytest.raises(ApiError) as exc:
        second_repository.get_execution(other_scope, record.workflow_id)
    assert exc.value.status_code == 404
