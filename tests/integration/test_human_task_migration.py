"""Phase 5 task migration and PostgreSQL concurrency on an isolated schema."""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Iterator
from uuid import uuid4

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from contracts import CreateTaskRequest, TaskDecisionRequest
from human_task_service.errors import TaskError
from human_task_service.models import TaskAuditRow, TaskBase, TaskOutboxRow, TaskRow
from human_task_service.repository import TaskRepository
from human_task_service.security import TaskPrincipal
from sqlalchemy import Engine, MetaData, create_engine, inspect, select, text
from workflow_api.models import Base

pytestmark = pytest.mark.postgres
ROOT = Path(__file__).resolve().parents[2]
SCOPE = {"tenant": "migration-tenant", "business_domain": "finance", "application": "adjustments"}
ACTOR = TaskPrincipal(
    subject="package-activity",
    permissions=frozenset({"tasks:create"}),
    groups=frozenset(),
    **SCOPE,
)


@pytest.fixture
def task_engine() -> Iterator[Engine]:
    url = os.environ.get("NEXUSFLOW_TEST_DATABASE_URL")
    if not url:
        pytest.skip("Set NEXUSFLOW_TEST_DATABASE_URL to an isolated PostgreSQL test database")
    admin = create_engine(url)
    if admin.dialect.name != "postgresql":
        admin.dispose()
        pytest.fail("NEXUSFLOW_TEST_DATABASE_URL must use PostgreSQL")
    schema = "nexusflow_task_" + uuid4().hex
    with admin.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(
        url, connect_args={"options": f"-csearch_path={schema}"}, pool_pre_ping=True
    )
    try:
        yield engine
    finally:
        engine.dispose()
        with admin.begin() as connection:
            connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin.dispose()


def _request(workflow_id: str = "workflow-1") -> CreateTaskRequest:
    return CreateTaskRequest(
        tenant=SCOPE["tenant"],
        business_domain=SCOPE["business_domain"],
        application=SCOPE["application"],
        workflow_id=workflow_id,
        run_id="run-1",
        first_execution_run_id="run-1",
        step_id="manager_approval",
        definition_version="1.0",
        correlation_id="correlation-1",
        business_reference="CASE-1",
        idempotency_key=f"{workflow_id}:manager_approval:1.0",
        assignee_group="approvers",
        timeout_seconds=300,
        package_id="customer-adjustment",
        package_release_id="customer-adjustment-0.2.0",
        build_id="customer-adjustment-0.2.0",
    )


def test_postgres_0004_matches_models_and_blocks_loss_of_retained_tasks(
    task_engine: Engine,
) -> None:
    config = Config(str(ROOT / "migrations/alembic.ini"))
    with task_engine.begin() as connection:
        config.attributes["connection"] = connection
        command.upgrade(config, "0003")
        assert "human_tasks" not in inspect(connection).get_table_names()
        command.upgrade(config, "head")
        expected = MetaData()
        for metadata in (Base.metadata, TaskBase.metadata):
            for table in metadata.sorted_tables:
                table.to_metadata(expected)
        assert compare_metadata(MigrationContext.configure(connection), expected) == []
    repository = TaskRepository(task_engine)
    assert repository.ping()
    created = repository.create(_request(), ACTOR)
    assert created.first_execution_run_id == "run-1"
    with task_engine.begin() as connection:
        config.attributes["connection"] = connection
        with pytest.raises(RuntimeError, match="Human-task provenance"):
            command.downgrade(config, "0003")
        assert "human_tasks" in inspect(connection).get_table_names()
    with repository.sessions() as session:
        assert session.get(TaskRow, created.task_id) is not None


def test_postgres_concurrent_create_preserves_one_task_and_audit(task_engine: Engine) -> None:
    config = Config(str(ROOT / "migrations/alembic.ini"))
    with task_engine.begin() as connection:
        config.attributes["connection"] = connection
        command.upgrade(config, "head")
    repository = TaskRepository(task_engine)
    request = _request()
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: repository.create(request, ACTOR), range(16)))
    assert len({item.task_id for item in results}) == 1
    assert sum(item.replayed for item in results) == 15
    with repository.sessions() as session:
        assert len(session.scalars(select(TaskRow)).all()) == 1
        assert len(session.scalars(select(TaskAuditRow)).all()) == 1


def test_postgres_conflicting_decisions_commit_one_outcome(task_engine: Engine) -> None:
    config = Config(str(ROOT / "migrations/alembic.ini"))
    with task_engine.begin() as connection:
        config.attributes["connection"] = connection
        command.upgrade(config, "head")
    repository = TaskRepository(task_engine)
    created = repository.create(_request(), ACTOR)
    approvers = (
        TaskPrincipal(
            subject="alice",
            permissions=frozenset({"tasks:act"}),
            groups=frozenset({"approvers"}),
            **SCOPE,
        ),
        TaskPrincipal(
            subject="bob",
            permissions=frozenset({"tasks:act"}),
            groups=frozenset({"approvers"}),
            **SCOPE,
        ),
    )

    def decide(index: int) -> str:
        try:
            response = repository.decide(
                created.task_id,
                "approved" if index == 0 else "rejected",
                TaskDecisionRequest(comment="Concurrent review"),
                approvers[index],
            )
            return response.status.value
        except TaskError as exc:
            assert exc.status_code == 409
            return "CONFLICT"

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(decide, range(2)))
    assert sorted(results) in (["APPROVED", "CONFLICT"], ["CONFLICT", "REJECTED"])
    with repository.sessions() as session:
        audit = session.scalars(select(TaskAuditRow)).all()
        outbox = session.scalars(select(TaskOutboxRow)).all()
    assert len(audit) == 2
    assert len(outbox) == 1
    assert outbox[0].payload["event_id"] == outbox[0].event_id
