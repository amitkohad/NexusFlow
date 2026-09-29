"""Durable registry, scoped metadata, concurrency, and audit invariants."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator
from uuid import uuid4

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from contracts import DefinitionDocument, DefinitionStatus, EndStep, ExecutionState
from contracts.api import RegisterDefinitionRequest
from human_task_service.models import TaskBase
from sqlalchemy import MetaData, create_engine, inspect
from workflow_api.errors import ApiError
from workflow_api.models import Base
from workflow_api.repository import ExecutionRecord, Scope, WorkflowRepository

NOW = datetime(2026, 9, 27, tzinfo=timezone.utc)
SCOPE = Scope("acme", "finance", "adjustments")
ROOT = Path(__file__).resolve().parents[2]
METADATA = MetaData()
for source in (Base.metadata, TaskBase.metadata):
    for table in source.tables.values():
        table.to_metadata(METADATA)


def definition_request(version: str = "1") -> RegisterDefinitionRequest:
    return RegisterDefinitionRequest(
        tenant=SCOPE.tenant,
        business_domain=SCOPE.business_domain,
        application=SCOPE.application,
        definition_id="customer-adjustment",
        workflow_type="adjustment",
        version=version,
        owner="business-team",
        definition_document=DefinitionDocument(
            start_at="done",
            steps={"done": EndStep()},
        ),
    )


def execution_record(key: str = "key-1") -> ExecutionRecord:
    return ExecutionRecord(
        workflow_id=str(uuid4()),
        scope=SCOPE,
        workflow_type="adjustment",
        definition_id="customer-adjustment",
        definition_version="1",
        definition_document=definition_request().definition_document,
        request={"amount": 100},
        variables={},
        business_reference="adjustment-123",
        correlation_id="trace-123",
        idempotency_key=key,
        request_fingerprint="a" * 64,
        created_by="starter",
        started_at=NOW,
        updated_at=NOW,
    )


@pytest.fixture
def repository(tmp_path: Path) -> Iterator[WorkflowRepository]:
    repo = WorkflowRepository(f"sqlite:///{tmp_path / 'control-plane.db'}")
    repo.create_schema()
    yield repo
    repo.close()


def test_readiness_requires_migrated_schema(tmp_path: Path) -> None:
    repo = WorkflowRepository(f"sqlite:///{tmp_path / 'ready.db'}")
    try:
        assert not repo.ping()
        repo.create_schema()
        assert repo.ping()
    finally:
        repo.close()


def test_registry_hashes_detached_immutable_revision(repository: WorkflowRepository) -> None:
    request = definition_request()
    registered = repository.register(request, "author", NOW)
    assert registered.status == DefinitionStatus.VALIDATED
    assert len(registered.content_hash) == 64
    request.definition_document.variables["changed"] = True
    assert repository.get_definition(SCOPE, "adjustment", "1").definition_document.variables == {}
    with pytest.raises(ApiError) as exc:
        repository.register(definition_request(), "author", NOW)
    assert (exc.value.status_code, exc.value.code) == (409, "definition_exists")


def test_invalid_graph_is_rejected_without_persistence(repository: WorkflowRepository) -> None:
    request = definition_request().model_copy(
        update={
            "definition_document": DefinitionDocument(start_at="missing", steps={"done": EndStep()})
        }
    )
    with pytest.raises(ApiError) as exc:
        repository.register(request, "author", NOW)
    assert exc.value.code == "definition_invalid"
    assert exc.value.issues
    assert repository.list_definitions(SCOPE, 100, 0) == []


def test_governance_requires_approval_and_resolves_environment(
    repository: WorkflowRepository,
) -> None:
    repository.register(definition_request(), "author", NOW)
    with pytest.raises(ApiError, match="approval"):
        repository.promote_definition(SCOPE, "adjustment", "1", "local", "promoter", NOW)
    approved = repository.approve_definition(SCOPE, "adjustment", "1", "reviewer", NOW)
    assert approved.approved_by == "reviewer"
    promoted = repository.promote_definition(SCOPE, "adjustment", "1", "local", "promoter", NOW)
    assert promoted.status == DefinitionStatus.PROMOTED
    assert repository.resolve_promoted(SCOPE, "adjustment", "local").version == "1"
    with pytest.raises(ApiError) as exc:
        repository.resolve_promoted(SCOPE, "adjustment", "prod")
    assert exc.value.code == "definition_not_promoted"
    repeated = repository.promote_definition(SCOPE, "adjustment", "1", "local", "other", NOW)
    assert repeated.promoted_by == "promoter"


def test_revision_binding_survives_new_promotion(repository: WorkflowRepository) -> None:
    for version in ("1", "2"):
        repository.register(definition_request(version), "author", NOW)
        repository.approve_definition(SCOPE, "adjustment", version, "reviewer", NOW)
    repository.promote_definition(SCOPE, "adjustment", "1", "local", "promoter", NOW)
    record, _ = repository.reserve_execution(execution_record())
    repository.promote_definition(SCOPE, "adjustment", "2", "local", "promoter", NOW)
    assert repository.resolve_promoted(SCOPE, "adjustment", "local").version == "2"
    with pytest.raises(ApiError) as exc:
        repository.resolve_promoted(SCOPE, "adjustment", "local", "1")
    assert exc.value.code == "definition_not_promoted"
    persisted = repository.get_execution(SCOPE, record.workflow_id)
    assert persisted.definition_version == "1"
    assert persisted.definition_document == definition_request().definition_document


@pytest.mark.parametrize(
    "other_scope",
    [
        Scope("other", SCOPE.business_domain, SCOPE.application),
        Scope(SCOPE.tenant, "other", SCOPE.application),
        Scope(SCOPE.tenant, SCOPE.business_domain, "other"),
    ],
)
def test_every_scope_dimension_isolates_records(
    repository: WorkflowRepository, other_scope: Scope
) -> None:
    repository.register(definition_request(), "author", NOW)
    record, _ = repository.reserve_execution(execution_record())
    assert repository.list_definitions(other_scope, 100, 0) == []
    assert repository.list_executions(other_scope, 100, 0) == []
    assert repository.get_by_idempotency(other_scope, "adjustment", "key-1") is None
    for operation in (
        lambda: repository.get_definition(other_scope, "adjustment", "1"),
        lambda: repository.get_execution(other_scope, record.workflow_id),
        lambda: repository.list_history(other_scope, record.workflow_id, 100, 0),
        lambda: repository.mark_started(other_scope, record.workflow_id, "run", NOW),
        lambda: repository.append_execution_event(
            other_scope, record.workflow_id, "cancel", "user", NOW
        ),
    ):
        with pytest.raises(ApiError) as exc:
            operation()
        assert exc.value.status_code == 404


def test_repository_restarts_preserve_execution_binding_and_history(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'durable.db'}"
    first = WorkflowRepository(url)
    first.create_schema()
    first.register(definition_request(), "author", NOW)
    record, _ = first.reserve_execution(execution_record())
    first.mark_started(SCOPE, record.workflow_id, "temporal-run", NOW)
    first.close()
    second = WorkflowRepository(url)
    try:
        persisted = second.get_execution(SCOPE, record.workflow_id)
        assert persisted.run_id == "temporal-run"
        assert persisted.state == ExecutionState.RUNNING
        assert persisted.request == {"amount": 100}
        assert persisted.started_at.tzinfo is not None
        history = second.list_history(SCOPE, record.workflow_id, 100, 0)
        assert [event.event_type for event in history] == ["execution.created", "execution.started"]
    finally:
        second.close()


def test_database_idempotency_race_reserves_one_execution(repository: WorkflowRepository) -> None:
    repository.register(definition_request(), "author", NOW)
    records = [execution_record() for _ in range(12)]
    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(repository.reserve_execution, records))
    assert sum(created for _, created in results) == 1
    assert len({record.workflow_id for record, _ in results}) == 1
    assert len(repository.list_executions(SCOPE, 100, 0)) == 1
    assert len(repository.list_history(SCOPE, results[0][0].workflow_id, 100, 0)) == 1


def test_idempotency_conflict_does_not_create_or_mutate_execution(
    repository: WorkflowRepository,
) -> None:
    repository.register(definition_request(), "author", NOW)
    original, _ = repository.reserve_execution(execution_record())
    conflicting = replace(execution_record(), request={"amount": 200}, request_fingerprint="b" * 64)
    with pytest.raises(ApiError) as exc:
        repository.reserve_execution(conflicting)
    assert (exc.value.status_code, exc.value.code) == (409, "idempotency_conflict")
    assert repository.get_execution(SCOPE, original.workflow_id).request == {"amount": 100}
    assert len(repository.list_executions(SCOPE, 100, 0)) == 1


def test_idempotency_keys_are_scoped_by_workflow_type_and_tenant(
    repository: WorkflowRepository,
) -> None:
    repository.register(definition_request(), "author", NOW)
    other_scope = Scope("other", SCOPE.business_domain, SCOPE.application)
    repository.register(
        definition_request().model_copy(update=other_scope.as_dict()), "author", NOW
    )
    repository.register(
        definition_request().model_copy(update={"workflow_type": "another"}), "author", NOW
    )
    records = [
        execution_record(),
        replace(execution_record(), scope=other_scope),
        replace(execution_record(), workflow_type="another"),
    ]
    for record in records:
        assert repository.reserve_execution(record)[1]


def test_started_binding_is_idempotent_and_cannot_switch_runs(
    repository: WorkflowRepository,
) -> None:
    repository.register(definition_request(), "author", NOW)
    record, _ = repository.reserve_execution(execution_record())
    repository.mark_started(SCOPE, record.workflow_id, "run-1", NOW)
    repository.mark_started(SCOPE, record.workflow_id, "run-1", NOW)
    with pytest.raises(ApiError) as exc:
        repository.mark_started(SCOPE, record.workflow_id, "run-2", NOW)
    assert exc.value.code == "run_id_conflict"
    assert len(repository.list_history(SCOPE, record.workflow_id, 100, 0)) == 2


def test_snapshot_projects_business_transitions_once_and_paginates(
    repository: WorkflowRepository,
) -> None:
    repository.register(definition_request(), "author", NOW)
    record, _ = repository.reserve_execution(execution_record())
    repository.mark_started(SCOPE, record.workflow_id, "run-1", NOW)
    transitions = [
        {"step": "done", "state": "STARTED", "detail": "end", "workflow_time": NOW.isoformat()},
        {"step": "done", "state": "COMPLETED", "detail": "end", "workflow_time": NOW.isoformat()},
    ]
    for _ in range(3):
        repository.update_snapshot(
            SCOPE,
            record.workflow_id,
            ExecutionState.COMPLETED,
            None,
            transitions,
            NOW + timedelta(seconds=1),
        )
    history = repository.list_history(SCOPE, record.workflow_id, 100, 0)
    assert len(history) == 5
    assert len({event.event_id for event in history}) == 5
    assert [
        event.metadata["ordinal"] for event in history if event.event_type == "execution.transition"
    ] == [0, 1]
    assert history == repository.list_history(
        SCOPE, record.workflow_id, 2, 0
    ) + repository.list_history(SCOPE, record.workflow_id, 3, 2)
    assert history[-1].previous_state == "RUNNING"
    assert history[-1].new_state == "COMPLETED"


def test_stale_and_terminal_snapshots_cannot_regress_execution(
    repository: WorkflowRepository,
) -> None:
    repository.register(definition_request(), "author", NOW)
    record, _ = repository.reserve_execution(execution_record())
    later = NOW + timedelta(seconds=10)
    repository.update_snapshot(
        SCOPE, record.workflow_id, ExecutionState.WAITING_FOR_APPROVAL, "approval", [], later
    )
    stale = repository.update_snapshot(
        SCOPE, record.workflow_id, ExecutionState.RUNNING, None, [], NOW
    )
    assert stale.state == ExecutionState.WAITING_FOR_APPROVAL
    complete = repository.update_snapshot(
        SCOPE,
        record.workflow_id,
        ExecutionState.FAILED,
        None,
        [],
        later,
        "activity_failed",
        "Activity did not complete",
    )
    assert complete.completed_at == later
    regression = repository.update_snapshot(
        SCOPE, record.workflow_id, ExecutionState.RUNNING, None, [], later + timedelta(seconds=10)
    )
    assert regression.state == ExecutionState.FAILED
    assert regression.failure_code == "activity_failed"


def test_snapshot_progress_overrides_request_wallclock_order(
    repository: WorkflowRepository,
) -> None:
    repository.register(definition_request(), "author", NOW)
    record, _ = repository.reserve_execution(execution_record())
    initial = [{"step": "done", "state": "STARTED", "detail": "timer"}]
    advanced = initial + [{"step": "approval", "state": "STARTED", "detail": "approval"}]
    later = NOW + timedelta(seconds=10)
    repository.update_snapshot(
        SCOPE, record.workflow_id, ExecutionState.RUNNING, "done", initial, later
    )
    # The earlier request reached the runtime later and observed more progress.
    newer = repository.update_snapshot(
        SCOPE, record.workflow_id, ExecutionState.WAITING_FOR_APPROVAL, "approval", advanced, NOW
    )
    assert newer.state == ExecutionState.WAITING_FOR_APPROVAL
    assert newer.current_step == "approval"
    assert newer.updated_at == later
    # A subsequently arriving older runtime prefix cannot regress the state,
    # even if its request timestamp is later than the committed observation.
    stale = repository.update_snapshot(
        SCOPE,
        record.workflow_id,
        ExecutionState.RUNNING,
        "done",
        initial,
        later + timedelta(seconds=10),
    )
    assert stale.state == ExecutionState.WAITING_FOR_APPROVAL
    assert stale.updated_at == later
    transitions = [
        event
        for event in repository.list_history(SCOPE, record.workflow_id, 100, 0)
        if event.event_type == "execution.transition"
    ]
    assert [event.metadata["ordinal"] for event in transitions] == [0, 1]


@pytest.mark.parametrize("terminal", [ExecutionState.FAILED, ExecutionState.CANCELLED])
def test_closed_snapshot_without_transitions_overrides_progress(
    repository: WorkflowRepository, terminal: ExecutionState
) -> None:
    repository.register(definition_request(), "author", NOW)
    record, _ = repository.reserve_execution(execution_record())
    later = NOW + timedelta(seconds=10)
    repository.update_snapshot(
        SCOPE,
        record.workflow_id,
        ExecutionState.WAITING_FOR_APPROVAL,
        "approval",
        [{"step": "approval", "state": "STARTED", "detail": "approval"}],
        later,
    )
    closed = repository.update_snapshot(
        SCOPE,
        record.workflow_id,
        terminal,
        None,
        [],
        NOW,
        "workflow_failed" if terminal == ExecutionState.FAILED else None,
        "Workflow failed" if terminal == ExecutionState.FAILED else None,
    )
    assert closed.state == terminal
    assert closed.completed_at == later
    assert closed.updated_at == later
    assert (
        len(
            [
                event
                for event in repository.list_history(SCOPE, record.workflow_id, 100, 0)
                if event.event_type == "execution.transition"
            ]
        )
        == 1
    )


def test_continued_as_new_reports_intervention_without_transition_payload(
    repository: WorkflowRepository,
) -> None:
    repository.register(definition_request(), "author", NOW)
    record, _ = repository.reserve_execution(execution_record())
    repository.update_snapshot(
        SCOPE,
        record.workflow_id,
        ExecutionState.RUNNING,
        "done",
        [{"step": "done", "state": "STARTED", "detail": "timer"}],
        NOW,
    )
    changed = repository.update_snapshot(
        SCOPE,
        record.workflow_id,
        ExecutionState.MANUAL_INTERVENTION,
        None,
        [],
        NOW,
        "runtime_run_changed",
        "Workflow advanced to another execution run",
    )
    assert changed.state == ExecutionState.MANUAL_INTERVENTION
    assert changed.failure_code == "runtime_run_changed"
    assert changed.completed_at is None
    # A delayed query of the previous run cannot erase its known closed-run
    # condition, even if that query carries a longer transition prefix.
    stale = repository.update_snapshot(
        SCOPE,
        record.workflow_id,
        ExecutionState.RUNNING,
        "next",
        [
            {"step": "done", "state": "STARTED", "detail": "timer"},
            {"step": "next", "state": "STARTED", "detail": "timer"},
        ],
        NOW + timedelta(seconds=10),
    )
    assert stale.state == ExecutionState.MANUAL_INTERVENTION
    assert stale.failure_code == "runtime_run_changed"


def test_execution_lists_use_stable_pagination(repository: WorkflowRepository) -> None:
    repository.register(definition_request(), "author", NOW)
    for index in range(5):
        repository.reserve_execution(execution_record(str(index)))
    all_records = repository.list_executions(SCOPE, 100, 0)
    assert all_records == repository.list_executions(SCOPE, 2, 0) + repository.list_executions(
        SCOPE, 3, 2
    )
    assert repository.list_executions(SCOPE, 2, 5) == []


def test_initial_migration_matches_models_and_roundtrips(tmp_path: Path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / 'migrated.db'}")
    config = Config(str(ROOT / "migrations/alembic.ini"))
    with engine.begin() as connection:
        config.attributes["connection"] = connection
        command.upgrade(config, "head")
        assert compare_metadata(MigrationContext.configure(connection), METADATA) == []
        command.downgrade(config, "base")
        assert inspect(connection).get_table_names() == ["alembic_version"]
        command.upgrade(config, "head")
        assert "workflow_executions" in inspect(connection).get_table_names()
    engine.dispose()
