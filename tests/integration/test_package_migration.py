"""Package schema upgrades preserve old histories and block unsafe provenance loss."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from contracts import (
    DefinitionDocument,
    ExecutorPool,
    PackageManifest,
    RegisterDefinitionRequest,
    WorkflowPackage,
)
from sqlalchemy import MetaData, Table, create_engine, inspect, select
from workflow_api.errors import ApiError
from workflow_api.models import ExecutorPoolRow, PackageQueueOwnerRow, PackageRow
from workflow_api.package_repository import PackageRepository
from workflow_api.repository import ExecutionRecord, Scope, WorkflowRepository
from workflow_sdk.packages import resolve_binding

from tests.contract.packages.factories import make_manifest, make_release
from tests.integration.test_workflow_postgres import postgres_repository as postgres_repository

ROOT = Path(__file__).resolve().parents[2]
NOW = datetime(2026, 9, 27, tzinfo=timezone.utc)
SCOPE = Scope("migration-package", "finance", "adjustments")


def test_upgrade_preserves_legacy_and_governed_v1_pending_bindings(tmp_path: Path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / 'old-runtime.db'}")
    config = Config(str(ROOT / "migrations/alembic.ini"))
    document = DefinitionDocument.model_validate(
        {"start_at": "done", "steps": {"done": {"type": "end"}}}
    )
    with engine.begin() as connection:
        config.attributes["connection"] = connection
        command.upgrade(config, "0002")
        table = Table("workflow_executions", MetaData(), autoload_with=connection)
        for profile, queue in (
            ("legacy", "old-custom-legacy-tq"),
            ("governed", "workflow-orchestration-tq"),
        ):
            connection.execute(
                table.insert().values(
                    **SCOPE.as_dict(),
                    workflow_id=f"old-{profile}",
                    workflow_type="legacy-adjustment",
                    definition_id="old-definition",
                    definition_version="1.0",
                    definition_document=document.model_dump(mode="json"),
                    request={"amount": 100},
                    variables={},
                    business_reference="old-case",
                    correlation_id="old-trace",
                    idempotency_key=f"old-{profile}-key",
                    request_fingerprint="a" * 64,
                    created_by="old-creator",
                    state="CREATED",
                    runtime_profile=profile,
                    runtime_task_queue=queue,
                    started_at=NOW,
                    updated_at=NOW,
                )
            )
        command.upgrade(config, "head")
        assert "workflow_package_queue_owners" in inspect(connection).get_table_names()
    repository = WorkflowRepository(engine)
    try:
        assert repository.ping()
        for profile, queue in (
            ("legacy", "old-custom-legacy-tq"),
            ("governed", "workflow-orchestration-tq"),
        ):
            record = repository.get_execution(SCOPE, f"old-{profile}")
            assert record.runtime_profile == profile and record.runtime_task_queue == queue
            assert record.package_binding is None
            assert (
                record.observed_initial_build_id is None
                and record.observed_current_build_id is None
            )
            assert record.definition_document == document and record.created_by == "old-creator"
        with engine.begin() as connection:
            config.attributes["connection"] = connection
            command.downgrade(config, "0002")
            assert "package_binding" not in {
                c["name"] for c in inspect(connection).get_columns("workflow_executions")
            }
            command.upgrade(config, "head")
        assert (
            repository.get_execution(SCOPE, "old-legacy").runtime_task_queue
            == "old-custom-legacy-tq"
        )
    finally:
        repository.close()


def test_downgrade_rejects_loss_of_package_execution_provenance(tmp_path: Path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / 'package-runtime.db'}")
    config = Config(str(ROOT / "migrations/alembic.ini"))
    manifest = make_manifest()
    binding = resolve_binding(manifest, make_release(manifest), manifest.definitions[0])
    with engine.begin() as connection:
        config.attributes["connection"] = connection
        command.upgrade(config, "head")
        table = Table("workflow_executions", MetaData(), autoload_with=connection)
        connection.execute(
            table.insert().values(
                **SCOPE.as_dict(),
                workflow_id="package-pending",
                workflow_type="validation_reference",
                definition_id=binding.definition_id,
                definition_version=binding.definition_version,
                definition_document=manifest.definitions[0].definition_document.model_dump(
                    mode="json"
                ),
                request={},
                variables={},
                business_reference="package-case",
                correlation_id="package-trace",
                idempotency_key="package-key",
                request_fingerprint="a" * 64,
                created_by="package-creator",
                state="CREATED",
                runtime_profile="package",
                runtime_task_queue=binding.workflow_task_queue,
                package_binding=binding.model_dump(mode="json"),
                started_at=NOW,
                updated_at=NOW,
            )
        )
    with engine.begin() as connection:
        config.attributes["connection"] = connection
        with pytest.raises(RuntimeError, match="Retain package provenance"):
            command.downgrade(config, "0002")
        assert "workflow_packages" in inspect(connection).get_table_names()
    repository = WorkflowRepository(engine)
    try:
        record = repository.get_execution(SCOPE, "package-pending")
        assert isinstance(record, ExecutionRecord) and record.package_binding == binding
        assert record.run_id is None
    finally:
        repository.close()


def seed_package(
    repository: WorkflowRepository, manifest: PackageManifest, scope: Scope
) -> PackageRepository:
    for revision in manifest.definitions:
        repository.register(
            RegisterDefinitionRequest(
                **scope.as_dict(),
                definition_id=revision.definition_id,
                workflow_type=revision.workflow_type,
                version=revision.version,
                owner="team",
                definition_document=revision.definition_document,
            ),
            "author",
            NOW,
        )
        repository.approve_definition(
            scope, revision.workflow_type, revision.version, "reviewer", NOW
        )
    packages = PackageRepository(repository)
    packages.register(
        WorkflowPackage(
            **scope.as_dict(),
            package_id=manifest.package_id,
            name=manifest.package_id,
            owner="team",
            workflow_types=tuple(d.workflow_type for d in manifest.definitions),
        ),
        "author",
        NOW,
    )
    return packages


@pytest.mark.postgres
def test_postgres_concurrent_queue_claim_has_one_global_owner(
    postgres_repository: WorkflowRepository,
) -> None:
    first = make_manifest()
    second = PackageManifest.model_validate(
        {
            **first.model_dump(),
            "package_id": "other-validation",
            "worker_deployment_name": "other-validation",
            "build_id": "other-validation-1.0.0",
        }
    )
    scopes = (SCOPE, Scope("other-migration", "finance", "adjustments"))
    manifests = (first, second)
    packages = PackageRepository(postgres_repository)
    for scope, manifest in zip(scopes, manifests, strict=True):
        seed_package(postgres_repository, manifest, scope)
        packages.publish(scope, make_release(manifest), manifest, "publisher", NOW)
        packages.approve(scope, make_release(manifest).package_release_id, "reviewer", NOW)

    def configure(index: int) -> str:
        manifest = manifests[index]
        pool = ExecutorPool(
            pool_id=f"{manifest.package_id}-mixed",
            package_id=manifest.package_id,
            package_release_id=make_release(manifest).package_release_id,
            worker_deployment_name=manifest.worker_deployment_name,
            build_id=manifest.build_id,
            queue_bindings={q.name: "contended-business-tq" for q in manifest.queues},
        )
        try:
            packages.put_pool(scopes[index], pool, "operator", NOW)
            return "accepted"
        except ApiError as exc:
            assert exc.status_code == 409
            return exc.code

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(configure, (0, 1)))
    assert outcomes.count("accepted") == 1 and outcomes.count("package_queue_conflict") == 1
    with postgres_repository.sessions() as session:
        assert len(session.scalars(select(PackageQueueOwnerRow)).all()) == 1
        assert len(session.scalars(select(ExecutorPoolRow)).all()) == 1


@pytest.mark.postgres
def test_postgres_concurrent_deployment_claim_cannot_cross_scopes(
    postgres_repository: WorkflowRepository,
) -> None:
    first = make_manifest()
    second = PackageManifest.model_validate(
        {
            **first.model_dump(),
            "package_id": "other-validation",
            "build_id": "other-validation-1.0.0",
        }
    )
    scopes = (SCOPE, Scope("other-migration", "finance", "adjustments"))
    manifests = (first, second)
    packages = PackageRepository(postgres_repository)
    for scope, manifest in zip(scopes, manifests, strict=True):
        seed_package(postgres_repository, manifest, scope)

    def publish(index: int) -> str:
        try:
            packages.publish(
                scopes[index], make_release(manifests[index]), manifests[index], "publisher", NOW
            )
            return "accepted"
        except ApiError as exc:
            assert exc.status_code == 409
            return "rejected"

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(publish, (0, 1)))
    assert sorted(outcomes) == ["accepted", "rejected"]
    with postgres_repository.sessions() as session:
        assert (
            len(
                session.scalars(
                    select(PackageRow).where(
                        PackageRow.deployment_name == first.worker_deployment_name
                    )
                ).all()
            )
            == 1
        )


@pytest.mark.postgres
def test_postgres_reservation_and_retirement_are_serialized(
    postgres_repository: WorkflowRepository,
) -> None:
    manifest = make_manifest()
    packages = seed_package(postgres_repository, manifest, SCOPE)
    release = make_release(manifest)
    packages.publish(SCOPE, release, manifest, "publisher", NOW)
    packages.approve(SCOPE, release.package_release_id, "reviewer", NOW)
    binding = resolve_binding(manifest, release, manifest.definitions[0])
    record = ExecutionRecord(
        workflow_id="race-start",
        scope=SCOPE,
        workflow_type="validation_reference",
        definition_id=binding.definition_id,
        definition_version=binding.definition_version,
        definition_document=manifest.definitions[0].definition_document,
        request={},
        variables={},
        business_reference="race-case",
        correlation_id="race-trace",
        idempotency_key="race-key",
        request_fingerprint="a" * 64,
        created_by="starter",
        started_at=NOW,
        updated_at=NOW,
        runtime_profile="package",
        runtime_task_queue=binding.workflow_task_queue,
        package_binding=binding,
    )

    def race(operation: str) -> str:
        try:
            if operation == "reserve":
                postgres_repository.reserve_execution(record)
            else:
                packages.retire(SCOPE, release.package_release_id, "operator", NOW)
            return f"{operation}-accepted"
        except ApiError as exc:
            assert exc.status_code == 409
            return f"{operation}-rejected"

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = set(executor.map(race, ("reserve", "retire")))
    assert results in (
        {"reserve-accepted", "retire-rejected"},
        {"reserve-rejected", "retire-accepted"},
    )
