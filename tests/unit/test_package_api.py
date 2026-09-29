"""Privileged package governance, serving evidence and frozen recovery bindings."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
from contracts import (
    DefinitionDocument,
    ExecutionState,
    PackageExecutionBinding,
    PackageManifest,
    PackageRuntimeStartRequest,
)
from fastapi.testclient import TestClient
from sqlalchemy import select
from workflow_api.api import PERMISSIONS, create_app
from workflow_api.backend import BackendUnavailable, BusinessSnapshot
from workflow_api.models import PackageEnvironmentRow, PackageQueueOwnerRow
from workflow_api.package_repository import PackageRepository
from workflow_api.repository import Scope, WorkflowRepository
from workflow_api.security import Principal, StaticTokenAuthenticator
from workflow_api.service import utcnow
from workflow_sdk.packages import definition_hash

from tests.contract.packages.factories import make_manifest, make_release
from tests.contract.test_workflow_api import FakeBackend

SCOPE = Scope("package-tenant", "finance", "adjustments")
OTHER_SCOPE = Scope("other-package-tenant", "finance", "adjustments")
AUTH = {"Authorization": "Bearer package-owner"}
OTHER_AUTH = {"Authorization": "Bearer other-owner"}


@dataclass
class FakePackageBackend(FakeBackend):
    package_starts: list[tuple[str, PackageRuntimeStartRequest]] = field(default_factory=list)
    routes: dict[str, tuple[str, str | None, float]] = field(default_factory=dict)
    serving: bool = True
    replica_count: int = 1
    omit_activity_evidence: bool = False
    route_failure: bool = False
    drainage_status: int = 2

    async def start_package(self, workflow_id: str, payload: PackageRuntimeStartRequest) -> str:
        self.package_starts.append((workflow_id, payload))
        if self.fail_once:
            self.fail_once = False
            raise BackendUnavailable()
        return await self.start(
            workflow_id,
            payload.context.workflow_type,
            payload.definition_document,
            payload.request,
            payload.variables,
            context=payload.context,
        )

    async def inspect_release(
        self, manifest: PackageManifest, namespace: str, queues: dict[str, str]
    ) -> dict[str, Any]:
        return {
            "ready": self.serving,
            "queues": [
                {
                    "task_queue": queues[q.name],
                    "task_type": 1 if q.role == "workflow" else 2,
                    "replicas": self.replica_count,
                    "identities": [
                        f"{manifest.package_id}:{manifest.build_id}:{make_release(manifest).package_release_id}-mixed:replica-{number}"
                        for number in range(self.replica_count)
                    ],
                }
                for q in manifest.queues
                if not (self.omit_activity_evidence and q.role == "activity")
            ],
            "drainage_status": self.drainage_status,
            "build_id": manifest.build_id,
        }

    async def route_release(
        self, manifest: PackageManifest, namespace: str, *, ramp_percentage: int | None = None
    ) -> None:
        if self.route_failure:
            raise BackendUnavailable()
        current = self.routes.get(manifest.worker_deployment_name, (manifest.build_id, None, 0.0))
        self.routes[manifest.worker_deployment_name] = (
            (manifest.build_id, None, 0.0)
            if ramp_percentage is None
            else (current[0], manifest.build_id, float(ramp_percentage))
        )

    async def routing(self, deployment_name: str, namespace: str) -> tuple[str, str | None, float]:
        return self.routes.get(deployment_name, ("", None, 0.0))

    async def observe_package(
        self, workflow_id: str, run_id: str, binding: PackageExecutionBinding
    ) -> tuple[str | None, str | None]:
        return binding.build_id, binding.build_id

    async def status_package(self, workflow_id: str, run_id: str) -> tuple[str, BusinessSnapshot]:
        return run_id, await self.status(workflow_id, run_id)


@dataclass
class PackageHarness:
    client: TestClient
    backend: FakePackageBackend
    repository: WorkflowRepository


@pytest.fixture
def package_harness(tmp_path: Path) -> Iterator[PackageHarness]:
    repository = WorkflowRepository(f"sqlite:///{tmp_path / 'packages.db'}")
    repository.create_schema()
    backend = FakePackageBackend()
    auth = StaticTokenAuthenticator(
        {
            "package-owner": Principal(
                "package-operator", **SCOPE.as_dict(), permissions=PERMISSIONS
            ),
            "other-owner": Principal(
                "other-operator", **OTHER_SCOPE.as_dict(), permissions=PERMISSIONS
            ),
            "reader": Principal(
                "reader",
                **SCOPE.as_dict(),
                permissions=frozenset({"packages:read", "workflows:read"}),
            ),
        }
    )
    with TestClient(
        create_app(repository, backend, authenticator=auth, runtime_profile="package"),
        raise_server_exceptions=False,
    ) as client:
        yield PackageHarness(client, backend, repository)
    repository.close()


def publish_fixture(
    harness: PackageHarness,
    manifest: PackageManifest | None = None,
    *,
    approve: bool = True,
    scope: Scope = SCOPE,
    auth: dict[str, str] = AUTH,
) -> PackageManifest:
    manifest = manifest or make_manifest()
    body = {
        **scope.as_dict(),
        "package_id": manifest.package_id,
        "name": manifest.package_id,
        "owner": "finance-team",
        "workflow_types": sorted({d.workflow_type for d in manifest.definitions}),
    }
    registered = harness.client.post("/api/v1/packages", json=body, headers=auth)
    assert registered.status_code in {201, 409}, registered.text
    for definition in manifest.definitions:
        body = {
            **scope.as_dict(),
            "definition_id": definition.definition_id,
            "workflow_type": definition.workflow_type,
            "version": definition.version,
            "owner": "finance-team",
            "definition_document": definition.definition_document.model_dump(mode="json"),
        }
        registered = harness.client.post("/api/v1/workflows", json=body, headers=auth)
        assert registered.status_code in {201, 409}, registered.text
        if approve:
            approved = harness.client.post(
                f"/api/v1/definitions/{definition.workflow_type}/{definition.version}/approve",
                json=scope.as_dict(),
                headers=auth,
            )
            assert approved.status_code == 200, approved.text
    published = harness.client.post(
        f"/api/v1/packages/{manifest.package_id}/releases",
        json={
            **scope.as_dict(),
            "release": make_release(manifest).model_dump(mode="json"),
            "manifest": manifest.model_dump(mode="json"),
        },
        headers=auth,
    )
    assert published.status_code == 201, published.text
    if approve:
        approved = harness.client.post(
            f"/api/v1/package-releases/{make_release(manifest).package_release_id}/approve",
            json=scope.as_dict(),
            headers=auth,
        )
        assert approved.status_code == 200, approved.text
    return manifest


def configure_pool(
    harness: PackageHarness,
    manifest: PackageManifest,
    *,
    role: str = "mixed",
    scope: Scope = SCOPE,
    auth: dict[str, str] = AUTH,
    queue: str | None = None,
) -> Any:
    release = make_release(manifest)
    pool_id = f"{release.package_release_id}-{role}"
    return harness.client.put(
        f"/api/v1/executor-pools/{pool_id}",
        json={
            **scope.as_dict(),
            "pool": {
                "pool_id": pool_id,
                "package_id": manifest.package_id,
                "package_release_id": release.package_release_id,
                "worker_deployment_name": manifest.worker_deployment_name,
                "build_id": manifest.build_id,
                "role": role,
                "queue_bindings": {
                    q.name: queue or f"{manifest.package_id}-tq" for q in manifest.queues
                },
                "observed_state": "ready",
                "observed_replicas": 99,
            },
        },
        headers=auth,
    )


def promote(harness: PackageHarness, manifest: PackageManifest) -> Any:
    return harness.client.post(
        f"/api/v1/package-releases/{make_release(manifest).package_release_id}/promote",
        json={
            **SCOPE.as_dict(),
            "queue_bindings": {q.name: f"{manifest.package_id}-tq" for q in manifest.queues},
        },
        headers=AUTH,
    )


def govern_definition(harness: PackageHarness, manifest: PackageManifest) -> None:
    definition = manifest.definitions[0]
    promoted = harness.client.post(
        f"/api/v1/definitions/{definition.workflow_type}/{definition.version}/promote",
        json=SCOPE.as_dict(),
        headers=AUTH,
    )
    assert promoted.status_code == 200, promoted.text


def start_request(key: str = "package-start") -> dict[str, Any]:
    return {
        **SCOPE.as_dict(),
        "business_reference": "case-42",
        "correlation_id": "trace-42",
        "idempotency_key": key,
        "request": {"amount": 100},
    }


def test_release_governance_cannot_claim_observed_capacity(package_harness: PackageHarness) -> None:
    manifest = publish_fixture(package_harness)
    assert promote(package_harness, manifest).status_code == 409
    configured = configure_pool(package_harness, manifest)
    assert configured.status_code == 200, configured.text
    assert configured.json()["observed_state"] == "pending"
    assert configured.json()["observed_replicas"] == 0
    package_harness.backend.serving = False
    failed = promote(package_harness, manifest)
    assert failed.status_code == 409 and failed.json()["code"] == "release_not_serving"
    package_harness.backend.serving = True
    assert promote(package_harness, manifest).status_code == 200
    response = package_harness.client.get(
        f"/api/v1/package-releases/{make_release(manifest).package_release_id}/executor-pools",
        headers=AUTH,
    )
    assert response.json()[0]["observed_state"] == "ready"


def test_approval_requires_every_included_governed_revision(
    package_harness: PackageHarness,
) -> None:
    manifest = publish_fixture(package_harness, approve=False)
    response = package_harness.client.post(
        f"/api/v1/package-releases/{make_release(manifest).package_release_id}/approve",
        json=SCOPE.as_dict(),
        headers=AUTH,
    )
    assert response.status_code == 409
    assert response.json()["code"] == "definition_not_approved"


def test_public_start_pins_private_release_and_hides_placement(
    package_harness: PackageHarness,
) -> None:
    manifest = publish_fixture(package_harness)
    govern_definition(package_harness, manifest)
    assert configure_pool(package_harness, manifest).status_code == 200
    assert promote(package_harness, manifest).status_code == 200
    start = package_harness.client.post(
        "/api/v1/workflows/validation_reference/start", json=start_request(), headers=AUTH
    )
    assert start.status_code == 202, start.text
    body = start.json()
    assert not {"package_id", "build_id", "artifact_digest", "task_queue", "package_binding"} & set(
        body
    )
    record = package_harness.repository.get_execution(SCOPE, body["workflow_id"])
    assert record.package_binding and record.package_binding.build_id == manifest.build_id
    assert record.runtime_profile == "package"
    assert package_harness.backend.package_starts[0][1].context.actor == "package-operator"


def test_durable_package_approval_cannot_bypass_human_task_api(
    package_harness: PackageHarness,
) -> None:
    manifest = PackageManifest.model_validate(
        {**make_manifest(approval=True).model_dump(mode="json"), "durable_human_tasks": True}
    )
    publish_fixture(package_harness, manifest)
    govern_definition(package_harness, manifest)
    assert configure_pool(package_harness, manifest).status_code == 200
    assert promote(package_harness, manifest).status_code == 200
    started = package_harness.client.post(
        "/api/v1/workflows/validation_reference/start",
        json=start_request("durable-approval"),
        headers=AUTH,
    )
    assert started.status_code == 202, started.text
    workflow_id = started.json()["workflow_id"]
    package_harness.backend.statuses[workflow_id] = BusinessSnapshot(
        ExecutionState.WAITING_FOR_APPROVAL, current_step="approval"
    )
    response = package_harness.client.post(
        f"/api/v1/workflows/{workflow_id}/signal",
        json={"approved": True},
        headers=AUTH,
    )
    assert response.status_code == 409
    assert response.json()["code"] == "task_api_required"
    assert not package_harness.backend.signals


def test_unavailable_compatible_release_does_not_reserve_a_start(
    package_harness: PackageHarness,
) -> None:
    manifest = publish_fixture(package_harness)
    govern_definition(package_harness, manifest)
    response = package_harness.client.post(
        "/api/v1/workflows/validation_reference/start", json=start_request(), headers=AUTH
    )
    assert response.status_code == 409
    assert (
        package_harness.repository.get_by_idempotency(
            SCOPE, "validation_reference", "package-start"
        )
        is None
    )


def test_pending_retry_retains_original_release_after_new_promotion(
    package_harness: PackageHarness,
) -> None:
    original = publish_fixture(package_harness)
    govern_definition(package_harness, original)
    assert configure_pool(package_harness, original).status_code == 200
    assert promote(package_harness, original).status_code == 200
    package_harness.backend.fail_once = True
    first = package_harness.client.post(
        "/api/v1/workflows/validation_reference/start", json=start_request(), headers=AUTH
    )
    assert first.status_code == 503, first.text
    pending = package_harness.repository.get_by_idempotency(
        SCOPE, "validation_reference", "package-start"
    )
    assert pending and pending.run_id is None
    newer = PackageManifest.model_validate(
        {
            **original.model_dump(),
            "package_version": "2.0.0",
            "build_id": "validation-reference-2.0.0",
        }
    )
    publish_fixture(package_harness, newer)
    assert configure_pool(package_harness, newer).status_code == 200
    assert promote(package_harness, newer).status_code == 200
    retry = package_harness.client.post(
        "/api/v1/workflows/validation_reference/start", json=start_request(), headers=AUTH
    )
    assert retry.status_code == 202, retry.text
    assert retry.json()["workflow_id"] == pending.workflow_id
    assert package_harness.backend.package_starts[-1][1].release_binding == pending.package_binding


def test_conflicting_queue_owners_and_legacy_queues_are_rejected(
    package_harness: PackageHarness,
) -> None:
    original = publish_fixture(package_harness)
    assert configure_pool(package_harness, original).status_code == 200
    other = PackageManifest.model_validate(
        {
            **original.model_dump(),
            "package_id": "other-validation",
            "worker_deployment_name": "other-validation",
            "build_id": "other-validation-1.0.0",
        }
    )
    publish_fixture(package_harness, other, scope=OTHER_SCOPE, auth=OTHER_AUTH)
    conflict = configure_pool(
        package_harness,
        other,
        scope=OTHER_SCOPE,
        auth=OTHER_AUTH,
        queue=f"{original.package_id}-tq",
    )
    assert conflict.status_code == 409 and conflict.json()["code"] == "package_queue_conflict"
    legacy = configure_pool(
        package_harness, other, scope=OTHER_SCOPE, auth=OTHER_AUTH, queue="validation-tq"
    )
    assert legacy.status_code == 409 and legacy.json()["code"] == "legacy_queue_reserved"


def test_cross_scope_deployment_identity_cannot_be_reused(package_harness: PackageHarness) -> None:
    original = publish_fixture(package_harness)
    other = PackageManifest.model_validate(
        {**original.model_dump(), "package_id": "other-validation", "build_id": "foreign-build"}
    )
    package_harness.client.post(
        "/api/v1/packages",
        json={
            **OTHER_SCOPE.as_dict(),
            "package_id": other.package_id,
            "name": other.package_id,
            "owner": "other",
            "workflow_types": ["validation_reference"],
        },
        headers=OTHER_AUTH,
    )
    response = package_harness.client.post(
        f"/api/v1/packages/{other.package_id}/releases",
        json={
            **OTHER_SCOPE.as_dict(),
            "release": make_release(other).model_dump(mode="json"),
            "manifest": other.model_dump(mode="json"),
        },
        headers=OTHER_AUTH,
    )
    assert response.status_code == 409


def test_missing_role_evidence_does_not_mark_pool_ready(package_harness: PackageHarness) -> None:
    manifest = publish_fixture(package_harness)
    assert configure_pool(package_harness, manifest).status_code == 200
    packages = PackageRepository(package_harness.repository)
    packages.observe_pools(
        SCOPE,
        make_release(manifest).package_release_id,
        {
            "ready": True,
            "queues": [{"task_queue": f"{manifest.package_id}-tq", "task_type": 1, "replicas": 10}],
        },
        utcnow(),
    )
    pool = packages.pools(SCOPE, make_release(manifest).package_release_id)[0]
    assert pool.observed_state == "pending" and pool.observed_replicas == 0


def test_workflow_only_pool_does_not_satisfy_complete_role_coverage(
    package_harness: PackageHarness,
) -> None:
    manifest = publish_fixture(package_harness)
    assert configure_pool(package_harness, manifest, role="workflow").status_code == 200
    response = promote(package_harness, manifest)
    assert response.status_code == 409 and response.json()["code"] == "executor_roles_incomplete"
    assert configure_pool(package_harness, manifest, role="activity").status_code == 200
    assert promote(package_harness, manifest).status_code == 200


def test_failed_temporal_routing_does_not_admit_new_starts(package_harness: PackageHarness) -> None:
    manifest = publish_fixture(package_harness)
    govern_definition(package_harness, manifest)
    assert configure_pool(package_harness, manifest).status_code == 200
    package_harness.backend.route_failure = True
    assert promote(package_harness, manifest).status_code == 503
    response = package_harness.client.post(
        "/api/v1/workflows/validation_reference/start", json=start_request(), headers=AUTH
    )
    assert response.status_code == 409
    with package_harness.repository.sessions() as session:
        slot = session.scalar(select(PackageEnvironmentRow))
        assert slot is not None and slot.routing_confirmed == 0


def test_business_reader_cannot_publish_or_configure_packages(
    package_harness: PackageHarness,
) -> None:
    response = package_harness.client.post(
        "/api/v1/packages",
        json={
            **SCOPE.as_dict(),
            "package_id": "blocked",
            "name": "blocked",
            "owner": "reader",
            "workflow_types": ["validation_reference"],
        },
        headers={"Authorization": "Bearer reader"},
    )
    assert response.status_code == 403


def test_queue_claims_persist_after_pool_reconfiguration(package_harness: PackageHarness) -> None:
    manifest = publish_fixture(package_harness)
    assert configure_pool(package_harness, manifest).status_code == 200
    assert (
        configure_pool(package_harness, manifest, queue="new-validation-reference-tq").status_code
        == 200
    )
    with package_harness.repository.sessions() as session:
        claims = session.scalars(select(PackageQueueOwnerRow)).all()
        assert {claim.task_queue for claim in claims} == {
            "validation-reference-tq",
            "new-validation-reference-tq",
        }


def test_observed_pool_does_not_claim_another_pools_pollers(
    package_harness: PackageHarness,
) -> None:
    manifest = publish_fixture(package_harness)
    assert configure_pool(package_harness, manifest).status_code == 200
    packages = PackageRepository(package_harness.repository)
    packages.observe_pools(
        SCOPE,
        make_release(manifest).package_release_id,
        {
            "ready": True,
            "queues": [
                {
                    "task_queue": f"{manifest.package_id}-tq",
                    "task_type": task_type,
                    "replicas": 2,
                    "identities": [
                        f"{manifest.package_id}:{manifest.build_id}:another-pool:replica-0"
                    ],
                }
                for task_type in (1, 2)
            ],
        },
        utcnow(),
    )
    observed = packages.pools(SCOPE, make_release(manifest).package_release_id)[0]
    assert observed.observed_replicas == 0 and observed.observed_state == "pending"


@pytest.mark.parametrize("terminal", [False, True])
def test_retirement_preserves_pending_work_but_allows_uppercase_terminal_state(
    package_harness: PackageHarness, terminal: bool
) -> None:
    original = publish_fixture(package_harness)
    govern_definition(package_harness, original)
    assert configure_pool(package_harness, original).status_code == 200
    assert promote(package_harness, original).status_code == 200
    started = package_harness.client.post(
        "/api/v1/workflows/validation_reference/start", json=start_request(), headers=AUTH
    )
    assert started.status_code == 202, started.text
    workflow_id = started.json()["workflow_id"]
    if terminal:
        package_harness.backend.statuses[workflow_id] = BusinessSnapshot(ExecutionState.COMPLETED)
        observed = package_harness.client.get(
            f"/api/v1/workflows/{workflow_id}/status", headers=AUTH
        )
        assert observed.status_code == 200 and observed.json()["state"] == "COMPLETED"
    newer = PackageManifest.model_validate(
        {
            **original.model_dump(),
            "package_version": "2.0.0",
            "build_id": "validation-reference-2.0.0",
        }
    )
    publish_fixture(package_harness, newer)
    assert configure_pool(package_harness, newer).status_code == 200
    assert promote(package_harness, newer).status_code == 200
    retired = package_harness.client.post(
        f"/api/v1/package-releases/{make_release(original).package_release_id}/retire",
        json=SCOPE.as_dict(),
        headers=AUTH,
    )
    assert retired.status_code == (200 if terminal else 409), retired.text
    if not terminal:
        assert retired.json()["code"] == "release_still_required"


def test_compatible_revision_cannot_change_retained_handler_logical_routes(
    package_harness: PackageHarness,
) -> None:
    raw = make_manifest().model_dump(mode="json")
    revision = raw["definitions"][0]
    revision["definition_document"]["steps"]["validate"]["next"] = "notify"
    revision["definition_document"]["steps"]["notify"] = {
        "type": "activity",
        "capability": "send_notification",
        "next": "done",
    }
    revision["content_hash"] = definition_hash(
        DefinitionDocument.model_validate(revision["definition_document"])
    )
    raw["queues"].append({"name": "notifications", "role": "activity"})
    raw["activity_registrations"].append(
        {
            "capability": "send_notification",
            "activity_name": "send_notification.pkg.v1",
            "entrypoint": "nexusflow_activities:send_notification",
            "logical_queue": "notifications",
        }
    )
    original = publish_fixture(package_harness, PackageManifest.model_validate(raw))
    assert configure_pool(package_harness, original).status_code == 200
    assert promote(package_harness, original).status_code == 200
    raw.update(package_version="2.0.0", build_id="validation-reference-2.0.0")
    raw["activity_registrations"][0]["logical_queue"] = "notifications"
    raw["activity_registrations"][1]["logical_queue"] = "activities"
    newer = publish_fixture(package_harness, PackageManifest.model_validate(raw))
    assert configure_pool(package_harness, newer).status_code == 200
    rejected = promote(package_harness, newer)
    assert rejected.status_code == 409 and rejected.json()["code"] == "release_handler_incompatible"
