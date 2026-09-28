"""Scoped package governance. Desired routing never substitutes for Temporal evidence."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4

from contracts import (
    ExecutionState,
    ExecutorPool,
    PackageExecutionBinding,
    PackageManifest,
    WorkflowDefinition,
    WorkflowPackage,
    WorkflowRelease,
)
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from workflow_sdk.packages import (
    PackageValidationError,
    resolve_binding,
    validate_manifest,
    validate_release,
)

from workflows.common.catalog import CAPABILITY_ROUTES, LEGACY_TASK_QUEUE, RUNTIME_TASK_QUEUE

from .errors import ApiError
from .models import (
    AuditRow,
    ExecutionRow,
    ExecutorPoolRow,
    PackageEnvironmentRow,
    PackageQueueOwnerRow,
    PackageReleaseRow,
    PackageRow,
)
from .repository import Scope, WorkflowRepository, _scope


class PackageRepository:
    def __init__(self, repository: WorkflowRepository) -> None:
        self.repository = repository

    @staticmethod
    def _package_owner(session: Session, scope: Scope, package_id: str) -> PackageRow:
        owner = session.scalar(
            select(PackageRow)
            .where(*_scope(PackageRow, scope), PackageRow.package_id == package_id)
            .with_for_update()
        )
        if owner is None:
            raise ApiError(404, "package_not_found", "Register the business package first")
        return owner

    @staticmethod
    def _claim_queues(
        session: Session, owner: PackageRow, namespace: str, queues: dict[str, str]
    ) -> None:
        reserved = {
            LEGACY_TASK_QUEUE,
            RUNTIME_TASK_QUEUE,
            *(route.task_queue for route in CAPABILITY_ROUTES.values()),
        }
        if reserved & set(queues.values()):
            raise ApiError(
                409,
                "legacy_queue_reserved",
                "Existing legacy and governed queues cannot be rebound to package executors",
            )
        for task_queue in sorted(set(queues.values())):
            statement = (
                select(PackageQueueOwnerRow)
                .where(
                    PackageQueueOwnerRow.temporal_namespace == namespace,
                    PackageQueueOwnerRow.task_queue == task_queue,
                )
                .with_for_update()
            )
            claim = session.scalar(statement)
            if claim is None:
                try:
                    claim = PackageQueueOwnerRow(
                        row_id=str(uuid4()),
                        temporal_namespace=namespace,
                        task_queue=task_queue,
                        package_owner_id=owner.row_id,
                    )
                    session.add(claim)
                    session.flush()
                except IntegrityError as exc:
                    raise ApiError(
                        409,
                        "package_queue_conflict",
                        "Concurrent queue ownership changed; reconcile the package configuration",
                    ) from exc
            if claim is None or claim.package_owner_id != owner.row_id:
                raise ApiError(
                    409,
                    "package_queue_conflict",
                    "Task queue belongs to another package in this namespace",
                )

    @staticmethod
    def _event(
        session: Session,
        scope: Scope,
        event_type: str,
        actor: str,
        now: datetime,
        metadata: dict[str, Any],
    ) -> None:
        session.add(
            AuditRow(
                **scope.as_dict(),
                event_id=str(uuid4()),
                event_type=event_type,
                actor=actor,
                correlation_id="package-governance",
                timestamp=now,
                event_metadata=metadata,
                retention_class="standard",
            )
        )

    @staticmethod
    def _release(session: Session, scope: Scope, release_id: str) -> PackageReleaseRow:
        row = session.scalar(
            select(PackageReleaseRow)
            .where(
                *_scope(PackageReleaseRow, scope),
                PackageReleaseRow.package_release_id == release_id,
            )
            .with_for_update()
        )
        if row is None:
            raise ApiError(404, "release_not_found", "Package release was not found")
        return row

    def register(self, package: WorkflowPackage, actor: str, now: datetime) -> WorkflowPackage:
        scope = Scope(package.tenant, package.business_domain, package.application)
        document = package.model_copy(
            update={"status": "active", "created_by": actor, "created_at": now}
        )
        try:
            with self.repository.sessions.begin() as session:
                session.add(
                    PackageRow(
                        row_id=str(uuid4()),
                        **scope.as_dict(),
                        package_id=package.package_id,
                        document=document.model_dump(mode="json"),
                    )
                )
                self._event(
                    session,
                    scope,
                    "package.registered",
                    actor,
                    now,
                    {"package_id": package.package_id},
                )
        except IntegrityError as exc:
            raise ApiError(409, "package_exists", "Package identity already exists") from exc
        return document

    def packages(self, scope: Scope) -> list[WorkflowPackage]:
        with self.repository.sessions() as session:
            return [
                WorkflowPackage.model_validate(row.document)
                for row in session.scalars(
                    select(PackageRow)
                    .where(*_scope(PackageRow, scope))
                    .order_by(PackageRow.package_id)
                ).all()
            ]

    def publish(
        self,
        scope: Scope,
        release: WorkflowRelease,
        manifest: PackageManifest,
        actor: str,
        now: datetime,
    ) -> dict[str, Any]:
        try:
            validate_manifest(manifest)
            validate_release(manifest, release)
        except (ValueError, PackageValidationError) as exc:
            raise ApiError(
                422,
                "package_invalid",
                "Package content, descriptor or dependency closure is invalid",
            ) from exc
        descriptor = release.model_copy(update={"published_by": actor, "published_at": now})
        try:
            with self.repository.sessions.begin() as session:
                package = session.scalar(
                    select(PackageRow)
                    .where(*_scope(PackageRow, scope), PackageRow.package_id == release.package_id)
                    .with_for_update()
                )
                if package is None:
                    raise ApiError(404, "package_not_found", "Register the business package first")
                declared = WorkflowPackage.model_validate(package.document)
                if declared.status != "active" or any(
                    d.workflow_type not in declared.workflow_types for d in manifest.definitions
                ):
                    raise ApiError(
                        409,
                        "package_identity_mismatch",
                        "Release does not belong to the active package",
                    )
                if (
                    package.deployment_name is not None
                    and package.deployment_name != manifest.worker_deployment_name
                ):
                    raise ApiError(
                        409,
                        "deployment_identity_changed",
                        "Worker Deployment name is stable for the package",
                    )
                package.deployment_name = manifest.worker_deployment_name
                # Prevent different package identities from competing for the same
                # scoped business workflow or Temporal deployment.
                for other in session.scalars(
                    select(PackageRow).where(
                        *_scope(PackageRow, scope), PackageRow.package_id != release.package_id
                    )
                ):
                    if set(WorkflowPackage.model_validate(other.document).workflow_types) & set(
                        declared.workflow_types
                    ):
                        raise ApiError(
                            409,
                            "package_workflow_conflict",
                            "Workflow types already belong to another package",
                        )
                existing = session.scalar(
                    select(PackageReleaseRow).where(
                        PackageReleaseRow.deployment_name == manifest.worker_deployment_name
                    )
                )
                if existing and (
                    (existing.tenant, existing.business_domain, existing.application)
                    != (scope.tenant, scope.business_domain, scope.application)
                    or existing.package_id != manifest.package_id
                ):
                    raise ApiError(
                        409,
                        "package_deployment_conflict",
                        "Worker Deployment belongs to another package",
                    )
                for revision in manifest.definitions:
                    definition = self.repository._definition_row(
                        session, scope, revision.workflow_type, revision.version
                    )
                    if (
                        definition.definition_id != revision.definition_id
                        or definition.content_hash != revision.content_hash
                    ):
                        raise ApiError(
                            409,
                            "package_definition_mismatch",
                            "Bundled revision must match the exact governed registry content",
                        )
                row = PackageReleaseRow(
                    row_id=str(uuid4()),
                    **scope.as_dict(),
                    package_release_id=release.package_release_id,
                    package_id=release.package_id,
                    package_version=release.package_version,
                    deployment_name=manifest.worker_deployment_name,
                    build_id=manifest.build_id,
                    descriptor=descriptor.model_dump(mode="json"),
                    manifest=manifest.model_dump(mode="json"),
                    status="published",
                )
                session.add(row)
                self._event(
                    session,
                    scope,
                    "package.release.published",
                    actor,
                    now,
                    {
                        "release_id": release.package_release_id,
                        "manifest_hash": release.manifest_hash,
                        "artifact_digest": release.artifact_digest,
                    },
                )
        except IntegrityError as exc:
            raise ApiError(
                409, "release_exists", "Release version, identity and Build ID are immutable"
            ) from exc
        return {"release": descriptor.model_dump(mode="json"), "status": "published"}

    def release(
        self, scope: Scope, release_id: str
    ) -> tuple[WorkflowRelease, PackageManifest, str]:
        with self.repository.sessions() as session:
            row = self._release(session, scope, release_id)
            return (
                WorkflowRelease.model_validate(row.descriptor),
                PackageManifest.model_validate(row.manifest),
                row.status,
            )

    def approve(self, scope: Scope, release_id: str, actor: str, now: datetime) -> None:
        with self.repository.sessions.begin() as session:
            row = self._release(session, scope, release_id)
            if row.status not in {"published", "approved"}:
                raise ApiError(409, "release_not_approvable", "Release cannot be approved")
            for revision in PackageManifest.model_validate(row.manifest).definitions:
                definition = self.repository._definition_row(
                    session, scope, revision.workflow_type, revision.version
                )
                if definition.status not in {"approved", "promoted"}:
                    raise ApiError(
                        409,
                        "definition_not_approved",
                        "Approve every included definition before the release",
                    )
            row.status, row.approved_by, row.approved_at = "approved", actor, now
            self._event(
                session, scope, "package.release.approved", actor, now, {"release_id": release_id}
            )

    def set_routing(
        self,
        scope: Scope,
        release_id: str,
        environment: str,
        namespace: str,
        queues: dict[str, str],
        actor: str,
        now: datetime,
        ramp_percentage: int | None = None,
    ) -> None:
        with self.repository.sessions.begin() as session:
            release = self._release(session, scope, release_id)
            if release.status != "approved":
                raise ApiError(
                    409, "release_not_approved", "Only approved releases can receive routing"
                )
            manifest = PackageManifest.model_validate(release.manifest)
            if set(queues) != {q.name for q in manifest.queues}:
                raise ApiError(422, "queue_binding_invalid", "Bind every declared logical queue")
            # Validate queue separation/version/closure using the same binding resolver.
            resolve_binding(
                manifest,
                WorkflowRelease.model_validate(release.descriptor),
                manifest.definitions[0],
                namespace=namespace,
                queue_bindings=queues,
            )
            owner = self._package_owner(session, scope, release.package_id)
            self._claim_queues(session, owner, namespace, queues)
            for other in session.scalars(
                select(PackageEnvironmentRow).where(
                    PackageEnvironmentRow.environment == environment,
                    PackageEnvironmentRow.temporal_namespace == namespace,
                )
            ):
                same_owner = (
                    other.tenant,
                    other.business_domain,
                    other.application,
                    other.package_id,
                ) == (scope.tenant, scope.business_domain, scope.application, release.package_id)
                if not same_owner and set(other.queue_bindings.values()) & set(queues.values()):
                    raise ApiError(
                        409,
                        "package_queue_conflict",
                        "Task queues belong to another package in this environment namespace",
                    )
            slot = session.scalar(
                select(PackageEnvironmentRow)
                .where(
                    *_scope(PackageEnvironmentRow, scope),
                    PackageEnvironmentRow.package_id == release.package_id,
                    PackageEnvironmentRow.environment == environment,
                )
                .with_for_update()
            )
            if slot is None:
                if ramp_percentage is not None:
                    raise ApiError(
                        409, "current_release_required", "Promote a Current release before ramping"
                    )
                slot = PackageEnvironmentRow(
                    row_id=str(uuid4()),
                    **scope.as_dict(),
                    package_id=release.package_id,
                    environment=environment,
                    current_release_id=release_id,
                    temporal_namespace=namespace,
                    queue_bindings=queues,
                    ramp_percentage=0,
                    routing_confirmed=0,
                )
                session.add(slot)
            else:
                if slot.temporal_namespace != namespace or slot.queue_bindings != queues:
                    raise ApiError(
                        409,
                        "stable_queue_required",
                        "Namespace and logical queues are stable across releases",
                    )
                previous = self._release(session, scope, slot.current_release_id)
                previous_manifest = PackageManifest.model_validate(previous.manifest)
                if previous_manifest.worker_deployment_name != manifest.worker_deployment_name:
                    raise ApiError(
                        409,
                        "deployment_identity_changed",
                        "Worker Deployment name is stable across releases",
                    )
                # Require exact old revision closure for upgrades and pending
                # starts; code promotion never silently changes definition content.
                revisions = {
                    (d.definition_id, d.version, d.content_hash, d.runtime_workflow_type)
                    for d in manifest.definitions
                }
                prior = {
                    (d.definition_id, d.version, d.content_hash, d.runtime_workflow_type)
                    for d in previous_manifest.definitions
                }
                if not prior.issubset(revisions):
                    raise ApiError(
                        409,
                        "release_revision_incompatible",
                        "Upgrade must retain Current definition revisions and handlers",
                    )
                prior_workflows = {
                    (
                        item.name,
                        item.entrypoint,
                        item.logical_queue,
                        item.versioning_behavior,
                        item.continue_as_new_policy,
                    )
                    for item in previous_manifest.workflow_registrations
                }
                workflows = {
                    (
                        item.name,
                        item.entrypoint,
                        item.logical_queue,
                        item.versioning_behavior,
                        item.continue_as_new_policy,
                    )
                    for item in manifest.workflow_registrations
                }
                prior_activities = {
                    (
                        item.capability,
                        item.activity_name,
                        item.contract_version,
                        item.entrypoint,
                        item.logical_queue,
                    )
                    for item in previous_manifest.activity_registrations
                }
                activities = {
                    (
                        item.capability,
                        item.activity_name,
                        item.contract_version,
                        item.entrypoint,
                        item.logical_queue,
                    )
                    for item in manifest.activity_registrations
                }
                if not prior_workflows <= workflows or not prior_activities <= activities:
                    raise ApiError(
                        409,
                        "release_handler_incompatible",
                        "Upgrade must retain registered handler names, contracts, logical routing and Workflow upgrade policy",
                    )
                if ramp_percentage is None:
                    slot.current_release_id, slot.ramping_release_id, slot.ramp_percentage = (
                        release_id,
                        None,
                        0,
                    )
                else:
                    slot.ramping_release_id, slot.ramp_percentage = release_id, ramp_percentage
                slot.routing_confirmed = 0
            self._event(
                session,
                scope,
                "package.routing.requested",
                actor,
                now,
                {
                    "release_id": release_id,
                    "environment": environment,
                    "ramp_percentage": ramp_percentage,
                },
            )

    def confirm_routing(
        self, scope: Scope, package_id: str, environment: str, actual: tuple[str, str | None, float]
    ) -> bool:
        with self.repository.sessions.begin() as session:
            slot = session.scalar(
                select(PackageEnvironmentRow)
                .where(
                    *_scope(PackageEnvironmentRow, scope),
                    PackageEnvironmentRow.package_id == package_id,
                    PackageEnvironmentRow.environment == environment,
                )
                .with_for_update()
            )
            if slot is None:
                return False
            current = self._release(session, scope, slot.current_release_id)
            ramp = (
                self._release(session, scope, slot.ramping_release_id)
                if slot.ramping_release_id
                else None
            )
            matches = actual == (
                current.build_id,
                ramp.build_id if ramp else None,
                float(slot.ramp_percentage),
            )
            slot.routing_confirmed = int(matches)
            return matches

    def select_binding(
        self, scope: Scope, definition: WorkflowDefinition, environment: str
    ) -> PackageExecutionBinding:
        with self.repository.sessions() as session:
            rows = session.scalars(
                select(PackageEnvironmentRow).where(
                    *_scope(PackageEnvironmentRow, scope),
                    PackageEnvironmentRow.environment == environment,
                )
            ).all()
            candidates: list[PackageExecutionBinding] = []
            for slot in rows:
                if not slot.routing_confirmed:
                    continue
                release_row = self._release(session, scope, slot.current_release_id)
                if release_row.status != "approved":
                    continue
                manifest = PackageManifest.model_validate(release_row.manifest)
                revision = next(
                    (
                        d
                        for d in manifest.definitions
                        if d.definition_id == definition.definition_id
                        and d.workflow_type == definition.workflow_type
                        and d.version == definition.version
                        and d.content_hash == definition.content_hash
                    ),
                    None,
                )
                if revision is None:
                    continue
                binding = resolve_binding(
                    manifest,
                    WorkflowRelease.model_validate(release_row.descriptor),
                    revision,
                    namespace=slot.temporal_namespace,
                    queue_bindings=slot.queue_bindings,
                )
                # AutoUpgrade initial assignment is current/ramp, never an
                # invented atomic Build ID binding. Freeze its permitted set.
                if (
                    binding.versioning_behavior == "auto_upgrade"
                    and slot.ramping_release_id
                    and slot.ramp_percentage
                ):
                    ramp = self._release(session, scope, slot.ramping_release_id)
                    binding = binding.model_copy(
                        update={"eligible_build_ids": (binding.build_id, ramp.build_id)}
                    )
                candidates.append(binding)
            if len(candidates) != 1:
                raise ApiError(
                    409,
                    "package_release_unavailable",
                    "An approved, reconciled release containing this exact revision is required",
                )
            return candidates[0]

    def put_pool(self, scope: Scope, pool: ExecutorPool, actor: str, now: datetime) -> ExecutorPool:
        # Observations are server-owned; user supplied readiness is never trusted.
        pool = pool.model_copy(
            update={"observed_state": "pending", "observed_replicas": 0, "last_reconciled_at": None}
        )
        with self.repository.sessions.begin() as session:
            release = self._release(session, scope, pool.package_release_id)
            manifest = PackageManifest.model_validate(release.manifest)
            if (pool.package_id, pool.build_id, pool.worker_deployment_name) != (
                manifest.package_id,
                manifest.build_id,
                manifest.worker_deployment_name,
            ):
                raise ApiError(
                    409, "pool_release_mismatch", "Executor pool must match its immutable release"
                )
            required = {q.name for q in manifest.queues}
            if set(pool.queue_bindings) != required:
                raise ApiError(
                    422,
                    "pool_queues_invalid",
                    "Every pool must bind the complete package routing, including destinations executed by other roles",
                )
            owner = self._package_owner(session, scope, pool.package_id)
            self._claim_queues(session, owner, pool.temporal_namespace, pool.queue_bindings)
            row = session.scalar(
                select(ExecutorPoolRow)
                .where(*_scope(ExecutorPoolRow, scope), ExecutorPoolRow.pool_id == pool.pool_id)
                .with_for_update()
            )
            if row is None:
                row = ExecutorPoolRow(
                    row_id=str(uuid4()),
                    **scope.as_dict(),
                    pool_id=pool.pool_id,
                    package_release_id=pool.package_release_id,
                    document=pool.model_dump(mode="json"),
                )
                session.add(row)
            else:
                row.package_release_id, row.document = (
                    pool.package_release_id,
                    pool.model_dump(mode="json"),
                )
            self._event(
                session,
                scope,
                "package.pool.configured",
                actor,
                now,
                {"pool_id": pool.pool_id, "replicas": pool.replicas},
            )
        return pool

    def pools(self, scope: Scope, release_id: str) -> list[ExecutorPool]:
        with self.repository.sessions() as session:
            self._release(session, scope, release_id)
            return [
                ExecutorPool.model_validate(row.document)
                for row in session.scalars(
                    select(ExecutorPoolRow).where(
                        *_scope(ExecutorPoolRow, scope),
                        ExecutorPoolRow.package_release_id == release_id,
                    )
                )
            ]

    def observe_pools(
        self, scope: Scope, release_id: str, evidence: dict[str, Any], now: datetime
    ) -> None:
        with self.repository.sessions.begin() as session:
            for row in session.scalars(
                select(ExecutorPoolRow)
                .where(
                    *_scope(ExecutorPoolRow, scope),
                    ExecutorPoolRow.package_release_id == release_id,
                )
                .with_for_update()
            ):
                pool = ExecutorPool.model_validate(row.document)
                release = self._release(session, scope, release_id)
                manifest = PackageManifest.model_validate(release.manifest)
                required = {
                    (pool.queue_bindings[queue.name], 1 if queue.role == "workflow" else 2)
                    for queue in manifest.queues
                    if pool.role == "mixed" or queue.role == pool.role
                }
                actual = {
                    (q["task_queue"], q["task_type"]): len(
                        {
                            identity
                            for identity in q.get("identities", ())
                            if identity.startswith(
                                f"{pool.package_id}:{pool.build_id}:{pool.pool_id}:"
                            )
                        }
                    )
                    for q in evidence["queues"]
                }
                replicas = min((actual.get(key, 0) for key in required), default=0)
                floor = 2 if pool.environment == "prod" else 1
                observed = pool.model_dump(mode="python")
                observed.update(
                    observed_replicas=replicas,
                    observed_state="ready"
                    if replicas >= floor and evidence["ready"]
                    else "pending",
                    last_reconciled_at=now,
                )
                row.document = ExecutorPool.model_validate(observed).model_dump(mode="json")

    def retire(self, scope: Scope, release_id: str, actor: str, now: datetime) -> None:
        with self.repository.sessions.begin() as session:
            row = self._release(session, scope, release_id)
            slots = session.scalars(
                select(PackageEnvironmentRow).where(*_scope(PackageEnvironmentRow, scope))
            ).all()
            if any(release_id in {s.current_release_id, s.ramping_release_id} for s in slots):
                raise ApiError(
                    409,
                    "release_still_routed",
                    "Remove Current and Ramping routing before retirement",
                )
            for execution in session.scalars(
                select(ExecutionRow).where(
                    *_scope(ExecutionRow, scope), ExecutionRow.package_binding.is_not(None)
                )
            ):
                binding = PackageExecutionBinding.model_validate(execution.package_binding)
                terminal = {
                    ExecutionState.COMPLETED.value,
                    ExecutionState.REJECTED.value,
                    ExecutionState.TIMED_OUT.value,
                    ExecutionState.CANCELLED.value,
                    ExecutionState.FAILED.value,
                }
                if row.build_id in binding.eligible_build_ids and (
                    execution.run_id is None or execution.state not in terminal
                ):
                    raise ApiError(
                        409,
                        "release_still_required",
                        "Release serves a pending or nonterminal reserved execution",
                    )
            row.status = "retired"
            self._event(
                session, scope, "package.release.retired", actor, now, {"release_id": release_id}
            )

    def observe_execution(
        self,
        scope: Scope,
        workflow_id: str,
        initial: str | None,
        current: str | None,
        now: datetime,
    ) -> None:
        with self.repository.sessions.begin() as session:
            row = session.scalar(
                select(ExecutionRow)
                .where(*_scope(ExecutionRow, scope), ExecutionRow.workflow_id == workflow_id)
                .with_for_update()
            )
            if row is None:
                raise ApiError(404, "workflow_not_found", "Workflow execution was not found")
            if initial and row.observed_initial_build_id is None:
                row.observed_initial_build_id = initial
            if current and row.observed_current_build_id != current:
                row.observed_current_build_id = current
                self._event(
                    session,
                    scope,
                    "package.execution.routing_observed",
                    "temporal",
                    now,
                    {
                        "workflow_id": workflow_id,
                        "initial_build_id": row.observed_initial_build_id,
                        "current_build_id": current,
                    },
                )

    def validate_observed_builds(
        self,
        scope: Scope,
        binding: PackageExecutionBinding,
        initial: str | None,
        current: str | None,
        previously_observed_initial: str | None,
    ) -> None:
        if initial and initial not in binding.eligible_build_ids:
            raise ApiError(
                409,
                "package_initial_routing_violation",
                "Temporal initial routing exceeded the reserved admission policy",
            )
        if previously_observed_initial and initial and previously_observed_initial != initial:
            raise ApiError(
                409,
                "package_initial_routing_violation",
                "Initial observed provenance cannot change",
            )
        if binding.versioning_behavior == "pinned":
            if any(build and build != binding.build_id for build in (initial, current)):
                raise ApiError(
                    409,
                    "package_routing_violation",
                    "Pinned execution changed its intended Build ID",
                )
            return
        # Admission provenance remains immutable. Later observed code releases
        # must be governed compatible releases; they never replace its revision.
        with self.repository.sessions() as session:
            for build in {initial, current} - {None}:
                if build in binding.eligible_build_ids:
                    continue
                row = session.scalar(
                    select(PackageReleaseRow).where(
                        *_scope(PackageReleaseRow, scope),
                        PackageReleaseRow.package_id == binding.package_id,
                        PackageReleaseRow.deployment_name == binding.worker_deployment_name,
                        PackageReleaseRow.build_id == build,
                    )
                )
                if row is None or row.status != "approved":
                    raise ApiError(
                        409,
                        "package_routing_violation",
                        "Observed upgrade is not an approved compatible package release",
                    )
                manifest = PackageManifest.model_validate(row.manifest)
                revision = next(
                    (
                        d
                        for d in manifest.definitions
                        if d.definition_id == binding.definition_id
                        and d.version == binding.definition_version
                        and d.content_hash == binding.definition_content_hash
                    ),
                    None,
                )
                activities = {
                    (a.capability, a.activity_name, a.contract_version, a.logical_queue)
                    for a in manifest.activity_registrations
                }
                if revision is None or any(
                    (a.capability, a.activity_name, a.contract_version, a.logical_queue)
                    not in activities
                    for a in binding.activity_bindings
                ):
                    raise ApiError(
                        409,
                        "package_routing_violation",
                        "Observed upgrade does not retain the selected revision and handlers",
                    )
