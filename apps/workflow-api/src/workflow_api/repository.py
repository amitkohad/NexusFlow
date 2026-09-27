"""Scoped, transactional registry, execution metadata, and business audit storage.

Every operation opens its own session. Callers may safely dispatch these synchronous
methods through ``asyncio.to_thread`` without sharing sessions across requests.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, TypedDict
from uuid import uuid4

from contracts import (
    AuditEvent,
    DefinitionDependency,
    DefinitionDocument,
    DefinitionStatus,
    ExecutionState,
    JsonObject,
    RuntimeProfile,
    WorkflowDefinition,
)
from contracts.api import RegisterDefinitionRequest
from sqlalchemy import Engine, create_engine, event, inspect, select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool
from sqlalchemy.sql.elements import ColumnElement
from workflow_sdk.definitions import DefinitionValidationError, validate_definition

from .errors import ApiError
from .models import AuditRow, Base, DefinitionRow, ExecutionRow, PromotionRow, ScopedModel

TERMINAL_STATES = {
    ExecutionState.COMPLETED,
    ExecutionState.REJECTED,
    ExecutionState.TIMED_OUT,
    ExecutionState.CANCELLED,
    ExecutionState.FAILED,
}


class ScopeFields(TypedDict):
    tenant: str
    business_domain: str
    application: str


@dataclass(frozen=True)
class Scope:
    tenant: str
    business_domain: str
    application: str

    def as_dict(self) -> ScopeFields:
        return {
            "tenant": self.tenant,
            "business_domain": self.business_domain,
            "application": self.application,
        }


@dataclass(frozen=True)
class ExecutionRecord:
    workflow_id: str
    scope: Scope
    workflow_type: str
    definition_id: str
    definition_version: str
    definition_document: DefinitionDocument
    request: JsonObject
    variables: JsonObject
    business_reference: str
    correlation_id: str
    idempotency_key: str
    request_fingerprint: str
    created_by: str
    started_at: datetime
    updated_at: datetime
    state: ExecutionState = ExecutionState.CREATED
    runtime_profile: RuntimeProfile = "legacy"
    runtime_task_queue: str = "lightweight-workflows"
    run_id: str | None = None
    current_step: str | None = None
    completed_at: datetime | None = None
    failure_code: str | None = None
    failure_summary: str | None = None


def _utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value


def _required_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value


def _scope(model: type[ScopedModel], scope: Scope) -> tuple[ColumnElement[bool], ...]:
    return (
        model.tenant == scope.tenant,
        model.business_domain == scope.business_domain,
        model.application == scope.application,
    )


def _definition(row: DefinitionRow) -> WorkflowDefinition:
    return WorkflowDefinition(
        definition_id=row.definition_id,
        workflow_type=row.workflow_type,
        version=row.version,
        tenant=row.tenant,
        business_domain=row.business_domain,
        application=row.application,
        status=DefinitionStatus(row.status),
        content_hash=row.content_hash,
        definition_document=DefinitionDocument.model_validate(row.definition_document),
        owner=row.owner,
        dependencies=tuple(DefinitionDependency.model_validate(item) for item in row.dependencies),
        created_at=_required_utc(row.created_at),
        created_by=row.created_by,
        approved_at=_utc(row.approved_at),
        approved_by=row.approved_by,
        promoted_at=_utc(row.promoted_at),
        promoted_by=row.promoted_by,
    )


def _execution(row: ExecutionRow) -> ExecutionRecord:
    if row.runtime_profile not in {"legacy", "governed"}:
        raise ApiError(503, "runtime_binding_invalid", "Execution runtime binding is unavailable")
    return ExecutionRecord(
        workflow_id=row.workflow_id,
        scope=Scope(row.tenant, row.business_domain, row.application),
        workflow_type=row.workflow_type,
        definition_id=row.definition_id,
        definition_version=row.definition_version,
        definition_document=DefinitionDocument.model_validate(row.definition_document),
        request=row.request,
        variables=row.variables,
        business_reference=row.business_reference,
        correlation_id=row.correlation_id,
        idempotency_key=row.idempotency_key,
        request_fingerprint=row.request_fingerprint,
        created_by=row.created_by,
        state=ExecutionState(row.state),
        runtime_profile="governed" if row.runtime_profile == "governed" else "legacy",
        runtime_task_queue=row.runtime_task_queue,
        run_id=row.run_id,
        current_step=row.current_step,
        started_at=_required_utc(row.started_at),
        updated_at=_required_utc(row.updated_at),
        completed_at=_utc(row.completed_at),
        failure_code=row.failure_code,
        failure_summary=row.failure_summary,
    )


def _audit(row: AuditRow) -> AuditEvent:
    return AuditEvent(
        event_id=row.event_id,
        event_type=row.event_type,
        tenant=row.tenant,
        business_domain=row.business_domain,
        application=row.application,
        workflow_id=row.workflow_id,
        run_id=row.run_id,
        workflow_type=row.workflow_type,
        definition_version=row.definition_version,
        business_reference=row.business_reference,
        step=row.step,
        previous_state=row.previous_state,
        new_state=row.new_state,
        actor=row.actor,
        correlation_id=row.correlation_id,
        timestamp=_required_utc(row.timestamp),
        metadata=row.event_metadata,
        retention_class=row.retention_class,
    )


class WorkflowRepository:
    def __init__(self, engine_or_url: Engine | str) -> None:
        if isinstance(engine_or_url, str):
            options: dict[str, Any] = {"pool_pre_ping": True}
            if engine_or_url.startswith("sqlite"):
                options["connect_args"] = {"check_same_thread": False, "timeout": 30}
                if engine_or_url in {"sqlite://", "sqlite:///:memory:"}:
                    options["poolclass"] = StaticPool
            self.engine = create_engine(engine_or_url, **options)
        else:
            self.engine = engine_or_url
        if self.engine.dialect.name == "sqlite":
            event.listen(self.engine, "connect", self._sqlite_foreign_keys)
        self.sessions = sessionmaker(self.engine, expire_on_commit=False)

    @staticmethod
    def _sqlite_foreign_keys(connection: Any, connection_record: Any) -> None:
        connection.execute("PRAGMA foreign_keys=ON")

    def create_schema(self) -> None:
        """Local/test convenience; deployment uses the versioned Alembic migration."""
        Base.metadata.create_all(self.engine)

    def close(self) -> None:
        self.engine.dispose()

    def ping(self) -> bool:
        """Readiness requires the control-plane schema as well as connectivity."""
        try:
            with self.engine.connect() as connection:
                present = set(inspect(connection).get_table_names())
                inspector = inspect(connection)
                return set(Base.metadata.tables).issubset(present) and all(
                    set(table.columns.keys()).issubset(
                        {column["name"] for column in inspector.get_columns(name)}
                    )
                    for name, table in Base.metadata.tables.items()
                )
        except SQLAlchemyError:
            return False

    def register(
        self, request: RegisterDefinitionRequest, actor: str, now: datetime
    ) -> WorkflowDefinition:
        try:
            document = validate_definition(request.definition_document)
        except DefinitionValidationError as exc:
            raise ApiError(
                422, "definition_invalid", "Definition failed validation", exc.issues
            ) from exc
        normalized = document.model_dump(mode="json")
        content_hash = hashlib.sha256(
            json.dumps(
                normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=False
            ).encode()
        ).hexdigest()
        scope = Scope(request.tenant, request.business_domain, request.application)
        row = DefinitionRow(
            registry_id=str(uuid4()),
            **scope.as_dict(),
            definition_id=request.definition_id,
            workflow_type=request.workflow_type,
            version=request.version,
            status=DefinitionStatus.VALIDATED.value,
            content_hash=content_hash,
            definition_document=normalized,
            owner=request.owner,
            dependencies=[
                dependency.model_dump(mode="json") for dependency in request.dependencies
            ],
            created_at=now,
            created_by=actor,
        )
        try:
            with self.sessions.begin() as session:
                session.add(row)
                session.flush()
                self._definition_event(session, row, "definition.registered", actor, now)
                result = _definition(row)
            return result
        except IntegrityError as exc:
            raise ApiError(
                409, "definition_exists", "This definition version already exists"
            ) from exc

    @staticmethod
    def _definition_row(
        session: Session, scope: Scope, workflow_type: str, version: str, *, lock: bool = False
    ) -> DefinitionRow:
        query = select(DefinitionRow).where(
            *_scope(DefinitionRow, scope),
            DefinitionRow.workflow_type == workflow_type,
            DefinitionRow.version == version,
        )
        row = session.scalar(query.with_for_update() if lock else query)
        if row is None:
            raise ApiError(404, "definition_not_found", "Definition version was not found")
        return row

    def get_definition(self, scope: Scope, workflow_type: str, version: str) -> WorkflowDefinition:
        with self.sessions() as session:
            return _definition(self._definition_row(session, scope, workflow_type, version))

    def approve_definition(
        self, scope: Scope, workflow_type: str, version: str, actor: str, now: datetime
    ) -> WorkflowDefinition:
        with self.sessions.begin() as session:
            row = self._definition_row(session, scope, workflow_type, version, lock=True)
            if row.status in {DefinitionStatus.APPROVED, DefinitionStatus.PROMOTED}:
                return _definition(row)
            if row.status != DefinitionStatus.VALIDATED:
                raise ApiError(
                    409, "invalid_definition_state", "Approval requires a validated revision"
                )
            previous = row.status
            row.status = DefinitionStatus.APPROVED.value
            row.approved_at, row.approved_by = now, actor
            self._definition_event(session, row, "definition.approved", actor, now, previous)
            return _definition(row)

    def promote_definition(
        self,
        scope: Scope,
        workflow_type: str,
        version: str,
        environment: str,
        actor: str,
        now: datetime,
    ) -> WorkflowDefinition:
        # A unique slot handles concurrent first promotion. Retrying after a conflict
        # obtains the committed slot and serializes subsequent updates with a row lock.
        for attempt in range(3):
            try:
                with self.sessions.begin() as session:
                    row = self._definition_row(session, scope, workflow_type, version, lock=True)
                    if row.status not in {DefinitionStatus.APPROVED, DefinitionStatus.PROMOTED}:
                        raise ApiError(
                            409, "definition_not_approved", "Promotion requires approval"
                        )
                    slot = session.scalar(
                        select(PromotionRow)
                        .where(
                            *_scope(PromotionRow, scope),
                            PromotionRow.workflow_type == workflow_type,
                            PromotionRow.environment == environment,
                        )
                        .with_for_update()
                    )
                    if slot is not None and slot.registry_id == row.registry_id:
                        return _definition(row)
                    if slot is None:
                        slot = PromotionRow(
                            promotion_id=str(uuid4()),
                            **scope.as_dict(),
                            workflow_type=workflow_type,
                            environment=environment,
                            registry_id=row.registry_id,
                            promoted_at=now,
                            promoted_by=actor,
                        )
                        session.add(slot)
                    else:
                        slot.registry_id, slot.promoted_at, slot.promoted_by = (
                            row.registry_id,
                            now,
                            actor,
                        )
                    previous = row.status
                    row.status = DefinitionStatus.PROMOTED.value
                    row.promoted_at, row.promoted_by = now, actor
                    session.flush()
                    self._definition_event(
                        session,
                        row,
                        "definition.promoted",
                        actor,
                        now,
                        previous,
                        {"environment": environment},
                    )
                    return _definition(row)
            except IntegrityError:
                if attempt == 2:
                    raise
        raise AssertionError("unreachable")

    def resolve_promoted(
        self, scope: Scope, workflow_type: str, environment: str, version: str | None = None
    ) -> WorkflowDefinition:
        with self.sessions() as session:
            row = session.scalar(
                select(DefinitionRow)
                .join(PromotionRow, PromotionRow.registry_id == DefinitionRow.registry_id)
                .where(
                    *_scope(DefinitionRow, scope),
                    *_scope(PromotionRow, scope),
                    PromotionRow.workflow_type == workflow_type,
                    PromotionRow.environment == environment,
                )
            )
            if row is None:
                raise ApiError(
                    409, "definition_not_promoted", "No revision is promoted in this environment"
                )
            if version is not None and row.version != version:
                raise ApiError(
                    409, "definition_not_promoted", "Requested revision is not actively promoted"
                )
            return _definition(row)

    def list_definitions(self, scope: Scope, limit: int, offset: int) -> list[WorkflowDefinition]:
        with self.sessions() as session:
            rows = session.scalars(
                select(DefinitionRow)
                .where(*_scope(DefinitionRow, scope))
                .order_by(DefinitionRow.created_at, DefinitionRow.registry_id)
                .limit(limit)
                .offset(offset)
            )
            return [_definition(row) for row in rows]

    def get_by_idempotency(
        self, scope: Scope, workflow_type: str, key: str
    ) -> ExecutionRecord | None:
        with self.sessions() as session:
            row = session.scalar(
                select(ExecutionRow).where(
                    *_scope(ExecutionRow, scope),
                    ExecutionRow.workflow_type == workflow_type,
                    ExecutionRow.idempotency_key == key,
                )
            )
            return _execution(row) if row is not None else None

    def reserve_execution(self, record: ExecutionRecord) -> tuple[ExecutionRecord, bool]:
        row = ExecutionRow(
            **record.scope.as_dict(),
            workflow_id=record.workflow_id,
            run_id=record.run_id,
            runtime_profile=record.runtime_profile,
            runtime_task_queue=record.runtime_task_queue,
            workflow_type=record.workflow_type,
            definition_id=record.definition_id,
            definition_version=record.definition_version,
            definition_document=record.definition_document.model_dump(mode="json"),
            request=record.request,
            variables=record.variables,
            business_reference=record.business_reference,
            correlation_id=record.correlation_id,
            idempotency_key=record.idempotency_key,
            request_fingerprint=record.request_fingerprint,
            created_by=record.created_by,
            state=record.state.value,
            current_step=record.current_step,
            started_at=record.started_at,
            updated_at=record.updated_at,
            completed_at=record.completed_at,
            failure_code=record.failure_code,
            failure_summary=record.failure_summary,
        )
        try:
            with self.sessions.begin() as session:
                definition = self._definition_row(
                    session, record.scope, record.workflow_type, record.definition_version
                )
                if definition.definition_id != record.definition_id:
                    raise ApiError(
                        409, "definition_mismatch", "Execution references another definition"
                    )
                session.add(row)
                session.flush()
                self._execution_event(
                    session,
                    row,
                    "execution.created",
                    record.created_by,
                    record.started_at,
                    new_state=record.state.value,
                    projection_key="created",
                )
                result = _execution(row)
            return result, True
        except IntegrityError as exc:
            winner = self.get_by_idempotency(
                record.scope, record.workflow_type, record.idempotency_key
            )
            if winner is None:
                raise ApiError(
                    409, "workflow_id_conflict", "Workflow identifier is already reserved"
                ) from exc
            if winner.request_fingerprint != record.request_fingerprint:
                raise ApiError(
                    409, "idempotency_conflict", "Idempotency key has a different request"
                ) from exc
            return winner, False

    @staticmethod
    def _execution_row(
        session: Session, scope: Scope, workflow_id: str, *, lock: bool = False
    ) -> ExecutionRow:
        query = select(ExecutionRow).where(
            *_scope(ExecutionRow, scope), ExecutionRow.workflow_id == workflow_id
        )
        row = session.scalar(query.with_for_update() if lock else query)
        if row is None:
            raise ApiError(404, "workflow_not_found", "Workflow execution was not found")
        return row

    def get_execution(self, scope: Scope, workflow_id: str) -> ExecutionRecord:
        with self.sessions() as session:
            return _execution(self._execution_row(session, scope, workflow_id))

    def mark_started(
        self, scope: Scope, workflow_id: str, run_id: str, now: datetime
    ) -> ExecutionRecord:
        with self.sessions.begin() as session:
            row = self._execution_row(session, scope, workflow_id, lock=True)
            if row.run_id is not None:
                if row.run_id != run_id:
                    raise ApiError(
                        409, "run_id_conflict", "Execution is bound to another Temporal run"
                    )
                return _execution(row)
            previous = row.state
            row.run_id = run_id
            if row.state == ExecutionState.CREATED:
                row.state = ExecutionState.RUNNING.value
            row.updated_at = now
            self._execution_event(
                session,
                row,
                "execution.started",
                row.created_by,
                now,
                previous_state=previous,
                new_state=row.state,
                projection_key="started",
            )
            return _execution(row)

    def list_executions(self, scope: Scope, limit: int, offset: int) -> list[ExecutionRecord]:
        with self.sessions() as session:
            rows = session.scalars(
                select(ExecutionRow)
                .where(*_scope(ExecutionRow, scope))
                .order_by(ExecutionRow.started_at, ExecutionRow.workflow_id)
                .limit(limit)
                .offset(offset)
            )
            return [_execution(row) for row in rows]

    def update_snapshot(
        self,
        scope: Scope,
        workflow_id: str,
        state: ExecutionState,
        current_step: str | None,
        transitions: list[dict[str, Any]],
        now: datetime,
        failure_code: str | None = None,
        failure_summary: str | None = None,
    ) -> ExecutionRecord:
        for attempt in range(3):
            try:
                with self.sessions.begin() as session:
                    row = self._execution_row(session, scope, workflow_id, lock=True)
                    existing = set(
                        session.scalars(
                            select(AuditRow.projection_key).where(
                                *_scope(AuditRow, scope),
                                AuditRow.workflow_id == workflow_id,
                                AuditRow.projection_key.is_not(None),
                            )
                        )
                    )
                    persisted_progress = sum(
                        key is not None and key.startswith("transition:") for key in existing
                    )
                    for index, transition in enumerate(transitions):
                        key = f"transition:{index}"
                        if key in existing:
                            continue
                        timestamp = transition.get("workflow_time")
                        transition_at = (
                            datetime.fromisoformat(timestamp) if isinstance(timestamp, str) else now
                        )
                        self._execution_event(
                            session,
                            row,
                            "execution.transition",
                            "workflow-runtime",
                            transition_at,
                            step=transition.get("step"),
                            new_state=transition.get("state"),
                            metadata={"detail": transition.get("detail", ""), "ordinal": index},
                            projection_key=key,
                        )
                    previous = row.state
                    incoming_progress = len(transitions)
                    terminal_observation = (
                        state in TERMINAL_STATES and ExecutionState(previous) not in TERMINAL_STATES
                    )
                    closed_run_intervention = (
                        state == ExecutionState.MANUAL_INTERVENTION
                        and failure_code == "runtime_run_changed"
                    )
                    # The legacy runtime returns an append-only transition
                    # prefix. Its progress orders observations even when an
                    # earlier request reaches the runtime after a later one.
                    # Closed Temporal executions can have no business payload
                    # (failure/cancellation), so those observations override a
                    # nonterminal projection regardless of prefix length.
                    fresh = (
                        terminal_observation
                        or closed_run_intervention
                        or incoming_progress > persisted_progress
                        or (
                            incoming_progress == persisted_progress
                            and _required_utc(now) >= _required_utc(row.updated_at)
                        )
                    )
                    terminal_regression = (
                        ExecutionState(previous) in TERMINAL_STATES and state.value != previous
                    )
                    pinned_run_closed = (
                        previous == ExecutionState.MANUAL_INTERVENTION.value
                        and row.failure_code == "runtime_run_changed"
                        and not closed_run_intervention
                    )
                    if fresh and not terminal_regression and not pinned_run_closed:
                        observed_at = max(_required_utc(now), _required_utc(row.updated_at))
                        row.state, row.current_step, row.updated_at = (
                            state.value,
                            current_step,
                            observed_at,
                        )
                        row.failure_code, row.failure_summary = failure_code, failure_summary
                        if state in TERMINAL_STATES and row.completed_at is None:
                            row.completed_at = observed_at
                        key = f"state:{state.value}:{len(transitions)}"
                        if previous != state.value and key not in existing:
                            self._execution_event(
                                session,
                                row,
                                "execution.state_changed",
                                "workflow-runtime",
                                now,
                                previous_state=previous,
                                new_state=state.value,
                                projection_key=key,
                            )
                    session.flush()
                    return _execution(row)
            except IntegrityError:
                if attempt == 2:
                    raise
        raise AssertionError("unreachable")

    def append_execution_event(
        self,
        scope: Scope,
        workflow_id: str,
        event_type: str,
        actor: str,
        now: datetime,
        metadata: JsonObject | None = None,
    ) -> AuditEvent:
        with self.sessions.begin() as session:
            row = self._execution_row(session, scope, workflow_id, lock=True)
            audit = self._execution_event(session, row, event_type, actor, now, metadata=metadata)
            session.flush()
            return _audit(audit)

    def list_history(
        self, scope: Scope, workflow_id: str, limit: int, offset: int
    ) -> list[AuditEvent]:
        with self.sessions() as session:
            self._execution_row(session, scope, workflow_id)
            rows = session.scalars(
                select(AuditRow)
                .where(
                    *_scope(AuditRow, scope),
                    AuditRow.workflow_id == workflow_id,
                )
                .order_by(AuditRow.ordinal)
                .limit(limit)
                .offset(offset)
            )
            return [_audit(row) for row in rows]

    @staticmethod
    def _execution_event(
        session: Session,
        row: ExecutionRow,
        event_type: str,
        actor: str,
        now: datetime,
        *,
        previous_state: str | None = None,
        new_state: str | None = None,
        step: str | None = None,
        metadata: JsonObject | None = None,
        projection_key: str | None = None,
    ) -> AuditRow:
        audit = AuditRow(
            event_id=str(uuid4()),
            event_type=event_type,
            tenant=row.tenant,
            business_domain=row.business_domain,
            application=row.application,
            workflow_id=row.workflow_id,
            run_id=row.run_id,
            workflow_type=row.workflow_type,
            definition_version=row.definition_version,
            business_reference=row.business_reference,
            step=step,
            previous_state=previous_state,
            new_state=new_state,
            actor=actor,
            correlation_id=row.correlation_id,
            timestamp=now,
            event_metadata=metadata or {},
            projection_key=projection_key,
            retention_class="standard",
        )
        session.add(audit)
        return audit

    @staticmethod
    def _definition_event(
        session: Session,
        row: DefinitionRow,
        event_type: str,
        actor: str,
        now: datetime,
        previous: str | None = None,
        metadata: JsonObject | None = None,
    ) -> None:
        session.add(
            AuditRow(
                event_id=str(uuid4()),
                event_type=event_type,
                tenant=row.tenant,
                business_domain=row.business_domain,
                application=row.application,
                workflow_type=row.workflow_type,
                definition_version=row.version,
                actor=actor,
                correlation_id=f"definition:{row.registry_id}",
                timestamp=now,
                previous_state=previous,
                new_state=row.status,
                event_metadata={"definition_id": row.definition_id, **(metadata or {})},
                retention_class="standard",
            )
        )
