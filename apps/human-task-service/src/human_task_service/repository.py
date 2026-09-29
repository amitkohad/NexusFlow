"""Transactional human-task lifecycle, audit and durable signal outbox."""

from __future__ import annotations

import hashlib
import json
from _thread import RLock
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, List
from uuid import uuid4
from weakref import WeakKeyDictionary

from contracts import (
    CompleteTaskRequest,
    CreateTaskRequest,
    DelegateTaskRequest,
    EscalateTaskRequest,
    ExpireTaskRequest,
    HumanTaskStatus,
    ReassignTaskRequest,
    TaskDecisionRequest,
    TaskMutationRequest,
    TaskPage,
    TaskResponse,
)
from sqlalchemy import Engine, and_, create_engine, event, inspect, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from .errors import TaskError
from .models import TaskAuditRow, TaskBase, TaskOutboxRow, TaskRow
from .security import TaskPrincipal

TERMINAL = frozenset({"APPROVED", "REJECTED", "COMPLETED", "EXPIRED", "CANCELLED"})
_SQLITE_LOCKS: WeakKeyDictionary[Engine, RLock] = WeakKeyDictionary()
_SQLITE_LOCKS_GUARD = RLock()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _aware(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return (
        value.replace(tzinfo=timezone.utc)
        if value.tzinfo is None
        else value.astimezone(timezone.utc)
    )


def _required_aware(value: datetime) -> datetime:
    aware = _aware(value)
    assert aware is not None
    return aware


def _fingerprint(body: CreateTaskRequest) -> str:
    # A Continue-As-New or uncertain Activity retry may have a new run ID while
    # still referring to the same logical approval. An eligible compatible
    # release may likewise execute its retried creation Activity. Retain the
    # initial run and release as task provenance.
    document = body.model_dump(mode="json", exclude={"run_id", "package_release_id", "build_id"})
    return hashlib.sha256(
        json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _response(row: TaskRow, *, replayed: bool = False) -> TaskResponse:
    if row.task_type != "approval":
        raise ValueError("Unsupported stored task type")
    return TaskResponse(
        task_id=row.task_id,
        tenant=row.tenant,
        business_domain=row.business_domain,
        application=row.application,
        workflow_id=row.workflow_id,
        run_id=row.run_id,
        first_execution_run_id=row.first_execution_run_id,
        step_id=row.step_id,
        definition_version=row.definition_version,
        correlation_id=row.correlation_id,
        business_reference=row.business_reference,
        assignee=row.assignee,
        assignee_group=row.assignee_group,
        status=HumanTaskStatus(row.status),
        form_schema_version=row.form_schema_version,
        payload_reference=row.payload_reference,
        due_at=_required_aware(row.due_at),
        sla_deadline=_aware(row.sla_deadline),
        escalation_policy=row.escalation_policy,
        outcome=row.outcome,
        actor=row.actor,
        evidence_reference=row.evidence_reference,
        created_at=_required_aware(row.created_at),
        claimed_at=_aware(row.claimed_at),
        completed_at=_aware(row.completed_at),
        expired_at=_aware(row.expired_at),
        task_type="approval",
        version=row.version,
        idempotency_key=row.idempotency_key,
        package_id=row.package_id,
        package_release_id=row.package_release_id,
        build_id=row.build_id,
        escalation_level=row.escalation_level,
        delegated_by=row.delegated_by,
        comment=row.comment,
        updated_at=_required_aware(row.updated_at),
        replayed=replayed,
    )


def _scope(row: type[TaskRow], principal: TaskPrincipal) -> tuple[Any, ...]:
    return (
        row.tenant == principal.tenant,
        row.business_domain == principal.business_domain,
        row.application == principal.application,
    )


def _visible(row: TaskRow, principal: TaskPrincipal) -> bool:
    return (
        "tasks:manage" in principal.permissions
        or row.assignee == principal.subject
        or (row.assignee_group is not None and row.assignee_group in principal.groups)
    )


def _can_act(row: TaskRow, principal: TaskPrincipal) -> bool:
    if row.status == "CLAIMED":
        return row.assignee == principal.subject
    return row.assignee == principal.subject or (
        row.assignee_group is not None and row.assignee_group in principal.groups
    )


def _audit(
    session: Session,
    row: TaskRow,
    event_type: str,
    previous_status: str | None,
    actor: str,
    now: datetime,
    reason: str | None = None,
) -> None:
    session.add(
        TaskAuditRow(
            event_id=str(uuid4()),
            task_id=row.task_id,
            event_type=event_type,
            previous_status=previous_status,
            new_status=row.status,
            actor=actor,
            reason=reason,
            correlation_id=row.correlation_id,
            created_at=now,
        )
    )


def _enqueue(session: Session, row: TaskRow, now: datetime, *, expired: bool) -> None:
    event_id = str(uuid4())
    payload: dict[str, Any] = {
        "task_id": row.task_id,
        "event_id": event_id,
        "idempotency_key": row.idempotency_key,
    }
    signal_name = "task_expired" if expired else "approve"
    if not expired:
        payload.update(
            approved=row.status == "APPROVED",
            approver=row.actor,
            comment=row.comment,
            evidence_reference=row.evidence_reference,
        )
    session.add(
        TaskOutboxRow(
            event_id=event_id,
            task_id=row.task_id,
            workflow_id=row.workflow_id,
            run_id=row.run_id,
            first_execution_run_id=row.first_execution_run_id,
            signal_name=signal_name,
            payload=payload,
            status="PENDING",
            attempts=0,
            available_at=now,
            lease_until=None,
            lease_token=None,
            delivered_at=None,
            created_at=now,
        )
    )


@dataclass(frozen=True)
class OutboxDispatch:
    event_id: str
    task_id: str
    workflow_id: str
    run_id: str
    first_execution_run_id: str
    signal_name: str
    payload: dict[str, Any]
    lease_token: str
    attempts: int


class TaskRepository:
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
        self._sqlite_lock: RLock | None = None
        if self.engine.dialect.name == "sqlite":
            event.listen(self.engine, "connect", self._sqlite_foreign_keys)
            with _SQLITE_LOCKS_GUARD:
                self._sqlite_lock = _SQLITE_LOCKS.setdefault(self.engine, RLock())
        self.sessions = sessionmaker(self.engine, expire_on_commit=False)

    @staticmethod
    def _sqlite_foreign_keys(connection: Any, connection_record: Any) -> None:
        connection.execute("PRAGMA foreign_keys=ON")

    def create_schema(self) -> None:
        """Local/test convenience; deployed services apply Alembic 0004."""
        with self._sqlite_lock or nullcontext():
            TaskBase.metadata.create_all(self.engine)

    def ping(self) -> bool:
        try:
            with self._sqlite_lock or nullcontext(), self.engine.connect() as connection:
                inspector = inspect(connection)
                present = set(inspector.get_table_names())
                return set(TaskBase.metadata.tables).issubset(present) and all(
                    set(table.columns.keys()).issubset(
                        {column["name"] for column in inspector.get_columns(name)}
                    )
                    for name, table in TaskBase.metadata.tables.items()
                )
        except Exception:
            return False

    def close(self) -> None:
        self.engine.dispose()

    def create(self, body: CreateTaskRequest, principal: TaskPrincipal) -> TaskResponse:
        if (body.tenant, body.business_domain, body.application) != (
            principal.tenant,
            principal.business_domain,
            principal.application,
        ):
            raise TaskError(403, "scope_forbidden", "Task scope is outside authenticated identity")
        fingerprint = _fingerprint(body)
        with self._sqlite_lock or nullcontext():
            try:
                with self.sessions.begin() as session:
                    existing = session.scalar(
                        select(TaskRow)
                        .where(
                            *_scope(TaskRow, principal),
                            TaskRow.idempotency_key == body.idempotency_key,
                        )
                        .with_for_update()
                    )
                    if existing is not None:
                        return self._existing_create(existing, fingerprint)
                    now = utcnow()
                    due_at = now + timedelta(seconds=body.timeout_seconds)
                    sla_deadline = _aware(body.sla_deadline) or due_at
                    if sla_deadline > due_at:
                        raise TaskError(
                            422, "sla_after_due", "SLA deadline must not exceed task due time"
                        )
                    row = TaskRow(
                        task_id=str(uuid4()),
                        tenant=body.tenant,
                        business_domain=body.business_domain,
                        application=body.application,
                        workflow_id=body.workflow_id,
                        run_id=body.run_id,
                        first_execution_run_id=body.first_execution_run_id or body.run_id,
                        step_id=body.step_id,
                        definition_version=body.definition_version,
                        correlation_id=body.correlation_id,
                        business_reference=body.business_reference,
                        idempotency_key=body.idempotency_key,
                        request_fingerprint=fingerprint,
                        task_type=body.task_type,
                        package_id=body.package_id,
                        package_release_id=body.package_release_id,
                        build_id=body.build_id,
                        assignee=body.assignee,
                        assignee_group=body.assignee_group,
                        delegated_by=None,
                        status="ASSIGNED",
                        version=0,
                        form_schema_version=body.form_schema_version,
                        payload_reference=body.payload_reference,
                        due_at=due_at,
                        sla_deadline=sla_deadline,
                        escalation_policy=body.escalation_policy.model_dump(mode="json"),
                        escalation_level=0,
                        outcome=None,
                        actor=None,
                        evidence_reference=None,
                        comment="",
                        created_at=now,
                        updated_at=now,
                        claimed_at=None,
                        completed_at=None,
                        expired_at=None,
                    )
                    session.add(row)
                    # The audit row references the task through an ID only, so
                    # make the insert order explicit on PostgreSQL.
                    session.flush()
                    _audit(session, row, "CREATED", None, principal.subject, now)
                    return _response(row)
            except IntegrityError:
                # PostgreSQL's unique constraint arbitrates concurrent Activity
                # retries even when both transactions missed the initial lookup.
                with self.sessions() as session:
                    existing = session.scalar(
                        select(TaskRow).where(
                            *_scope(TaskRow, principal),
                            TaskRow.idempotency_key == body.idempotency_key,
                        )
                    )
                    if existing is None:
                        raise
                    return self._existing_create(existing, fingerprint)

    @staticmethod
    def _existing_create(row: TaskRow, fingerprint: str) -> TaskResponse:
        if row.request_fingerprint != fingerprint:
            raise TaskError(
                409, "task_idempotency_conflict", "Idempotency key belongs to a different task"
            )
        return _response(row, replayed=True)

    def list(
        self,
        principal: TaskPrincipal,
        *,
        status: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> TaskPage:
        if not 1 <= limit <= 100 or offset < 0:
            raise TaskError(422, "page_invalid", "Task page bounds are invalid")
        filters: list[Any] = [*_scope(TaskRow, principal)]
        if status is not None:
            filters.append(TaskRow.status == status)
        if "tasks:manage" not in principal.permissions:
            filters.append(
                or_(
                    TaskRow.assignee == principal.subject,
                    TaskRow.assignee_group.in_(tuple(principal.groups))
                    if principal.groups
                    else TaskRow.assignee == principal.subject,
                )
            )
        with self._sqlite_lock or nullcontext(), self.sessions() as session:
            rows = session.scalars(
                select(TaskRow)
                .where(*filters)
                .order_by(TaskRow.created_at, TaskRow.task_id)
                .offset(offset)
                .limit(limit + 1)
            ).all()
        return TaskPage(
            items=tuple(_response(row) for row in rows[:limit]),
            limit=limit,
            offset=offset,
            next_offset=offset + limit if len(rows) > limit else None,
        )

    def get(self, task_id: str, principal: TaskPrincipal) -> TaskResponse:
        with self._sqlite_lock or nullcontext(), self.sessions() as session:
            row = session.scalar(
                select(TaskRow).where(*_scope(TaskRow, principal), TaskRow.task_id == task_id)
            )
            if row is None or not _visible(row, principal):
                raise TaskError(404, "task_not_found", "Task was not found")
            return _response(row)

    def _locked(self, session: Session, task_id: str, principal: TaskPrincipal) -> TaskRow:
        row = session.scalar(
            select(TaskRow)
            .where(*_scope(TaskRow, principal), TaskRow.task_id == task_id)
            .with_for_update()
        )
        if row is None:
            raise TaskError(404, "task_not_found", "Task was not found")
        return row

    @staticmethod
    def _version(row: TaskRow, body: TaskMutationRequest) -> None:
        if body.expected_version is not None and row.version != body.expected_version:
            raise TaskError(409, "task_version_conflict", "Task has changed since it was read")

    def claim(
        self, task_id: str, body: TaskMutationRequest, principal: TaskPrincipal
    ) -> TaskResponse:
        with self._sqlite_lock or nullcontext(), self.sessions.begin() as session:
            row = self._locked(session, task_id, principal)
            if row.status == "CLAIMED" and row.assignee == principal.subject:
                return _response(row, replayed=True)
            self._version(row, body)
            if row.status not in {"CREATED", "ASSIGNED"}:
                raise TaskError(409, "task_not_claimable", "Task cannot be claimed")
            if not _can_act(row, principal):
                raise TaskError(403, "task_actor_forbidden", "Actor is not assigned to this task")
            now = utcnow()
            if _required_aware(row.due_at) <= now:
                raise TaskError(409, "task_overdue", "Task is past its deadline")
            if row.sla_deadline is not None and _required_aware(row.sla_deadline) <= now:
                raise TaskError(409, "task_sla_overdue", "Task has crossed its SLA threshold")
            previous = row.status
            row.status = "CLAIMED"
            row.assignee = principal.subject
            row.claimed_at = now
            row.updated_at = now
            row.version += 1
            _audit(session, row, "CLAIMED", previous, principal.subject, now)
            return _response(row)

    def decide(
        self,
        task_id: str,
        outcome: str,
        body: TaskDecisionRequest | CompleteTaskRequest,
        principal: TaskPrincipal,
    ) -> TaskResponse:
        status = {"approved": "APPROVED", "rejected": "REJECTED"}.get(outcome)
        if status is None:
            raise TaskError(422, "task_outcome_invalid", "Unsupported task outcome")
        with self._sqlite_lock or nullcontext(), self.sessions.begin() as session:
            row = self._locked(session, task_id, principal)
            if row.status in {"APPROVED", "REJECTED"}:
                if (
                    row.status == status
                    and row.actor == principal.subject
                    and row.evidence_reference == body.evidence_reference
                    and row.comment == body.comment
                ):
                    return _response(row, replayed=True)
                raise TaskError(409, "task_already_completed", "Task has a terminal outcome")
            self._version(row, body)
            if row.status in TERMINAL:
                raise TaskError(409, "task_already_completed", "Task has a terminal outcome")
            if not _can_act(row, principal):
                raise TaskError(403, "task_actor_forbidden", "Actor is not assigned to this task")
            now = utcnow()
            if _required_aware(row.due_at) <= now:
                raise TaskError(409, "task_overdue", "Task is past its deadline")
            if row.sla_deadline is not None and _required_aware(row.sla_deadline) <= now:
                raise TaskError(409, "task_sla_overdue", "Task has crossed its SLA threshold")
            previous = row.status
            row.status = status
            row.outcome = outcome
            row.actor = principal.subject
            row.evidence_reference = body.evidence_reference
            row.comment = body.comment
            row.completed_at = now
            row.updated_at = now
            row.version += 1
            _audit(session, row, status, previous, principal.subject, now)
            _enqueue(session, row, now, expired=False)
            return _response(row)

    def reassign(
        self, task_id: str, body: ReassignTaskRequest, principal: TaskPrincipal
    ) -> TaskResponse:
        with self._sqlite_lock or nullcontext(), self.sessions.begin() as session:
            row = self._locked(session, task_id, principal)
            self._version(row, body)
            if row.status in TERMINAL:
                raise TaskError(409, "task_terminal", "Terminal task cannot be reassigned")
            previous = row.status
            now = utcnow()
            row.assignee = body.assignee
            row.assignee_group = body.assignee_group
            row.claimed_at = None
            row.delegated_by = None
            row.status = "ASSIGNED"
            row.version += 1
            row.updated_at = now
            _audit(session, row, "REASSIGNED", previous, principal.subject, now, body.reason)
            return _response(row)

    def delegate(
        self, task_id: str, body: DelegateTaskRequest, principal: TaskPrincipal
    ) -> TaskResponse:
        with self._sqlite_lock or nullcontext(), self.sessions.begin() as session:
            row = self._locked(session, task_id, principal)
            self._version(row, body)
            if row.status in TERMINAL:
                raise TaskError(409, "task_terminal", "Terminal task cannot be delegated")
            if not _can_act(row, principal) and "tasks:manage" not in principal.permissions:
                raise TaskError(403, "task_actor_forbidden", "Actor is not assigned to this task")
            previous = row.status
            now = utcnow()
            row.assignee = body.assignee
            row.assignee_group = None
            row.claimed_at = None
            row.delegated_by = principal.subject
            row.status = "ASSIGNED"
            row.version += 1
            row.updated_at = now
            _audit(session, row, "DELEGATED", previous, principal.subject, now, body.reason)
            return _response(row)

    def escalate(
        self, task_id: str, body: EscalateTaskRequest, principal: TaskPrincipal
    ) -> TaskResponse:
        with self._sqlite_lock or nullcontext(), self.sessions.begin() as session:
            row = self._locked(session, task_id, principal)
            self._version(row, body)
            if row.status in TERMINAL:
                raise TaskError(409, "task_terminal", "Terminal task cannot be escalated")
            target = body.assignee_group or row.escalation_policy.get("target_group")
            if not isinstance(target, str) or not target:
                raise TaskError(
                    409, "escalation_target_missing", "No escalation group is configured"
                )
            now = utcnow()
            self._escalate_row(session, row, target, principal.subject, now, body.reason)
            return _response(row)

    @staticmethod
    def _escalate_row(
        session: Session, row: TaskRow, target: str, actor: str, now: datetime, reason: str
    ) -> None:
        previous = row.status
        row.assignee = None
        row.assignee_group = target
        row.claimed_at = None
        row.status = "ASSIGNED"
        row.escalation_level += 1
        extension = row.escalation_policy.get("extend_seconds", 0)
        if isinstance(extension, int) and extension > 0:
            row.sla_deadline = min(_required_aware(row.due_at), now + timedelta(seconds=extension))
        else:
            row.sla_deadline = None
        row.version += 1
        row.updated_at = now
        _audit(session, row, "ESCALATED", previous, actor, now, reason)

    def expire(
        self, task_id: str, body: ExpireTaskRequest, principal: TaskPrincipal
    ) -> TaskResponse:
        with self._sqlite_lock or nullcontext(), self.sessions.begin() as session:
            row = self._locked(session, task_id, principal)
            if row.status == "EXPIRED":
                return _response(row, replayed=True)
            self._version(row, body)
            if row.status in TERMINAL:
                raise TaskError(409, "task_terminal", "Terminal task cannot be expired")
            now = utcnow()
            self._expire_row(session, row, principal.subject, now, body.reason)
            return _response(row)

    @staticmethod
    def _expire_row(session: Session, row: TaskRow, actor: str, now: datetime, reason: str) -> None:
        previous = row.status
        row.status = "EXPIRED"
        row.outcome = "expired"
        row.actor = actor
        row.expired_at = now
        row.updated_at = now
        row.version += 1
        _audit(session, row, "EXPIRED", previous, actor, now, reason)
        _enqueue(session, row, now, expired=True)

    def process_due_tasks(self, now: datetime | None = None, *, limit: int = 100) -> int:
        current = _aware(now) or utcnow()
        if not 1 <= limit <= 1000:
            raise ValueError("Due sweep limit must be 1..1000")
        with self._sqlite_lock or nullcontext(), self.sessions() as session:
            ids = session.scalars(
                select(TaskRow.task_id)
                .where(
                    TaskRow.status.in_(("CREATED", "ASSIGNED", "CLAIMED")),
                    or_(TaskRow.due_at <= current, TaskRow.sla_deadline <= current),
                )
                .order_by(TaskRow.due_at, TaskRow.task_id)
                .limit(limit)
            ).all()
        applied = 0
        for task_id in ids:
            with self._sqlite_lock or nullcontext(), self.sessions.begin() as session:
                row = session.scalar(
                    select(TaskRow).where(TaskRow.task_id == task_id).with_for_update()
                )
                if row is None or row.status in TERMINAL:
                    continue
                if _required_aware(row.due_at) <= current:
                    self._expire_row(
                        session, row, "task-sla-sweeper", current, "Task due time elapsed"
                    )
                    applied += 1
                elif row.sla_deadline is not None and _required_aware(row.sla_deadline) <= current:
                    policy = row.escalation_policy
                    target = policy.get("target_group")
                    if (
                        row.escalation_level == 0
                        and policy.get("action") == "escalate"
                        and isinstance(target, str)
                        and target
                    ):
                        self._escalate_row(
                            session, row, target, "task-sla-sweeper", current, "Task SLA elapsed"
                        )
                    else:
                        self._expire_row(
                            session, row, "task-sla-sweeper", current, "Task SLA elapsed"
                        )
                    applied += 1
        return applied

    def lease_outbox(self, *, limit: int = 100, lease_seconds: int = 30) -> List[OutboxDispatch]:
        if not 1 <= limit <= 1000 or not 1 <= lease_seconds <= 3600:
            raise ValueError("Invalid outbox lease bounds")
        now = utcnow()
        with self._sqlite_lock or nullcontext(), self.sessions.begin() as session:
            statement = (
                select(TaskOutboxRow)
                .where(
                    or_(
                        and_(TaskOutboxRow.status == "PENDING", TaskOutboxRow.available_at <= now),
                        and_(TaskOutboxRow.status == "LEASED", TaskOutboxRow.lease_until <= now),
                    )
                )
                .order_by(TaskOutboxRow.created_at, TaskOutboxRow.event_id)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
            rows = session.scalars(statement).all()
            leases: List[OutboxDispatch] = []
            for row in rows:
                token = str(uuid4())
                row.status = "LEASED"
                row.lease_token = token
                row.lease_until = now + timedelta(seconds=lease_seconds)
                row.attempts += 1
                leases.append(
                    OutboxDispatch(
                        event_id=row.event_id,
                        task_id=row.task_id,
                        workflow_id=row.workflow_id,
                        run_id=row.run_id,
                        first_execution_run_id=row.first_execution_run_id,
                        signal_name=row.signal_name,
                        payload=dict(row.payload),
                        lease_token=token,
                        attempts=row.attempts,
                    )
                )
            return leases

    def mark_delivered(self, event_id: str, lease_token: str) -> None:
        with self._sqlite_lock or nullcontext(), self.sessions.begin() as session:
            row = session.get(TaskOutboxRow, event_id, with_for_update=True)
            if row is None or row.status != "LEASED" or row.lease_token != lease_token:
                return
            row.status = "DELIVERED"
            row.delivered_at = utcnow()
            row.lease_until = None
            row.lease_token = None

    def mark_retry(self, event_id: str, lease_token: str, attempts: int) -> None:
        with self._sqlite_lock or nullcontext(), self.sessions.begin() as session:
            row = session.get(TaskOutboxRow, event_id, with_for_update=True)
            if row is None or row.status != "LEASED" or row.lease_token != lease_token:
                return
            row.status = "PENDING"
            row.available_at = utcnow() + timedelta(seconds=min(2 ** min(attempts, 6), 60))
            row.lease_until = None
            row.lease_token = None

    def mark_blocked(self, event_id: str, lease_token: str, reason: str) -> None:
        """Retain an undeliverable event for operator review without retry loops."""
        if reason not in {"workflow_chain_mismatch", "workflow_closed", "workflow_missing"}:
            raise ValueError("Unsupported outbox block reason")
        with self._sqlite_lock or nullcontext(), self.sessions.begin() as session:
            event_row = session.get(TaskOutboxRow, event_id, with_for_update=True)
            if (
                event_row is None
                or event_row.status != "LEASED"
                or event_row.lease_token != lease_token
            ):
                return
            event_row.status = "BLOCKED"
            event_row.lease_until = None
            event_row.lease_token = None
            task_row = session.get(TaskRow, event_row.task_id, with_for_update=True)
            if task_row is not None:
                _audit(
                    session,
                    task_row,
                    "DISPATCH_BLOCKED",
                    task_row.status,
                    "task-outbox",
                    utcnow(),
                    reason,
                )

    def audit_events(self, task_id: str, principal: TaskPrincipal) -> List[TaskAuditRow]:
        self.get(task_id, principal)
        with self._sqlite_lock or nullcontext(), self.sessions() as session:
            return list(
                session.scalars(
                    select(TaskAuditRow)
                    .where(TaskAuditRow.task_id == task_id)
                    .order_by(TaskAuditRow.created_at, TaskAuditRow.event_id)
                ).all()
            )
