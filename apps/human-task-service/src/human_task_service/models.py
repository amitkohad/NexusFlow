"""Dedicated task tables in the shared business database."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import JSON, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class TaskBase(DeclarativeBase):
    pass


class TaskRow(TaskBase):
    __tablename__ = "human_tasks"
    __table_args__ = (
        UniqueConstraint(
            "tenant",
            "business_domain",
            "application",
            "idempotency_key",
            name="uq_human_task_idempotency",
        ),
        Index(
            "ix_human_task_inbox",
            "tenant",
            "business_domain",
            "application",
            "status",
            "created_at",
        ),
        Index("ix_human_task_due", "status", "due_at"),
        Index("ix_human_task_sla", "status", "sla_deadline"),
    )

    task_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    tenant: Mapped[str] = mapped_column(String(128), nullable=False)
    business_domain: Mapped[str] = mapped_column(String(128), nullable=False)
    application: Mapped[str] = mapped_column(String(128), nullable=False)
    workflow_id: Mapped[str] = mapped_column(String(256), nullable=False)
    run_id: Mapped[str] = mapped_column(String(256), nullable=False)
    first_execution_run_id: Mapped[str] = mapped_column(String(256), nullable=False)
    step_id: Mapped[str] = mapped_column(String(256), nullable=False)
    definition_version: Mapped[str] = mapped_column(String(256), nullable=False)
    correlation_id: Mapped[str] = mapped_column(String(128), nullable=False)
    business_reference: Mapped[str] = mapped_column(String(256), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(256), nullable=False)
    request_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    task_type: Mapped[str] = mapped_column(String(32), nullable=False)
    package_id: Mapped[str] = mapped_column(String(256), nullable=False)
    package_release_id: Mapped[str] = mapped_column(String(256), nullable=False)
    build_id: Mapped[str] = mapped_column(String(256), nullable=False)
    assignee: Mapped[str | None] = mapped_column(String(256))
    assignee_group: Mapped[str | None] = mapped_column(String(256))
    delegated_by: Mapped[str | None] = mapped_column(String(256))
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    form_schema_version: Mapped[str | None] = mapped_column(String(256))
    payload_reference: Mapped[str | None] = mapped_column(String(256))
    due_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    sla_deadline: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    escalation_policy: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    escalation_level: Mapped[int] = mapped_column(Integer, nullable=False)
    outcome: Mapped[str | None] = mapped_column(String(32))
    actor: Mapped[str | None] = mapped_column(String(256))
    evidence_reference: Mapped[str | None] = mapped_column(String(256))
    comment: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    expired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class TaskAuditRow(TaskBase):
    __tablename__ = "human_task_audit_events"
    __table_args__ = (Index("ix_human_task_audit_task", "task_id", "created_at"),)

    event_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    task_id: Mapped[str] = mapped_column(ForeignKey("human_tasks.task_id"), nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    previous_status: Mapped[str | None] = mapped_column(String(32))
    new_status: Mapped[str] = mapped_column(String(32), nullable=False)
    actor: Mapped[str] = mapped_column(String(256), nullable=False)
    reason: Mapped[str | None] = mapped_column(String(256))
    correlation_id: Mapped[str] = mapped_column(String(128), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class TaskOutboxRow(TaskBase):
    __tablename__ = "human_task_outbox"
    __table_args__ = (
        UniqueConstraint("task_id", name="uq_human_task_outcome_event"),
        Index("ix_human_task_outbox_due", "status", "available_at"),
    )

    event_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    task_id: Mapped[str] = mapped_column(ForeignKey("human_tasks.task_id"), nullable=False)
    workflow_id: Mapped[str] = mapped_column(String(256), nullable=False)
    run_id: Mapped[str] = mapped_column(String(256), nullable=False)
    first_execution_run_id: Mapped[str] = mapped_column(String(256), nullable=False)
    signal_name: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_token: Mapped[str | None] = mapped_column(String(36))
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
