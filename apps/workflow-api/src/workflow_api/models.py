"""SQLAlchemy persistence models for the governed control plane."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import JSON, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class ScopedModel:
    tenant: Mapped[str] = mapped_column(String(128), nullable=False)
    business_domain: Mapped[str] = mapped_column(String(128), nullable=False)
    application: Mapped[str] = mapped_column(String(128), nullable=False)


class DefinitionRow(ScopedModel, Base):
    __tablename__ = "workflow_definitions"
    __table_args__ = (
        UniqueConstraint(
            "tenant",
            "business_domain",
            "application",
            "workflow_type",
            "version",
            name="uq_definition_revision",
        ),
        Index("ix_definition_scope", "tenant", "business_domain", "application", "created_at"),
    )

    registry_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    definition_id: Mapped[str] = mapped_column(String(256), nullable=False)
    workflow_type: Mapped[str] = mapped_column(String(128), nullable=False)
    version: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    definition_document: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    owner: Mapped[str] = mapped_column(String(256), nullable=False)
    dependencies: Mapped[list[dict[str, Any]]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_by: Mapped[str] = mapped_column(String(256), nullable=False)
    approved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    approved_by: Mapped[str | None] = mapped_column(String(256))
    promoted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    promoted_by: Mapped[str | None] = mapped_column(String(256))


class PromotionRow(ScopedModel, Base):
    __tablename__ = "workflow_promotions"
    __table_args__ = (
        UniqueConstraint(
            "tenant",
            "business_domain",
            "application",
            "workflow_type",
            "environment",
            name="uq_promotion_slot",
        ),
    )

    promotion_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    workflow_type: Mapped[str] = mapped_column(String(128), nullable=False)
    environment: Mapped[str] = mapped_column(String(128), nullable=False)
    registry_id: Mapped[str] = mapped_column(
        ForeignKey("workflow_definitions.registry_id"), nullable=False
    )
    promoted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    promoted_by: Mapped[str] = mapped_column(String(256), nullable=False)


class ExecutionRow(ScopedModel, Base):
    __tablename__ = "workflow_executions"
    __table_args__ = (
        UniqueConstraint(
            "tenant",
            "business_domain",
            "application",
            "workflow_type",
            "idempotency_key",
            name="uq_execution_idempotency",
        ),
        Index("ix_execution_scope", "tenant", "business_domain", "application", "started_at"),
    )

    workflow_id: Mapped[str] = mapped_column(String(256), primary_key=True)
    run_id: Mapped[str | None] = mapped_column(String(256))
    runtime_profile: Mapped[str] = mapped_column(
        String(32), nullable=False, default="legacy", server_default="legacy"
    )
    runtime_task_queue: Mapped[str] = mapped_column(
        String(256),
        nullable=False,
        default="lightweight-workflows",
        server_default="lightweight-workflows",
    )
    workflow_type: Mapped[str] = mapped_column(String(128), nullable=False)
    definition_id: Mapped[str] = mapped_column(String(256), nullable=False)
    definition_version: Mapped[str] = mapped_column(String(64), nullable=False)
    definition_document: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    request: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    variables: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    business_reference: Mapped[str] = mapped_column(String(256), nullable=False)
    correlation_id: Mapped[str] = mapped_column(String(256), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(256), nullable=False)
    request_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    created_by: Mapped[str] = mapped_column(String(256), nullable=False)
    state: Mapped[str] = mapped_column(String(32), nullable=False)
    current_step: Mapped[str | None] = mapped_column(String(256))
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    failure_code: Mapped[str | None] = mapped_column(String(256))
    failure_summary: Mapped[str | None] = mapped_column(Text)
    package_binding: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    observed_initial_build_id: Mapped[str | None] = mapped_column(String(128))
    observed_current_build_id: Mapped[str | None] = mapped_column(String(128))


class AuditRow(ScopedModel, Base):
    __tablename__ = "workflow_audit_events"
    __table_args__ = (
        UniqueConstraint("workflow_id", "projection_key", name="uq_audit_projection"),
        Index("ix_audit_scope_workflow", "tenant", "business_domain", "application", "workflow_id"),
    )

    ordinal: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String(36), nullable=False, unique=True)
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    workflow_id: Mapped[str | None] = mapped_column(ForeignKey("workflow_executions.workflow_id"))
    projection_key: Mapped[str | None] = mapped_column(String(128))
    run_id: Mapped[str | None] = mapped_column(String(256))
    workflow_type: Mapped[str | None] = mapped_column(String(128))
    definition_version: Mapped[str | None] = mapped_column(String(64))
    business_reference: Mapped[str | None] = mapped_column(String(256))
    step: Mapped[str | None] = mapped_column(String(256))
    previous_state: Mapped[str | None] = mapped_column(String(32))
    new_state: Mapped[str | None] = mapped_column(String(32))
    actor: Mapped[str] = mapped_column(String(256), nullable=False)
    correlation_id: Mapped[str] = mapped_column(String(256), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    event_metadata: Mapped[dict[str, Any]] = mapped_column("metadata", JSON, nullable=False)
    retention_class: Mapped[str] = mapped_column(String(64), nullable=False, default="standard")


class PackageRow(ScopedModel, Base):
    __tablename__ = "workflow_packages"
    __table_args__ = (
        UniqueConstraint(
            "tenant", "business_domain", "application", "package_id", name="uq_package_scope"
        ),
        UniqueConstraint("deployment_name", name="uq_package_deployment_owner"),
    )
    row_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    package_id: Mapped[str] = mapped_column(String(128), nullable=False)
    document: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    deployment_name: Mapped[str | None] = mapped_column(String(128))


class PackageReleaseRow(ScopedModel, Base):
    __tablename__ = "workflow_package_releases"
    __table_args__ = (
        UniqueConstraint(
            "tenant",
            "business_domain",
            "application",
            "package_release_id",
            name="uq_release_scope",
        ),
        UniqueConstraint(
            "tenant",
            "business_domain",
            "application",
            "package_id",
            "package_version",
            name="uq_package_version",
        ),
        UniqueConstraint(
            "tenant",
            "business_domain",
            "application",
            "deployment_name",
            "build_id",
            name="uq_package_build",
        ),
        UniqueConstraint("deployment_name", "build_id", name="uq_global_package_build"),
    )
    row_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    package_release_id: Mapped[str] = mapped_column(String(128), nullable=False)
    package_id: Mapped[str] = mapped_column(String(128), nullable=False)
    package_version: Mapped[str] = mapped_column(String(128), nullable=False)
    deployment_name: Mapped[str] = mapped_column(String(128), nullable=False)
    build_id: Mapped[str] = mapped_column(String(128), nullable=False)
    descriptor: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    manifest: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    approved_by: Mapped[str | None] = mapped_column(String(256))
    approved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class PackageEnvironmentRow(ScopedModel, Base):
    __tablename__ = "workflow_package_environments"
    __table_args__ = (
        UniqueConstraint(
            "tenant",
            "business_domain",
            "application",
            "package_id",
            "environment",
            name="uq_package_environment",
        ),
    )
    row_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    package_id: Mapped[str] = mapped_column(String(128), nullable=False)
    environment: Mapped[str] = mapped_column(String(128), nullable=False)
    current_release_id: Mapped[str] = mapped_column(String(128), nullable=False)
    ramping_release_id: Mapped[str | None] = mapped_column(String(128))
    ramp_percentage: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    routing_confirmed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    temporal_namespace: Mapped[str] = mapped_column(String(256), nullable=False)
    queue_bindings: Mapped[dict[str, str]] = mapped_column(JSON, nullable=False)


class ExecutorPoolRow(ScopedModel, Base):
    __tablename__ = "workflow_executor_pools"
    __table_args__ = (
        UniqueConstraint(
            "tenant", "business_domain", "application", "pool_id", name="uq_executor_pool"
        ),
    )
    row_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    pool_id: Mapped[str] = mapped_column(String(128), nullable=False)
    package_release_id: Mapped[str] = mapped_column(String(128), nullable=False)
    document: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)


class PackageQueueOwnerRow(Base):
    __tablename__ = "workflow_package_queue_owners"
    __table_args__ = (
        UniqueConstraint("temporal_namespace", "task_queue", name="uq_package_queue_owner"),
    )
    row_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    temporal_namespace: Mapped[str] = mapped_column(String(256), nullable=False)
    task_queue: Mapped[str] = mapped_column(String(256), nullable=False)
    package_owner_id: Mapped[str] = mapped_column(
        ForeignKey("workflow_packages.row_id"), nullable=False
    )
