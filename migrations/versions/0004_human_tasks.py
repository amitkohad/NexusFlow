"""Add shared human-task lifecycle, audit, and durable signal outbox."""

import sqlalchemy as sa
from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "human_tasks",
        sa.Column("task_id", sa.String(36), primary_key=True),
        sa.Column("tenant", sa.String(128), nullable=False),
        sa.Column("business_domain", sa.String(128), nullable=False),
        sa.Column("application", sa.String(128), nullable=False),
        sa.Column("workflow_id", sa.String(256), nullable=False),
        sa.Column("run_id", sa.String(256), nullable=False),
        sa.Column("first_execution_run_id", sa.String(256), nullable=False),
        sa.Column("step_id", sa.String(256), nullable=False),
        sa.Column("definition_version", sa.String(256), nullable=False),
        sa.Column("correlation_id", sa.String(128), nullable=False),
        sa.Column("business_reference", sa.String(256), nullable=False),
        sa.Column("idempotency_key", sa.String(256), nullable=False),
        sa.Column("request_fingerprint", sa.String(64), nullable=False),
        sa.Column("task_type", sa.String(32), nullable=False),
        sa.Column("package_id", sa.String(256), nullable=False),
        sa.Column("package_release_id", sa.String(256), nullable=False),
        sa.Column("build_id", sa.String(256), nullable=False),
        sa.Column("assignee", sa.String(256)),
        sa.Column("assignee_group", sa.String(256)),
        sa.Column("delegated_by", sa.String(256)),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("form_schema_version", sa.String(256)),
        sa.Column("payload_reference", sa.String(256)),
        sa.Column("due_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("sla_deadline", sa.DateTime(timezone=True)),
        sa.Column("escalation_policy", sa.JSON(), nullable=False),
        sa.Column("escalation_level", sa.Integer(), nullable=False),
        sa.Column("outcome", sa.String(32)),
        sa.Column("actor", sa.String(256)),
        sa.Column("evidence_reference", sa.String(256)),
        sa.Column("comment", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("claimed_at", sa.DateTime(timezone=True)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("expired_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint(
            "tenant",
            "business_domain",
            "application",
            "idempotency_key",
            name="uq_human_task_idempotency",
        ),
    )
    op.create_index(
        "ix_human_task_inbox",
        "human_tasks",
        ["tenant", "business_domain", "application", "status", "created_at"],
    )
    op.create_index("ix_human_task_due", "human_tasks", ["status", "due_at"])
    op.create_index("ix_human_task_sla", "human_tasks", ["status", "sla_deadline"])

    op.create_table(
        "human_task_audit_events",
        sa.Column("event_id", sa.String(36), primary_key=True),
        sa.Column("task_id", sa.String(36), sa.ForeignKey("human_tasks.task_id"), nullable=False),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("previous_status", sa.String(32)),
        sa.Column("new_status", sa.String(32), nullable=False),
        sa.Column("actor", sa.String(256), nullable=False),
        sa.Column("reason", sa.String(256)),
        sa.Column("correlation_id", sa.String(128), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_human_task_audit_task", "human_task_audit_events", ["task_id", "created_at"]
    )

    op.create_table(
        "human_task_outbox",
        sa.Column("event_id", sa.String(36), primary_key=True),
        sa.Column("task_id", sa.String(36), sa.ForeignKey("human_tasks.task_id"), nullable=False),
        sa.Column("workflow_id", sa.String(256), nullable=False),
        sa.Column("run_id", sa.String(256), nullable=False),
        sa.Column("first_execution_run_id", sa.String(256), nullable=False),
        sa.Column("signal_name", sa.String(64), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("lease_until", sa.DateTime(timezone=True)),
        sa.Column("lease_token", sa.String(36)),
        sa.Column("delivered_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("task_id", name="uq_human_task_outcome_event"),
    )
    op.create_index("ix_human_task_outbox_due", "human_task_outbox", ["status", "available_at"])


def downgrade() -> None:
    connection = op.get_bind()
    retained = connection.execute(sa.text("SELECT COUNT(*) FROM human_tasks")).scalar_one()
    if retained:
        raise RuntimeError("Human-task provenance must remain available while tasks are retained")
    op.drop_index("ix_human_task_outbox_due", table_name="human_task_outbox")
    op.drop_table("human_task_outbox")
    op.drop_index("ix_human_task_audit_task", table_name="human_task_audit_events")
    op.drop_table("human_task_audit_events")
    op.drop_index("ix_human_task_sla", table_name="human_tasks")
    op.drop_index("ix_human_task_due", table_name="human_tasks")
    op.drop_index("ix_human_task_inbox", table_name="human_tasks")
    op.drop_table("human_tasks")
