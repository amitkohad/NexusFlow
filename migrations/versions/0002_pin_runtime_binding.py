"""Pin runtime type and queue for rollout-safe pending start recovery.

Revision ID: 0002
Revises: 0001_governed_workflow_api
"""

import sqlalchemy as sa
from alembic import op

revision = "0002"
down_revision = "0001_governed_workflow_api"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "workflow_executions",
        sa.Column("runtime_profile", sa.String(32), nullable=False, server_default="legacy"),
    )
    op.add_column(
        "workflow_executions",
        sa.Column(
            "runtime_task_queue",
            sa.String(256),
            nullable=False,
            server_default="lightweight-workflows",
        ),
    )


def downgrade() -> None:
    op.drop_column("workflow_executions", "runtime_task_queue")
    op.drop_column("workflow_executions", "runtime_profile")
