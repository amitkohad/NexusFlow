"""Add package governance and nullable provenance without rewriting old bindings."""

import sqlalchemy as sa
from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for name, kind in (
        ("package_binding", sa.JSON()),
        ("observed_initial_build_id", sa.String(128)),
        ("observed_current_build_id", sa.String(128)),
    ):
        op.add_column("workflow_executions", sa.Column(name, kind, nullable=True))
    scope = ("tenant", "business_domain", "application")
    tables = {
        "workflow_packages": (
            [
                sa.Column("package_id", sa.String(128), nullable=False),
                sa.Column("document", sa.JSON(), nullable=False),
                sa.Column("deployment_name", sa.String(128)),
            ],
            [
                ("uq_package_scope", (*scope, "package_id")),
                ("uq_package_deployment_owner", ("deployment_name",)),
            ],
        ),
        "workflow_package_releases": (
            [
                sa.Column("package_release_id", sa.String(128), nullable=False),
                sa.Column("package_id", sa.String(128), nullable=False),
                sa.Column("package_version", sa.String(128), nullable=False),
                sa.Column("deployment_name", sa.String(128), nullable=False),
                sa.Column("build_id", sa.String(128), nullable=False),
                sa.Column("descriptor", sa.JSON(), nullable=False),
                sa.Column("manifest", sa.JSON(), nullable=False),
                sa.Column("status", sa.String(32), nullable=False),
                sa.Column("approved_by", sa.String(256)),
                sa.Column("approved_at", sa.DateTime(timezone=True)),
            ],
            [
                ("uq_release_scope", (*scope, "package_release_id")),
                ("uq_package_version", (*scope, "package_id", "package_version")),
                ("uq_package_build", (*scope, "deployment_name", "build_id")),
                ("uq_global_package_build", ("deployment_name", "build_id")),
            ],
        ),
        "workflow_package_environments": (
            [
                sa.Column("package_id", sa.String(128), nullable=False),
                sa.Column("environment", sa.String(128), nullable=False),
                sa.Column("current_release_id", sa.String(128), nullable=False),
                sa.Column("ramping_release_id", sa.String(128)),
                sa.Column("ramp_percentage", sa.Integer(), nullable=False),
                sa.Column("routing_confirmed", sa.Integer(), nullable=False),
                sa.Column("temporal_namespace", sa.String(256), nullable=False),
                sa.Column("queue_bindings", sa.JSON(), nullable=False),
            ],
            [("uq_package_environment", (*scope, "package_id", "environment"))],
        ),
        "workflow_executor_pools": (
            [
                sa.Column("pool_id", sa.String(128), nullable=False),
                sa.Column("package_release_id", sa.String(128), nullable=False),
                sa.Column("document", sa.JSON(), nullable=False),
            ],
            [("uq_executor_pool", (*scope, "pool_id"))],
        ),
    }
    for table, (columns, constraints) in tables.items():
        op.create_table(
            table,
            sa.Column("row_id", sa.String(36), primary_key=True),
            *(sa.Column(name, sa.String(128), nullable=False) for name in scope),
            *columns,
            *(sa.UniqueConstraint(*fields, name=name) for name, fields in constraints),
        )
    op.create_table(
        "workflow_package_queue_owners",
        sa.Column("row_id", sa.String(36), primary_key=True),
        sa.Column("temporal_namespace", sa.String(256), nullable=False),
        sa.Column("task_queue", sa.String(256), nullable=False),
        sa.Column(
            "package_owner_id",
            sa.String(36),
            sa.ForeignKey("workflow_packages.row_id"),
            nullable=False,
        ),
        sa.UniqueConstraint("temporal_namespace", "task_queue", name="uq_package_queue_owner"),
    )


def downgrade() -> None:
    # Removing frozen package provenance while executions exist would make
    # recovery unsafe. Operators must drain/retain these before rollback.
    connection = op.get_bind()
    active = connection.execute(
        sa.text("SELECT COUNT(*) FROM workflow_executions WHERE package_binding IS NOT NULL")
    ).scalar_one()
    if active:
        raise RuntimeError(
            "Retain package provenance; restore the older API without downgrading 0003"
        )
    for table in (
        "workflow_package_queue_owners",
        "workflow_executor_pools",
        "workflow_package_environments",
        "workflow_package_releases",
        "workflow_packages",
    ):
        op.drop_table(table)
    for column in ("observed_current_build_id", "observed_initial_build_id", "package_binding"):
        op.drop_column("workflow_executions", column)
