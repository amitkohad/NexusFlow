"""Migration runner; schema changes are explicit versioned revisions."""

from __future__ import annotations

import os

from alembic import context
from human_task_service.models import TaskBase
from sqlalchemy import create_engine, pool
from workflow_api.models import Base

config = context.config
database_url = (
    os.environ.get("NEXUSFLOW_DATABASE_URL") or config.get_main_option("sqlalchemy.url") or ""
)
if not database_url:
    raise RuntimeError("A migration database URL is required")
target_metadata = [Base.metadata, TaskBase.metadata]


def run_migrations_offline() -> None:
    context.configure(
        url=database_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    supplied_connection = config.attributes.get("connection")
    if supplied_connection is not None:
        context.configure(connection=supplied_connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()
        return
    engine = create_engine(database_url, poolclass=pool.NullPool)
    with engine.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()
    engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
