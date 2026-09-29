"""Standalone local task API factory with Temporal outbox delivery."""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from typing import Any

from fastapi import FastAPI
from sqlalchemy.engine import make_url
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter

from .api import create_app
from .dispatch import TemporalTaskDispatcher
from .repository import TaskRepository
from .security import StaticTokenAuthenticator, TaskPrincipal

logger = logging.getLogger("human_task_service")


class ConnectedDispatcher:
    def __init__(self) -> None:
        self.delegate: TemporalTaskDispatcher | None = None

    async def ready(self) -> bool:
        return self.delegate is not None and await self.delegate.ready()

    async def signal(
        self,
        workflow_id: str,
        first_execution_run_id: str,
        signal_name: str,
        payload: dict[str, Any],
    ) -> None:
        if self.delegate is None:
            raise RuntimeError("Temporal task dispatcher is unavailable")
        await self.delegate.signal(workflow_id, first_execution_run_id, signal_name, payload)


def create_app_from_env() -> FastAPI:
    environment = os.environ.get("NEXUSFLOW_ENVIRONMENT", "local")
    if environment not in {"local", "test"}:
        raise RuntimeError(
            "Inject a verified enterprise Authenticator in the deployment application factory"
        )
    database_url = os.environ.get("NEXUSFLOW_DATABASE_URL", "")
    service_token = os.environ.get("NEXUSFLOW_HUMAN_TASK_SERVICE_TOKEN", "")
    actor_token = os.environ.get("NEXUSFLOW_HUMAN_TASK_ACTOR_TOKEN", "")
    if (
        not database_url
        or len(service_token) < 32
        or len(actor_token) < 32
        or service_token == actor_token
    ):
        raise RuntimeError("Task database URL and distinct local/test bearer tokens are required")
    try:
        driver = make_url(database_url).drivername
    except Exception:
        raise RuntimeError("Invalid human-task database URL") from None
    if driver not in {"sqlite", "postgresql+psycopg"}:
        raise RuntimeError("Unsupported task database driver")
    address = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
    namespace = os.environ.get("TEMPORAL_NAMESPACE", "default")
    groups = frozenset(
        group.strip()
        for group in os.environ.get("NEXUSFLOW_HUMAN_TASK_GROUPS", "approvers").split(",")
        if group.strip()
    )
    scope = {
        "tenant": os.environ.get("NEXUSFLOW_HUMAN_TASK_TENANT", "demo"),
        "business_domain": os.environ.get("NEXUSFLOW_HUMAN_TASK_DOMAIN", "customer-services"),
        "application": os.environ.get("NEXUSFLOW_HUMAN_TASK_APPLICATION", "adjustments"),
    }
    service_principal = TaskPrincipal(
        subject="package-activity",
        tenant=scope["tenant"],
        business_domain=scope["business_domain"],
        application=scope["application"],
        permissions=frozenset({"tasks:create"}),
    )
    actor_principal = TaskPrincipal(
        subject=os.environ.get("NEXUSFLOW_HUMAN_TASK_ACTOR", "local-task-operator"),
        tenant=scope["tenant"],
        business_domain=scope["business_domain"],
        application=scope["application"],
        permissions=frozenset({"tasks:read", "tasks:act", "tasks:manage"}),
        groups=groups,
    )
    repository = TaskRepository(database_url)
    dispatcher = ConnectedDispatcher()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        background: asyncio.Task[None] | None = None
        try:
            try:
                client = await Client.connect(
                    address, namespace=namespace, data_converter=pydantic_data_converter
                )
                dispatcher.delegate = TemporalTaskDispatcher(client)
            except Exception:
                raise RuntimeError("Temporal task dispatcher connection failed") from None

            async def serve_due_and_outbox() -> None:
                while True:
                    try:
                        await asyncio.to_thread(repository.process_due_tasks)
                        await app.state.task_service.dispatch_pending()
                    except Exception:
                        logger.exception("task_sweep_failed")
                    await asyncio.sleep(1)

            background = asyncio.create_task(serve_due_and_outbox(), name="human-task-outbox")
            yield
        finally:
            if background is not None:
                background.cancel()
                with suppress(asyncio.CancelledError):
                    await background
            dispatcher.delegate = None
            repository.close()

    return create_app(
        repository,
        dispatcher,
        authenticator=StaticTokenAuthenticator(
            {service_token: service_principal, actor_token: actor_principal}
        ),
        environment=environment,
        lifespan=lifespan,
    )
