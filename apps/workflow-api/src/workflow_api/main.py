"""Uvicorn factory. Migration and runtime connections are explicit operations."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from contracts import DefinitionDocument, JsonObject
from fastapi import FastAPI
from temporalio.client import Client

from .api import PERMISSIONS, create_app
from .backend import BackendUnavailable, BusinessSnapshot, TemporalBackend, WorkflowBackend
from .config import load_api_settings
from .repository import WorkflowRepository
from .security import Principal, StaticTokenAuthenticator


class ConnectedBackend:
    """Filled by startup on the serving loop, never by an import side effect."""

    def __init__(self) -> None:
        self.delegate: WorkflowBackend | None = None

    def connected(self) -> WorkflowBackend:
        if self.delegate is None:
            raise BackendUnavailable()
        return self.delegate

    async def start(
        self,
        workflow_id: str,
        workflow_type: str,
        definition: DefinitionDocument,
        request: JsonObject,
        variables: JsonObject,
    ) -> str:
        return await self.connected().start(
            workflow_id, workflow_type, definition, request, variables
        )

    async def status(self, workflow_id: str, run_id: str) -> BusinessSnapshot:
        return await self.connected().status(workflow_id, run_id)

    async def signal(
        self, workflow_id: str, run_id: str, signalname: str, payload: dict[str, Any]
    ) -> None:
        await self.connected().signal(workflow_id, run_id, signalname, payload)

    async def cancel(self, workflow_id: str, run_id: str) -> None:
        await self.connected().cancel(workflow_id, run_id)

    async def ready(self) -> bool:
        return self.delegate is not None and await self.delegate.ready()


def create_app_from_env() -> FastAPI:
    settings = load_api_settings()
    # A deployed provider must be injected by its own application factory. This
    # standalone local factory cannot pretend to authenticate enterprise tokens.
    if settings.environment not in {"local", "test"}:
        raise RuntimeError(
            "Configure an enterprise Authenticator in the deployment application factory"
        )
    try:
        repository = WorkflowRepository(settings.database_url.get_secret_value())
    except Exception:
        raise RuntimeError("Business database configuration failed") from None
    backend = ConnectedBackend()
    authenticator = None
    if settings.local_token is not None:
        authenticator = StaticTokenAuthenticator(
            {
                settings.local_token.get_secret_value(): Principal(
                    subject=settings.local_actor,
                    tenant=settings.local_tenant,
                    business_domain=settings.local_business_domain,
                    application=settings.local_application,
                    permissions=PERMISSIONS,
                )
            }
        )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        try:
            try:
                client = await Client.connect(
                    settings.temporal_address,
                    namespace=settings.temporal_namespace,
                    tls=settings.temporal_tls,
                )
                backend.delegate = TemporalBackend(client, settings.temporal_task_queue)
            except Exception:
                raise RuntimeError("Workflow runtime connection failed") from None
            yield
        finally:
            backend.delegate = None
            repository.close()

    return create_app(
        repository,
        backend,
        authenticator=authenticator,
        environment=settings.environment,
        max_request_bytes=settings.max_payload_bytes,
        max_definition_steps=settings.max_definition_steps,
        lifespan=lifespan,
    )
