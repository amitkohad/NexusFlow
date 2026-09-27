"""Versioned business HTTP API with injected persistence, runtime, and identity."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from http import HTTPStatus
from typing import Annotated, Any

from contracts import (
    ApiBusinessContext,
    ApproveDefinitionRequest,
    CancelResponse,
    CancelWorkflowRequest,
    DefinitionPage,
    ExecutionPage,
    HistoryPage,
    ProblemDetails,
    PromoteDefinitionRequest,
    RegisterDefinitionRequest,
    RuntimeProfile,
    SignalResponse,
    SignalWorkflowRequest,
    StartWorkflowRequest,
    ValidationIssue,
    WorkflowDefinition,
    WorkflowExecutionResponse,
    WorkflowStatusResponse,
)
from contracts.api import DefinitionVersion, WorkflowType
from fastapi import Depends, FastAPI, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.exc import SQLAlchemyError
from starlette.exceptions import HTTPException
from starlette.responses import JSONResponse

from .backend import BackendNotFound, BackendUnavailable, WorkflowBackend
from .errors import ApiError
from .middleware import RequestBoundary, problem_response
from .repository import Scope, WorkflowRepository
from .security import Authenticator, DenyAuthenticator, Principal, StaticTokenAuthenticator
from .service import WorkflowService, execution_response, utcnow

PERMISSIONS = frozenset(
    {
        "definitions:read",
        "definitions:write",
        "definitions:approve",
        "definitions:promote",
        "workflows:read",
        "workflows:start",
        "workflows:signal",
        "workflows:cancel",
    }
)
PageLimit = Annotated[int, Query(ge=1, le=100)]
PageOffset = Annotated[int, Query(ge=0)]
Lifespan = Callable[[FastAPI], AbstractAsyncContextManager[None]]


def _scope_matches(context: ApiBusinessContext, principal: Principal) -> None:
    if Scope(context.tenant, context.business_domain, context.application) != principal.scope:
        raise ApiError(
            403, "scope_forbidden", "Business context is outside the authenticated scope"
        )


def create_app(
    repository: WorkflowRepository,
    backend: WorkflowBackend,
    *,
    authenticator: Authenticator | None = None,
    environment: str = "local",
    max_request_bytes: int = 1024 * 1024,
    max_definition_steps: int = 500,
    runtime_profile: RuntimeProfile = "governed",
    runtime_task_queue: str | None = None,
    lifespan: Lifespan | None = None,
) -> FastAPI:
    if environment not in {"local", "dev", "test", "prod"}:
        raise ValueError("Invalid environment")
    if environment not in {"local", "test"} and isinstance(authenticator, StaticTokenAuthenticator):
        raise ValueError("Static token authentication is limited to local/test environments")
    provider = authenticator if authenticator is not None else DenyAuthenticator()
    if runtime_profile not in {"governed", "legacy"}:
        raise ValueError("Unsupported runtime profile")
    if runtime_profile == "governed" and not 1 <= max_definition_steps <= 500:
        raise ValueError("Version 1 runtime supports at most 500 steps")
    service = WorkflowService(
        repository,
        backend,
        environment,
        max_definition_steps,
        runtime_profile,
        runtime_task_queue or getattr(backend, "task_queue", None),
    )
    bearer = HTTPBearer(auto_error=False)
    problems: dict[int | str, dict[str, Any]] = {
        status: {
            "description": HTTPStatus(status).phrase,
            "content": {
                "application/problem+json": {
                    "schema": {"$ref": "#/components/schemas/ProblemDetails"}
                }
            },
            "model": ProblemDetails,
        }
        for status in (400, 401, 403, 404, 409, 413, 422, 500, 503)
    }
    app = FastAPI(
        title="NexusFlow Governed Workflow API",
        version="1.0.0",
        openapi_url="/api/v1/openapi.json",
        responses=problems,
        lifespan=lifespan,
    )
    app.add_middleware(RequestBoundary, max_request_bytes=max_request_bytes)
    app.state.service = service

    async def authenticated(
        credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
    ) -> Principal:
        if credentials is None:
            raise ApiError(401, "authentication_required", "A verified bearer identity is required")
        principal = await provider.authenticate(credentials.credentials)
        if principal is None:
            raise ApiError(401, "authentication_failed", "Bearer identity could not be verified")
        return principal

    def require(permission: str) -> Callable[..., Any]:
        async def permitted(principal: Principal = Depends(authenticated)) -> Principal:
            if permission not in principal.permissions:
                raise ApiError(
                    403, "permission_denied", "Identity lacks permission for this operation"
                )
            return principal

        return permitted

    @app.exception_handler(ApiError)
    async def api_error(request: Request, error: ApiError) -> JSONResponse:
        response = problem_response(
            error.status_code,
            error.code,
            error.message,
            request.url.path,
            request.state.correlation_id,
        )
        if error.issues:
            problem = ProblemDetails(
                title=HTTPStatus(error.status_code).phrase,
                status=error.status_code,
                detail=error.message,
                code=error.code,
                instance=request.url.path,
                correlation_id=request.state.correlation_id,
                issues=error.issues,
            )
            response = JSONResponse(
                problem.model_dump(mode="json"),
                status_code=error.status_code,
                media_type="application/problem+json",
            )
        if error.status_code == 401:
            response.headers["WWW-Authenticate"] = "Bearer"
        return response

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request: Request, error: RequestValidationError) -> JSONResponse:
        # Never return Pydantic input/ctx: they can echo tokens or business data.
        issues = tuple(
            ValidationIssue(
                code="request.invalid",
                path="/" + "/".join(str(part) for part in issue["loc"]),
                message="Value does not satisfy the request contract",
            )
            for issue in error.errors()[:20]
        )
        return await api_error(
            request, ApiError(422, "request_invalid", "Request failed validation", issues)
        )

    @app.exception_handler(BackendUnavailable)
    async def unavailable(request: Request, error: BackendUnavailable) -> JSONResponse:
        return await api_error(
            request, ApiError(503, "runtime_unavailable", "Workflow runtime is unavailable")
        )

    @app.exception_handler(BackendNotFound)
    async def missing_backend(request: Request, error: BackendNotFound) -> JSONResponse:
        return await api_error(
            request, ApiError(404, "workflow_not_found", "Workflow execution was not found")
        )

    @app.exception_handler(SQLAlchemyError)
    async def unavailable_database(request: Request, error: SQLAlchemyError) -> JSONResponse:
        return await api_error(
            request, ApiError(503, "database_unavailable", "Business metadata store is unavailable")
        )

    @app.exception_handler(HTTPException)
    async def http_error(request: Request, error: HTTPException) -> JSONResponse:
        return await api_error(
            request, ApiError(error.status_code, "http_error", HTTPStatus(error.status_code).phrase)
        )

    @app.exception_handler(Exception)
    async def internal_error(request: Request, error: Exception) -> JSONResponse:
        response = await api_error(
            request, ApiError(500, "internal_error", "Request could not be completed")
        )
        response.headers["X-Correlation-ID"] = request.state.correlation_id
        return response

    @app.get("/health", include_in_schema=False)
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/ready", include_in_schema=False)
    async def ready() -> dict[str, str]:
        database_ready, runtime_ready = await asyncio.gather(
            asyncio.to_thread(repository.ping), backend.ready()
        )
        if not database_ready or not runtime_ready:
            raise ApiError(503, "service_not_ready", "Required services are unavailable")
        return {"status": "ready"}

    @app.post("/api/v1/workflows", status_code=201, response_model=WorkflowDefinition)
    async def register(
        body: RegisterDefinitionRequest,
        principal: Principal = Depends(require("definitions:write")),
    ) -> WorkflowDefinition:
        _scope_matches(body, principal)
        return await service.register(body, principal.subject)

    @app.get("/api/v1/definitions", response_model=DefinitionPage)
    async def definitions(
        limit: PageLimit = 50,
        offset: PageOffset = 0,
        principal: Principal = Depends(require("definitions:read")),
    ) -> DefinitionPage:
        items = await asyncio.to_thread(
            repository.list_definitions, principal.scope, limit + 1, offset
        )
        return DefinitionPage(
            items=tuple(items[:limit]),
            limit=limit,
            offset=offset,
            next_offset=offset + limit if len(items) > limit else None,
        )

    @app.get("/api/v1/definitions/{workflow_type}/{version}", response_model=WorkflowDefinition)
    async def definition(
        workflow_type: WorkflowType,
        version: DefinitionVersion,
        principal: Principal = Depends(require("definitions:read")),
    ) -> WorkflowDefinition:
        return await asyncio.to_thread(
            repository.get_definition, principal.scope, workflow_type, version
        )

    @app.post(
        "/api/v1/definitions/{workflow_type}/{version}/approve", response_model=WorkflowDefinition
    )
    async def approve(
        workflow_type: WorkflowType,
        version: DefinitionVersion,
        body: ApproveDefinitionRequest,
        principal: Principal = Depends(require("definitions:approve")),
    ) -> WorkflowDefinition:
        _scope_matches(body, principal)
        return await asyncio.to_thread(
            repository.approve_definition,
            principal.scope,
            workflow_type,
            version,
            principal.subject,
            utcnow(),
        )

    @app.post(
        "/api/v1/definitions/{workflow_type}/{version}/promote", response_model=WorkflowDefinition
    )
    async def promote(
        workflow_type: WorkflowType,
        version: DefinitionVersion,
        body: PromoteDefinitionRequest,
        principal: Principal = Depends(require("definitions:promote")),
    ) -> WorkflowDefinition:
        _scope_matches(body, principal)
        if body.environment != environment:
            raise ApiError(
                409, "environment_mismatch", "Promotion must target this API environment"
            )
        return await asyncio.to_thread(
            repository.promote_definition,
            principal.scope,
            workflow_type,
            version,
            environment,
            principal.subject,
            utcnow(),
        )

    @app.post(
        "/api/v1/workflows/{workflow_type}/start",
        status_code=202,
        response_model=WorkflowExecutionResponse,
        responses={200: {"model": WorkflowExecutionResponse, "description": "Idempotent replay"}},
    )
    async def start(
        workflow_type: WorkflowType,
        body: StartWorkflowRequest,
        request: Request,
        response: Response,
        principal: Principal = Depends(require("workflows:start")),
    ) -> WorkflowExecutionResponse:
        _scope_matches(body, principal)
        if (
            request.state.correlation_header_provided
            and request.state.correlation_id != body.correlation_id
        ):
            raise ApiError(
                400, "correlation_mismatch", "Header and body correlation identifiers must match"
            )
        request.state.correlation_id = body.correlation_id
        result, created = await service.start(workflow_type, body, principal)
        response.status_code = 202 if created else 200
        response.headers["Location"] = result.links.self
        return result

    @app.get("/api/v1/workflows", response_model=ExecutionPage)
    async def executions(
        limit: PageLimit = 50,
        offset: PageOffset = 0,
        principal: Principal = Depends(require("workflows:read")),
    ) -> ExecutionPage:
        items = await asyncio.to_thread(
            repository.list_executions, principal.scope, limit + 1, offset
        )
        return ExecutionPage(
            items=tuple(execution_response(item) for item in items[:limit]),
            limit=limit,
            offset=offset,
            next_offset=offset + limit if len(items) > limit else None,
        )

    @app.get("/api/v1/workflows/{workflow_id}", response_model=WorkflowExecutionResponse)
    async def execution(
        workflow_id: str,
        principal: Principal = Depends(require("workflows:read")),
    ) -> WorkflowExecutionResponse:
        return execution_response(
            await asyncio.to_thread(repository.get_execution, principal.scope, workflow_id)
        )

    @app.get("/api/v1/workflows/{workflow_id}/status", response_model=WorkflowStatusResponse)
    async def status(
        workflow_id: str,
        principal: Principal = Depends(require("workflows:read")),
    ) -> WorkflowStatusResponse:
        record = await service.refresh(principal, workflow_id)
        return WorkflowStatusResponse.model_validate(
            execution_response(record).model_dump(exclude={"replayed"})
        )

    @app.get("/api/v1/workflows/{workflow_id}/history", response_model=HistoryPage)
    async def history(
        workflow_id: str,
        limit: PageLimit = 50,
        offset: PageOffset = 0,
        principal: Principal = Depends(require("workflows:read")),
    ) -> HistoryPage:
        items, refreshed = await service.history(principal, workflow_id, limit + 1, offset)
        return HistoryPage(
            items=tuple(items[:limit]),
            limit=limit,
            offset=offset,
            next_offset=offset + limit if len(items) > limit else None,
            refreshed=refreshed,
        )

    @app.post(
        "/api/v1/workflows/{workflow_id}/signal", status_code=202, response_model=SignalResponse
    )
    async def signal(
        workflow_id: str,
        body: SignalWorkflowRequest,
        principal: Principal = Depends(require("workflows:signal")),
    ) -> SignalResponse:
        record = await service.signal(principal, workflow_id, body)
        return SignalResponse(workflow_id=workflow_id, correlation_id=record.correlation_id)

    @app.post(
        "/api/v1/workflows/{workflow_id}/cancel", status_code=202, response_model=CancelResponse
    )
    async def cancel(
        workflow_id: str,
        body: CancelWorkflowRequest,
        principal: Principal = Depends(require("workflows:cancel")),
    ) -> CancelResponse:
        record = await service.cancel(principal, workflow_id, body.reason)
        return CancelResponse(workflow_id=workflow_id, correlation_id=record.correlation_id)

    return app
