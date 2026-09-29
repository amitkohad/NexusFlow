"""Scoped task inbox and lifecycle HTTP API."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from http import HTTPStatus
from typing import Annotated, Any

from contracts import (
    CompleteTaskRequest,
    CreateTaskRequest,
    DelegateTaskRequest,
    EscalateTaskRequest,
    ExpireTaskRequest,
    ProblemDetails,
    ReassignTaskRequest,
    TaskDecisionRequest,
    TaskMutationRequest,
    TaskPage,
    TaskResponse,
    ValidationIssue,
)
from fastapi import Depends, FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.exc import SQLAlchemyError
from starlette.exceptions import HTTPException
from starlette.responses import JSONResponse

from .boundary import RequestBoundary, problem_response
from .dispatch import TaskSignalDispatcher
from .errors import TaskError
from .repository import TaskRepository
from .security import Authenticator, DenyAuthenticator, StaticTokenAuthenticator, TaskPrincipal
from .service import TaskService

PageLimit = Annotated[int, Query(ge=1, le=100)]
PageOffset = Annotated[int, Query(ge=0)]
Lifespan = Callable[[FastAPI], AbstractAsyncContextManager[None]]
PERMISSIONS = frozenset({"tasks:create", "tasks:read", "tasks:act", "tasks:manage"})


def create_app(
    repository: TaskRepository,
    dispatcher: TaskSignalDispatcher,
    *,
    authenticator: Authenticator | None = None,
    environment: str = "local",
    max_request_bytes: int = 1024 * 1024,
    lifespan: Lifespan | None = None,
) -> FastAPI:
    if environment not in {"local", "dev", "test", "prod"}:
        raise ValueError("Invalid environment")
    if environment not in {"local", "test"} and isinstance(authenticator, StaticTokenAuthenticator):
        raise ValueError("Static token authentication is limited to local/test environments")
    provider = authenticator if authenticator is not None else DenyAuthenticator()
    service = TaskService(repository, dispatcher)
    bearer = HTTPBearer(auto_error=False)
    app = FastAPI(
        title="NexusFlow Human Task API",
        version="1.0.0",
        openapi_url="/api/v1/openapi.json",
        responses={
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
        },
        lifespan=lifespan,
    )
    app.add_middleware(RequestBoundary, max_request_bytes=max_request_bytes)
    app.state.task_service = service

    async def authenticated(
        credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
    ) -> TaskPrincipal:
        if credentials is None:
            raise TaskError(
                401, "authentication_required", "A verified bearer identity is required"
            )
        principal = await provider.authenticate(credentials.credentials)
        if principal is None:
            raise TaskError(401, "authentication_failed", "Bearer identity could not be verified")
        return principal

    def require(permission: str) -> Callable[..., Any]:
        async def permitted(principal: TaskPrincipal = Depends(authenticated)) -> TaskPrincipal:
            if permission not in principal.permissions and not (
                permission == "tasks:read" and "tasks:manage" in principal.permissions
            ):
                raise TaskError(403, "permission_denied", "Identity lacks task permission")
            return principal

        return permitted

    @app.exception_handler(TaskError)
    async def task_error(request: Request, error: TaskError) -> JSONResponse:
        response = problem_response(
            error.status_code,
            error.code,
            error.message,
            request.url.path,
            request.state.correlation_id,
        )
        if error.status_code == 401:
            response.headers["WWW-Authenticate"] = "Bearer"
        return response

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request: Request, error: RequestValidationError) -> JSONResponse:
        issues = tuple(
            ValidationIssue(
                code="request.invalid",
                path="/" + "/".join(str(part) for part in issue["loc"]),
                message="Value does not satisfy the request contract",
            )
            for issue in error.errors()[:20]
        )
        problem = ProblemDetails(
            title=HTTPStatus(422).phrase,
            status=422,
            detail="Request failed validation",
            code="request_invalid",
            instance=request.url.path,
            correlation_id=request.state.correlation_id,
            issues=issues,
        )
        return JSONResponse(
            problem.model_dump(mode="json"), status_code=422, media_type="application/problem+json"
        )

    @app.exception_handler(SQLAlchemyError)
    async def unavailable_database(request: Request, error: SQLAlchemyError) -> JSONResponse:
        return await task_error(
            request, TaskError(503, "database_unavailable", "Human-task store is unavailable")
        )

    @app.exception_handler(HTTPException)
    async def http_error(request: Request, error: HTTPException) -> JSONResponse:
        return await task_error(
            request,
            TaskError(error.status_code, "http_error", HTTPStatus(error.status_code).phrase),
        )

    @app.exception_handler(Exception)
    async def internal_error(request: Request, error: Exception) -> JSONResponse:
        return await task_error(request, TaskError(500, "internal_error", "Task request failed"))

    @app.get("/health", include_in_schema=False)
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/ready", include_in_schema=False)
    async def ready() -> dict[str, str]:
        database_ready = await asyncio.to_thread(repository.ping)
        readiness = getattr(dispatcher, "ready", None)
        runtime_ready = await readiness() if callable(readiness) else True
        if not database_ready or not runtime_ready:
            raise TaskError(503, "service_not_ready", "Required task services are unavailable")
        return {"status": "ready"}

    @app.post("/api/v1/tasks", status_code=201, response_model=TaskResponse)
    async def create_task(
        body: CreateTaskRequest,
        principal: TaskPrincipal = Depends(require("tasks:create")),
    ) -> TaskResponse:
        return await asyncio.to_thread(repository.create, body, principal)

    @app.get("/api/v1/tasks", response_model=TaskPage)
    async def list_tasks(
        status: str | None = Query(
            default=None, pattern="^(CREATED|ASSIGNED|CLAIMED|APPROVED|REJECTED|EXPIRED|CANCELLED)$"
        ),
        limit: PageLimit = 50,
        offset: PageOffset = 0,
        principal: TaskPrincipal = Depends(require("tasks:read")),
    ) -> TaskPage:
        return await asyncio.to_thread(
            repository.list, principal, status=status, limit=limit, offset=offset
        )

    @app.get("/api/v1/tasks/{task_id}", response_model=TaskResponse)
    async def get_task(
        task_id: str, principal: TaskPrincipal = Depends(require("tasks:read"))
    ) -> TaskResponse:
        return await asyncio.to_thread(repository.get, task_id, principal)

    @app.post("/api/v1/tasks/{task_id}/claim", response_model=TaskResponse)
    async def claim_task(
        task_id: str,
        body: TaskMutationRequest,
        principal: TaskPrincipal = Depends(require("tasks:act")),
    ) -> TaskResponse:
        return await asyncio.to_thread(repository.claim, task_id, body, principal)

    @app.post("/api/v1/tasks/{task_id}/complete", response_model=TaskResponse)
    async def complete_task(
        task_id: str,
        body: CompleteTaskRequest,
        principal: TaskPrincipal = Depends(require("tasks:act")),
    ) -> TaskResponse:
        return await asyncio.to_thread(repository.decide, task_id, body.outcome, body, principal)

    @app.post("/api/v1/tasks/{task_id}/approve", response_model=TaskResponse)
    async def approve_task(
        task_id: str,
        body: TaskDecisionRequest,
        principal: TaskPrincipal = Depends(require("tasks:act")),
    ) -> TaskResponse:
        return await asyncio.to_thread(repository.decide, task_id, "approved", body, principal)

    @app.post("/api/v1/tasks/{task_id}/reject", response_model=TaskResponse)
    async def reject_task(
        task_id: str,
        body: TaskDecisionRequest,
        principal: TaskPrincipal = Depends(require("tasks:act")),
    ) -> TaskResponse:
        return await asyncio.to_thread(repository.decide, task_id, "rejected", body, principal)

    @app.post("/api/v1/tasks/{task_id}/reassign", response_model=TaskResponse)
    async def reassign_task(
        task_id: str,
        body: ReassignTaskRequest,
        principal: TaskPrincipal = Depends(require("tasks:manage")),
    ) -> TaskResponse:
        return await asyncio.to_thread(repository.reassign, task_id, body, principal)

    @app.post("/api/v1/tasks/{task_id}/delegate", response_model=TaskResponse)
    async def delegate_task(
        task_id: str,
        body: DelegateTaskRequest,
        principal: TaskPrincipal = Depends(require("tasks:act")),
    ) -> TaskResponse:
        return await asyncio.to_thread(repository.delegate, task_id, body, principal)

    @app.post("/api/v1/tasks/{task_id}/escalate", response_model=TaskResponse)
    async def escalate_task(
        task_id: str,
        body: EscalateTaskRequest,
        principal: TaskPrincipal = Depends(require("tasks:manage")),
    ) -> TaskResponse:
        return await asyncio.to_thread(repository.escalate, task_id, body, principal)

    @app.post("/api/v1/tasks/{task_id}/expire", response_model=TaskResponse)
    async def expire_task(
        task_id: str,
        body: ExpireTaskRequest,
        principal: TaskPrincipal = Depends(require("tasks:manage")),
    ) -> TaskResponse:
        return await asyncio.to_thread(repository.expire, task_id, body, principal)

    return app
