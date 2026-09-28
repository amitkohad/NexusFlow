"""Operator API for business package releases and desired executor capacity."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any, Literal, cast

from contracts import (
    ApiBusinessContext,
    ExecutorPool,
    PackageManifest,
    WorkflowPackage,
    WorkflowRelease,
)
from contracts.packages import PackageIdentifier, PackageText
from fastapi import Depends, FastAPI
from pydantic import Field, StrictInt
from workflow_sdk.packages import PackageValidationError

from .backend import WorkflowBackend
from .errors import ApiError
from .package_backend import PackageBackend
from .package_repository import PackageRepository
from .repository import Scope, WorkflowRepository
from .security import Principal
from .service import utcnow


class RegisterPackageRequest(ApiBusinessContext):
    package_id: PackageIdentifier
    name: PackageText
    owner: PackageText
    workflow_types: tuple[PackageIdentifier, ...] = Field(min_length=1, max_length=100)


class PublishReleaseRequest(ApiBusinessContext):
    release: WorkflowRelease
    manifest: PackageManifest


class RouteReleaseRequest(ApiBusinessContext):
    environment: Literal["local", "dev", "test", "prod"] = "local"
    temporal_namespace: PackageText = "default"
    queue_bindings: dict[PackageIdentifier, PackageText] = Field(min_length=1, max_length=100)
    ramp_percentage: StrictInt | None = Field(default=None, ge=0, le=100)


class ConfigurePoolRequest(ApiBusinessContext):
    pool: ExecutorPool


def register_package_routes(
    app: FastAPI,
    repository: WorkflowRepository,
    backend: WorkflowBackend,
    require: Callable[..., Any],
    environment: str,
) -> None:
    packages = PackageRepository(repository)
    runtime = cast(PackageBackend, backend)

    def scope(body: ApiBusinessContext, principal: Principal) -> Scope:
        requested = Scope(body.tenant, body.business_domain, body.application)
        if requested != principal.scope:
            raise ApiError(
                403, "scope_forbidden", "Business context is outside the authenticated scope"
            )
        return requested

    def bindings(
        manifest: PackageManifest, pools: list[ExecutorPool], *, serving_only: bool = True
    ) -> tuple[str, dict[str, str]]:
        serving = [
            p
            for p in pools
            if p.environment == environment and (p.desired_state == "serving" or not serving_only)
        ]
        if not serving:
            raise ApiError(
                409,
                "executor_capacity_required",
                "Configure serving executor pools before release reconciliation",
            )
        namespaces = {p.temporal_namespace for p in serving}
        roles = {
            role
            for pool in serving
            for role in (("workflow", "activity") if pool.role == "mixed" else (pool.role,))
        }
        required_roles = {queue.role for queue in manifest.queues}
        queues: dict[str, str] = {}
        for pool in serving:
            for name, queue in pool.queue_bindings.items():
                if name in queues and queues[name] != queue:
                    raise ApiError(
                        409,
                        "pool_queues_conflict",
                        "Serving pools disagree on logical queue bindings",
                    )
                queues[name] = queue
        if (
            len(namespaces) != 1
            or set(queues) != {q.name for q in manifest.queues}
            or not required_roles <= roles
        ):
            raise ApiError(
                409,
                "executor_roles_incomplete",
                "Serving pools must cover every package role in one namespace",
            )
        return next(iter(namespaces)), queues

    async def evidence(
        principal: Principal, release_id: str
    ) -> tuple[PackageManifest, str, dict[str, str], dict[str, Any]]:
        _, manifest, _ = await asyncio.to_thread(packages.release, principal.scope, release_id)
        pools = await asyncio.to_thread(packages.pools, principal.scope, release_id)
        namespace, queues = bindings(manifest, pools)
        actual = await runtime.inspect_release(manifest, namespace, queues)
        await asyncio.to_thread(
            packages.observe_pools, principal.scope, release_id, actual, utcnow()
        )
        floor = 2 if environment == "prod" else 1
        if not actual["ready"] or any(q["replicas"] < floor for q in actual["queues"]):
            raise ApiError(
                409,
                "release_not_serving",
                "Temporal has not observed the required pollers for every package role",
            )
        return manifest, namespace, queues, actual

    @app.post("/api/v1/packages", status_code=201, response_model=WorkflowPackage)
    async def register(
        body: RegisterPackageRequest, principal: Principal = Depends(require("packages:write"))
    ) -> WorkflowPackage:
        scope(body, principal)
        return await asyncio.to_thread(
            packages.register, WorkflowPackage(**body.model_dump()), principal.subject, utcnow()
        )

    @app.get("/api/v1/packages", response_model=list[WorkflowPackage])
    async def listing(
        principal: Principal = Depends(require("packages:read")),
    ) -> list[WorkflowPackage]:
        return await asyncio.to_thread(packages.packages, principal.scope)

    @app.post("/api/v1/packages/{package_id}/releases", status_code=201)
    async def publish(
        package_id: str,
        body: PublishReleaseRequest,
        principal: Principal = Depends(require("packages:write")),
    ) -> dict[str, Any]:
        requested = scope(body, principal)
        if body.release.package_id != package_id or body.manifest.package_id != package_id:
            raise ApiError(
                422,
                "package_identity_mismatch",
                "URL, manifest and descriptor must identify the same package",
            )
        return await asyncio.to_thread(
            packages.publish, requested, body.release, body.manifest, principal.subject, utcnow()
        )

    @app.get("/api/v1/package-releases/{release_id}")
    async def release(
        release_id: str, principal: Principal = Depends(require("packages:read"))
    ) -> dict[str, Any]:
        descriptor, manifest, status = await asyncio.to_thread(
            packages.release, principal.scope, release_id
        )
        return {
            "release": descriptor.model_dump(mode="json"),
            "manifest": manifest.model_dump(mode="json"),
            "status": status,
        }

    @app.post("/api/v1/package-releases/{release_id}/approve")
    async def approve(
        release_id: str,
        body: ApiBusinessContext,
        principal: Principal = Depends(require("packages:approve")),
    ) -> dict[str, str]:
        await asyncio.to_thread(
            packages.approve, scope(body, principal), release_id, principal.subject, utcnow()
        )
        return {"status": "approved"}

    @app.put("/api/v1/executor-pools/{pool_id}", response_model=ExecutorPool)
    async def pool(
        pool_id: str,
        body: ConfigurePoolRequest,
        principal: Principal = Depends(require("packages:deploy")),
    ) -> ExecutorPool:
        if body.pool.pool_id != pool_id or body.pool.environment != environment:
            raise ApiError(
                422, "pool_identity_mismatch", "Pool URL and environment must match configuration"
            )
        return await asyncio.to_thread(
            packages.put_pool, scope(body, principal), body.pool, principal.subject, utcnow()
        )

    @app.get(
        "/api/v1/package-releases/{release_id}/executor-pools", response_model=list[ExecutorPool]
    )
    async def pools(
        release_id: str, principal: Principal = Depends(require("packages:read"))
    ) -> list[ExecutorPool]:
        return await asyncio.to_thread(packages.pools, principal.scope, release_id)

    @app.post("/api/v1/package-releases/{release_id}/reconcile")
    async def reconcile(
        release_id: str,
        body: ApiBusinessContext,
        principal: Principal = Depends(require("packages:deploy")),
    ) -> dict[str, Any]:
        scope(body, principal)
        manifest, namespace, _, actual = await evidence(principal, release_id)
        routing = await runtime.routing(manifest.worker_deployment_name, namespace)
        confirmed = await asyncio.to_thread(
            packages.confirm_routing, principal.scope, manifest.package_id, environment, routing
        )
        return {"serving": actual, "routing_confirmed": confirmed}

    @app.post("/api/v1/package-releases/{release_id}/promote")
    async def promote(
        release_id: str,
        body: RouteReleaseRequest,
        principal: Principal = Depends(require("packages:deploy")),
    ) -> dict[str, Any]:
        requested = scope(body, principal)
        if body.environment != environment:
            raise ApiError(
                409, "environment_mismatch", "Promotion must target this control-plane environment"
            )
        manifest, namespace, queues, _ = await evidence(principal, release_id)
        if (namespace, queues) != (body.temporal_namespace, body.queue_bindings):
            raise ApiError(
                409,
                "pool_queues_conflict",
                "Promotion queues must match the observed serving pools",
            )
        try:
            await asyncio.to_thread(
                packages.set_routing,
                requested,
                release_id,
                environment,
                namespace,
                queues,
                principal.subject,
                utcnow(),
                body.ramp_percentage,
            )
        except PackageValidationError as exc:
            raise ApiError(
                422, "queue_binding_invalid", "Package queue bindings are invalid"
            ) from exc
        await runtime.route_release(manifest, namespace, ramp_percentage=body.ramp_percentage)
        actual = await runtime.routing(manifest.worker_deployment_name, namespace)
        confirmed = await asyncio.to_thread(
            packages.confirm_routing, requested, manifest.package_id, environment, actual
        )
        if not confirmed:
            raise ApiError(
                409,
                "routing_not_confirmed",
                "Desired routing differs from Temporal; reconcile before admission",
            )
        return {
            "status": "current" if body.ramp_percentage is None else "ramping",
            "routing_confirmed": True,
        }

    @app.post("/api/v1/package-releases/{release_id}/retire")
    async def retire(
        release_id: str,
        body: ApiBusinessContext,
        principal: Principal = Depends(require("packages:deploy")),
    ) -> dict[str, str]:
        requested = scope(body, principal)
        _, manifest, _ = await asyncio.to_thread(packages.release, requested, release_id)
        pools = await asyncio.to_thread(packages.pools, requested, release_id)
        namespace, queues = bindings(manifest, pools, serving_only=False)
        actual = await runtime.inspect_release(manifest, namespace, queues)
        if actual["drainage_status"] != 2:
            raise ApiError(
                409,
                "release_not_drained",
                "Temporal must confirm the version is drained before retirement",
            )
        await asyncio.to_thread(packages.retire, requested, release_id, principal.subject, utcnow())
        return {"status": "retired"}
