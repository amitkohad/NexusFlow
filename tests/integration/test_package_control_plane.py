"""Business HTTP admission with actual generic package executors and routing."""

from __future__ import annotations

import asyncio
import os
import shutil
from contextlib import AsyncExitStack
from dataclasses import replace
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import pytest
from contracts import ExecutorPool, WorkflowRelease
from nexusflow_common.executor import load_executor_settings
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.runtime import Runtime, TelemetryConfig
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from workflow_api.api import PERMISSIONS, create_app
from workflow_api.package_backend import PackageTemporalBackend
from workflow_api.repository import WorkflowRepository
from workflow_api.security import Principal, StaticTokenAuthenticator
from workflow_executor import load_executor_package
from workflow_executor.boundary import PackageBoundaryInterceptor
from workflow_executor.host import registrations_for_pool, worker_options
from workflow_sdk.packages import manifest_hash

pytestmark = [pytest.mark.integration, pytest.mark.e2e, pytest.mark.asyncio(loop_scope="module")]


async def test_package_http_admission_observes_two_generic_replicas_and_completes() -> None:
    cli = os.environ.get("TEMPORAL_CLI_PATH") or shutil.which("temporal")
    assert cli and Path(cli).is_file(), "Set TEMPORAL_CLI_PATH"
    package = load_executor_package("customer-adjustment", development_source=True)
    manifest = package.manifest
    release = WorkflowRelease(
        package_release_id="customer-adjustment-0.1.0",
        package_id=manifest.package_id,
        package_version=manifest.package_version,
        build_id=manifest.build_id,
        manifest_hash=package.manifest_hash,
        artifact_digest="sha256:" + "a" * 64,
    )
    scope = {"tenant": "demo", "business_domain": "customer-services", "application": "adjustments"}
    principal = Principal(subject="operator", permissions=PERMISSIONS, **scope)
    repository = WorkflowRepository("sqlite://")
    repository.create_schema()
    pool = ExecutorPool(
        pool_id="customer-adjustment-mixed",
        package_id=manifest.package_id,
        package_release_id=release.package_release_id,
        worker_deployment_name=manifest.worker_deployment_name,
        build_id=manifest.build_id,
        replicas=2,
        max_replicas=4,
        queue_bindings={q.name: "customer-adjustment-tq" for q in manifest.queues},
    )
    try:
        async with await WorkflowEnvironment.start_local(
            dev_server_existing_path=cli,
            dev_server_log_level="error",
            ui=False,
            data_converter=pydantic_data_converter,
        ) as server:
            backend = PackageTemporalBackend(server.client, runtime_profile="package")
            app = create_app(
                repository,
                backend,
                runtime_profile="package",
                authenticator=StaticTokenAuthenticator({"operator-token": principal}),
            )
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://local",
                headers={"Authorization": "Bearer operator-token"},
            ) as http:

                async def post(
                    path: str, body: dict[str, Any], status: int = 200
                ) -> dict[str, Any]:
                    response = await http.post(path, json=body)
                    assert response.status_code == status, response.text
                    return response.json()

                for revision in manifest.definitions:
                    await post(
                        "/api/v1/workflows",
                        {
                            **scope,
                            "definition_id": revision.definition_id,
                            "workflow_type": revision.workflow_type,
                            "version": revision.version,
                            "owner": "customer-team",
                            "definition_document": revision.definition_document.model_dump(
                                mode="json"
                            ),
                        },
                        201,
                    )
                    await post(
                        f"/api/v1/definitions/{revision.workflow_type}/{revision.version}/approve",
                        scope,
                    )
                    await post(
                        f"/api/v1/definitions/{revision.workflow_type}/{revision.version}/promote",
                        {**scope, "environment": "local"},
                    )
                await post(
                    "/api/v1/packages",
                    {
                        **scope,
                        "package_id": manifest.package_id,
                        "name": "Customer Adjustment",
                        "owner": "customer-team",
                        "workflow_types": ["customer-adjustment"],
                    },
                    201,
                )
                await post(
                    f"/api/v1/packages/{manifest.package_id}/releases",
                    {
                        **scope,
                        "release": release.model_dump(mode="json"),
                        "manifest": manifest.model_dump(mode="json"),
                    },
                    201,
                )
                await post(f"/api/v1/package-releases/{release.package_release_id}/approve", scope)
                response = await http.put(
                    f"/api/v1/executor-pools/{pool.pool_id}",
                    json={**scope, "pool": pool.model_dump(mode="json")},
                )
                assert response.status_code == 200, response.text
                assert response.json()["observed_state"] == "pending"
                async with AsyncExitStack() as stack:
                    for replica in range(2):
                        settings = load_executor_settings(
                            pool, {"NEXUSFLOW_EXECUTOR_INSTANCE_ID": f"replica-{replica}"}
                        )
                        worker_client = await Client.connect(
                            server.client.service_client.config.target_host,
                            data_converter=pydantic_data_converter,
                            identity=settings.identity,
                            runtime=Runtime(telemetry=TelemetryConfig()),
                        )
                        for queue, handlers in registrations_for_pool(package, settings).items():
                            await stack.enter_async_context(
                                Worker(
                                    worker_client,
                                    task_queue=queue,
                                    workflows=handlers["workflows"],
                                    activities=handlers["activities"],
                                    interceptors=[PackageBoundaryInterceptor(manifest, pool)],
                                    **worker_options(settings),
                                )
                            )
                    evidence: dict[str, Any] = {}
                    for _ in range(300):
                        try:
                            evidence = await backend.inspect_release(
                                manifest, "default", pool.queue_bindings
                            )
                            if evidence["ready"] and all(
                                q["replicas"] == 2 for q in evidence["queues"]
                            ):
                                break
                        except Exception:
                            pass
                        await asyncio.sleep(0.1)
                    assert evidence.get("ready"), evidence
                    assert all(q["replicas"] == 2 for q in evidence["queues"]), evidence
                    await post(
                        f"/api/v1/package-releases/{release.package_release_id}/promote",
                        {**scope, "queue_bindings": pool.queue_bindings},
                    )
                    request = {
                        **scope,
                        "business_reference": "CASE-1",
                        "correlation_id": uuid4().hex,
                        "idempotency_key": uuid4().hex,
                        "request": {"amount": 100, "customer_id": "CUST-1"},
                    }
                    started = await post(
                        "/api/v1/workflows/customer-adjustment/start", request, 202
                    )
                    result = await asyncio.wait_for(
                        server.client.get_workflow_handle(started["workflow_id"]).result(), 20
                    )
                    assert result["state"] == "COMPLETED"
                    response = await http.get(f"/api/v1/workflows/{started['workflow_id']}/status")
                    assert response.status_code == 200, response.text
                    assert response.json()["state"] == "COMPLETED"
                    stored = repository.get_execution(principal.scope, started["workflow_id"])
                    assert stored.package_binding == release_binding(
                        repository, principal, started["workflow_id"]
                    )
                    assert stored.observed_initial_build_id == manifest.build_id
                    assert stored.observed_current_build_id == manifest.build_id
                    assert "build_id" not in response.text
    finally:
        repository.close()


def release_binding(repository: WorkflowRepository, principal: Principal, workflow_id: str) -> Any:
    binding = repository.get_execution(principal.scope, workflow_id).package_binding
    assert binding is not None
    assert binding.package_id == "customer-adjustment"
    assert binding.definition_version == "1.0"
    return binding


async def test_modern_backend_promotes_ramps_and_clears_previous_ramp() -> None:
    cli = os.environ.get("TEMPORAL_CLI_PATH") or shutil.which("temporal")
    assert cli and Path(cli).is_file(), "Set TEMPORAL_CLI_PATH"
    original = load_executor_package("validation-reference", development_source=True)
    namespace = "default"
    deployment = "control-plane-routing-" + uuid4().hex
    queue = deployment + "-tq"
    first_manifest = original.manifest.model_copy(update={"worker_deployment_name": deployment})
    second_manifest = first_manifest.model_copy(
        update={"build_id": "validation-reference-0.2.0", "package_version": "0.2.0"}
    )
    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=cli,
        dev_server_log_level="error",
        ui=False,
        data_converter=pydantic_data_converter,
    ) as server:
        backend = PackageTemporalBackend(server.client)
        async with AsyncExitStack() as stack:
            for manifest in (first_manifest, second_manifest):
                package = replace(
                    original, manifest=manifest, manifest_hash=manifest_hash(manifest)
                )
                pool = ExecutorPool(
                    pool_id=manifest.build_id,
                    package_id=manifest.package_id,
                    package_release_id=manifest.build_id,
                    worker_deployment_name=deployment,
                    build_id=manifest.build_id,
                    queue_bindings={q.name: queue for q in manifest.queues},
                )
                settings = load_executor_settings(pool, {})
                for concrete, handlers in registrations_for_pool(package, settings).items():
                    await stack.enter_async_context(
                        Worker(
                            server.client,
                            task_queue=concrete,
                            workflows=handlers["workflows"],
                            activities=handlers["activities"],
                            interceptors=[PackageBoundaryInterceptor(manifest, pool)],
                            **worker_options(settings),
                        )
                    )
            for manifest in (first_manifest, second_manifest):
                for _ in range(300):
                    try:
                        actual = await backend.inspect_release(
                            manifest, namespace, {q.name: queue for q in manifest.queues}
                        )
                        if actual["ready"]:
                            break
                    except Exception:
                        pass
                    await asyncio.sleep(0.1)
                else:
                    pytest.fail("Worker Deployment Version did not register all role queues")
            await backend.route_release(first_manifest, namespace)
            assert await backend.routing(deployment, namespace) == (
                first_manifest.build_id,
                None,
                0.0,
            )
            await backend.route_release(second_manifest, namespace, ramp_percentage=100)
            assert await backend.routing(deployment, namespace) == (
                first_manifest.build_id,
                second_manifest.build_id,
                100.0,
            )
            await backend.route_release(second_manifest, namespace)
            assert await backend.routing(deployment, namespace) == (
                second_manifest.build_id,
                None,
                0.0,
            )
