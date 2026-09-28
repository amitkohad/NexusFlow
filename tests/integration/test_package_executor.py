"""Actual generic executor replicas, owned role queues and bounded Activity slots."""

from __future__ import annotations

import asyncio
import os
import shutil
import socket
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Literal
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from contracts import ActivityResponse, ExecutorPool, PackageActivityRequest
from nexusflow_common.executor import load_executor_settings
from temporalio import activity
from temporalio.common import PinnedVersioningOverride, WorkerDeploymentVersion
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.testing import WorkflowEnvironment
from workflow_executor import run_executor

from tests.package_fixtures import source_package, start_request

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="module")]


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def executor_environment() -> AsyncIterator[WorkflowEnvironment]:
    cli_path = os.environ.get("TEMPORAL_CLI_PATH") or shutil.which("temporal")
    if not cli_path or not Path(cli_path).is_file():
        pytest.fail("Package executor integration requires a local Temporal CLI")
    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=cli_path,
        dev_server_log_level="error",
        ui=False,
        data_converter=pydantic_data_converter,
    ) as environment:
        yield environment


def available_port() -> int:
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        return int(reservation.getsockname()[1])


def executor_pool(
    name: str, role: Literal["mixed", "workflow", "activity"] = "mixed", *, split: bool = False
) -> ExecutorPool:
    package = source_package(name)
    manifest = package.manifest
    queues = (
        {"workflow": name + "-workflow-tq", "activities": name + "-activities-tq"}
        if split
        else {
            "workflow": name + "-tq",
            "activities": name + "-tq",
        }
    )
    return ExecutorPool(
        pool_id=name + "-" + role,
        package_id=name,
        package_release_id=name + "-" + manifest.package_version,
        role=role,
        worker_deployment_name=manifest.worker_deployment_name,
        build_id=manifest.build_id,
        queue_bindings=queues,
        activity_task_slots=1,
        workflow_task_slots=2,
        activity_task_pollers=1,
        workflow_task_pollers=1,
        shutdown_grace_seconds=5,
    )


async def wait_probe(port: int, status: int) -> None:
    async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", trust_env=False) as client:
        async with asyncio.timeout(10):
            while True:
                try:
                    if (await client.get("/ready")).status_code == status:
                        return
                except httpx.ConnectError:
                    pass
                await asyncio.sleep(0.05)


async def test_two_replicas_enforce_activity_slots_and_drain_before_probe_close(
    executor_environment: WorkflowEnvironment,
) -> None:
    package = source_package("validation-reference")
    pool = executor_pool("validation-reference")
    stops = [asyncio.Event(), asyncio.Event()]
    releases = [asyncio.Event(), asyncio.Event()]
    started = [asyncio.Event(), asyncio.Event()]
    active = [0, 0]
    peak = [0, 0]
    consumed: list[int] = []
    processes: list[asyncio.Task[None]] = []
    ports: list[int] = []

    def delayed_validation(
        replica: int,
    ) -> Callable[[PackageActivityRequest], Awaitable[ActivityResponse]]:
        @activity.defn(name="validate_request.pkg.v1")
        async def handler(payload: PackageActivityRequest) -> ActivityResponse:
            active[replica] += 1
            peak[replica] = max(peak[replica], active[replica])
            consumed.append(replica)
            started[replica].set()
            try:
                await releases[replica].wait()
                return ActivityResponse(
                    capability="validate_request", output={"valid": True, "replica": replica}
                )
            finally:
                active[replica] -= 1

        return handler

    try:
        for replica in range(2):
            port = available_port()
            ports.append(port)
            settings = load_executor_settings(
                pool,
                {
                    "TEMPORAL_ADDRESS": executor_environment.client.service_client.config.target_host,
                    "NEXUSFLOW_PROBE_PORT": str(port),
                    "NEXUSFLOW_EXECUTOR_INSTANCE_ID": f"replica-{replica}",
                },
            )
            modified = replace(package, activities=(delayed_validation(replica),))
            processes.append(
                asyncio.create_task(run_executor(modified, settings, shutdown_event=stops[replica]))
            )
        await asyncio.gather(*(wait_probe(port, 200) for port in ports))
        handles = [
            await executor_environment.client.start_workflow(
                "PackageWorkflowV1",
                start_request(package),
                id=f"replica-{uuid4().hex}",
                task_queue=pool.queue_bindings["workflow"],
                execution_timeout=timedelta(seconds=30),
                versioning_override=PinnedVersioningOverride(
                    WorkerDeploymentVersion(pool.worker_deployment_name, pool.build_id)
                ),
            )
            for _ in range(4)
        ]
        await asyncio.wait_for(asyncio.gather(*(event.wait() for event in started)), timeout=15)
        assert set(consumed) == {0, 1}
        assert peak == [1, 1] and active == [1, 1]
        stops[0].set()
        await wait_probe(ports[0], 503)
        assert not processes[0].done()
        async with httpx.AsyncClient(trust_env=False) as probes:
            assert (await probes.get(f"http://127.0.0.1:{ports[0]}/health")).status_code == 200
        releases[0].set()
        await asyncio.wait_for(processes[0], timeout=10)
        releases[1].set()
        results = await asyncio.wait_for(
            asyncio.gather(*(handle.result() for handle in handles)), timeout=15
        )
        assert all(result["state"] == "COMPLETED" for result in results)
        assert peak == [1, 1]
        async with httpx.AsyncClient(trust_env=False) as probes:
            with pytest.raises(httpx.ConnectError):
                await probes.get(f"http://127.0.0.1:{ports[0]}/health")
    finally:
        for event in (*stops, *releases):
            event.set()
        if processes:
            await asyncio.wait_for(asyncio.gather(*processes), timeout=15)


async def test_same_artifact_workflow_and_activity_roles_route_across_owned_queues(
    executor_environment: WorkflowEnvironment,
) -> None:
    package = source_package("customer-adjustment")
    processes: list[asyncio.Task[None]] = []
    stops = [asyncio.Event(), asyncio.Event()]
    workflow_pool = executor_pool("customer-adjustment", "workflow", split=True)
    roles: tuple[Literal["workflow", "activity"], ...] = ("workflow", "activity")
    try:
        for role, stop in zip(roles, stops, strict=True):
            pool = executor_pool("customer-adjustment", role, split=True)
            port = available_port()
            settings = load_executor_settings(
                pool,
                {
                    "TEMPORAL_ADDRESS": executor_environment.client.service_client.config.target_host,
                    "NEXUSFLOW_PROBE_PORT": str(port),
                },
            )
            processes.append(
                asyncio.create_task(run_executor(package, settings, shutdown_event=stop))
            )
            await wait_probe(port, 200)
        request = start_request(package, queue_bindings=workflow_pool.queue_bindings)
        handle = await executor_environment.client.start_workflow(
            "PackageWorkflowV1",
            request,
            id=f"role-pools-{uuid4().hex}",
            task_queue=workflow_pool.queue_bindings["workflow"],
            execution_timeout=timedelta(seconds=30),
            versioning_override=PinnedVersioningOverride(
                WorkerDeploymentVersion(
                    workflow_pool.worker_deployment_name, workflow_pool.build_id
                )
            ),
        )
        result = await asyncio.wait_for(handle.result(), timeout=15)
        assert result["state"] == "COMPLETED"
        assert result["actual_build_id"] == workflow_pool.build_id
        history = await handle.fetch_history()
        scheduled = [
            event.activity_task_scheduled_event_attributes
            for event in history.events
            if event.HasField("activity_task_scheduled_event_attributes")
        ]
        assert {event.task_queue.name for event in scheduled} == {
            workflow_pool.queue_bindings["activities"]
        }
        assert {event.activity_type.name for event in scheduled} == {
            "validate_request.pkg.v1",
            "risk_check.pkg.v1",
            "post_adjustment.pkg.v1",
            "send_notification.pkg.v1",
        }
    finally:
        for stop in stops:
            stop.set()
        if processes:
            await asyncio.wait_for(asyncio.gather(*processes), timeout=15)
