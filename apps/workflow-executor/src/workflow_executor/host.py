"""Manifest-driven worker pools with modern Temporal Worker Deployment versions."""

from __future__ import annotations

import asyncio
import json
import logging
import signal
import threading
from contextlib import AsyncExitStack
from datetime import timedelta
from importlib.metadata import entry_points
from typing import Any

from nexusflow_common.errors import ConfigurationError
from nexusflow_common.executor import ExecutorSettings
from nexusflow_common.worker import ProbeServer
from temporalio.api.deployment.v1 import WorkerDeploymentVersion as DeploymentVersion
from temporalio.api.workflowservice.v1 import DescribeWorkerDeploymentVersionRequest
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import (
    PollerBehaviorSimpleMaximum,
    Worker,
    WorkerDeploymentConfig,
    WorkerDeploymentVersion,
)
from workflow_sdk.packages import LoadedPackage, load_installed_package

from .boundary import PackageBoundaryInterceptor


def load_executor_package(package_name: str, *, development_source: bool = False) -> LoadedPackage:
    """Select installed operator-trusted distribution metadata, never payload imports."""
    candidates = tuple(entry_points(group="nexusflow.workflow_packages"))
    selected = [entry for entry in candidates if entry.name == package_name]
    if len(selected) == 1:
        module, separator, attribute = selected[0].value.partition(":")
        if not separator or attribute != "registration":
            raise ConfigurationError("Installed workflow package entrypoint is invalid")
    elif development_source and package_name in {"customer-adjustment", "validation-reference"}:
        module = {
            "customer-adjustment": "customer_adjustment_package",
            "validation-reference": "validation_reference_package",
        }[package_name]
    else:
        raise ConfigurationError("Select exactly one installed trusted workflow package")
    return load_installed_package(
        module,
        trusted_package_modules=(module,),
        verify_dependencies=not development_source,
    )


def registrations_for_pool(
    package: LoadedPackage, settings: ExecutorSettings
) -> dict[str, dict[str, list[Any]]]:
    """Every replica gets the complete compatible registration set for its role."""
    manifest = package.manifest
    pool = settings.pool
    if (
        pool.package_id != manifest.package_id
        or pool.build_id != manifest.build_id
        or pool.worker_deployment_name != manifest.worker_deployment_name
    ):
        raise ConfigurationError("Executor pool does not match its installed package release")
    declared = {queue.name: queue.role for queue in manifest.queues}
    if set(pool.queue_bindings) != set(declared):
        raise ConfigurationError("Executor pool must resolve every package-owned logical queue")
    grouped: dict[str, dict[str, list[Any]]] = {}
    if pool.role in {"mixed", "workflow"}:
        for registration, handler in zip(
            manifest.workflow_registrations, package.workflows, strict=True
        ):
            queue = pool.queue_bindings[registration.logical_queue]
            group = grouped.setdefault(queue, {"workflows": [], "activities": []})
            group["workflows"].append(handler)
    if pool.role in {"mixed", "activity"}:
        for activity_registration, activity_handler in zip(
            manifest.activity_registrations, package.activities, strict=True
        ):
            queue = pool.queue_bindings[activity_registration.logical_queue]
            group = grouped.setdefault(queue, {"workflows": [], "activities": []})
            group["activities"].append(activity_handler)
    if not grouped or not any(
        group["workflows"] or group["activities"] for group in grouped.values()
    ):
        raise ConfigurationError("Executor role has no compatible package registrations")
    if pool.desired_state not in {"serving", "retained"}:
        raise ConfigurationError("Only serving or retained executor pools may poll")
    return grouped


def worker_options(settings: ExecutorSettings) -> dict[str, Any]:
    pool = settings.pool
    return {
        "identity": settings.identity,
        # A single Workflow poller is supported only without a sticky cache.
        "max_cached_workflows": 0 if pool.workflow_task_pollers == 1 else 1000,
        "max_concurrent_workflow_tasks": pool.workflow_task_slots,
        "max_concurrent_activities": pool.activity_task_slots,
        "workflow_task_poller_behavior": PollerBehaviorSimpleMaximum(pool.workflow_task_pollers),
        "activity_task_poller_behavior": PollerBehaviorSimpleMaximum(pool.activity_task_pollers),
        "max_activities_per_second": pool.max_activities_per_second,
        "max_task_queue_activities_per_second": pool.max_task_queue_activities_per_second,
        "graceful_shutdown_timeout": timedelta(seconds=pool.shutdown_grace_seconds),
        "disable_eager_activity_execution": True,
        "deployment_config": WorkerDeploymentConfig(
            version=WorkerDeploymentVersion(pool.worker_deployment_name, pool.build_id),
            use_worker_versioning=True,
        ),
    }


async def run_executor(
    package: LoadedPackage,
    settings: ExecutorSettings,
    *,
    shutdown_event: asyncio.Event | None = None,
    ready_event: asyncio.Event | None = None,
) -> None:
    """Run one replica; deployment tooling owns the desired replica count."""
    grouped = registrations_for_pool(package, settings)
    options = worker_options(settings)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logger = logging.getLogger("nexusflow.executors")
    stop = shutdown_event if shutdown_event is not None else asyncio.Event()
    loop = asyncio.get_running_loop()
    previous_signals: dict[signal.Signals, Any] = {}
    if shutdown_event is None and threading.current_thread() is threading.main_thread():
        for name in (signal.SIGINT, signal.SIGTERM):
            previous_signals[name] = signal.signal(
                name, lambda signum, frame: loop.call_soon_threadsafe(stop.set)
            )
    client: Client | None = None
    workers: list[Worker] = []
    ready = False

    async def is_ready() -> bool:
        if not ready or client is None or not workers or not all(w.is_running for w in workers):
            return False
        try:
            if not await client.service_client.check_health(timeout=timedelta(seconds=2)):
                return False
            response = await client.workflow_service.describe_worker_deployment_version(
                DescribeWorkerDeploymentVersionRequest(
                    namespace=settings.pool.temporal_namespace,
                    deployment_version=DeploymentVersion(
                        deployment_name=settings.pool.worker_deployment_name,
                        build_id=settings.pool.build_id,
                    ),
                ),
                timeout=timedelta(seconds=2),
            )
            members = {
                (queue.name, queue.type)
                for queue in response.worker_deployment_version_info.task_queue_infos
            }
            required = {
                (queue, task_type)
                for queue, registrations in grouped.items()
                for task_type, kind in ((1, "workflows"), (2, "activities"))
                if registrations[kind]
            }
            return required <= members
        except Exception:
            return False

    probes = ProbeServer(settings.probe_host, settings.probe_port, is_ready)
    try:
        await probes.start()
        try:
            client = await Client.connect(
                settings.connection.temporal_address,
                namespace=settings.pool.temporal_namespace,
                tls=settings.connection.temporal_tls,
                data_converter=pydantic_data_converter,
                identity=settings.identity,
            )
        except Exception:
            raise RuntimeError("Executor runtime connection failed") from None
        async with AsyncExitStack() as stack:
            for queue, registrations in grouped.items():
                worker = Worker(
                    client,
                    task_queue=queue,
                    workflows=registrations["workflows"],
                    activities=registrations["activities"],
                    **options,
                    interceptors=[PackageBoundaryInterceptor(package.manifest, settings.pool)],
                )
                await stack.enter_async_context(worker)
                workers.append(worker)
            ready = True
            # SDK startup precedes server-side version queue registration. Wait
            # for membership before announcing that this replica can serve it.
            async with asyncio.timeout(30):
                while not await is_ready():
                    if stop.is_set():
                        return
                    await asyncio.sleep(0.05)
            if ready_event is not None:
                ready_event.set()
            logger.info(
                json.dumps(
                    {
                        "event": "executor_ready",
                        "package_id": settings.pool.package_id,
                        "package_release_id": settings.pool.package_release_id,
                        "build_id": settings.pool.build_id,
                        "pool_id": settings.pool.pool_id,
                        "role": settings.pool.role,
                        "instance_id": settings.instance_id,
                        "task_queues": sorted(grouped),
                        "manifest_hash": package.manifest_hash,
                    }
                )
            )
            await stop.wait()
            ready = False
            if ready_event is not None:
                ready_event.clear()
            logger.info(json.dumps({"event": "executor_draining", "identity": settings.identity}))
    finally:
        ready = False
        if ready_event is not None:
            ready_event.clear()
        try:
            await probes.close()
        finally:
            for name, previous in previous_signals.items():
                signal.signal(name, previous)
