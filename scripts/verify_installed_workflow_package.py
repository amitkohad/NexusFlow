"""Execute a complete installed artifact using its own isolated Python runtime.

Called by build_workflow_packages.py with -I. No repository imports, API modules,
test helpers or separate capability workers are available to this interpreter.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import os
import shutil
import socket
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

from contracts import ExecutorPool, PackageRuntimeStartRequest, RuntimeContext, WorkflowRelease
from nexusflow_common.executor import load_executor_settings
from temporalio.common import PinnedVersioningOverride, WorkerDeploymentVersion
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.testing import WorkflowEnvironment
from workflow_executor import load_executor_package, run_executor
from workflow_sdk.packages import resolve_binding


def available_port() -> int:
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        return int(reservation.getsockname()[1])


async def verify(name: str, release_path: Path) -> None:
    cli = os.environ.get("TEMPORAL_CLI_PATH") or shutil.which("temporal")
    if not cli or not Path(cli).is_file():
        raise RuntimeError(
            "Installed-package execution requires local Temporal CLI or TEMPORAL_CLI_PATH"
        )
    assert all(
        importlib.util.find_spec(module) is None
        for module in (
            "workflow_api",
            "app",
            "validation_worker",
            "notification_worker",
            "integration_worker",
            "human_task_worker",
            "sample_business_worker",
        )
    )
    other_module = (
        "validation_reference_package"
        if name == "customer-adjustment"
        else "customer_adjustment_package"
    )
    assert importlib.util.find_spec(other_module) is None
    package = load_executor_package(name)
    manifest = package.manifest
    release = WorkflowRelease.model_validate_json(release_path.read_text())
    definition = manifest.definitions[0]
    binding = resolve_binding(manifest, release, definition)
    pool = ExecutorPool(
        pool_id=name + "-installed-mixed",
        package_id=name,
        package_release_id=release.package_release_id,
        worker_deployment_name=manifest.worker_deployment_name,
        build_id=manifest.build_id,
        queue_bindings={"workflow": name + "-tq", "activities": name + "-tq"},
        replicas=2,
        min_replicas=2,
        max_replicas=2,
        workflow_task_slots=2,
        activity_task_slots=2,
        workflow_task_pollers=2,
        activity_task_pollers=2,
        shutdown_grace_seconds=5,
    )
    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=cli,
        dev_server_log_level="error",
        ui=False,
        data_converter=pydantic_data_converter,
    ) as environment:
        stops = [asyncio.Event(), asyncio.Event()]
        ready = [asyncio.Event(), asyncio.Event()]
        tasks = []
        for replica in range(2):
            settings = load_executor_settings(
                pool,
                {
                    "TEMPORAL_ADDRESS": environment.client.service_client.config.target_host,
                    "NEXUSFLOW_PROBE_PORT": str(available_port()),
                    "NEXUSFLOW_EXECUTOR_INSTANCE_ID": "installed-" + str(replica),
                },
            )
            tasks.append(
                asyncio.create_task(
                    run_executor(
                        package,
                        settings,
                        shutdown_event=stops[replica],
                        ready_event=ready[replica],
                    )
                )
            )
        try:
            await asyncio.wait_for(asyncio.gather(*(event.wait() for event in ready)), timeout=15)
            decisions = (True, False) if name == "customer-adjustment" else (True,)
            invoked: set[str] = set()
            for decision in decisions:
                request = PackageRuntimeStartRequest(
                    context=RuntimeContext(
                        tenant="installed-test",
                        business_domain="finance",
                        application="adjustments",
                        workflow_type=definition.workflow_type,
                        definition_id=definition.definition_id,
                        definition_version=definition.version,
                        business_reference="installed-" + uuid4().hex,
                        correlation_id="installed-correlation",
                        actor="installed-artifact-test",
                    ),
                    definition_document=definition.definition_document,
                    release_binding=binding,
                    request={
                        "amount": 7500 if name == "customer-adjustment" else 1000,
                        "simulate_transient_failure": name == "customer-adjustment",
                    },
                )
                handle = await environment.client.start_workflow(
                    definition.runtime_workflow_type,
                    request,
                    id="installed-" + uuid4().hex,
                    task_queue=binding.workflow_task_queue,
                    execution_timeout=timedelta(seconds=40),
                    versioning_override=PinnedVersioningOverride(
                        WorkerDeploymentVersion(manifest.worker_deployment_name, manifest.build_id)
                    ),
                )
                if name == "customer-adjustment":
                    async with asyncio.timeout(20):
                        while (await handle.query("status"))["state"] != "WAITING_FOR_APPROVAL":
                            await asyncio.sleep(0.05)
                    await handle.signal(
                        "approve",
                        {
                            "approved": decision,
                            "approver": "installed-operator",
                            "comment": "artifact test",
                        },
                    )
                result = await asyncio.wait_for(handle.result(), timeout=20)
                assert result["state"] == ("COMPLETED" if decision else "REJECTED")
                assert result["actual_build_id"] == manifest.build_id
                assert result["release_binding"]["artifact_digest"] == release.artifact_digest
                history = await handle.fetch_history()
                risk_schedules = {
                    event.event_id
                    for event in history.events
                    if event.HasField("activity_task_scheduled_event_attributes")
                    and event.activity_task_scheduled_event_attributes.activity_type.name
                    == "risk_check.pkg.v1"
                }
                if name == "customer-adjustment":
                    risk_starts = [
                        event.activity_task_started_event_attributes
                        for event in history.events
                        if event.HasField("activity_task_started_event_attributes")
                        and event.activity_task_started_event_attributes.scheduled_event_id
                        in risk_schedules
                    ]
                    assert risk_starts and {started.attempt for started in risk_starts} == {2}
                    assert all(
                        started.last_failure.application_failure_info.type == "TechnicalError"
                        for started in risk_starts
                    )
                for event in history.events:
                    if event.HasField("activity_task_scheduled_event_attributes"):
                        scheduled = event.activity_task_scheduled_event_attributes
                        invoked.add(scheduled.activity_type.name)
                        assert scheduled.task_queue.name == binding.activity_bindings[0].task_queue
            assert invoked == {
                registration.activity_name for registration in manifest.activity_registrations
            }
            print("Installed artifact real Temporal execution passed:", name, sorted(invoked))
        finally:
            for stop in stops:
                stop.set()
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=15)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("package")
    parser.add_argument("release", type=Path)
    arguments = parser.parse_args()
    asyncio.run(verify(arguments.package, arguments.release))


if __name__ == "__main__":
    main()
