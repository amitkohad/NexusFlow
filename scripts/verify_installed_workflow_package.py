"""Execute a complete installed artifact using its own isolated Python runtime.

Called by build_workflow_packages.py with -I. No repository imports, API modules,
test helpers or separate capability workers are available to this interpreter.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib.util
import json
import os
import shutil
import socket
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import RLock, Thread
from typing import Any, cast
from uuid import uuid4

from contracts import ExecutorPool, PackageRuntimeStartRequest, RuntimeContext, WorkflowRelease
from nexusflow_common.executor import load_executor_settings
from temporalio.common import PinnedVersioningOverride, WorkerDeploymentVersion
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.testing import WorkflowEnvironment
from workflow_executor import load_executor_package, run_executor
from workflow_sdk.packages import resolve_binding


class ReferenceTaskServer(ThreadingHTTPServer):
    """An explicit external HTTP reference used only by isolated artifact tests."""

    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), ReferenceTaskHandler)
        self.tasks: dict[tuple[str, str, str, str], str] = {}
        self.task_keys: dict[str, str] = {}
        self.lock = RLock()


class ReferenceTaskHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        if (
            self.path != "/api/v1/tasks"
            or self.headers.get("Authorization") != "Bearer installed-test"
        ):
            self.send_error(403)
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length < 1 or length > 64 * 1024:
                raise ValueError("Invalid body size")
            body: Any = json.loads(self.rfile.read(length))
            if not isinstance(body, dict) or body.get("task_type") != "approval":
                raise ValueError("Invalid task")
            key = (
                str(body["tenant"]),
                str(body["business_domain"]),
                str(body["application"]),
                str(body["idempotency_key"]),
            )
            identity = json.dumps([body["workflow_id"], body["step_id"]], separators=(",", ":"))
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            self.send_error(422)
            return
        server = cast(ReferenceTaskServer, self.server)
        with server.lock:
            task_id = server.tasks.setdefault(
                key, "HT-" + hashlib.sha256(identity.encode()).hexdigest()[:24]
            )
            server.task_keys[task_id] = key[3]
        echoed = {
            field: body[field]
            for field in (
                "tenant",
                "business_domain",
                "application",
                "workflow_id",
                "first_execution_run_id",
                "step_id",
                "definition_version",
                "correlation_id",
                "business_reference",
                "idempotency_key",
                "task_type",
                "package_id",
                "package_release_id",
                "build_id",
            )
        }
        content = json.dumps({"task_id": task_id, "durable": True, **echoed}).encode()
        self.send_response(201)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def log_message(self, format: str, *args: Any) -> None:
        return


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
    task_server: ReferenceTaskServer | None = None
    task_thread: Thread | None = None
    if name == "customer-adjustment":
        task_server = ReferenceTaskServer()
        task_thread = Thread(target=task_server.serve_forever, daemon=True)
        task_thread.start()
        os.environ["NEXUSFLOW_HUMAN_TASK_SERVICE_URL"] = (
            f"http://127.0.0.1:{task_server.server_address[1]}"
        )
        os.environ["NEXUSFLOW_HUMAN_TASK_SERVICE_TOKEN"] = "installed-test"
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
                    waiting = await handle.query("status")
                    assert task_server is not None
                    task_id = waiting["approval"]["task_id"]
                    with task_server.lock:
                        task_key = task_server.task_keys[task_id]
                    await handle.signal(
                        "approve",
                        {
                            "task_id": task_id,
                            "event_id": "installed-decision-" + uuid4().hex,
                            "idempotency_key": task_key,
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
            if task_server is not None and task_thread is not None:
                task_server.shutdown()
                task_server.server_close()
                task_thread.join(timeout=5)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("package")
    parser.add_argument("release", type=Path)
    arguments = parser.parse_args()
    asyncio.run(verify(arguments.package, arguments.release))


if __name__ == "__main__":
    main()
