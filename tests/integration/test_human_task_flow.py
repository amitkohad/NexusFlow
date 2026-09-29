"""Durable human-task decisions through the installed package executor and Temporal."""

from __future__ import annotations

import asyncio
import os
import shutil
import socket
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import nexusflow_activities
import pytest
import pytest_asyncio
import uvicorn
from contracts import ActivityResponse, DefinitionDocument, ExecutorPool, PackageActivityRequest
from fastapi import FastAPI
from httpx import AsyncClient
from human_task_service.api import create_app
from human_task_service.dispatch import TemporalTaskDispatcher
from human_task_service.models import TaskAuditRow, TaskOutboxRow
from human_task_service.repository import TaskRepository
from human_task_service.security import StaticTokenAuthenticator, TaskPrincipal
from nexusflow_common.executor import load_executor_settings
from sqlalchemy import select
from temporalio import activity
from temporalio.api.workflowservice.v1 import SetWorkerDeploymentCurrentVersionRequest
from temporalio.client import Client, WorkflowHandle
from temporalio.common import PinnedVersioningOverride, WorkerDeploymentVersion
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.testing import WorkflowEnvironment
from workflow_executor import run_executor
from workflow_sdk.packages import LoadedPackage, definition_hash, manifest_hash, validate_manifest

from tests.package_fixtures import source_package, start_request

pytestmark = [pytest.mark.integration, pytest.mark.e2e, pytest.mark.asyncio(loop_scope="module")]

SCOPE = {"tenant": "test-tenant", "business_domain": "finance", "application": "adjustments"}
SERVICE_TOKEN = "integration-service-token-000000000"
APPROVER_TOKEN = "integration-approver-token-00000000"
MANAGER_TOKEN = "integration-manager-token-000000000"


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def temporal_environment() -> AsyncIterator[WorkflowEnvironment]:
    cli_path = os.environ.get("TEMPORAL_CLI_PATH") or shutil.which("temporal")
    if not cli_path or not Path(cli_path).is_file():
        pytest.fail("Human-task integration requires a local Temporal CLI")
    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=cli_path,
        dev_server_log_level="error",
        ui=False,
        data_converter=pydantic_data_converter,
    ) as environment:
        yield environment


def _available_port() -> int:
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        return int(reservation.getsockname()[1])


@asynccontextmanager
async def _task_http(app: FastAPI) -> AsyncIterator[str]:
    port = _available_port()
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, lifespan="on", log_level="error")
    )
    process = asyncio.create_task(server.serve())
    try:
        async with asyncio.timeout(10):
            while not server.started:
                if process.done():
                    await process
                    raise RuntimeError("Human-task HTTP service did not start")
                await asyncio.sleep(0.02)
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        await asyncio.wait_for(process, 10)


def _pool(package: LoadedPackage, queue: str) -> ExecutorPool:
    manifest = package.manifest
    return ExecutorPool(
        pool_id=f"{manifest.package_id}-{manifest.package_version}-human-test",
        package_id=manifest.package_id,
        package_release_id=f"{manifest.package_id}-{manifest.package_version}",
        worker_deployment_name=manifest.worker_deployment_name,
        build_id=manifest.build_id,
        queue_bindings={item.name: queue for item in manifest.queues},
        workflow_task_pollers=1,
        activity_task_pollers=1,
        workflow_task_slots=2,
        activity_task_slots=2,
        shutdown_grace_seconds=5,
    )


@asynccontextmanager
async def _executor(
    package: LoadedPackage,
    pool: ExecutorPool,
    environment: WorkflowEnvironment,
    instance: str,
) -> AsyncIterator[None]:
    stop = asyncio.Event()
    ready = asyncio.Event()
    settings = load_executor_settings(
        pool,
        {
            "TEMPORAL_ADDRESS": environment.client.service_client.config.target_host,
            "NEXUSFLOW_PROBE_PORT": str(_available_port()),
            "NEXUSFLOW_EXECUTOR_INSTANCE_ID": instance,
        },
    )
    process = asyncio.create_task(
        run_executor(package, settings, shutdown_event=stop, ready_event=ready)
    )
    try:
        async with asyncio.timeout(15):
            while not ready.is_set():
                if process.done():
                    await process
                    raise RuntimeError("Package executor stopped before readiness")
                await asyncio.sleep(0.02)
        yield
    finally:
        stop.set()
        await asyncio.wait_for(process, 15)


async def _start_customer(
    client: Client,
    package: LoadedPackage,
    pool: ExecutorPool,
    *,
    amount: int = 6000,
    workflow_id: str | None = None,
) -> WorkflowHandle[Any, dict[str, Any]]:
    return await client.start_workflow(
        "PackageWorkflowV1",
        start_request(package, amount, queue_bindings=pool.queue_bindings),
        id=workflow_id or "human-task-" + uuid4().hex,
        task_queue=pool.queue_bindings["workflow"],
        execution_timeout=timedelta(seconds=90),
        versioning_override=PinnedVersioningOverride(
            WorkerDeploymentVersion(pool.worker_deployment_name, pool.build_id)
        ),
    )


async def _wait_status(handle: WorkflowHandle[Any, dict[str, Any]], state: str) -> dict[str, Any]:
    async with asyncio.timeout(15):
        while True:
            status = await handle.query("status")
            if status["state"] == state:
                return status
            await asyncio.sleep(0.05)


def _task_app(database: Path, client: Client) -> tuple[FastAPI, TaskRepository]:
    repository = TaskRepository(f"sqlite:///{database.as_posix()}")
    repository.create_schema()
    authenticator = StaticTokenAuthenticator(
        {
            SERVICE_TOKEN: TaskPrincipal(
                subject="package-adapter",
                permissions=frozenset({"tasks:create", "tasks:read"}),
                groups=frozenset(),
                **SCOPE,
            ),
            APPROVER_TOKEN: TaskPrincipal(
                subject="alice",
                permissions=frozenset({"tasks:read", "tasks:act"}),
                groups=frozenset({"operations-managers"}),
                **SCOPE,
            ),
            MANAGER_TOKEN: TaskPrincipal(
                subject="manager",
                permissions=frozenset({"tasks:read", "tasks:act", "tasks:manage"}),
                groups=frozenset({"operations-managers"}),
                **SCOPE,
            ),
        }
    )
    return (
        create_app(
            repository,
            TemporalTaskDispatcher(client),
            authenticator=authenticator,
            environment="test",
        ),
        repository,
    )


async def _post(
    http: AsyncClient, path: str, body: dict[str, Any], status: int = 200
) -> dict[str, Any]:
    response = await http.post(path, json=body)
    assert response.status_code == status, response.text
    return response.json()


async def _get_task(http: AsyncClient, task_id: str) -> dict[str, Any]:
    response = await http.get(f"/api/v1/tasks/{task_id}")
    assert response.status_code == 200, response.text
    return response.json()


def _compatible_package(
    original: LoadedPackage, version: str, *, timeout_seconds: int | None = None
) -> LoadedPackage:
    """Fixture release retains all handlers; optional exact new revision shortens a timer."""
    raw = original.manifest.model_dump(mode="json")
    raw["package_version"] = version
    raw["build_id"] = f"{original.manifest.package_id}-{version}"
    if timeout_seconds is not None:
        definition = raw["definitions"][0]
        definition["definition_document"]["steps"]["manager_approval"]["timeout_seconds"] = (
            timeout_seconds
        )
        definition["content_hash"] = definition_hash(
            DefinitionDocument.model_validate(definition["definition_document"])
        )
    manifest = validate_manifest(raw)
    return replace(original, manifest=manifest, manifest_hash=manifest_hash(manifest))


def _isolated_package(original: LoadedPackage) -> LoadedPackage:
    raw = original.manifest.model_dump(mode="json")
    raw["worker_deployment_name"] = "human-task-test-" + uuid4().hex
    manifest = validate_manifest(raw)
    return replace(original, manifest=manifest, manifest_hash=manifest_hash(manifest))


async def _make_current(client: Client, pool: ExecutorPool) -> None:
    await client.workflow_service.set_worker_deployment_current_version(
        SetWorkerDeploymentCurrentVersionRequest(
            namespace=client.namespace,
            deployment_name=pool.worker_deployment_name,
            build_id=pool.build_id,
            identity="human-task-integration",
        )
    )


async def test_package_approval_and_rejection_resume_once_via_durable_task_api(
    temporal_environment: WorkflowEnvironment, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    package = _isolated_package(source_package())
    queue = "human-task-decisions-" + uuid4().hex
    pool = _pool(package, queue)
    app, repository = _task_app(tmp_path / "tasks.db", temporal_environment.client)
    try:
        async with _task_http(app) as url:
            monkeypatch.setenv("NEXUSFLOW_HUMAN_TASK_SERVICE_URL", url)
            monkeypatch.setenv("NEXUSFLOW_HUMAN_TASK_SERVICE_TOKEN", SERVICE_TOKEN)
            async with _executor(package, pool, temporal_environment, "human-replica-1"):
                async with AsyncClient(
                    base_url=url,
                    headers={"Authorization": f"Bearer {APPROVER_TOKEN}"},
                    trust_env=False,
                ) as http:
                    for action, expected in (("approve", "COMPLETED"), ("reject", "REJECTED")):
                        handle = await _start_customer(temporal_environment.client, package, pool)
                        waiting = await _wait_status(handle, "WAITING_FOR_APPROVAL")
                        task_id = waiting["approval"]["task_id"]
                        task = await _get_task(http, task_id)
                        assert task["workflow_id"] == handle.id
                        assert task["step_id"] == "manager_approval"
                        assert task["package_release_id"] == pool.package_release_id
                        assert task["assignee_group"] == "operations-managers"
                        # A caller cannot resolve another task's approval wait by guessing a signal.
                        await handle.signal(
                            "approve",
                            {
                                "task_id": "wrong-task",
                                "event_id": uuid4().hex,
                                "approved": True,
                                "approver": "mallory",
                            },
                        )
                        assert (await handle.query("status"))["state"] == "WAITING_FOR_APPROVAL"
                        await _post(http, f"/api/v1/tasks/{task_id}/claim", {})
                        completed = await _post(
                            http,
                            f"/api/v1/tasks/{task_id}/{action}",
                            {"evidence_reference": "doc://signed-review", "comment": "Reviewed"},
                        )
                        assert completed["actor"] == "alice"
                        await app.state.task_service.dispatch_pending()
                        result = await asyncio.wait_for(handle.result(), 20)
                        assert result["state"] == expected
                        assert result["results"]["manager_approval"]["task_id"] == task_id
                        assert (
                            sum(
                                event["state"]
                                == ("APPROVED" if action == "approve" else "REJECTED")
                                for event in result["transitions"]
                                if event["step"] == "manager_approval"
                            )
                            == 1
                        )
                        duplicate = await http.post(
                            f"/api/v1/tasks/{task_id}/{action}",
                            json={
                                "evidence_reference": "doc://signed-review",
                                "comment": "Reviewed",
                            },
                        )
                        assert duplicate.status_code == 200, duplicate.text
                        assert duplicate.json()["replayed"] is True
                        after = await _get_task(http, task_id)
                        assert after["actor"] == "alice"
                        assert after["evidence_reference"] == "doc://signed-review"
    finally:
        repository.close()


async def test_open_task_survives_executor_restart_and_new_compatible_release(
    temporal_environment: WorkflowEnvironment, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    older = _isolated_package(source_package())
    newer = _compatible_package(older, "0.3.0")
    queue = "human-task-rollover-" + uuid4().hex
    old_pool, new_pool = _pool(older, queue), _pool(newer, queue)
    app, repository = _task_app(tmp_path / "rollover.db", temporal_environment.client)
    try:
        async with _task_http(app) as url:
            monkeypatch.setenv("NEXUSFLOW_HUMAN_TASK_SERVICE_URL", url)
            monkeypatch.setenv("NEXUSFLOW_HUMAN_TASK_SERVICE_TOKEN", SERVICE_TOKEN)
            async with AsyncClient(
                base_url=url,
                headers={"Authorization": f"Bearer {APPROVER_TOKEN}"},
                trust_env=False,
            ) as http:
                async with _executor(older, old_pool, temporal_environment, "old-original"):
                    await _make_current(temporal_environment.client, old_pool)
                    old_handle = await _start_customer(temporal_environment.client, older, old_pool)
                    old_waiting = await _wait_status(old_handle, "WAITING_FOR_APPROVAL")
                    old_task_id = old_waiting["approval"]["task_id"]
                # There is no old executor now; the task remains committed while it waits.
                assert (await _get_task(http, old_task_id))["task_id"] == old_task_id
                async with _executor(newer, new_pool, temporal_environment, "new-release"):
                    await _make_current(temporal_environment.client, new_pool)
                    new_handle = await _start_customer(temporal_environment.client, newer, new_pool)
                    new_waiting = await _wait_status(new_handle, "WAITING_FOR_APPROVAL")
                    new_task_id = new_waiting["approval"]["task_id"]
                    assert new_task_id != old_task_id
                    async with _executor(older, old_pool, temporal_environment, "old-restarted"):
                        old_task = await _get_task(http, old_task_id)
                        new_task = await _get_task(http, new_task_id)
                        assert old_task["package_release_id"] == old_pool.package_release_id
                        assert new_task["package_release_id"] == new_pool.package_release_id
                        assert old_task["workflow_id"] == old_handle.id
                        assert new_task["workflow_id"] == new_handle.id
                        await old_handle.signal(
                            "approve",
                            {
                                "task_id": new_task_id,
                                "event_id": uuid4().hex,
                                "approved": True,
                                "approver": "alice",
                            },
                        )
                        assert (await old_handle.query("status"))["state"] == "WAITING_FOR_APPROVAL"
                        for task_id in (new_task_id, old_task_id):
                            await _post(http, f"/api/v1/tasks/{task_id}/claim", {})
                            await _post(
                                http,
                                f"/api/v1/tasks/{task_id}/approve",
                                {"evidence_reference": f"doc://{task_id}"},
                            )
                        await app.state.task_service.dispatch_pending()
                        old_result, new_result = await asyncio.wait_for(
                            asyncio.gather(old_handle.result(), new_handle.result()), 20
                        )
                        assert old_result["state"] == new_result["state"] == "COMPLETED"
                        assert old_result["actual_build_id"] == old_pool.build_id
                        assert new_result["actual_build_id"] == new_pool.build_id
                        assert old_result["results"]["manager_approval"]["task_id"] == old_task_id
                        assert new_result["results"]["manager_approval"]["task_id"] == new_task_id
                        assert old_result["definition_version"] == new_result["definition_version"]
    finally:
        repository.close()


async def test_escalation_expiry_and_committed_decision_survive_deadline_delivery_delay(
    temporal_environment: WorkflowEnvironment, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = _isolated_package(source_package())
    short = _compatible_package(original, "0.3.1", timeout_seconds=3)
    original_pool = _pool(original, "human-task-expiry-" + uuid4().hex)
    short_pool = _pool(short, "human-task-timeout-" + uuid4().hex)
    app, repository = _task_app(tmp_path / "expiration.db", temporal_environment.client)
    try:
        async with _task_http(app) as url:
            monkeypatch.setenv("NEXUSFLOW_HUMAN_TASK_SERVICE_URL", url)
            monkeypatch.setenv("NEXUSFLOW_HUMAN_TASK_SERVICE_TOKEN", SERVICE_TOKEN)
            async with AsyncClient(
                base_url=url,
                headers={"Authorization": f"Bearer {MANAGER_TOKEN}"},
                trust_env=False,
            ) as http:
                async with _executor(original, original_pool, temporal_environment, "expiry"):
                    handle = await _start_customer(
                        temporal_environment.client, original, original_pool
                    )
                    waiting = await _wait_status(handle, "WAITING_FOR_APPROVAL")
                    task_id = waiting["approval"]["task_id"]
                    escalated = await _post(
                        http,
                        f"/api/v1/tasks/{task_id}/escalate",
                        {"assignee_group": "urgent-managers", "reason": "deadline escalation"},
                    )
                    assert escalated["escalation_level"] == 1
                    assert escalated["assignee_group"] == "urgent-managers"
                    simulated_now = datetime.now(UTC) + timedelta(days=1)
                    app.state.task_service.process_due_tasks(now=simulated_now)
                    with monkeypatch.context() as clock:
                        clock.setattr("human_task_service.repository.utcnow", lambda: simulated_now)
                        await app.state.task_service.dispatch_pending()
                    result = await asyncio.wait_for(handle.result(), 20)
                    assert result["state"] == "TIMED_OUT"
                    assert (await _get_task(http, task_id))["status"] == "EXPIRED"
                    assert (
                        sum(
                            item["state"] == "TIMED_OUT"
                            for item in result["transitions"]
                            if item["step"] == "manager_approval"
                        )
                        == 1
                    )
                    assert (
                        await http.post(f"/api/v1/tasks/{task_id}/approve", json={})
                    ).status_code == 409

                async with _executor(short, short_pool, temporal_environment, "service-deadline"):
                    approved = await _start_customer(temporal_environment.client, short, short_pool)
                    approved_wait = await _wait_status(approved, "WAITING_FOR_APPROVAL")
                    approved_task_id = approved_wait["approval"]["task_id"]
                    await _post(http, f"/api/v1/tasks/{approved_task_id}/claim", {})
                    await _post(http, f"/api/v1/tasks/{approved_task_id}/approve", {})
                    # A committed decision wins even if Temporal delivery is
                    # delayed beyond the deadline; only the task service owns
                    # the expiry decision for a durable task.
                    await asyncio.sleep(3.2)
                    assert (await approved.query("status"))["state"] == "WAITING_FOR_APPROVAL"
                    assert (await _get_task(http, approved_task_id))["status"] == "APPROVED"
                    await app.state.task_service.dispatch_pending()
                    approved_result = await asyncio.wait_for(approved.result(), 15)
                    assert approved_result["state"] == "COMPLETED"

                    timed = await _start_customer(temporal_environment.client, short, short_pool)
                    timed_wait = await _wait_status(timed, "WAITING_FOR_APPROVAL")
                    timed_task_id = timed_wait["approval"]["task_id"]
                    await asyncio.sleep(3.2)
                    assert (await timed.query("status"))["state"] == "WAITING_FOR_APPROVAL"
                    app.state.task_service.process_due_tasks()
                    await app.state.task_service.dispatch_pending()
                    timed_result = await asyncio.wait_for(timed.result(), 15)
                    assert timed_result["state"] == "TIMED_OUT"
                    assert timed_result["results"]["manager_approval"]["task_id"] == timed_task_id
                    assert (await _get_task(http, timed_task_id))["workflow_id"] == timed.id
                    assert (await _get_task(http, timed_task_id))["status"] == "EXPIRED"
    finally:
        repository.close()


async def test_terminal_signal_sent_before_create_activity_result_is_buffered_once(
    temporal_environment: WorkflowEnvironment, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    package = _isolated_package(source_package())
    created = asyncio.Event()
    release_activity = asyncio.Event()

    @activity.defn(name="create_approval_task.pkg.v1")
    async def delayed_task_creation(payload: PackageActivityRequest) -> ActivityResponse:
        response = await nexusflow_activities.create_approval_task(payload)
        created.set()
        await release_activity.wait()
        return response

    wrapped = tuple(
        delayed_task_creation if registration.capability == "create_approval_task" else handler
        for registration, handler in zip(
            package.manifest.activity_registrations, package.activities, strict=True
        )
    )
    delayed_package = replace(package, activities=wrapped)
    pool = _pool(delayed_package, "human-task-early-signal-" + uuid4().hex)
    app, repository = _task_app(tmp_path / "early-signal.db", temporal_environment.client)
    try:
        async with _task_http(app) as url:
            monkeypatch.setenv("NEXUSFLOW_HUMAN_TASK_SERVICE_URL", url)
            monkeypatch.setenv("NEXUSFLOW_HUMAN_TASK_SERVICE_TOKEN", SERVICE_TOKEN)
            async with _executor(delayed_package, pool, temporal_environment, "early-signal"):
                handle = await _start_customer(temporal_environment.client, delayed_package, pool)
                await asyncio.wait_for(created.wait(), 15)
                async with (
                    AsyncClient(
                        base_url=url,
                        headers={"Authorization": f"Bearer {MANAGER_TOKEN}"},
                        trust_env=False,
                    ) as manager,
                    AsyncClient(
                        base_url=url,
                        headers={"Authorization": f"Bearer {APPROVER_TOKEN}"},
                        trust_env=False,
                    ) as approver,
                ):
                    listed = await manager.get("/api/v1/tasks")
                    assert listed.status_code == 200, listed.text
                    tasks = [
                        item for item in listed.json()["items"] if item["workflow_id"] == handle.id
                    ]
                    assert len(tasks) == 1
                    task_id = tasks[0]["task_id"]
                    await _post(approver, f"/api/v1/tasks/{task_id}/claim", {})
                    await _post(
                        approver,
                        f"/api/v1/tasks/{task_id}/approve",
                        {"evidence_reference": "doc://early-review"},
                    )
                    assert await app.state.task_service.dispatch_pending() == 1
                    with repository.sessions() as session:
                        outbox = session.scalar(
                            select(TaskOutboxRow).where(TaskOutboxRow.task_id == task_id)
                        )
                        assert outbox is not None
                        decision = dict(outbox.payload)
                    # Duplicate delivery and a conflicting later signal cannot
                    # replace the committed first decision while creation waits.
                    await handle.signal("approve", decision)
                    await handle.signal(
                        "approve",
                        {**decision, "event_id": uuid4().hex, "approved": False},
                    )
                    release_activity.set()
                    result = await asyncio.wait_for(handle.result(), 20)
                    assert result["state"] == "COMPLETED"
                    assert result["results"]["manager_approval"]["task_id"] == task_id
                    assert (
                        sum(
                            item["state"] == "APPROVED"
                            for item in result["transitions"]
                            if item["step"] == "manager_approval"
                        )
                        == 1
                    )
                    history = await handle.fetch_history()
                    approval_schedule = next(
                        event.event_id
                        for event in history.events
                        if event.HasField("activity_task_scheduled_event_attributes")
                        and event.activity_task_scheduled_event_attributes.activity_type.name
                        == "create_approval_task.pkg.v1"
                    )
                    completion = next(
                        event.event_id
                        for event in history.events
                        if event.HasField("activity_task_completed_event_attributes")
                        and event.activity_task_completed_event_attributes.scheduled_event_id
                        == approval_schedule
                    )
                    decision_signal = next(
                        event.event_id
                        for event in history.events
                        if event.HasField("workflow_execution_signaled_event_attributes")
                        and event.workflow_execution_signaled_event_attributes.signal_name
                        == "approve"
                    )
                    assert decision_signal < completion
    finally:
        release_activity.set()
        repository.close()


async def test_outbox_chain_guard_rejects_reused_workflow_id_without_touching_new_task(
    temporal_environment: WorkflowEnvironment, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    package = _isolated_package(source_package())
    pool = _pool(package, "human-task-reused-id-" + uuid4().hex)
    app, repository = _task_app(tmp_path / "reused-id.db", temporal_environment.client)
    try:
        async with _task_http(app) as url:
            monkeypatch.setenv("NEXUSFLOW_HUMAN_TASK_SERVICE_URL", url)
            monkeypatch.setenv("NEXUSFLOW_HUMAN_TASK_SERVICE_TOKEN", SERVICE_TOKEN)
            async with _executor(package, pool, temporal_environment, "reused-id"):
                async with AsyncClient(
                    base_url=url,
                    headers={"Authorization": f"Bearer {APPROVER_TOKEN}"},
                    trust_env=False,
                ) as http:
                    old = await _start_customer(temporal_environment.client, package, pool)
                    old_waiting = await _wait_status(old, "WAITING_FOR_APPROVAL")
                    old_task_id = old_waiting["approval"]["task_id"]
                    old_task = await _get_task(http, old_task_id)
                    assert old_task["first_execution_run_id"] == old.first_execution_run_id
                    await _post(http, f"/api/v1/tasks/{old_task_id}/claim", {})
                    await _post(http, f"/api/v1/tasks/{old_task_id}/approve", {})
                    # Simulate a signal accepted by Temporal just before the
                    # outbox's delivery acknowledgement was lost.
                    with repository.sessions() as session:
                        stale_event = session.scalar(
                            select(TaskOutboxRow).where(TaskOutboxRow.task_id == old_task_id)
                        )
                        assert stale_event is not None
                        stale_payload = dict(stale_event.payload)
                    await old.signal("approve", stale_payload)
                    assert (await asyncio.wait_for(old.result(), 20))["state"] == "COMPLETED"
                    reused = await _start_customer(
                        temporal_environment.client, package, pool, workflow_id=old.id
                    )
                    waiting = await _wait_status(reused, "WAITING_FOR_APPROVAL")
                    task_id = waiting["approval"]["task_id"]
                    task = await _get_task(http, task_id)
                    assert task_id != old_task_id
                    assert task["idempotency_key"] != old_task["idempotency_key"]
                    assert task["first_execution_run_id"] == reused.first_execution_run_id
                    assert task["first_execution_run_id"] != old_task["first_execution_run_id"]
                    assert await app.state.task_service.dispatch_pending() == 0
                    with repository.sessions() as session:
                        blocked = session.scalar(
                            select(TaskOutboxRow).where(TaskOutboxRow.task_id == old_task_id)
                        )
                        assert blocked is not None
                        assert blocked.status == "BLOCKED"
                        audit = session.scalars(
                            select(TaskAuditRow).where(TaskAuditRow.task_id == old_task_id)
                        ).all()
                    assert any(event.event_type == "DISPATCH_BLOCKED" for event in audit)
                    assert (await reused.query("status"))["state"] == "WAITING_FOR_APPROVAL"
                    await _post(http, f"/api/v1/tasks/{task_id}/claim", {})
                    await _post(http, f"/api/v1/tasks/{task_id}/approve", {})
                    assert await app.state.task_service.dispatch_pending() == 1
                    result = await asyncio.wait_for(reused.result(), 20)
                    assert result["state"] == "COMPLETED"
                    assert result["results"]["manager_approval"]["approver"] == "alice"
    finally:
        repository.close()
