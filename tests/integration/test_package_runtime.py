"""Real modern Worker Deployment routing with immutable package revisions."""

from __future__ import annotations

import asyncio
import os
import shutil
from collections.abc import AsyncIterator, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import timedelta
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

import pytest
import pytest_asyncio
from contracts import (
    ActivityResponse,
    ActivityStep,
    ApprovalStep,
    DefinitionDocument,
    ExecutorPool,
    PackageActivityRequest,
    PackageManifest,
    PackageRuntimeStartRequest,
)
from temporalio import activity
from temporalio.api.deployment.v1 import WorkerDeploymentVersion as ProtoDeploymentVersion
from temporalio.api.workflowservice.v1 import (
    DescribeWorkerDeploymentVersionRequest,
    SetWorkerDeploymentCurrentVersionRequest,
    SetWorkerDeploymentRampingVersionRequest,
)
from temporalio.client import Client, WorkflowFailureError, WorkflowHandle
from temporalio.common import (
    AutoUpgradeVersioningOverride,
    PinnedVersioningOverride,
    WorkerDeploymentVersion,
)
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import ApplicationError, CancelledError
from temporalio.service import RPCError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker, WorkerDeploymentConfig
from workflow_executor.boundary import PackageBoundaryInterceptor
from workflow_sdk.packages.validation import definition_hash, manifest_hash, validate_manifest
from workflow_sdk.runtime import PackageAutoUpgradeWorkflowV1, PackageWorkflowV1

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="module")]
PackageHandle = WorkflowHandle[Any, dict[str, Any]]


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def package_environment() -> AsyncIterator[WorkflowEnvironment]:
    cli_path = os.environ.get("TEMPORAL_CLI_PATH") or shutil.which("temporal")
    if not cli_path or not Path(cli_path).is_file():
        pytest.fail("Package runtime tests require a local Temporal CLI or TEMPORAL_CLI_PATH")
    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=cli_path,
        dev_server_log_level="error",
        ui=False,
        data_converter=pydantic_data_converter,
    ) as environment:
        yield environment


def build_activity(build: str) -> Callable[..., Any]:
    @activity.defn(name="validate_request.pkg.v1")
    async def validate(request: PackageActivityRequest) -> ActivityResponse:
        return ActivityResponse(
            capability="validate_request",
            output={
                "worker_build": build,
                "definition_version": request.context.definition_version,
                "intended_build": request.release_binding.build_id,
                "amount": request.input.get("amount"),
            },
        )

    return validate


@activity.defn(name="create_approval_task.pkg.v1")
async def create_task(request: PackageActivityRequest) -> ActivityResponse:
    return ActivityResponse(
        capability="create_approval_task",
        output={"task_id": f"task-{request.workflow_id}-{request.step_id}"},
    )


def approval_definition() -> dict[str, Any]:
    return {
        "start_at": "approval",
        "steps": {
            "approval": {
                "type": "approval",
                "timeout_seconds": 30,
                "on_approved": "validate",
                "on_rejected": "validate",
                "on_timeout": "validate",
            },
            "validate": {
                "type": "activity",
                "capability": "validate_request",
                "input": {"amount": "${request.amount}"},
                "next": "done",
            },
            "done": {"type": "end"},
        },
    }


def payload(
    deployment: str,
    document: dict[str, Any],
    *,
    behavior: Literal["pinned", "auto_upgrade"] = "pinned",
    eligible: tuple[str, ...] = ("build-1", "build-2"),
) -> PackageRuntimeStartRequest:
    definition = DefinitionDocument.model_validate(document)
    required = {
        step.capability for step in definition.steps.values() if isinstance(step, ActivityStep)
    }
    if any(isinstance(step, ApprovalStep) for step in definition.steps.values()):
        required.add("create_approval_task")
    start = PackageRuntimeStartRequest.model_validate(
        {
            "context": {
                "tenant": "tenant",
                "business_domain": "finance",
                "application": "adjustments",
                "workflow_type": "adjustment",
                "definition_id": "definition",
                "definition_version": "1.0",
                "business_reference": "BIZ-1",
                "correlation_id": uuid4().hex,
                "actor": "verified-user",
            },
            "definition_document": definition,
            "request": {"amount": 1250},
            "release_binding": {
                "package_id": "adjustments",
                "package_release_id": "release-1",
                "package_version": "1.0",
                "build_id": "build-1",
                "manifest_hash": "a" * 64,
                "artifact_digest": "sha256:" + "b" * 64,
                "worker_deployment_name": deployment,
                "temporal_namespace": "default",
                "workflow_task_queue": deployment + "-workflow",
                "definition_id": "definition",
                "definition_version": "1.0",
                "definition_content_hash": definition_hash(definition),
                "activity_bindings": [
                    {
                        "capability": capability,
                        "activity_name": capability + ".pkg.v1",
                        "logical_queue": "activities",
                        "task_queue": deployment + "-activity",
                    }
                    for capability in sorted(required)
                ],
                "eligible_build_ids": eligible,
                "versioning_behavior": behavior,
            },
        }
    )
    if behavior == "auto_upgrade":
        initial_manifest, _ = installed_content(start, "build-1")
        start = start.model_copy(
            update={
                "release_binding": start.release_binding.model_copy(
                    update={"manifest_hash": manifest_hash(initial_manifest)}
                )
            }
        )
    return start


async def wait_for_version(client: Client, deployment: str, build: str) -> None:
    deadline = asyncio.get_running_loop().time() + 15
    while asyncio.get_running_loop().time() < deadline:
        try:
            result = await client.workflow_service.describe_worker_deployment_version(
                DescribeWorkerDeploymentVersionRequest(
                    namespace=client.namespace,
                    deployment_version=ProtoDeploymentVersion(
                        deployment_name=deployment, build_id=build
                    ),
                )
            )
            # Both queues must belong to the version before changing its routing.
            if len(result.worker_deployment_version_info.task_queue_infos) >= 2:
                return
        except RPCError:
            pass
        await asyncio.sleep(0.1)
    pytest.fail(f"Worker deployment {deployment}/{build} did not register both queues")


async def make_current(client: Client, deployment: str, build: str) -> None:
    await wait_for_version(client, deployment, build)
    await client.workflow_service.set_worker_deployment_current_version(
        SetWorkerDeploymentCurrentVersionRequest(
            namespace=client.namespace,
            deployment_name=deployment,
            build_id=build,
            identity="package-runtime-integration",
        )
    )


async def wait_execution_version(handle: PackageHandle, build: str) -> None:
    """Observe actual workflow routing after the server's queue cache converges."""
    deadline = asyncio.get_running_loop().time() + 15
    while asyncio.get_running_loop().time() < deadline:
        # A nonempty continuation payload is deliberately ignored by the runtime;
        # its workflow task still exercises server routing without a business effect.
        await handle.signal("continue_execution", {"routing_probe": True})
        description = await handle.describe()
        version = description.raw_description.workflow_execution_info.versioning_info
        if version.deployment_version.build_id == build:
            return
        await asyncio.sleep(0.1)
    pytest.fail(f"Workflow did not converge to deployment build {build}")


@asynccontextmanager
async def version_workers(
    client: Client,
    deployment: str,
    *,
    builds: tuple[str, ...] = ("build-1", "build-2"),
    runtime: bool = True,
    retained_start: PackageRuntimeStartRequest | None = None,
    incompatible_build: str | None = None,
) -> AsyncIterator[None]:
    async with AsyncExitStack() as stack:
        for build in builds:
            config = WorkerDeploymentConfig(
                version=WorkerDeploymentVersion(deployment, build), use_worker_versioning=True
            )
            interceptors = []
            if retained_start is not None:
                manifest, pool = installed_content(retained_start, build)
                if incompatible_build == build:
                    raw = manifest.model_dump(mode="json")
                    raw["definitions"] = raw["definitions"][1:]
                    manifest = PackageManifest.model_validate(raw)
                interceptors = [PackageBoundaryInterceptor(manifest, pool)]
            if runtime:
                await stack.enter_async_context(
                    Worker(
                        client,
                        task_queue=deployment + "-workflow",
                        workflows=[PackageWorkflowV1, PackageAutoUpgradeWorkflowV1],
                        deployment_config=config,
                        interceptors=interceptors,
                        graceful_shutdown_timeout=timedelta(seconds=0),
                    )
                )
            await stack.enter_async_context(
                Worker(
                    client,
                    task_queue=deployment + "-activity",
                    activities=[build_activity(build), create_task],
                    deployment_config=config,
                    interceptors=interceptors,
                    graceful_shutdown_timeout=timedelta(seconds=0),
                )
            )
        if runtime:
            for build in builds:
                await wait_for_version(client, deployment, build)
        yield


def installed_content(
    start: PackageRuntimeStartRequest, build: str
) -> tuple[PackageManifest, ExecutorPool]:
    """Two validated test releases: build 2 retains revision 1 and adds revision 2."""
    binding = start.release_binding
    revised = start.definition_document.model_dump(mode="json")
    revised["steps"]["validate"]["input"] = {"amount": 9999}
    newer_definition = DefinitionDocument.model_validate(revised)
    revisions = [
        {
            "definition_id": binding.definition_id,
            "workflow_type": start.context.workflow_type,
            "version": binding.definition_version,
            "content_hash": binding.definition_content_hash,
            "definition_document": start.definition_document,
            "runtime_workflow_type": "PackageAutoUpgradeWorkflowV1",
        }
    ]
    if build != "build-1":
        revisions.append(
            {
                **revisions[0],
                "version": "2.0",
                "content_hash": definition_hash(newer_definition),
                "definition_document": newer_definition,
            }
        )
    manifest = validate_manifest(
        {
            "package_id": binding.package_id,
            "package_version": "1.0" if build == "build-1" else "2.0",
            "build_id": build,
            "worker_deployment_name": binding.worker_deployment_name,
            "definitions": revisions,
            "workflow_registrations": [
                {
                    "name": "PackageAutoUpgradeWorkflowV1",
                    "entrypoint": "workflow_sdk.runtime:PackageAutoUpgradeWorkflowV1",
                    "logical_queue": "workflow",
                    "versioning_behavior": "auto_upgrade",
                }
            ],
            "activity_registrations": [
                {
                    "capability": route.capability,
                    "activity_name": route.activity_name,
                    "contract_version": route.contract_version,
                    "entrypoint": "nexusflow_activities:" + route.capability,
                    "logical_queue": route.logical_queue,
                }
                for route in binding.activity_bindings
            ],
            "queues": [
                {"name": "workflow", "role": "workflow"},
                {"name": "activities", "role": "activity"},
            ],
            "dependencies": [
                {"name": name, "version": version}
                for name, version in (
                    ("nexusflow-contracts", "0.1.0"),
                    ("nexusflow-common", "0.1.0"),
                    ("nexusflow-workflow-sdk", "0.1.0"),
                    ("nexusflow-workflows", "0.1.0"),
                    ("nexusflow-activities", "0.1.0"),
                    ("temporalio", "1.33.0"),
                    ("pydantic", "2.13.4"),
                    ("packaging", "26.3"),
                )
            ],
        }
    )
    pool = ExecutorPool(
        pool_id="pool-" + build,
        package_id=binding.package_id,
        package_release_id="release-" + build,
        worker_deployment_name=binding.worker_deployment_name,
        build_id=build,
        queue_bindings={
            "workflow": binding.workflow_task_queue,
            "activities": binding.activity_bindings[0].task_queue,
        },
    )
    return manifest, pool


async def start_package(client: Client, start: PackageRuntimeStartRequest) -> PackageHandle:
    binding = start.release_binding
    pinned = binding.versioning_behavior == "pinned"
    return await client.start_workflow(
        "PackageWorkflowV1" if pinned else "PackageAutoUpgradeWorkflowV1",
        start.model_dump(mode="json"),
        id="package-" + uuid4().hex,
        task_queue=binding.workflow_task_queue,
        execution_timeout=timedelta(seconds=40),
        versioning_override=(
            PinnedVersioningOverride(
                version=WorkerDeploymentVersion(binding.worker_deployment_name, binding.build_id)
            )
            if pinned
            else AutoUpgradeVersioningOverride()
        ),
    )


async def wait_status(
    handle: PackageHandle,
    *,
    state: str | None = None,
    step: str | None = None,
    actual: str | None = None,
) -> dict[str, Any]:
    deadline = asyncio.get_running_loop().time() + 15
    last: dict[str, Any] = {}
    while asyncio.get_running_loop().time() < deadline:
        last = await handle.query("status")
        if (
            (state is None or last["state"] == state)
            and (step is None or last["current_step"] == step)
            and (actual is None or last.get("actual_build_id") == actual)
        ):
            return last
        await asyncio.sleep(0.1)
    pytest.fail(f"Package runtime did not reach expected status: {last}")


async def test_pinned_old_run_and_new_current_version_coexist_without_activity_mixing(
    package_environment: WorkflowEnvironment,
) -> None:
    client = package_environment.client
    deployment = "pinned-" + uuid4().hex
    async with version_workers(client, deployment):
        await make_current(client, deployment, "build-1")
        handle = await start_package(client, payload(deployment, approval_definition()))
        await wait_status(handle, state="WAITING_FOR_APPROVAL")
        await make_current(client, deployment, "build-2")
        await handle.signal("approve", {"approved": True, "approver": "manager"})
        result = await asyncio.wait_for(handle.result(), 15)
        assert result["actual_build_id"] == "build-1"
        assert result["results"]["validate"]["worker_build"] == "build-1"
        assert result["definition_version"] == "1.0"
        assert result["state"] == "COMPLETED"


async def test_auto_upgrade_changes_eligible_worker_and_keeps_captured_revision(
    package_environment: WorkflowEnvironment,
) -> None:
    client = package_environment.client
    deployment = "auto-" + uuid4().hex
    async with version_workers(client, deployment):
        await make_current(client, deployment, "build-1")
        start = payload(deployment, approval_definition(), behavior="auto_upgrade")
        handle = await start_package(client, start)
        waiting = await wait_status(handle, state="WAITING_FOR_APPROVAL")
        task_id = waiting["approval"]["task_id"]
        await make_current(client, deployment, "build-2")
        await wait_execution_version(handle, "build-2")
        await handle.signal("approve", {"approved": True, "approver": "manager"})
        result = await asyncio.wait_for(handle.result(), 15)
        assert result["actual_build_id"] == "build-2"
        assert result["results"]["validate"]["worker_build"] == "build-2"
        assert result["results"]["approval"]["task_id"] == task_id
        assert result["results"]["validate"]["intended_build"] == "build-1"
        assert result["release_binding"] == start.release_binding.model_dump(mode="json")
        assert result["definition_version"] == "1.0"


async def test_auto_upgrade_outside_reserved_policy_fails_before_next_activity(
    package_environment: WorkflowEnvironment,
) -> None:
    client = package_environment.client
    deployment = "inadmissible-" + uuid4().hex
    async with version_workers(client, deployment):
        await make_current(client, deployment, "build-1")
        handle = await start_package(
            client,
            payload(
                deployment,
                approval_definition(),
                behavior="auto_upgrade",
                eligible=("build-1",),
            ),
        )
        await wait_status(handle, state="WAITING_FOR_APPROVAL")
        await make_current(client, deployment, "build-2")
        await wait_execution_version(handle, "build-2")
        await handle.signal("approve", {"approved": True, "approver": "manager"})
        with pytest.raises(WorkflowFailureError) as caught:
            await asyncio.wait_for(handle.result(), 15)
        assert isinstance(caught.value.cause, ApplicationError)
        assert caught.value.cause.type == "ValidationError"
        history = await handle.fetch_history()
        scheduled = [
            event.activity_task_scheduled_event_attributes.activity_type.name
            for event in history.events
            if event.HasField("activity_task_scheduled_event_attributes")
        ]
        assert scheduled == ["create_approval_task.pkg.v1"]


async def test_ramping_routes_auto_upgrade_and_activities_to_the_same_eligible_version(
    package_environment: WorkflowEnvironment,
) -> None:
    client = package_environment.client
    deployment = "ramp-" + uuid4().hex
    async with version_workers(client, deployment):
        await make_current(client, deployment, "build-1")
        await client.workflow_service.set_worker_deployment_ramping_version(
            SetWorkerDeploymentRampingVersionRequest(
                namespace=client.namespace,
                deployment_name=deployment,
                build_id="build-2",
                percentage=100,
                identity="package-runtime-integration",
            )
        )
        handle = await start_package(
            client, payload(deployment, approval_definition(), behavior="auto_upgrade")
        )
        await wait_status(handle, state="WAITING_FOR_APPROVAL", actual="build-2")
        await handle.signal("approve", {"approved": True, "approver": "manager"})
        result = await asyncio.wait_for(handle.result(), 15)
        assert result["results"]["validate"]["worker_build"] == "build-2"


async def test_trusted_new_release_admits_later_auto_upgrade_and_retains_selected_definition(
    package_environment: WorkflowEnvironment,
) -> None:
    client = package_environment.client
    deployment = "retained-" + uuid4().hex
    start = payload(
        deployment, approval_definition(), behavior="auto_upgrade", eligible=("build-1",)
    )
    initial_manifest, _ = installed_content(start, "build-1")
    start = start.model_copy(
        update={
            "release_binding": start.release_binding.model_copy(
                update={"manifest_hash": manifest_hash(initial_manifest)}
            )
        }
    )
    async with version_workers(client, deployment, retained_start=start):
        await make_current(client, deployment, "build-1")
        handle = await start_package(client, start)
        await wait_status(handle, state="WAITING_FOR_APPROVAL")
        await make_current(client, deployment, "build-2")
        await wait_execution_version(handle, "build-2")
        await handle.signal("approve", {"approved": True, "approver": "manager"})
        result = await asyncio.wait_for(handle.result(), 15)
        assert result["actual_build_id"] == "build-2"
        assert result["release_binding"] == start.release_binding.model_dump(mode="json")
        assert result["definition_version"] == "1.0"
        assert result["results"]["validate"]["amount"] == 1250
        assert result["results"]["validate"]["worker_build"] == "build-2"
    retained_manifest, retained_pool = installed_content(start, "build-2")
    replay = await Replayer(
        workflows=[PackageAutoUpgradeWorkflowV1],
        data_converter=pydantic_data_converter,
        namespace=client.namespace,
        interceptors=[PackageBoundaryInterceptor(retained_manifest, retained_pool)],
    ).replay_workflow(await handle.fetch_history())
    assert replay.replay_failure is None


async def test_new_release_missing_retained_revision_fails_before_next_side_effect(
    package_environment: WorkflowEnvironment,
) -> None:
    client = package_environment.client
    deployment = "missing-retention-" + uuid4().hex
    start = payload(
        deployment, approval_definition(), behavior="auto_upgrade", eligible=("build-1",)
    )
    async with version_workers(
        client, deployment, retained_start=start, incompatible_build="build-2"
    ):
        await make_current(client, deployment, "build-1")
        handle = await start_package(client, start)
        await wait_status(handle, state="WAITING_FOR_APPROVAL")
        await make_current(client, deployment, "build-2")
        await wait_execution_version(handle, "build-2")
        await handle.signal("approve", {"approved": True, "approver": "manager"})
        with pytest.raises(WorkflowFailureError) as caught:
            await asyncio.wait_for(handle.result(), 15)
        assert isinstance(caught.value.cause, ApplicationError)
        assert caught.value.cause.type == "PackageCompatibilityError"
        history = await handle.fetch_history()
        scheduled = [
            event.activity_task_scheduled_event_attributes.activity_type.name
            for event in history.events
            if event.HasField("activity_task_scheduled_event_attributes")
        ]
        assert scheduled == ["create_approval_task.pkg.v1"]


async def test_initial_routing_race_cannot_expand_reserved_admission_even_on_compatible_host(
    package_environment: WorkflowEnvironment,
) -> None:
    client = package_environment.client
    deployment = "admission-race-" + uuid4().hex
    start = payload(
        deployment, approval_definition(), behavior="auto_upgrade", eligible=("build-1",)
    )
    async with version_workers(client, deployment, retained_start=start):
        # Simulate a promotion after reservation but before Temporal submission.
        # The compatible new host's attestation applies to later tasks only.
        await make_current(client, deployment, "build-2")
        handle = await start_package(client, start)
        with pytest.raises(WorkflowFailureError) as caught:
            await asyncio.wait_for(handle.result(), 15)
        assert isinstance(caught.value.cause, ApplicationError)
        assert caught.value.cause.type == "ValidationError"
        history = await handle.fetch_history()
        assert not any(
            event.HasField("activity_task_scheduled_event_attributes") for event in history.events
        )


async def test_inherited_continuation_waits_for_approval_and_preserves_pinned_state(
    package_environment: WorkflowEnvironment,
) -> None:
    client = package_environment.client
    deployment = "continue-" + uuid4().hex
    async with version_workers(client, deployment):
        await make_current(client, deployment, "build-1")
        start = payload(deployment, approval_definition())
        handle = await start_package(client, start)
        waiting = await wait_status(handle, state="WAITING_FOR_APPROVAL")
        await handle.signal("continue_execution", {})
        await make_current(client, deployment, "build-2")
        still_waiting = await wait_status(handle, state="WAITING_FOR_APPROVAL")
        assert still_waiting["continuation_count"] == 0
        assert still_waiting["approval"]["task_id"] == waiting["approval"]["task_id"]
        await handle.signal("approve", {"approved": True, "approver": "manager"})
        result = await asyncio.wait_for(handle.result(), 15)
        assert result["continuation_count"] == 1
        assert result["actual_build_id"] == "build-1"
        assert result["results"]["validate"]["worker_build"] == "build-1"
        assert result["results"]["approval"]["approver"] == "manager"
        assert result["release_binding"] == start.release_binding.model_dump(mode="json")
        assert any(item["state"] == "CONTINUED_AS_NEW" for item in result["transitions"])
        initial = client.get_workflow_handle(handle.id, run_id=handle.first_execution_run_id)
        history = await initial.fetch_history()
        assert any(
            event.HasField("workflow_execution_continued_as_new_event_attributes")
            for event in history.events
        )
        continued_history = await handle.fetch_history()
    for run_history in (history, continued_history):
        replay = await Replayer(
            workflows=[PackageWorkflowV1],
            data_converter=pydantic_data_converter,
            namespace=client.namespace,
        ).replay_workflow(run_history)
        assert replay.replay_failure is None


async def test_package_timer_cancellation_is_recorded_by_temporal(
    package_environment: WorkflowEnvironment,
) -> None:
    client = package_environment.client
    deployment = "cancel-" + uuid4().hex
    document = {
        "start_at": "wait",
        "steps": {
            "wait": {"type": "timer", "seconds": 300, "next": "done"},
            "done": {"type": "end"},
        },
    }
    async with version_workers(client, deployment, builds=("build-1",)):
        await make_current(client, deployment, "build-1")
        handle = await start_package(client, payload(deployment, document, eligible=("build-1",)))
        await wait_status(handle, step="wait")
        await handle.cancel()
        with pytest.raises(WorkflowFailureError) as caught:
            await asyncio.wait_for(handle.result(), 15)
        assert isinstance(caught.value.cause, CancelledError)
        history = await handle.fetch_history()
        assert any(event.HasField("timer_started_event_attributes") for event in history.events)
        assert any(
            event.HasField("workflow_execution_canceled_event_attributes")
            for event in history.events
        )


async def test_continuation_does_not_restart_an_active_timer(
    package_environment: WorkflowEnvironment,
) -> None:
    client = package_environment.client
    deployment = "timer-continue-" + uuid4().hex
    document = {
        "start_at": "wait",
        "steps": {
            "wait": {"type": "timer", "seconds": 2, "next": "validate"},
            "validate": {
                "type": "activity",
                "capability": "validate_request",
                "next": "done",
            },
            "done": {"type": "end"},
        },
    }
    async with version_workers(client, deployment, builds=("build-1",)):
        await make_current(client, deployment, "build-1")
        handle = await start_package(client, payload(deployment, document, eligible=("build-1",)))
        await wait_status(handle, step="wait")
        await handle.signal("continue_execution", {})
        pending = await wait_status(handle, step="wait")
        assert pending["continuation_count"] == 0
        result = await asyncio.wait_for(handle.result(), 15)
        assert result["continuation_count"] == 1
        assert result["results"]["validate"]["worker_build"] == "build-1"
        first = client.get_workflow_handle(handle.id, run_id=handle.first_execution_run_id)
        first_history = await first.fetch_history()
        assert (
            sum(event.HasField("timer_started_event_attributes") for event in first_history.events)
            == 1
        )
        assert (
            sum(event.HasField("timer_fired_event_attributes") for event in first_history.events)
            == 1
        )
        continuation_history = await handle.fetch_history()
        assert not any(
            event.HasField("timer_started_event_attributes")
            for event in continuation_history.events
        )


async def test_package_open_approval_cancels_without_scheduling_following_activity(
    package_environment: WorkflowEnvironment,
) -> None:
    client = package_environment.client
    deployment = "approval-cancel-" + uuid4().hex
    async with version_workers(client, deployment, builds=("build-1",)):
        await make_current(client, deployment, "build-1")
        handle = await start_package(
            client, payload(deployment, approval_definition(), eligible=("build-1",))
        )
        await wait_status(handle, state="WAITING_FOR_APPROVAL")
        await handle.cancel()
        with pytest.raises(WorkflowFailureError) as caught:
            await asyncio.wait_for(handle.result(), 15)
        assert isinstance(caught.value.cause, CancelledError)
        history = await handle.fetch_history()
        scheduled = [
            event.activity_task_scheduled_event_attributes.activity_type.name
            for event in history.events
            if event.HasField("activity_task_scheduled_event_attributes")
        ]
        assert scheduled == ["create_approval_task.pkg.v1"]


async def test_package_open_approval_survives_worker_restart_and_replays(
    package_environment: WorkflowEnvironment,
) -> None:
    client = package_environment.client
    deployment = "restart-" + uuid4().hex
    async with version_workers(client, deployment, builds=("build-1",)):
        await make_current(client, deployment, "build-1")
        handle = await start_package(
            client, payload(deployment, approval_definition(), eligible=("build-1",))
        )
        before = await wait_status(handle, state="WAITING_FOR_APPROVAL")
    async with version_workers(client, deployment, builds=("build-1",)):
        after = await wait_status(handle, state="WAITING_FOR_APPROVAL")
        assert after["approval"]["task_id"] == before["approval"]["task_id"]
        await handle.signal("approve", {"approved": True, "approver": "after-restart"})
        result = await asyncio.wait_for(handle.result(), 15)
        assert result["results"]["approval"]["approver"] == "after-restart"
    history = await handle.fetch_history()
    replay = await Replayer(
        workflows=[PackageWorkflowV1, PackageAutoUpgradeWorkflowV1],
        data_converter=pydantic_data_converter,
        namespace=client.namespace,
    ).replay_workflow(history)
    assert replay.replay_failure is None
