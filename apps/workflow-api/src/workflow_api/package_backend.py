"""Modern Temporal Worker Deployment routing; no business package imports."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Protocol, cast

from contracts import PackageExecutionBinding, PackageManifest, PackageRuntimeStartRequest
from temporalio.api.deployment.v1 import WorkerDeploymentVersion as DeploymentVersion
from temporalio.api.enums.v1 import DescribeTaskQueueMode, TaskQueueType
from temporalio.api.taskqueue.v1 import TaskQueue, TaskQueueVersionSelection
from temporalio.api.workflowservice.v1 import (
    DescribeTaskQueueRequest,
    DescribeWorkerDeploymentRequest,
    DescribeWorkerDeploymentVersionRequest,
    SetWorkerDeploymentCurrentVersionRequest,
    SetWorkerDeploymentRampingVersionRequest,
)
from temporalio.common import (
    AutoUpgradeVersioningOverride,
    PinnedVersioningOverride,
    WorkerDeploymentVersion,
    WorkflowIDConflictPolicy,
    WorkflowIDReusePolicy,
)
from temporalio.exceptions import WorkflowAlreadyStartedError

from .backend import BackendUnavailable, BusinessSnapshot, TemporalBackend, _runtime_error


class PackageBackend(Protocol):
    async def start_package(self, workflow_id: str, payload: PackageRuntimeStartRequest) -> str: ...
    async def inspect_release(
        self, manifest: PackageManifest, namespace: str, queues: dict[str, str]
    ) -> dict[str, Any]: ...
    async def route_release(
        self, manifest: PackageManifest, namespace: str, *, ramp_percentage: int | None = None
    ) -> None: ...
    async def routing(
        self, deployment_name: str, namespace: str
    ) -> tuple[str, str | None, float]: ...
    async def observe_package(
        self, workflow_id: str, run_id: str, binding: PackageExecutionBinding
    ) -> tuple[str | None, str | None]: ...
    async def status_package(
        self, workflow_id: str, run_id: str
    ) -> tuple[str, BusinessSnapshot]: ...


class PackageTemporalBackend(TemporalBackend):
    async def start_package(self, workflow_id: str, payload: PackageRuntimeStartRequest) -> str:
        binding = payload.release_binding
        if binding.temporal_namespace != self.client.namespace:
            raise BackendUnavailable()
        override = (
            PinnedVersioningOverride(
                WorkerDeploymentVersion(binding.worker_deployment_name, binding.build_id)
            )
            if binding.versioning_behavior == "pinned"
            else AutoUpgradeVersioningOverride()
        )
        name = (
            "PackageWorkflowV1"
            if binding.versioning_behavior == "pinned"
            else "PackageAutoUpgradeWorkflowV1"
        )
        try:
            try:
                handle = await self.client.start_workflow(
                    name,
                    payload,
                    id=workflow_id,
                    task_queue=binding.workflow_task_queue,
                    id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                    id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
                    versioning_override=override,
                    rpc_timeout=self.rpc_timeout,
                )
                run_id = handle.result_run_id
            except WorkflowAlreadyStartedError:
                description = await self.client.get_workflow_handle(workflow_id).describe(
                    rpc_timeout=self.rpc_timeout
                )
                run_id = (
                    description.raw_description.workflow_execution_info.first_run_id
                    or description.run_id
                )
            if not run_id:
                raise BackendUnavailable()
            return run_id
        except Exception as exc:
            raise BackendUnavailable() from exc

    async def inspect_release(
        self, manifest: PackageManifest, namespace: str, queues: dict[str, str]
    ) -> dict[str, Any]:
        if namespace != self.client.namespace:
            raise BackendUnavailable()
        try:
            response = await self.client.workflow_service.describe_worker_deployment_version(
                DescribeWorkerDeploymentVersionRequest(
                    namespace=namespace,
                    deployment_version=DeploymentVersion(
                        deployment_name=manifest.worker_deployment_name, build_id=manifest.build_id
                    ),
                ),
                timeout=self.rpc_timeout,
            )
            info = response.worker_deployment_version_info
            members = {(q.name, q.type) for q in info.task_queue_infos}
            required = {(queues[q.name], 1 if q.role == "workflow" else 2) for q in manifest.queues}
            observed: list[dict[str, Any]] = []
            now = datetime.now(timezone.utc)
            for queue, task_type in sorted(required):
                described = await self.client.workflow_service.describe_task_queue(
                    DescribeTaskQueueRequest(
                        namespace=namespace,
                        task_queue=TaskQueue(name=queue),
                        # DEFAULT reports one partition's pollers. ENHANCED
                        # aggregates every read/write partition on this server.
                        # The root Workflow queue is the aggregation endpoint;
                        # task_queue_types selects the actual role to inspect.
                        task_queue_type=TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW,
                        api_mode=DescribeTaskQueueMode.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
                        versions=TaskQueueVersionSelection(
                            build_ids=[f"{manifest.worker_deployment_name}:{manifest.build_id}"]
                        ),
                        task_queue_types=[cast(TaskQueueType.ValueType, task_type)],
                        report_pollers=True,
                    ),
                    timeout=self.rpc_timeout,
                )
                identities = {
                    p.identity
                    for version in described.versions_info.values()
                    for role_info in (version.types_info.get(task_type),)
                    if role_info is not None
                    for p in role_info.pollers
                    if p.identity
                    and p.deployment_options.deployment_name == manifest.worker_deployment_name
                    and p.deployment_options.build_id == manifest.build_id
                    and (now - p.last_access_time.ToDatetime(tzinfo=timezone.utc)).total_seconds()
                    < 60
                }
                observed.append(
                    {
                        "task_queue": queue,
                        "task_type": task_type,
                        "replicas": len(identities),
                        "identities": sorted(identities),
                    }
                )
            return {
                "ready": required.issubset(members) and all(q["replicas"] > 0 for q in observed),
                "queues": observed,
                "drainage_status": int(info.drainage_info.status),
                "build_id": manifest.build_id,
            }
        except Exception as exc:
            raise BackendUnavailable() from exc

    async def routing(self, deployment_name: str, namespace: str) -> tuple[str, str | None, float]:
        if namespace != self.client.namespace:
            raise BackendUnavailable()
        try:
            info = await self.client.workflow_service.describe_worker_deployment(
                DescribeWorkerDeploymentRequest(
                    namespace=namespace, deployment_name=deployment_name
                ),
                timeout=self.rpc_timeout,
            )
            routing = info.worker_deployment_info.routing_config
            current = (
                routing.current_deployment_version.build_id
                or routing.current_version.removeprefix(deployment_name + ".")
            )
            ramp = (
                routing.ramping_deployment_version.build_id
                or routing.ramping_version.removeprefix(deployment_name + ".")
            )
            return current, ramp or None, routing.ramping_version_percentage
        except Exception as exc:
            raise BackendUnavailable() from exc

    async def route_release(
        self, manifest: PackageManifest, namespace: str, *, ramp_percentage: int | None = None
    ) -> None:
        if namespace != self.client.namespace:
            raise BackendUnavailable()
        try:
            described = await self.client.workflow_service.describe_worker_deployment(
                DescribeWorkerDeploymentRequest(
                    namespace=namespace, deployment_name=manifest.worker_deployment_name
                ),
                timeout=self.rpc_timeout,
            )
            fields: dict[str, Any] = dict(
                namespace=namespace,
                deployment_name=manifest.worker_deployment_name,
                build_id=manifest.build_id,
                conflict_token=described.conflict_token,
                identity="nexusflow-package-control-plane",
            )
            if ramp_percentage is None:
                await self.client.workflow_service.set_worker_deployment_current_version(
                    SetWorkerDeploymentCurrentVersionRequest(**fields),
                    timeout=self.rpc_timeout,
                )
                # Clear a previous ramp explicitly; changing Current alone can
                # leave a server ramp targeting a different version.
                described = await self.client.workflow_service.describe_worker_deployment(
                    DescribeWorkerDeploymentRequest(
                        namespace=namespace, deployment_name=manifest.worker_deployment_name
                    ),
                    timeout=self.rpc_timeout,
                )
                if (
                    described.worker_deployment_info.routing_config.ramping_version
                    or described.worker_deployment_info.routing_config.ramping_deployment_version.build_id
                ):
                    await self.client.workflow_service.set_worker_deployment_ramping_version(
                        SetWorkerDeploymentRampingVersionRequest(
                            namespace=namespace,
                            deployment_name=manifest.worker_deployment_name,
                            build_id="",
                            percentage=0,
                            conflict_token=described.conflict_token,
                            identity="nexusflow-package-control-plane",
                        ),
                        timeout=self.rpc_timeout,
                    )
            else:
                await self.client.workflow_service.set_worker_deployment_ramping_version(
                    SetWorkerDeploymentRampingVersionRequest(**fields, percentage=ramp_percentage),
                    timeout=self.rpc_timeout,
                )
        except Exception as exc:
            raise BackendUnavailable() from exc

    async def status_package(self, workflow_id: str, run_id: str) -> tuple[str, BusinessSnapshot]:
        # The latest run must belong to the reserved chain. A workflow ID reused
        # outside that chain can never receive business signals or projections.
        try:
            initial = await self.client.get_workflow_handle(workflow_id, run_id=run_id).describe(
                rpc_timeout=self.rpc_timeout
            )
            first = initial.raw_description.workflow_execution_info.first_run_id or initial.run_id
            for _ in range(3):
                latest = await self.client.get_workflow_handle(workflow_id).describe(
                    rpc_timeout=self.rpc_timeout
                )
                if (
                    latest.raw_description.workflow_execution_info.first_run_id or latest.run_id
                ) != first:
                    raise BackendUnavailable()
                snapshot = await super().status(workflow_id, latest.run_id)
                if snapshot.failure_code != "runtime_run_changed":
                    return latest.run_id, snapshot
            # Never persist the old adapter's terminal-looking projection for a
            # valid package continuation that races the status request.
            raise BackendUnavailable()
        except Exception as exc:
            raise _runtime_error(exc) from exc

    async def observe_package(
        self, workflow_id: str, run_id: str, binding: PackageExecutionBinding
    ) -> tuple[str | None, str | None]:
        try:
            initial = await self.client.get_workflow_handle(workflow_id, run_id=run_id).describe(
                rpc_timeout=self.rpc_timeout
            )
            latest = await self.client.get_workflow_handle(workflow_id).describe(
                rpc_timeout=self.rpc_timeout
            )
            if (initial.raw_description.workflow_execution_info.first_run_id or initial.run_id) != (
                latest.raw_description.workflow_execution_info.first_run_id or latest.run_id
            ):
                raise BackendUnavailable()
            first_build: str | None = None
            # A description reports the current code version of a RUN, which
            # may already have upgraded. The first completed Workflow Task is
            # durable evidence of the actual initial code assignment.
            async for event in self.client.get_workflow_handle(
                workflow_id, run_id=run_id
            ).fetch_history_events(rpc_timeout=self.rpc_timeout):
                if event.HasField("workflow_task_completed_event_attributes"):
                    attributes = event.workflow_task_completed_event_attributes
                    first_build = (
                        attributes.deployment_version.build_id
                        or attributes.worker_deployment_version.removeprefix(
                            binding.worker_deployment_name + "."
                        )
                        or None
                    )
                    if (
                        attributes.deployment_version.deployment_name
                        and attributes.deployment_version.deployment_name
                        != binding.worker_deployment_name
                    ):
                        raise BackendUnavailable()
                    break
            builds: list[str | None] = [first_build]
            for description in (latest,):
                info = description.raw_description.workflow_execution_info.versioning_info
                version = info.deployment_version
                build = version.build_id or info.version.removeprefix(
                    binding.worker_deployment_name + "."
                )
                deployment = version.deployment_name
                if build and (
                    (deployment and deployment != binding.worker_deployment_name)
                    or (binding.versioning_behavior == "pinned" and build != binding.build_id)
                ):
                    raise BackendUnavailable()
                builds.append(build or None)
            return builds[0], builds[1]
        except Exception as exc:
            raise BackendUnavailable() from exc
