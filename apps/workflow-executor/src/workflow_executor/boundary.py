"""Pure installed-content guards, including retained revisions after upgrades."""

from __future__ import annotations

from typing import Any

from contracts import (
    ExecutorPool,
    PackageActivityRequest,
    PackageExecutionBinding,
    PackageManifest,
    PackageRuntimeStartRequest,
)
from temporalio import activity, workflow
from temporalio.exceptions import ApplicationError
from temporalio.worker import (
    ActivityInboundInterceptor,
    ExecuteActivityInput,
    ExecuteWorkflowInput,
    Interceptor,
    WorkflowInboundInterceptor,
    WorkflowInterceptorClassInput,
)
from workflow_sdk.packages import definition_hash, manifest_hash


def validate_execution_content(
    manifest: PackageManifest,
    pool: ExecutorPool,
    binding: PackageExecutionBinding,
    runtime_workflow_type: str,
) -> None:
    """Validate immutable old provenance against the actually installed release."""
    revisions = [
        item
        for item in manifest.definitions
        if (
            item.definition_id == binding.definition_id
            and item.version == binding.definition_version
            and item.content_hash == binding.definition_content_hash
            and item.runtime_workflow_type == runtime_workflow_type
        )
    ]
    registrations = {item.name: item for item in manifest.workflow_registrations}
    registration = registrations.get(runtime_workflow_type)
    valid = (
        len(revisions) == 1
        and registration is not None
        and binding.package_id == manifest.package_id
        and binding.worker_deployment_name == manifest.worker_deployment_name
        and binding.temporal_namespace == pool.temporal_namespace
    )
    if registration is not None:
        valid = valid and (
            registration.versioning_behavior == binding.versioning_behavior
            and registration.continue_as_new_policy == binding.continue_as_new_policy
            and pool.queue_bindings[registration.logical_queue] == binding.workflow_task_queue
        )
    if binding.versioning_behavior == "pinned":
        valid = valid and pool.build_id == binding.build_id
    if manifest.build_id == binding.build_id:
        valid = valid and (
            manifest_hash(manifest) == binding.manifest_hash
            and manifest.package_version == binding.package_version
        )
    activities = {item.capability: item for item in manifest.activity_registrations}
    for captured in binding.activity_bindings:
        current = activities.get(captured.capability)
        valid = valid and current is not None
        if current is not None:
            valid = valid and (
                current.activity_name == captured.activity_name
                and current.contract_version == captured.contract_version
                and current.logical_queue == captured.logical_queue
                and pool.queue_bindings[current.logical_queue] == captured.task_queue
            )
    if not valid:
        raise ApplicationError(
            "Installed package does not retain the execution's exact revision and handler contracts",
            type="PackageCompatibilityError",
            non_retryable=True,
        )


class PackageBoundaryInterceptor(Interceptor):
    """Capture validated installed content once; Workflow checks do no external I/O."""

    def __init__(self, manifest: PackageManifest, pool: ExecutorPool) -> None:
        self.manifest = manifest.model_copy(deep=True)
        self.pool = pool.model_copy(deep=True)

    def workflow_interceptor_class(
        self, input: WorkflowInterceptorClassInput
    ) -> type[WorkflowInboundInterceptor]:
        manifest = self.manifest
        pool = self.pool

        class PackageWorkflowBoundary(WorkflowInboundInterceptor):
            async def execute_workflow(self, input: ExecuteWorkflowInput) -> Any:
                request = PackageRuntimeStartRequest.model_validate(input.args[0])
                try:
                    validate_execution_content(
                        manifest, pool, request.release_binding, workflow.info().workflow_type
                    )
                    if (
                        definition_hash(request.definition_document)
                        != request.release_binding.definition_content_hash
                    ):
                        raise ApplicationError(
                            "Execution definition differs from its retained revision",
                            type="PackageCompatibilityError",
                            non_retryable=True,
                        )
                except ApplicationError:
                    # Existing commands must replay before rejecting a forced
                    # incompatible move at its first live execution boundary.
                    workflow.instance().reject_loaded_release(manifest.build_id)
                else:
                    workflow.instance().authorize_loaded_release(manifest.build_id)
                # This attestation comes only from installed manifest validation.
                # It is instance memory, never an accepted business payload/signal.
                return await self.next.execute_workflow(input)

        return PackageWorkflowBoundary

    def intercept_activity(
        self, next_interceptor: ActivityInboundInterceptor
    ) -> ActivityInboundInterceptor:
        manifest = self.manifest
        pool = self.pool

        class PackageActivityBoundary(ActivityInboundInterceptor):
            async def execute_activity(self, input: ExecuteActivityInput) -> Any:
                request = PackageActivityRequest.model_validate(input.args[0])
                info = activity.info()
                validate_execution_content(
                    manifest, pool, request.release_binding, info.workflow_type or ""
                )
                captured = next(
                    (
                        item
                        for item in request.release_binding.activity_bindings
                        if item.capability == request.capability
                    ),
                    None,
                )
                if (
                    captured is None
                    or captured.activity_name != info.activity_type
                    or captured.task_queue != info.task_queue
                    or info.workflow_id != request.workflow_id
                    or info.workflow_run_id != request.run_id
                    or info.workflow_namespace != request.release_binding.temporal_namespace
                ):
                    raise ApplicationError(
                        "Activity task differs from the retained package routing",
                        type="PackageCompatibilityError",
                        non_retryable=True,
                    )
                return await self.next.execute_activity(input)

        return PackageActivityBoundary(next_interceptor)
