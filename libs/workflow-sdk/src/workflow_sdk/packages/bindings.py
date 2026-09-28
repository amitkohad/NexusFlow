"""Resolve trusted immutable content into a private execution routing snapshot."""

from __future__ import annotations

from collections.abc import Mapping

from contracts import (
    ActivityStep,
    ApprovalStep,
    PackageActivityBinding,
    PackageDefinitionRevision,
    PackageExecutionBinding,
    PackageManifest,
    WorkflowRelease,
)

from .validation import PackageValidationError, manifest_hash, validate_manifest


def validate_release(manifest: PackageManifest, release: WorkflowRelease) -> WorkflowRelease:
    manifest = validate_manifest(manifest)
    release = WorkflowRelease.model_validate(release.model_dump(mode="python"))
    if (
        release.package_id != manifest.package_id
        or release.package_version != manifest.package_version
        or release.build_id != manifest.build_id
        or release.manifest_hash != manifest_hash(manifest)
    ):
        raise PackageValidationError(
            "Release descriptor does not match the immutable installed manifest"
        )
    return release


def resolve_binding(
    manifest: PackageManifest,
    release: WorkflowRelease,
    definition: PackageDefinitionRevision,
    *,
    namespace: str = "default",
    queue_bindings: Mapping[str, str] | None = None,
    eligible_build_ids: tuple[str, ...] | None = None,
) -> PackageExecutionBinding:
    manifest = validate_manifest(manifest)
    release = validate_release(manifest, release)
    if definition not in manifest.definitions:
        raise PackageValidationError(
            "Requested definition revision is not included in the package release"
        )
    logical = {queue.name for queue in manifest.queues}
    bindings = (
        dict(queue_bindings)
        if queue_bindings is not None
        else {name: f"{manifest.package_id}-tq" for name in logical}
    )
    if set(bindings) != logical:
        raise PackageValidationError(
            "Environment bindings must cover exactly the package-owned queues"
        )
    workflow = next(
        item
        for item in manifest.workflow_registrations
        if item.name == definition.runtime_workflow_type
    )
    required_capabilities = {
        step.capability
        for step in definition.definition_document.steps.values()
        if isinstance(step, ActivityStep)
    }
    if any(
        isinstance(step, ApprovalStep) for step in definition.definition_document.steps.values()
    ):
        required_capabilities.add("create_approval_task")
    return PackageExecutionBinding(
        package_id=manifest.package_id,
        package_release_id=release.package_release_id,
        package_version=manifest.package_version,
        build_id=manifest.build_id,
        manifest_hash=release.manifest_hash,
        artifact_digest=release.artifact_digest,
        image_digest=release.image_digest,
        worker_deployment_name=manifest.worker_deployment_name,
        temporal_namespace=namespace,
        workflow_task_queue=bindings[workflow.logical_queue],
        definition_id=definition.definition_id,
        definition_version=definition.version,
        definition_content_hash=definition.content_hash,
        activity_bindings=tuple(
            PackageActivityBinding(
                capability=item.capability,
                activity_name=item.activity_name,
                contract_version=item.contract_version,
                logical_queue=item.logical_queue,
                task_queue=bindings[item.logical_queue],
            )
            for item in manifest.activity_registrations
            if item.capability in required_capabilities
        ),
        eligible_build_ids=eligible_build_ids or (manifest.build_id,),
        versioning_behavior=workflow.versioning_behavior,
        continue_as_new_policy=workflow.continue_as_new_policy,
    )
