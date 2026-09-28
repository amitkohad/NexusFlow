"""Small exact package fixtures for contract and closure rejection checks."""

from __future__ import annotations

from contracts import DefinitionDocument, PackageManifest, WorkflowRelease
from workflow_sdk.packages import definition_hash, manifest_hash


def make_manifest(*, approval: bool = False) -> PackageManifest:
    definition = DefinitionDocument.model_validate(
        {
            "start_at": "validate",
            "steps": {
                "validate": {
                    "type": "activity",
                    "capability": "validate_request",
                    "next": "approval" if approval else "done",
                },
                **(
                    {
                        "approval": {
                            "type": "approval",
                            "on_approved": "done",
                            "on_rejected": "done",
                            "on_timeout": "done",
                        }
                    }
                    if approval
                    else {}
                ),
                "done": {"type": "end"},
            },
        }
    )
    capabilities = ["validate_request", *(["create_approval_task"] if approval else [])]
    return PackageManifest.model_validate(
        {
            "package_id": "validation-reference",
            "package_version": "1.0.0",
            "build_id": "validation-reference-1.0.0",
            "worker_deployment_name": "validation-reference",
            "definitions": [
                {
                    "definition_id": "validation-ref",
                    "workflow_type": "validation_reference",
                    "version": "1.0",
                    "content_hash": definition_hash(definition),
                    "definition_document": definition.model_dump(mode="json"),
                }
            ],
            "workflow_registrations": [
                {
                    "name": "PackageWorkflowV1",
                    "entrypoint": "workflow_sdk.runtime:PackageWorkflowV1",
                    "logical_queue": "workflow",
                }
            ],
            "activity_registrations": [
                {
                    "capability": capability,
                    "activity_name": f"{capability}.pkg.v1",
                    "entrypoint": f"nexusflow_activities:{capability}",
                    "logical_queue": "activities",
                }
                for capability in capabilities
            ],
            "queues": [
                {"name": "workflow", "role": "workflow"},
                {"name": "activities", "role": "activity"},
            ],
            "dependencies": [
                {
                    "name": name,
                    "version": "1.33.0"
                    if name == "temporalio"
                    else "26.3"
                    if name == "packaging"
                    else "2.13.4"
                    if name == "pydantic"
                    else "0.1.0",
                }
                for name in (
                    "nexusflow-contracts",
                    "nexusflow-common",
                    "nexusflow-workflow-sdk",
                    "nexusflow-workflows",
                    "nexusflow-activities",
                    "temporalio",
                    "pydantic",
                    "packaging",
                )
            ],
        }
    )


def make_release(manifest: PackageManifest) -> WorkflowRelease:
    return WorkflowRelease(
        package_release_id=f"{manifest.package_id}-{manifest.package_version}",
        package_id=manifest.package_id,
        package_version=manifest.package_version,
        build_id=manifest.build_id,
        manifest_hash=manifest_hash(manifest),
        artifact_digest="sha256:" + "a" * 64,
    )
