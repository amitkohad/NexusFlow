"""Immutable workflow-package content and private execution transport contracts.

Artifact digests belong to an external descriptor, never the embedded manifest.
Desired deployment capacity and observed readiness are separate control-plane data.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import Field, StrictBool, StrictInt, StrictStr, StringConstraints, model_validator

from .base import (
    ContractModel,
    Identifier,
    JsonObject,
    NonNegativeInt,
    OffsetDatetime,
    PositiveNumber,
    SHA256Hash,
)
from .definitions import DefinitionDocument
from .domain import BusinessContext
from .runtime import ActivityRequest, RuntimeStartRequest

PackageIdentifier = Annotated[
    StrictStr,
    StringConstraints(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]*$"),
]
PackageText = Annotated[StrictStr, StringConstraints(min_length=1, max_length=256, pattern=r"\S")]
ArtifactDigest = Annotated[StrictStr, StringConstraints(pattern=r"^sha256:[a-f0-9]{64}$")]
EntryPoint = Annotated[
    StrictStr,
    StringConstraints(
        max_length=256,
        pattern=r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*:[A-Za-z_][A-Za-z0-9_]*$",
    ),
]
VersioningBehavior = Literal["pinned", "auto_upgrade"]
ContinueAsNewPolicy = Literal["inherit", "explicit_upgrade"]
ExecutorRole = Literal["mixed", "workflow", "activity"]


class LogicalQueue(ContractModel):
    name: PackageIdentifier
    role: Literal["workflow", "activity"]


class LockedDependency(ContractModel):
    name: Annotated[
        StrictStr, StringConstraints(max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
    ]
    version: Annotated[
        StrictStr,
        StringConstraints(max_length=64, pattern=r"^[0-9]+(?:\.[0-9]+)*(?:[a-z][a-z0-9.]*)?$"),
    ]


class PackageDefinitionRevision(ContractModel):
    definition_id: PackageText
    workflow_type: PackageIdentifier
    version: PackageIdentifier
    content_hash: SHA256Hash
    definition_document: DefinitionDocument
    runtime_workflow_type: PackageIdentifier = "PackageWorkflowV1"


class WorkflowRegistration(ContractModel):
    name: PackageIdentifier
    entrypoint: EntryPoint
    logical_queue: PackageIdentifier
    versioning_behavior: VersioningBehavior = "pinned"
    continue_as_new_policy: ContinueAsNewPolicy = "inherit"

    @model_validator(mode="after")
    def validate_upgrade_policy(self) -> WorkflowRegistration:
        if self.versioning_behavior == "auto_upgrade" and self.continue_as_new_policy != "inherit":
            raise ValueError("Continue-As-New upgrade policy applies only to Pinned workflows")
        return self


class ActivityRegistration(ContractModel):
    capability: PackageIdentifier
    activity_name: PackageText
    contract_version: Literal["1.0"] = "1.0"
    entrypoint: EntryPoint
    logical_queue: PackageIdentifier


class PackageManifest(ContractModel):
    schema_version: Literal["1.0"] = "1.0"
    package_id: PackageIdentifier
    package_version: PackageIdentifier
    build_id: PackageIdentifier
    worker_deployment_name: PackageIdentifier
    definitions: tuple[PackageDefinitionRevision, ...] = Field(min_length=1, max_length=100)
    workflow_registrations: tuple[WorkflowRegistration, ...] = Field(min_length=1, max_length=100)
    activity_registrations: tuple[ActivityRegistration, ...] = Field(default=(), max_length=1000)
    queues: tuple[LogicalQueue, ...] = Field(min_length=1, max_length=100)
    dependencies: tuple[LockedDependency, ...] = Field(min_length=1, max_length=1000)
    dependency_lock_hash: SHA256Hash | None = None
    secret_references: tuple[PackageText, ...] = Field(default=(), max_length=100)
    durable_human_tasks: StrictBool = False


class WorkflowPackage(BusinessContext):
    package_id: PackageIdentifier
    name: PackageText
    owner: PackageText
    workflow_types: tuple[PackageIdentifier, ...] = Field(min_length=1, max_length=100)
    status: Literal["active", "deprecated", "retired"] = "active"
    created_by: PackageText | None = None
    created_at: OffsetDatetime | None = None


class WorkflowRelease(ContractModel):
    """Post-build immutable provenance; local artifacts need no container image."""

    schema_version: Literal["1.0"] = "1.0"
    package_release_id: PackageIdentifier
    package_id: PackageIdentifier
    package_version: PackageIdentifier
    build_id: PackageIdentifier
    manifest_hash: SHA256Hash
    artifact_digest: ArtifactDigest
    image_digest: ArtifactDigest | None = None
    source_revision: PackageText | None = None
    published_by: PackageText | None = None
    published_at: OffsetDatetime | None = None


class ExecutorPool(ContractModel):
    pool_id: PackageIdentifier
    package_id: PackageIdentifier
    package_release_id: PackageIdentifier
    environment: Literal["local", "dev", "test", "prod"] = "local"
    temporal_namespace: PackageText = "default"
    role: ExecutorRole = "mixed"
    worker_deployment_name: PackageIdentifier
    build_id: PackageIdentifier
    queue_bindings: dict[PackageIdentifier, PackageText] = Field(min_length=1, max_length=100)
    replicas: Annotated[StrictInt, Field(ge=0, le=10000)] = 1
    min_replicas: Annotated[StrictInt, Field(ge=0, le=10000)] = 1
    max_replicas: Annotated[StrictInt, Field(ge=1, le=10000)] = 1
    workflow_task_slots: Annotated[StrictInt, Field(ge=1, le=10000)] = 100
    activity_task_slots: Annotated[StrictInt, Field(ge=1, le=10000)] = 100
    workflow_task_pollers: Annotated[StrictInt, Field(ge=1, le=100)] = 5
    activity_task_pollers: Annotated[StrictInt, Field(ge=1, le=100)] = 5
    max_activities_per_second: PositiveNumber | None = None
    max_task_queue_activities_per_second: PositiveNumber | None = None
    shutdown_grace_seconds: Annotated[StrictInt, Field(ge=1, le=3600)] = 30
    resources: JsonObject = Field(default_factory=dict)
    desired_state: Literal["serving", "draining", "retained", "retired"] = "serving"
    observed_state: Literal[
        "pending", "starting", "ready", "draining", "retained", "retired", "failed"
    ] = "pending"
    observed_replicas: Annotated[StrictInt, Field(ge=0, le=10000)] = 0
    last_reconciled_at: OffsetDatetime | None = None

    @model_validator(mode="after")
    def validate_capacity(self) -> ExecutorPool:
        if not self.min_replicas <= self.replicas <= self.max_replicas:
            raise ValueError("Replica count must be within minimum and maximum bounds")
        if (
            self.environment == "prod"
            and self.desired_state in {"serving", "retained"}
            and self.min_replicas < 2
        ):
            raise ValueError("Production serving pools require at least two replicas")
        if self.observed_state == "ready" and self.observed_replicas < 1:
            raise ValueError("Observed readiness requires actual polling replicas")
        return self


class PackageActivityBinding(ContractModel):
    capability: PackageIdentifier
    activity_name: PackageText
    contract_version: Literal["1.0"] = "1.0"
    logical_queue: PackageIdentifier
    task_queue: PackageText


class PackageExecutionBinding(ContractModel):
    """Immutable intended provenance; Temporal-confirmed routing is recorded separately."""

    package_id: PackageIdentifier
    package_release_id: PackageIdentifier
    package_version: PackageIdentifier
    build_id: PackageIdentifier
    manifest_hash: SHA256Hash
    artifact_digest: ArtifactDigest
    image_digest: ArtifactDigest | None = None
    worker_deployment_name: PackageIdentifier
    temporal_namespace: PackageText
    workflow_task_queue: PackageText
    definition_id: PackageText
    definition_version: PackageIdentifier
    definition_content_hash: SHA256Hash
    activity_bindings: tuple[PackageActivityBinding, ...] = Field(default=(), max_length=1000)
    eligible_build_ids: tuple[PackageIdentifier, ...] = Field(min_length=1, max_length=100)
    versioning_behavior: VersioningBehavior = "pinned"
    continue_as_new_policy: ContinueAsNewPolicy = "inherit"
    task_api_required: StrictBool = False

    @model_validator(mode="after")
    def validate_routing(self) -> PackageExecutionBinding:
        if self.build_id not in self.eligible_build_ids:
            raise ValueError(
                "Intended Build ID must belong to the captured eligible routing policy"
            )
        if len(set(self.eligible_build_ids)) != len(self.eligible_build_ids):
            raise ValueError("Eligible Build IDs must be unique")
        capabilities = [item.capability for item in self.activity_bindings]
        if len(set(capabilities)) != len(capabilities):
            raise ValueError("Activity binding capabilities must be unique")
        if self.versioning_behavior == "auto_upgrade" and self.continue_as_new_policy != "inherit":
            raise ValueError("Continue-As-New upgrade policy applies only to Pinned workflows")
        return self


class PackageContinuationState(ContractModel):
    next_step: Identifier
    results: JsonObject = Field(default_factory=dict)
    transitions: tuple[JsonObject, ...] = ()
    continuation_count: NonNegativeInt = 0


class PackageRuntimeStartRequest(RuntimeStartRequest):
    release_binding: PackageExecutionBinding
    continuation: PackageContinuationState | None = None


class PackageActivityRequest(ActivityRequest):
    release_binding: PackageExecutionBinding
    first_execution_run_id: PackageText | None = None
