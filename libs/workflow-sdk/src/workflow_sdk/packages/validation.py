"""Pure package closure and canonical content validation; no installed-code lookup."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from typing import Any

from contracts import ActivityStep, ApprovalStep, DefinitionDocument, PackageManifest
from pydantic import ValidationError

from workflow_sdk.definitions import validate_definition

ACTIVITY_ENTRYPOINTS = {
    capability: f"nexusflow_activities:{capability}"
    for capability in (
        "validate_request",
        "send_notification",
        "post_adjustment",
        "create_approval_task",
        "risk_check",
        "record_rejection",
    )
}
WORKFLOW_ENTRYPOINTS = {
    "PackageWorkflowV1": "workflow_sdk.runtime:PackageWorkflowV1",
    "PackageAutoUpgradeWorkflowV1": "workflow_sdk.runtime:PackageAutoUpgradeWorkflowV1",
}
REQUIRED_DEPENDENCIES = frozenset(
    {
        "nexusflow-contracts",
        "nexusflow-common",
        "nexusflow-workflow-sdk",
        "nexusflow-workflows",
        "temporalio",
        "pydantic",
        "packaging",
    }
)


class PackageValidationError(ValueError):
    """A safe configuration error raised before code registration or polling."""

    code = "package_invalid"


def canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def definition_hash(document: DefinitionDocument) -> str:
    return hashlib.sha256(canonical_bytes(document.model_dump(mode="json"))).hexdigest()


def canonical_manifest_document(manifest: PackageManifest) -> dict[str, Any]:
    """Keep the original v1 digest when the new task mode is absent/disabled."""

    document = manifest.model_dump(mode="json")
    if not manifest.durable_human_tasks:
        document.pop("durable_human_tasks")
    return document


def manifest_hash(manifest: PackageManifest) -> str:
    return hashlib.sha256(canonical_bytes(canonical_manifest_document(manifest))).hexdigest()


def dependency_lock_hash(manifest: PackageManifest) -> str:
    values = sorted(
        (
            {"name": normalize_distribution(item.name), "version": item.version}
            for item in manifest.dependencies
        ),
        key=lambda item: item["name"],
    )
    return hashlib.sha256(canonical_bytes(values)).hexdigest()


def normalize_distribution(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _unique_json(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise PackageValidationError("Package JSON must not contain duplicate object keys")
        result[key] = value
    return result


def _invalid_number(value: str) -> Any:
    raise PackageValidationError("Package JSON must contain finite numbers")


def _unique(values: list[object], label: str) -> None:
    if len(set(values)) != len(values):
        raise PackageValidationError(f"Duplicate {label} in package manifest")


def validate_manifest(
    document: PackageManifest | Mapping[str, Any] | str | bytes,
) -> PackageManifest:
    """Validate syntax, exact revisions, deterministic graph and executable closure."""
    try:
        if isinstance(document, (str, bytes)):
            raw = json.loads(
                document, object_pairs_hook=_unique_json, parse_constant=_invalid_number
            )
        elif isinstance(document, PackageManifest):
            raw = document.model_dump(mode="python")
        else:
            raw = dict(document)
        manifest = PackageManifest.model_validate(raw)
    except (ValidationError, TypeError, RecursionError, json.JSONDecodeError) as exc:
        raise PackageValidationError(
            "Package manifest does not match the closed version 1 schema"
        ) from exc
    _unique([item.name for item in manifest.queues], "logical queue")
    _unique([item.name for item in manifest.workflow_registrations], "Workflow registration")
    _unique([item.capability for item in manifest.activity_registrations], "capability")
    _unique(
        [item.activity_name for item in manifest.activity_registrations], "Activity registration"
    )
    _unique(
        [(item.workflow_type, item.version) for item in manifest.definitions], "definition revision"
    )
    _unique(
        [(item.definition_id, item.version) for item in manifest.definitions],
        "definition identity/version",
    )
    _unique(
        [normalize_distribution(item.name) for item in manifest.dependencies], "locked dependency"
    )
    queues = {item.name: item.role for item in manifest.queues}
    workflows = {item.name: item for item in manifest.workflow_registrations}
    activities = {item.capability: item for item in manifest.activity_registrations}
    used_queues: set[str] = set()
    for workflow_registration in manifest.workflow_registrations:
        if WORKFLOW_ENTRYPOINTS.get(workflow_registration.name) != workflow_registration.entrypoint:
            raise PackageValidationError(
                "Workflow entrypoint is not an approved installed registration"
            )
        expected_behavior = (
            "auto_upgrade"
            if workflow_registration.name == "PackageAutoUpgradeWorkflowV1"
            else "pinned"
        )
        if workflow_registration.versioning_behavior != expected_behavior:
            raise PackageValidationError(
                "Workflow registration versioning behavior differs from its executable type"
            )
        if workflow_registration.continue_as_new_policy != "inherit":
            raise PackageValidationError(
                "Explicit Continue-As-New upgrade requires an approved mechanism unavailable in version 1"
            )
        if queues.get(workflow_registration.logical_queue) != "workflow":
            raise PackageValidationError("Workflow queue is not an owned Workflow-role queue")
        used_queues.add(workflow_registration.logical_queue)
    for registration in manifest.activity_registrations:
        if ACTIVITY_ENTRYPOINTS.get(registration.capability) != registration.entrypoint:
            raise PackageValidationError(
                "Activity entrypoint is not an approved installed registration"
            )
        if registration.activity_name != f"{registration.capability}.pkg.v1":
            raise PackageValidationError(
                "Activity name is incompatible with its capability contract"
            )
        if queues.get(registration.logical_queue) != "activity":
            raise PackageValidationError("Activity queue is not an owned Activity-role queue")
        used_queues.add(registration.logical_queue)
    if used_queues != set(queues):
        raise PackageValidationError("Manifest queues must have compatible declared registrations")
    required_capabilities: set[str] = set()
    used_workflows: set[str] = set()
    for revision in manifest.definitions:
        if revision.content_hash != definition_hash(revision.definition_document):
            raise PackageValidationError(
                "Included definition content does not match its exact hash"
            )
        if revision.runtime_workflow_type not in workflows:
            raise PackageValidationError(
                "Included definition has no registered executable Workflow"
            )
        used_workflows.add(revision.runtime_workflow_type)
        validate_definition(
            revision.definition_document,
            allowed_capabilities=set(activities) - {"create_approval_task"},
        )
        for step in revision.definition_document.steps.values():
            if isinstance(step, ActivityStep):
                required_capabilities.add(step.capability)
                if step.compensation is not None:
                    raise PackageValidationError(
                        "Compensation is not implemented by package runtime version 1"
                    )
                activity_registration = activities[step.capability]
                if step.task_queue not in {None, activity_registration.logical_queue}:
                    raise PackageValidationError(
                        "Definitions may declare only package-owned logical queues"
                    )
                if step.contract_version not in {None, activity_registration.contract_version}:
                    raise PackageValidationError(
                        "Definition Activity contract is incompatible with its registration"
                    )
            elif isinstance(step, ApprovalStep):
                required_capabilities.add("create_approval_task")
    if required_capabilities != set(activities):
        raise PackageValidationError(
            "Package must include exactly its complete Activity dependency closure, including approval creation"
        )
    if used_workflows != set(workflows):
        raise PackageValidationError("Package must not register unrelated Workflows")
    dependencies = {normalize_distribution(item.name) for item in manifest.dependencies}
    required_dependencies = REQUIRED_DEPENDENCIES | (
        {"nexusflow-activities"} if activities else set()
    )
    if not required_dependencies <= dependencies:
        raise PackageValidationError(
            "Package dependency lock omits a required runtime distribution"
        )
    if (
        manifest.dependency_lock_hash is not None
        and manifest.dependency_lock_hash != dependency_lock_hash(manifest)
    ):
        raise PackageValidationError("Dependency lock does not match its canonical hash")
    return manifest
