"""Strict versioned package transport and immutable provenance boundaries."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from contracts import (
    ExecutorPool,
    LockedDependency,
    PackageActivityRequest,
    PackageManifest,
    PackageRuntimeStartRequest,
    RuntimeContext,
    WorkflowRegistration,
    WorkflowRelease,
)
from jsonschema import Draft202012Validator
from pydantic import ValidationError
from workflow_sdk.packages import manifest_hash, resolve_binding
from workflow_sdk.packages.schema import schema_text

from tests.contract.packages.factories import make_manifest, make_release


def test_manifest_roundtrips_without_containing_digest() -> None:
    manifest = make_manifest()
    assert PackageManifest.model_validate_json(manifest.model_dump_json()) == manifest
    assert "artifact_digest" not in PackageManifest.model_fields
    assert "image_digest" not in PackageManifest.model_fields
    release = make_release(manifest)
    assert release.image_digest is None
    assert WorkflowRelease.model_validate_json(release.model_dump_json()) == release
    assert release.manifest_hash == manifest_hash(manifest)


@pytest.mark.parametrize(
    "field", ["artifact_digest", "image_digest", "manifest_hash", "source_code"]
)
def test_manifest_forbids_self_digest_and_executable_source(field: str) -> None:
    with pytest.raises(ValidationError, match="extra_forbidden"):
        PackageManifest.model_validate({**make_manifest().model_dump(), field: "a" * 64})


@pytest.mark.parametrize("value", ["latest", "1.*", ">=1.0", "main", "1.0+mutable", True, 1])
def test_dependencies_require_exact_scalar_versions(value: Any) -> None:
    with pytest.raises(ValidationError):
        LockedDependency(name="nexusflow-contracts", version=value)


@pytest.mark.parametrize(
    "field,value",
    [
        ("build_id", True),
        ("package_id", "bad/package"),
        ("package_version", ""),
        ("worker_deployment_name", " bad "),
    ],
)
def test_manifest_identity_is_bounded_and_strict(field: str, value: Any) -> None:
    with pytest.raises(ValidationError):
        PackageManifest.model_validate({**make_manifest().model_dump(), field: value})


@pytest.mark.parametrize("value", ["a" * 64, "sha256:bad", "sha256:" + "A" * 64, True])
def test_release_descriptor_requires_sha256_digest(value: Any) -> None:
    manifest = make_manifest()
    with pytest.raises(ValidationError):
        WorkflowRelease.model_validate(
            {**make_release(manifest).model_dump(), "artifact_digest": value}
        )


def test_binding_is_frozen_and_detaches_transport_nested_payloads() -> None:
    manifest = make_manifest()
    binding = resolve_binding(manifest, make_release(manifest), manifest.definitions[0])
    with pytest.raises(ValidationError, match="frozen"):
        binding.build_id = "different"
    context = RuntimeContext(
        tenant="acme",
        business_domain="finance",
        application="adjustments",
        workflow_type="validation_reference",
        definition_id="validation-ref",
        definition_version="1.0",
        business_reference="A-1",
        correlation_id="C-1",
        actor="requester",
    )
    start = PackageRuntimeStartRequest(
        context=context,
        definition_document=manifest.definitions[0].definition_document,
        release_binding=binding,
    )
    activity = PackageActivityRequest(
        workflow_id="W-1",
        run_id="R-1",
        step_id="validate",
        capability="validate_request",
        context=context,
        idempotency_key="W-1:validate:1.0",
        release_binding=binding,
    )
    assert PackageRuntimeStartRequest.model_validate_json(start.model_dump_json()) == start
    assert PackageActivityRequest.model_validate_json(activity.model_dump_json()) == activity
    assert start.continuation is None


def make_pool(**changes: Any) -> ExecutorPool:
    return ExecutorPool.model_validate(
        {
            "pool_id": "mixed",
            "package_id": "validation-reference",
            "package_release_id": "validation-reference-1.0.0",
            "worker_deployment_name": "validation-reference",
            "build_id": "validation-reference-1.0.0",
            "queue_bindings": {
                "workflow": "validation-reference-tq",
                "activities": "validation-reference-tq",
            },
            **changes,
        }
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"replicas": True},
        {"replicas": 2},
        {"min_replicas": 3},
        {"workflow_task_slots": 0},
        {"activity_task_pollers": 101},
        {"shutdown_grace_seconds": 0},
        {"max_activities_per_second": float("inf")},
        {"max_task_queue_activities_per_second": "2"},
        {"observed_state": "ready"},
        {"environment": "prod"},
    ],
)
def test_executor_rejects_ambiguous_capacity_or_false_readiness(changes: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        make_pool(**changes)


def test_production_replicas_are_separate_from_concurrency_and_observation() -> None:
    pool = make_pool(
        environment="prod",
        min_replicas=2,
        replicas=3,
        max_replicas=10,
        workflow_task_slots=20,
        activity_task_slots=50,
    )
    assert pool.replicas == 3 and pool.workflow_task_slots == 20
    assert pool.desired_state == "serving" and pool.observed_state == "pending"
    assert pool.observed_replicas == 0


@pytest.mark.parametrize("behavior", ["continue_as_new", "Auto-Upgrade", "PINNED"])
def test_continue_as_new_is_not_a_third_temporal_behavior(behavior: str) -> None:
    with pytest.raises(ValidationError):
        WorkflowRegistration.model_validate(
            {
                "name": "PackageWorkflowV1",
                "entrypoint": "workflow_sdk.runtime:PackageWorkflowV1",
                "logical_queue": "workflow",
                "versioning_behavior": behavior,
            }
        )


def test_checked_in_manifest_schema_matches_contract_and_validates_fixture() -> None:
    schema_path = (
        Path(__file__).resolve().parents[3]
        / "workflow-packages/schema/workflow-package-manifest-v1.schema.json"
    )
    assert schema_path.read_text(encoding="utf-8") == schema_text()
    schema = json.loads(schema_text())
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema).validate(make_manifest().model_dump(mode="json"))
