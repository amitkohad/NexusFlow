"""Reject incomplete closure and routing before importing executable code."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest
from contracts import PackageManifest
from workflow_sdk.packages import (
    PackageValidationError,
    definition_hash,
    dependency_lock_hash,
    load_installed_package,
    manifest_hash,
    resolve_binding,
    validate_manifest,
    validate_release,
)

from tests.contract.packages.factories import make_manifest, make_release


def test_exact_approval_dependency_closure_includes_internal_creation() -> None:
    manifest = make_manifest(approval=True)
    assert validate_manifest(manifest) == manifest
    raw = manifest.model_dump(mode="json")
    raw["activity_registrations"] = raw["activity_registrations"][:1]
    with pytest.raises(PackageValidationError, match="approval creation"):
        validate_manifest(raw)


@pytest.mark.parametrize(
    "field",
    ["queues", "workflow_registrations", "activity_registrations", "definitions", "dependencies"],
)
def test_duplicate_manifest_ownership_is_rejected(field: str) -> None:
    raw = make_manifest().model_dump(mode="json")
    raw[field].append(deepcopy(raw[field][0]))
    with pytest.raises(PackageValidationError, match="Duplicate"):
        validate_manifest(raw)


@pytest.mark.parametrize(
    "field,value",
    [
        ("entrypoint", "os:system"),
        ("activity_name", "execute_capability"),
        ("logical_queue", "foreign"),
    ],
)
def test_activity_registration_must_be_named_and_trusted(field: str, value: Any) -> None:
    raw = make_manifest().model_dump(mode="json")
    raw["activity_registrations"][0][field] = value
    with pytest.raises(PackageValidationError):
        validate_manifest(raw)


def test_retained_revision_needs_exact_content_not_compatibility_claim() -> None:
    raw = make_manifest().model_dump(mode="json")
    retained = deepcopy(raw["definitions"][0])
    retained["version"] = "0.9"
    retained["definition_document"]["variables"] = {"old": True}
    raw["definitions"].append(retained)
    with pytest.raises(PackageValidationError, match="exact hash"):
        validate_manifest(raw)
    retained["content_hash"] = definition_hash(
        PackageManifest.model_validate(raw).definitions[1].definition_document
    )
    assert len(validate_manifest(raw).definitions) == 2


@pytest.mark.parametrize(
    "field,value", [("task_queue", "validation-tq"), ("contract_version", "2.0")]
)
def test_definition_rejects_legacy_concrete_queue_or_contract_drift(field: str, value: str) -> None:
    raw = make_manifest().model_dump(mode="json")
    revision = raw["definitions"][0]
    revision["definition_document"]["steps"]["validate"][field] = value
    parsed = PackageManifest.model_validate(raw)
    revision["content_hash"] = definition_hash(parsed.definitions[0].definition_document)
    with pytest.raises(PackageValidationError):
        validate_manifest(raw)


def test_canonical_hashes_reject_dependency_tampering_and_are_order_stable() -> None:
    manifest = make_manifest()
    raw = manifest.model_dump(mode="json")
    raw["dependency_lock_hash"] = dependency_lock_hash(manifest)
    assert validate_manifest(raw).dependency_lock_hash == dependency_lock_hash(manifest)
    raw["dependencies"][0]["version"] = "0.2.0"
    with pytest.raises(PackageValidationError, match="canonical hash"):
        validate_manifest(raw)
    assert manifest_hash(
        PackageManifest.model_validate(dict(reversed(list(manifest.model_dump().items()))))
    ) == manifest_hash(manifest)


@pytest.mark.parametrize(
    "raw", ['{"package_id":"x","package_id":"y"}', '{"number":NaN}', '{"number":Infinity}']
)
def test_manifest_json_rejects_duplicate_keys_and_nonfinite_numbers(raw: str) -> None:
    with pytest.raises(PackageValidationError):
        validate_manifest(raw)


def test_wrong_release_provenance_and_foreign_queue_mapping_are_rejected() -> None:
    manifest = make_manifest()
    release = make_release(manifest)
    with pytest.raises(PackageValidationError, match="descriptor"):
        validate_release(manifest, release.model_copy(update={"manifest_hash": "b" * 64}))
    with pytest.raises(PackageValidationError, match="owned queues"):
        resolve_binding(manifest, release, manifest.definitions[0], queue_bindings={"foreign": "x"})
    mixed = resolve_binding(manifest, release, manifest.definitions[0])
    assert mixed.workflow_task_queue == mixed.activity_bindings[0].task_queue
    roles = resolve_binding(
        manifest,
        release,
        manifest.definitions[0],
        queue_bindings={
            "workflow": "validation-workflow-tq",
            "activities": "validation-activities-tq",
        },
    )
    assert roles.workflow_task_queue != roles.activity_bindings[0].task_queue
    assert roles.build_id == mixed.build_id


def test_untrusted_package_never_imports_or_reads_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    monkeypatch.setattr("importlib.resources.files", lambda module: calls.append(module))
    with pytest.raises(PackageValidationError, match="not trusted"):
        load_installed_package("os", trusted_package_modules=())
    assert calls == []


def test_explicit_upgrade_rejected_until_approved_server_mechanism_exists() -> None:
    raw = make_manifest().model_dump(mode="json")
    raw["workflow_registrations"][0]["continue_as_new_policy"] = "explicit_upgrade"
    with pytest.raises(PackageValidationError, match="unavailable"):
        validate_manifest(raw)


def test_auto_upgrade_requires_distinct_compatible_registered_workflow() -> None:
    raw = make_manifest().model_dump(mode="json")
    raw["workflow_registrations"][0]["versioning_behavior"] = "auto_upgrade"
    with pytest.raises(PackageValidationError, match="versioning behavior"):
        validate_manifest(raw)
    raw["workflow_registrations"][0].update(
        name="PackageAutoUpgradeWorkflowV1",
        entrypoint="workflow_sdk.runtime:PackageAutoUpgradeWorkflowV1",
    )
    raw["definitions"][0]["runtime_workflow_type"] = "PackageAutoUpgradeWorkflowV1"
    assert validate_manifest(raw).workflow_registrations[0].versioning_behavior == "auto_upgrade"


def test_binding_contains_selected_revision_closure_while_manifest_contains_union() -> None:
    raw = make_manifest(approval=True).model_dump(mode="json")
    old = make_manifest().definitions[0].model_dump(mode="json")
    old["version"] = "0.9"
    raw["definitions"].append(old)
    manifest = validate_manifest(raw)
    binding = resolve_binding(manifest, make_release(manifest), manifest.definitions[1])
    assert {item.capability for item in manifest.activity_registrations} == {
        "validate_request",
        "create_approval_task",
    }
    assert {item.capability for item in binding.activity_bindings} == {"validate_request"}
