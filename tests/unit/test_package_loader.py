"""Installed code must match trusted manifest registrations and exact locks."""

from __future__ import annotations

import importlib.metadata
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from contracts import ActivityResponse, PackageActivityRequest, PackageRuntimeStartRequest
from temporalio import activity, workflow
from temporalio.common import VersioningBehavior
from workflow_sdk.packages import (
    PackageValidationError,
    load_installed_package,
    manifest_hash,
    verify_installed_dependencies,
)

from tests.contract.packages.factories import make_manifest


@workflow.defn(name="PackageWorkflowV1", versioning_behavior=VersioningBehavior.PINNED)
class TrustedWorkflow:
    @workflow.run
    async def run(self, payload: PackageRuntimeStartRequest) -> dict[str, Any]:
        return {"package": payload.release_binding.package_id}


@activity.defn(name="validate_request.pkg.v1")
async def trusted_activity(payload: PackageActivityRequest) -> ActivityResponse:
    return ActivityResponse(capability=payload.capability, output={"valid": True})


@activity.defn(name="validate_request.pkg.v1")
async def incompatible_activity(payload: dict[str, Any]) -> dict[str, Any]:
    return payload


def install_fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "manifest.json").write_text(make_manifest().model_dump_json(), encoding="utf-8")
    monkeypatch.setattr("importlib.resources.files", lambda module: tmp_path)
    monkeypatch.setattr(
        "workflow_sdk.packages.loader._entrypoint",
        lambda value: TrustedWorkflow if value.startswith("workflow_sdk") else trusted_activity,
    )


def test_loader_accepts_operator_trusted_module_with_matching_named_handlers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    install_fixture(tmp_path, monkeypatch)
    loaded = load_installed_package(
        "installed_reference",
        trusted_package_modules=("installed_reference",),
        verify_dependencies=False,
    )
    assert loaded.workflows == (TrustedWorkflow,)
    assert loaded.activities == (trusted_activity,)
    assert loaded.manifest_hash == manifest_hash(make_manifest())


def test_unapproved_manifest_hash_fails_before_importing_handlers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    install_fixture(tmp_path, monkeypatch)
    calls: list[str] = []
    monkeypatch.setattr(
        "workflow_sdk.packages.loader._entrypoint", lambda value: calls.append(value)
    )
    with pytest.raises(PackageValidationError, match="approved release hash"):
        load_installed_package(
            "installed_reference",
            trusted_package_modules=("installed_reference",),
            expected_manifest_hash="b" * 64,
            verify_dependencies=False,
        )
    assert calls == []


def test_named_activity_with_wrong_transport_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    install_fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(
        "workflow_sdk.packages.loader._entrypoint",
        lambda value: (
            TrustedWorkflow if value.startswith("workflow_sdk") else incompatible_activity
        ),
    )
    with pytest.raises(PackageValidationError, match="transport contract"):
        load_installed_package(
            "installed_reference",
            trusted_package_modules=("installed_reference",),
            verify_dependencies=False,
        )


@pytest.mark.parametrize(
    "scenario",
    ["missing", "different_version", "missing_transitive", "wrong_transitive", "direct_url"],
)
def test_installed_dependency_closure_checks_actual_distributions(
    monkeypatch: pytest.MonkeyPatch, scenario: str
) -> None:
    manifest = make_manifest()
    locked = {item.name: item.version for item in manifest.dependencies}

    def distribution(name: str) -> Any:
        if name == "nexusflow-workflow-sdk":
            if scenario == "missing":
                raise importlib.metadata.PackageNotFoundError(name)
            if scenario == "different_version":
                return SimpleNamespace(version="9.0", requires=[])
            requirement = {
                "missing_transitive": "unlocked-runtime>=1.0",
                "wrong_transitive": "temporalio<1.0",
                "direct_url": "temporalio @ https://example.com/mutable.whl",
            }[scenario]
            return SimpleNamespace(version=locked[name], requires=[requirement])
        return SimpleNamespace(version=locked[name], requires=[])

    monkeypatch.setattr("importlib.metadata.distribution", distribution)
    with pytest.raises(PackageValidationError):
        verify_installed_dependencies(manifest)


def test_inactive_optional_dependency_does_not_expand_runtime_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = make_manifest()
    versions = {item.name: item.version for item in manifest.dependencies}
    monkeypatch.setattr(
        "importlib.metadata.distribution",
        lambda name: SimpleNamespace(
            version=versions[name], requires=["not-installed>=1; extra == 'optional'"]
        ),
    )
    verify_installed_dependencies(manifest)
