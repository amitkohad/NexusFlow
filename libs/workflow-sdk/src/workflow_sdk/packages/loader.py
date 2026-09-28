"""Installed-package loading is restricted to operator-trusted registrations."""

from __future__ import annotations

import importlib
import importlib.metadata
import importlib.resources
from collections.abc import Callable, Collection
from dataclasses import dataclass
from typing import Any, get_origin

from contracts import (
    ActivityResponse,
    PackageActivityRequest,
    PackageManifest,
    PackageRuntimeStartRequest,
)
from packaging.requirements import Requirement

from .validation import (
    PackageValidationError,
    manifest_hash,
    normalize_distribution,
    validate_manifest,
)

DEFAULT_PACKAGE_MODULES = ("customer_adjustment_package", "validation_reference_package")


@dataclass(frozen=True)
class LoadedPackage:
    manifest: PackageManifest
    workflows: tuple[type, ...]
    activities: tuple[Callable[..., Any], ...]
    manifest_hash: str


def verify_installed_dependencies(manifest: PackageManifest) -> None:
    """Verify exact installed versions and every active transitive dependency."""
    locked = {normalize_distribution(item.name): item.version for item in manifest.dependencies}
    for name, expected in locked.items():
        try:
            installed = importlib.metadata.distribution(name)
        except importlib.metadata.PackageNotFoundError as exc:
            raise PackageValidationError("A locked package dependency is not installed") from exc
        if installed.version != expected:
            raise PackageValidationError(
                "Installed dependency version differs from the package lock"
            )
        for requirement_text in installed.requires or ():
            requirement = Requirement(requirement_text)
            if requirement.marker is not None and not requirement.marker.evaluate({"extra": ""}):
                continue
            dependency = normalize_distribution(requirement.name)
            if requirement.url is not None:
                raise PackageValidationError(
                    "Direct URL dependencies are not supported in immutable package locks"
                )
            if dependency not in locked or locked[dependency] not in requirement.specifier:
                raise PackageValidationError(
                    "Package lock omits or conflicts with an active transitive dependency"
                )


def _entrypoint(value: str) -> Any:
    module, name = value.split(":", 1)
    try:
        return getattr(importlib.import_module(module), name)
    except (ImportError, AttributeError) as exc:
        raise PackageValidationError("An approved installed handler is unavailable") from exc


def load_installed_package(
    package_module: str,
    *,
    trusted_package_modules: Collection[str] = DEFAULT_PACKAGE_MODULES,
    expected_manifest_hash: str | None = None,
    verify_dependencies: bool = True,
) -> LoadedPackage:
    """Validate content and closure before importing any executable handlers.

    The host resolves trusted modules from installed distribution entrypoints.
    The bounded default list is for the shipped reference packages, not requests.
    """
    if package_module not in trusted_package_modules:
        raise PackageValidationError("Workflow package module is not trusted by this executor")
    try:
        source = importlib.resources.files(package_module).joinpath("manifest.json").read_bytes()
    except (ImportError, FileNotFoundError, TypeError) as exc:
        raise PackageValidationError("Trusted package has no installed manifest resource") from exc
    if len(source) > 4 * 1024 * 1024:
        raise PackageValidationError("Installed package manifest exceeds the supported size")
    manifest = validate_manifest(source)
    digest = manifest_hash(manifest)
    if expected_manifest_hash is not None and digest != expected_manifest_hash:
        raise PackageValidationError(
            "Installed package manifest differs from the approved release hash"
        )
    if verify_dependencies:
        verify_installed_dependencies(manifest)
    from temporalio import activity, workflow
    from temporalio.common import VersioningBehavior

    workflows: list[type] = []
    activities: list[Callable[..., Any]] = []
    for workflow_registration in manifest.workflow_registrations:
        handler = _entrypoint(workflow_registration.entrypoint)
        if not isinstance(handler, type):
            raise PackageValidationError("Workflow entrypoint is not a registered class")
        definition = workflow._Definition.from_class(handler)
        if definition is None or definition.name != workflow_registration.name:
            raise PackageValidationError(
                "Workflow executable registration differs from its manifest"
            )
        accepts_package = definition.arg_types == [PackageRuntimeStartRequest]
        accepts_validated_dict = bool(
            definition.arg_types
            and len(definition.arg_types) == 1
            and get_origin(definition.arg_types[0]) is dict
        )
        if not (accepts_package or accepts_validated_dict):
            raise PackageValidationError(
                "Installed Workflow does not accept the package runtime contract"
            )
        expected_behavior = (
            VersioningBehavior.PINNED
            if workflow_registration.versioning_behavior == "pinned"
            else VersioningBehavior.AUTO_UPGRADE
        )
        if definition.versioning_behavior != expected_behavior:
            raise PackageValidationError(
                "Installed Workflow versioning behavior differs from its manifest"
            )
        workflows.append(handler)
    for registration in manifest.activity_registrations:
        handler = _entrypoint(registration.entrypoint)
        if not callable(handler):
            raise PackageValidationError("Activity entrypoint is not callable")
        activity_definition = activity._Definition.from_callable(handler)
        if activity_definition is None or activity_definition.name != registration.activity_name:
            raise PackageValidationError(
                "Activity executable registration differs from its manifest"
            )
        if (
            activity_definition.arg_types != [PackageActivityRequest]
            or activity_definition.ret_type is not ActivityResponse
        ):
            raise PackageValidationError(
                "Installed Activity does not implement the package transport contract"
            )
        activities.append(handler)
    return LoadedPackage(manifest, tuple(workflows), tuple(activities), digest)


load_package = load_installed_package
