"""Synthetic release provenance and trusted source package helpers for tests only."""

from __future__ import annotations

from contracts import PackageManifest, PackageRuntimeStartRequest, RuntimeContext, WorkflowRelease
from workflow_executor import load_executor_package
from workflow_sdk.packages import LoadedPackage, manifest_hash, resolve_binding


def source_package(name: str = "customer-adjustment") -> LoadedPackage:
    return load_executor_package(name, development_source=True)


def synthetic_release(manifest: PackageManifest) -> WorkflowRelease:
    """Synthetic artifact digest: integration fixtures do not publish releases."""
    return WorkflowRelease(
        package_release_id=f"{manifest.package_id}-{manifest.package_version}",
        package_id=manifest.package_id,
        package_version=manifest.package_version,
        build_id=manifest.build_id,
        manifest_hash=manifest_hash(manifest),
        artifact_digest="sha256:" + "0" * 64,
    )


def start_request(
    package: LoadedPackage,
    amount: int = 1000,
    *,
    queue_bindings: dict[str, str] | None = None,
) -> PackageRuntimeStartRequest:
    definition = package.manifest.definitions[0]
    return PackageRuntimeStartRequest(
        context=RuntimeContext(
            tenant="test-tenant",
            business_domain="finance",
            application="adjustments",
            workflow_type=definition.workflow_type,
            definition_id=definition.definition_id,
            definition_version=definition.version,
            business_reference="reference-1",
            correlation_id="correlation-1",
            actor="trusted-test-service",
        ),
        definition_document=definition.definition_document,
        request={"amount": amount},
        release_binding=resolve_binding(
            package.manifest,
            synthetic_release(package.manifest),
            definition,
            queue_bindings=queue_bindings,
        ),
    )
