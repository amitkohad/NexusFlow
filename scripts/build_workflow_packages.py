"""Build a complete local workflow release archive and external digest descriptor.

Each platform-specific artifact includes all exact dependency wheels. Verification
installs with --no-index in an isolated environment without old worker services.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

from contracts import WorkflowRelease
from workflow_sdk.packages import (
    canonical_bytes,
    canonical_manifest_document,
    manifest_hash,
    validate_manifest,
)

ROOT = Path(__file__).resolve().parents[1]
LIBRARIES = (
    "libs/contracts",
    "workflows",
    "libs/common",
    "libs/workflow-sdk",
    "libs/activities",
    "apps/workflow-executor",
)
PACKAGES = {
    "customer-adjustment": ("workflow-packages/customer-adjustment", "customer_adjustment_package"),
    "validation-reference": (
        "tests/fixtures/workflow-packages/validation-reference",
        "validation_reference_package",
    ),
}


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify_artifact(artifact: Path, descriptor: WorkflowRelease, name: str) -> None:
    if descriptor.artifact_digest != f"sha256:{sha256(artifact)}":
        raise RuntimeError("Artifact differs from immutable release descriptor")
    with tempfile.TemporaryDirectory(prefix="nexusflow-package-check-") as temporary:
        directory = Path(temporary)
        with zipfile.ZipFile(artifact) as archive:
            archive.extractall(directory / "artifact")
        wheelhouse = directory / "artifact/wheelhouse"
        checksums = json.loads((directory / "artifact/checksums.json").read_text())
        if {wheel.name for wheel in wheelhouse.glob("*.whl")} != set(checksums):
            raise RuntimeError("Artifact wheel closure differs from recorded checksums")
        if any(sha256(wheelhouse / wheel) != digest for wheel, digest in checksums.items()):
            raise RuntimeError("Artifact contains a wheel with mismatched content")
        subprocess.run(
            ["uv", "venv", "--python", sys.executable, str(directory / "env")], check=True
        )
        python = (
            directory / "env" / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
        )
        subprocess.run(
            [
                "uv",
                "pip",
                "install",
                "--python",
                str(python),
                "--no-index",
                "--find-links",
                str(wheelhouse),
                f"nexusflow-{name}-package=={descriptor.package_version}",
            ],
            check=True,
        )
        code = """
import importlib.util, json, sys
from workflow_executor import load_executor_package
package = load_executor_package(sys.argv[1])
assert package.manifest_hash == sys.argv[2]
assert all(importlib.util.find_spec(module) is None for module in (
    'workflow_api', 'app', 'validation_worker', 'notification_worker',
    'integration_worker', 'human_task_worker', 'sample_business_worker',
))
other = 'validation_reference_package' if sys.argv[1] == 'customer-adjustment' else 'customer_adjustment_package'
assert importlib.util.find_spec(other) is None
from nexusflow_common.executor import load_executor_settings
from workflow_executor.host import registrations_for_pool
from contracts import ExecutorPool
manifest = package.manifest
for role in ('mixed', 'workflow', 'activity'):
    pool = ExecutorPool(
        pool_id=manifest.package_id+'-'+role,
        package_id=manifest.package_id,
        package_release_id=manifest.package_id+'-'+manifest.package_version,
        worker_deployment_name=manifest.worker_deployment_name,
        build_id=manifest.build_id,
        role=role,
        queue_bindings={'workflow':manifest.package_id+'-tq','activities':manifest.package_id+'-tq'},
    )
    registrations_for_pool(package, load_executor_settings(pool, {}))
print('Isolated complete workflow package passed:', manifest.package_id)
"""
        subprocess.run(
            [str(python), "-I", "-c", code, name, descriptor.manifest_hash],
            check=True,
            cwd=directory,
        )
        release_path = directory / "release.json"
        release_path.write_text(descriptor.model_dump_json(), encoding="utf-8")
        subprocess.run(
            [
                str(python),
                "-I",
                str(ROOT / "scripts/verify_installed_workflow_package.py"),
                name,
                str(release_path),
            ],
            check=True,
            cwd=directory,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify", action="store_true")
    arguments = parser.parse_args()
    output = ROOT / "dist/workflow-packages"
    output.mkdir(parents=True, exist_ok=True)
    wheels = output / "build-wheels"
    wheels.mkdir(exist_ok=True)
    for directory in (*LIBRARIES, *(item[0] for item in PACKAGES.values())):
        subprocess.run(
            [
                sys.executable,
                "-m",
                "build",
                "--no-isolation",
                "--outdir",
                str(wheels),
                str(ROOT / directory),
            ],
            check=True,
            cwd=ROOT,
        )
    for name, (directory, module) in PACKAGES.items():
        manifest_path = ROOT / directory / "src" / module / "manifest.json"
        manifest = validate_manifest(manifest_path.read_text(encoding="utf-8"))
        with tempfile.TemporaryDirectory(prefix="nexusflow-package-closure-") as temporary:
            staging = Path(temporary)
            wheelhouse = staging / "wheelhouse"
            wheelhouse.mkdir()
            lock = staging / "requirements.txt"
            lock.write_text(
                "\n".join(f"{item.name}=={item.version}" for item in manifest.dependencies)
                + f"\nnexusflow-{name}-package=={manifest.package_version}\n",
                encoding="utf-8",
            )
            subprocess.run(
                [
                    "uv",
                    "run",
                    "--no-project",
                    "--python",
                    sys.executable,
                    "--with",
                    "pip==26.2.1",
                    "python",
                    "-m",
                    "pip",
                    "download",
                    "--only-binary=:all:",
                    "--find-links",
                    str(wheels),
                    "--dest",
                    str(wheelhouse),
                    "--requirement",
                    str(lock),
                ],
                check=True,
                cwd=ROOT,
            )
            checksums = {item.name: sha256(item) for item in sorted(wheelhouse.glob("*.whl"))}
            (staging / "checksums.json").write_bytes(canonical_bytes(checksums))
            (staging / "manifest.json").write_bytes(
                canonical_bytes(canonical_manifest_document(manifest))
            )
            (staging / "dependency-lock.json").write_bytes(
                canonical_bytes([item.model_dump(mode="json") for item in manifest.dependencies])
            )
            # Build contents first; final digest lives only in an external descriptor.
            staged_artifact = staging / "artifact.zip"
            with zipfile.ZipFile(staged_artifact, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                members = [
                    staging / item
                    for item in ("checksums.json", "manifest.json", "dependency-lock.json")
                ]
                members.extend(sorted(wheelhouse.glob("*.whl")))
                for member in members:
                    info = zipfile.ZipInfo(
                        member.relative_to(staging).as_posix(), date_time=(2020, 1, 1, 0, 0, 0)
                    )
                    info.compress_type = zipfile.ZIP_DEFLATED
                    archive.writestr(info, member.read_bytes())
            digest = sha256(staged_artifact)
            artifact = output / f"{name}-{manifest.package_version}-{digest[:12]}.zip"
            artifact.write_bytes(staged_artifact.read_bytes())
            descriptor = WorkflowRelease(
                package_release_id=f"{name}-{manifest.package_version}",
                package_id=manifest.package_id,
                package_version=manifest.package_version,
                build_id=manifest.build_id,
                manifest_hash=manifest_hash(manifest),
                artifact_digest=f"sha256:{digest}",
            )
            release_path = artifact.with_suffix(".release.json")
            release_path.write_text(descriptor.model_dump_json(indent=2) + "\n", encoding="utf-8")
            if arguments.verify:
                verify_artifact(artifact, descriptor, name)
            print(f"Complete workflow artifact: {artifact.name}")
            print(f"Immutable release descriptor: {release_path.name}")


if __name__ == "__main__":
    main()
