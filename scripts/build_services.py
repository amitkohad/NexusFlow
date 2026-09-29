"""Build independently deployable runtime, worker and task-service wheels.

Use --verify to install each service alone, using the locked runtime constraints,
and confirm that no API or other worker is needed in its environment.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LIBRARIES = ("libs/contracts", "workflows", "libs/common", "libs/workflow-sdk")
LOCAL_LIBRARY_WHEELS = (
    "nexusflow_contracts-0.1.0-py3-none-any.whl",
    "nexusflow_workflows-0.1.0-py3-none-any.whl",
    "nexusflow_common-0.1.0-py3-none-any.whl",
    "nexusflow_workflow_sdk-0.1.0-py3-none-any.whl",
)
SERVICES = {
    "apps/human-task-service": ("nexusflow_human_task_service", "human_task_service"),
    "apps/workflow-runtime": ("nexusflow_workflow_runtime", "workflow_runtime"),
    "workers/validation-worker": ("nexusflow_validation_worker", "validation_worker"),
    "workers/notification-worker": ("nexusflow_notification_worker", "notification_worker"),
    "workers/integration-worker": ("nexusflow_integration_worker", "integration_worker"),
    "workers/human-task-worker": ("nexusflow_human_task_worker", "human_task_worker"),
    "workers/sample-business-worker": (
        "nexusflow_sample_business_worker",
        "sample_business_worker",
    ),
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()
    output = ROOT / "dist/services"
    output.mkdir(parents=True, exist_ok=True)
    for directory in (*LIBRARIES, *SERVICES):
        subprocess.run(
            [
                sys.executable,
                "-m",
                "build",
                "--no-isolation",
                "--outdir",
                str(output),
                str(ROOT / directory),
            ],
            check=True,
            cwd=ROOT,
        )
    for distribution, module in SERVICES.values():
        wheel = output / f"{distribution}-0.1.0-py3-none-any.whl"
        with zipfile.ZipFile(wheel) as archive:
            paths = archive.namelist()
            packages = {path.split("/", 1)[0] for path in paths if ".dist-info/" not in path}
            if packages != {module}:
                raise RuntimeError(f"Service wheel includes unrelated packages: {distribution}")
            metadata = next(path for path in paths if path.endswith(".dist-info/METADATA"))
            if "Requires-Dist: nexusflow==" in archive.read(metadata).decode():
                raise RuntimeError("Service depends on the development aggregate distribution")
        if not args.verify:
            continue
        with tempfile.TemporaryDirectory(prefix="nexusflow-service-check-") as temporary:
            environment = Path(temporary).resolve() / "environment"
            subprocess.run(["uv", "venv", "--python", sys.executable, str(environment)], check=True)
            python = environment / (
                "Scripts/python.exe" if sys.platform == "win32" else "bin/python"
            )
            subprocess.run(
                [
                    "uv",
                    "pip",
                    "install",
                    "--python",
                    str(python),
                    "--find-links",
                    str(output),
                    "--constraint",
                    str(ROOT / "requirements.txt"),
                    str(wheel),
                    *(str(output / library_wheel) for library_wheel in LOCAL_LIBRARY_WHEELS),
                ],
                check=True,
            )
            other_modules = [candidate for _, candidate in SERVICES.values() if candidate != module]
            code = (
                "import importlib, importlib.util, sys; "
                "importlib.import_module(sys.argv[1]); "
                "importlib.import_module(sys.argv[1]+'.__main__'); "
                "from workflow_sdk.definitions import validate_definition; "
                "validate_definition({'start_at':'done','steps':{'done':{'type':'end'}}}); "
                "assert importlib.util.find_spec('workflow_api') is None; "
                "assert importlib.util.find_spec('app') is None; "
                "assert all(importlib.util.find_spec(name) is None for name in sys.argv[2:]); "
                "print('Standalone imports passed:',sys.argv[1])"
            )
            subprocess.run(
                [str(python), "-I", "-c", code, module, *other_modules], check=True, cwd=temporary
            )
    print("Independent service builds and package boundaries passed")


if __name__ == "__main__":
    main()
