"""Cross-platform entry points shared by Make and PowerShell users."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=("dev", "test", "lint", "format", "build", "docker-build")
    )
    args, extra = parser.parse_known_args()
    if extra and args.command != "test":
        parser.error("Additional arguments are supported only for test")

    python = sys.executable
    commands: dict[str, list[list[str]]] = {
        "dev": [[python, "-m", "app.worker"]],
        "test": [[python, "-m", "pytest", *extra]],
        "lint": [
            [python, "-m", "ruff", "check", "."],
            [python, "-m", "ruff", "format", "--check", "."],
            [python, "-m", "mypy"],
        ],
        "format": [[python, "-m", "ruff", "format", "."]],
        "build": [[python, "-m", "build", "--no-isolation"]],
    }
    if args.command == "docker-build":
        print(
            "Container packaging is deferred to T056. No Dockerfile exists in Phase 1.",
            file=sys.stderr,
        )
        return 2

    try:
        for command in commands[args.command]:
            result = subprocess.run(command, cwd=ROOT, check=False)
            if result.returncode:
                return result.returncode
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
