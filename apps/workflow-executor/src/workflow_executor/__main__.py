"""Launch one generic executor replica from an operator-controlled pool file."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from contracts import ExecutorPool
from nexusflow_common.executor import load_executor_settings

from .host import load_executor_package, run_executor


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--package", required=True, help="Installed workflow package entrypoint name"
    )
    parser.add_argument("--pool", required=True, type=Path, help="Approved ExecutorPool JSON file")
    parser.add_argument(
        "--development-source",
        action="store_true",
        help="Only local aggregate development: use known source packages without installed locks",
    )
    arguments = parser.parse_args()
    package = load_executor_package(
        arguments.package, development_source=arguments.development_source
    )
    pool = ExecutorPool.model_validate_json(arguments.pool.read_text(encoding="utf-8"))
    settings = load_executor_settings(pool)
    if arguments.development_source and settings.pool.environment not in {"local", "test"}:
        parser.error("Source-package mode is restricted to local/test")
    asyncio.run(run_executor(package, settings))


if __name__ == "__main__":
    main()
