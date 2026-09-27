"""Run the packaged, versioned database migration as a separate operation."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from alembic import command
from alembic.config import Config


def migration_config() -> Config:
    if not os.environ.get("NEXUSFLOW_DATABASE_URL"):
        raise RuntimeError("Set NEXUSFLOW_DATABASE_URL for the migration database")
    directory = Path(__file__).parent / "migrations"
    if not directory.is_dir():
        directory = Path(__file__).resolve().parents[4] / "migrations"
    if not (directory / "alembic.ini").is_file():
        raise RuntimeError("Packaged database migrations are unavailable")
    return Config(str(directory / "alembic.ini"))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("upgrade", "current"))
    operation = parser.parse_args().operation
    config = migration_config()
    if operation == "upgrade":
        command.upgrade(config, "head")
    else:
        command.current(config)


if __name__ == "__main__":
    main()
