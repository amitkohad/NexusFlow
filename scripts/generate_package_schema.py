"""Generate or check the versioned installed package manifest schema."""

from __future__ import annotations

import argparse
from pathlib import Path

from workflow_sdk.packages.schema import schema_text

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = ROOT / "workflow-packages/schema/workflow-package-manifest-v1.schema.json"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    arguments = parser.parse_args()
    schema = schema_text()
    if arguments.check:
        if not SCHEMA.is_file() or SCHEMA.read_text(encoding="utf-8") != schema:
            print("Package schema drift: run scripts/generate_package_schema.py")
            return 1
        print("Package schema matches the manifest contract")
    else:
        SCHEMA.parent.mkdir(parents=True, exist_ok=True)
        SCHEMA.write_text(schema, encoding="utf-8", newline="\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
