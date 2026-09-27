"""Generate the checked-in JSON Schema from the version 1 domain contract."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from workflow_sdk.definitions.schema import schema_text

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "workflows/definitions/schema/workflow-definition-v1.schema.json"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check", action="store_true", help="Fail if the checked-in schema drifted"
    )
    args = parser.parse_args()
    expected = schema_text()
    if args.check:
        if not SCHEMA_PATH.is_file() or SCHEMA_PATH.read_text(encoding="utf-8") != expected:
            print(
                "Definition schema drift: run uv run --locked python scripts/generate_definition_schema.py",
                file=sys.stderr,
            )
            return 1
        print("Definition schema matches the domain contract")
        return 0
    SCHEMA_PATH.parent.mkdir(parents=True, exist_ok=True)
    SCHEMA_PATH.write_text(expected, encoding="utf-8", newline="\n")
    print(f"Generated {SCHEMA_PATH.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
