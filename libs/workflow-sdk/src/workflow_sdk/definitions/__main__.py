"""Command-line file adapter for the pure definition validator."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from workflow_sdk.definitions import DefinitionValidationError, validate_definition


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate a version 1 workflow definition")
    parser.add_argument("definition", type=Path)
    parser.add_argument("--capability", action="append", default=None)
    parser.add_argument("--max-steps", type=int, default=500)
    args = parser.parse_args()
    try:
        definition = validate_definition(
            args.definition.read_bytes(),
            allowed_capabilities=args.capability,
            max_steps=args.max_steps,
        )
    except (DefinitionValidationError, OSError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(f"Valid definition: schema {definition.schema_version}, {len(definition.steps)} steps")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
