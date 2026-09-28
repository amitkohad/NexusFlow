"""Versioned manifest syntax schema; executable closure requires SDK validation."""

from __future__ import annotations

import json

from contracts import PackageManifest


def schema_document() -> dict[str, object]:
    schema = PackageManifest.model_json_schema(mode="validation")
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schema["$id"] = "urn:nexusflow:workflow-package-manifest:v1"
    schema["$comment"] = (
        "Syntax only; use workflow_sdk.packages.validate_manifest for hashes, trusted registrations and dependency closure."
    )
    return schema


def schema_text() -> str:
    return json.dumps(schema_document(), indent=2, sort_keys=True, ensure_ascii=False) + "\n"
