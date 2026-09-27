"""Generate definition syntax schema from the same model used by the parser.

Graph, capability, and cross-field policy checks belong to validate_definition;
JSON Schema describes the contract's syntax and scalar constraints.
"""

from __future__ import annotations

import json

from contracts import DefinitionDocument


def schema_document() -> dict[str, object]:
    schema = DefinitionDocument.model_json_schema(mode="validation")
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schema["$id"] = "urn:nexusflow:workflow-definition:v1"
    schema["$comment"] = (
        "This schema validates syntax. Use workflow_sdk.validate_definition for graph, "
        "capability policy, JSON nesting, and cross-field constraints."
    )
    return schema


def schema_text() -> str:
    return json.dumps(schema_document(), indent=2, sort_keys=True, ensure_ascii=False) + "\n"
