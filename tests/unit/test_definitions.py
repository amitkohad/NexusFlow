"""Definition contract, graph policy, and generated-schema acceptance checks."""

from __future__ import annotations

import json
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
from contracts import MAX_JSON_DEPTH, DefinitionDocument
from jsonschema import Draft202012Validator
from nexusflow_common.errors import classify_error
from workflow_sdk.definitions import (
    DefinitionValidationError,
    parse_definition,
    validate_definition,
)
from workflow_sdk.definitions.__main__ import main as validation_main
from workflow_sdk.definitions.schema import schema_document, schema_text

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = ROOT / "workflows/definitions/schema/workflow-definition-v1.schema.json"


@pytest.fixture
def simple_definition() -> dict[str, Any]:
    return {
        "start_at": "wait",
        "steps": {"wait": {"type": "timer", "seconds": 0, "next": "done"}, "done": {"type": "end"}},
    }


def issue_codes(error: DefinitionValidationError) -> set[str]:
    return {issue.code for issue in error.issues}


def test_existing_sample_parses_and_validates_against_generated_schema() -> None:
    document = json.loads((ROOT / "examples/customer_adjustment.json").read_text(encoding="utf-8"))
    parsed = validate_definition(document)
    assert isinstance(parsed, DefinitionDocument)
    assert parsed.schema_version == "1.0"
    assert parsed.start_at == "validate"
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema).validate(document)
    assert schema == schema_document()
    assert SCHEMA_PATH.read_text(encoding="utf-8") == schema_text()


def test_mapping_json_and_bytes_have_the_same_defaults(simple_definition: dict[str, Any]) -> None:
    mapping = parse_definition(simple_definition)
    text = parse_definition(json.dumps(simple_definition))
    encoded = parse_definition(json.dumps(simple_definition).encode())
    assert mapping == text == encoded
    assert mapping.variables == {}
    assert mapping.request == {}


@pytest.mark.parametrize(
    "text",
    [
        "not JSON",
        "{",
        b"\xff",
        '{"start_at":"done","start_at":"other","steps":{"done":{"type":"end"}}}',
        '{"start_at":"done","steps":{"done":{"type":"end","type":"timer"}}}',
        '{"start_at":"done","steps":{"done":{"type":"end"}},"request":{"x":NaN}}',
        '{"start_at":"done","steps":{"done":{"type":"end"}},"request":{"x":Infinity}}',
        '{"start_at":"done","steps":{"done":{"type":"end"}},"request":{"x":-Infinity}}',
    ],
)
def test_malformed_or_ambiguous_json_is_rejected(text: str | bytes) -> None:
    with pytest.raises(DefinitionValidationError) as caught:
        parse_definition(text)
    assert any(code.startswith("json.") for code in issue_codes(caught.value))


@pytest.mark.parametrize("kind", ["array", "object"])
def test_cyclic_mapping_payload_is_reported_as_a_definition_error(
    simple_definition: dict[str, Any], kind: str
) -> None:
    payload: Any = [] if kind == "array" else {}
    if kind == "array":
        payload.append(payload)
    else:
        payload["self"] = payload
    simple_definition["request"] = {"cycle": payload}
    with pytest.raises(DefinitionValidationError) as caught:
        parse_definition(simple_definition)
    assert issue_codes(caught.value) == {"definition.invalid_structure"}


def test_deeply_nested_json_is_reported_as_a_definition_error() -> None:
    payload = "[" * 1500 + "0" + "]" * 1500
    text = '{"start_at":"done","steps":{"done":{"type":"end"}},"request":{"deep":' + payload + "}}"
    with pytest.raises(DefinitionValidationError) as caught:
        parse_definition(text)
    assert issue_codes(caught.value) == {"json.invalid"}


def test_json_number_overflow_is_rejected() -> None:
    with pytest.raises(DefinitionValidationError) as caught:
        parse_definition(
            '{"start_at":"done","steps":{"done":{"type":"end"}},"request":{"x":1e999}}'
        )
    assert issue_codes(caught.value) == {"definition.invalid_structure"}


@pytest.mark.parametrize("depth", [MAX_JSON_DEPTH - 1, MAX_JSON_DEPTH])
def test_mapping_and_json_payloads_share_the_nesting_limit(
    simple_definition: dict[str, Any], depth: int
) -> None:
    payload: Any = 0
    for _ in range(depth):
        payload = [payload]
    simple_definition["request"] = {"nested": payload}
    for document in (simple_definition, json.dumps(simple_definition)):
        if depth == MAX_JSON_DEPTH - 1:
            assert parse_definition(document).start_at == "wait"
        else:
            with pytest.raises(DefinitionValidationError) as caught:
                parse_definition(document)
            assert issue_codes(caught.value) == {"definition.invalid_structure"}


@pytest.mark.parametrize(
    "change",
    [
        {"schema_version": "2.0"},
        {"start_at": True},
        {"unknown": "ignored"},
        {"steps": {}},
        {"steps": {"bad id": {"type": "end"}}},
        {"steps": {"wait": {"type": "timer", "seconds": True, "next": "done"}}},
        {"steps": {"wait": {"type": "timer", "seconds": "1", "next": "done"}}},
        {"steps": {"wait": {"type": "timer", "seconds": -1, "next": "done"}}},
        {"steps": {"wait": {"type": "timer", "next": None}}},
        {"steps": {"wait": {"type": "end", "outcome": "UNKNOWN"}}},
        {"steps": {"wait": {"type": "shell", "command": "x"}}},
        {"request": {"value": float("nan")}},
        {"variables": {"value": float("inf")}},
    ],
)
def test_strict_structural_contract_rejects_invalid_types(
    simple_definition: dict[str, Any], change: dict[str, Any]
) -> None:
    simple_definition.update(change)
    with pytest.raises(DefinitionValidationError) as caught:
        parse_definition(simple_definition)
    assert issue_codes(caught.value) == {"definition.invalid_structure"}
    assert all(issue.message and issue.path for issue in caught.value.issues)


def test_schema_rejects_coercion_unknown_fields_and_missing_edges(
    simple_definition: dict[str, Any],
) -> None:
    validator = Draft202012Validator(schema_document())
    for value in ["1", True, -1]:
        changed = deepcopy(simple_definition)
        changed["steps"]["wait"]["seconds"] = value
        assert list(validator.iter_errors(changed))
    changed = deepcopy(simple_definition)
    changed["steps"]["wait"].pop("next")
    assert list(validator.iter_errors(changed))
    changed = deepcopy(simple_definition)
    changed["steps"]["done"]["unknown"] = True
    assert list(validator.iter_errors(changed))


def test_schema_syntax_checks_require_additional_graph_and_cross_field_validation() -> None:
    schema_validator = Draft202012Validator(schema_document())
    bad_graph = {"start_at": "absent", "steps": {"done": {"type": "end"}}}
    schema_validator.validate(bad_graph)
    with pytest.raises(DefinitionValidationError) as caught:
        validate_definition(bad_graph)
    assert "graph.invalid_start" in issue_codes(caught.value)

    invalid_intervals = {
        "start_at": "run",
        "steps": {
            "run": {
                "type": "activity",
                "capability": "custom",
                "next": "done",
                "retry": {"initial_interval_seconds": 10, "maximum_interval_seconds": 1},
            },
            "done": {"type": "end"},
        },
    }
    schema_validator.validate(invalid_intervals)
    with pytest.raises(DefinitionValidationError):
        parse_definition(invalid_intervals)


def test_error_paths_identify_the_actual_field(simple_definition: dict[str, Any]) -> None:
    simple_definition["steps"]["wait"]["seconds"] = "fast"
    with pytest.raises(DefinitionValidationError) as caught:
        parse_definition(simple_definition)
    assert caught.value.issues[0].path == "/steps/wait/seconds"
    classified = classify_error(caught.value)
    assert classified.code == "definition_invalid"
    assert classified.category == "validation"
    assert classified.retryable is False
    assert classified.issues == caught.value.issues


@pytest.mark.parametrize(
    ("document", "code", "path"),
    [
        (
            {"start_at": "absent", "steps": {"done": {"type": "end"}}},
            "graph.invalid_start",
            "/start_at",
        ),
        (
            {"start_at": "wait", "steps": {"wait": {"type": "timer", "next": "absent"}}},
            "graph.missing_target",
            "/steps/wait/next",
        ),
        (
            {
                "start_at": "done",
                "steps": {"done": {"type": "end"}, "unreachable": {"type": "end"}},
            },
            "graph.unreachable_step",
            "/steps/unreachable",
        ),
        (
            {"start_at": "wait", "steps": {"wait": {"type": "timer", "next": "wait"}}},
            "graph.cycle",
            "/steps",
        ),
        (
            {"start_at": "wait", "steps": {"wait": {"type": "timer", "next": "wait"}}},
            "graph.missing_terminal",
            "/steps",
        ),
        (
            {"start_at": "wait", "steps": {"wait": {"type": "timer", "next": "wait"}}},
            "graph.non_terminating_path",
            "/steps/wait",
        ),
    ],
)
def test_invalid_graphs_have_actionable_codes_and_paths(
    document: dict[str, Any], code: str, path: str
) -> None:
    with pytest.raises(DefinitionValidationError) as caught:
        validate_definition(document)
    assert any(issue.code == code and issue.path == path for issue in caught.value.issues)


def decision_definition(operator: str, value: Any) -> dict[str, Any]:
    return {
        "start_at": "route",
        "steps": {
            "route": {
                "type": "decision",
                "field": "request.value",
                "operator": operator,
                "value": value,
                "on_true": "yes",
                "on_false": "no",
            },
            "yes": {"type": "end"},
            "no": {"type": "end", "outcome": "REJECTED"},
        },
    }


def test_every_decision_branch_must_terminate_even_if_an_end_is_reachable() -> None:
    document = decision_definition("==", True)
    document["steps"]["no"] = {"type": "timer", "next": "no"}
    with pytest.raises(DefinitionValidationError) as caught:
        validate_definition(document)
    assert {"graph.cycle", "graph.non_terminating_path"} <= issue_codes(caught.value)
    assert "graph.missing_terminal" not in issue_codes(caught.value)


def test_shared_decision_targets_are_valid() -> None:
    document = decision_definition("==", True)
    document["steps"]["route"]["on_false"] = "yes"
    document["steps"].pop("no")
    assert validate_definition(document).start_at == "route"


@pytest.mark.parametrize(
    ("operator", "value"),
    [("contains", []), (">", True), ("<=", None), ("<", []), (">=", {}), ("in", 2), ("in", False)],
)
def test_invalid_decision_operators_and_literals_are_rejected(operator: str, value: Any) -> None:
    with pytest.raises(DefinitionValidationError) as caught:
        validate_definition(decision_definition(operator, value))
    expected = (
        "definition.invalid_structure" if operator == "contains" else "decision.invalid_literal"
    )
    assert expected in issue_codes(caught.value)


@pytest.mark.parametrize(
    ("operator", "value"),
    [
        ("==", True),
        ("!=", None),
        (">", 3),
        ("<=", 0.5),
        ("<", "b"),
        ("in", []),
        ("in", "abc"),
        ("in", {"x": 1}),
    ],
)
def test_supported_decision_literals_validate(operator: str, value: Any) -> None:
    assert validate_definition(decision_definition(operator, value)).start_at == "route"


def test_capability_policy_checks_activity_and_compensation() -> None:
    document = {
        "start_at": "run",
        "steps": {
            "run": {
                "type": "activity",
                "capability": "custom",
                "compensation": {"capability": "undo"},
                "next": "done",
            },
            "done": {"type": "end"},
        },
    }
    assert validate_definition(document)
    assert validate_definition(document, allowed_capabilities={"custom", "undo"})
    with pytest.raises(DefinitionValidationError) as caught:
        validate_definition(document, allowed_capabilities=set())
    assert {issue.path for issue in caught.value.issues} == {
        "/steps/run/capability",
        "/steps/run/compensation/capability",
    }


def test_steps_are_bounded_and_large_graphs_do_not_recurse() -> None:
    steps: dict[str, Any] = {
        f"step{i}": {"type": "timer", "next": f"step{i + 1}"} for i in range(1200)
    }
    steps["step1200"] = {"type": "end"}
    document = {"start_at": "step0", "steps": steps}
    with pytest.raises(DefinitionValidationError) as caught:
        validate_definition(document)
    assert issue_codes(caught.value) == {"definition.step_limit"}
    assert len(validate_definition(document, max_steps=1500).steps) == 1201


def test_model_instances_are_revalidated_after_nested_mutation(
    simple_definition: dict[str, Any],
) -> None:
    parsed = parse_definition(simple_definition)
    parsed.steps.clear()
    with pytest.raises(DefinitionValidationError):
        validate_definition(parsed)


@pytest.mark.parametrize("limit", [0, -1, True])
def test_step_limit_must_be_a_positive_integer(
    simple_definition: dict[str, Any], limit: int
) -> None:
    with pytest.raises(ValueError, match="max_steps must be a positive integer"):
        validate_definition(simple_definition, max_steps=limit)


def test_cli_reports_invalid_step_limit(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        ["validate", str(ROOT / "examples/customer_adjustment.json"), "--max-steps", "0"],
    )
    assert validation_main() == 2
    assert "max_steps must be a positive integer" in capsys.readouterr().err
