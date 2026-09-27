from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import cast

import pytest

from workflows.common import (
    JSONValue,
    SemanticsError,
    compare_values,
    make_transition,
    resolve_path,
    resolve_template,
    route_decision,
    transition_target,
    validate_path,
)


@pytest.fixture
def context() -> dict[str, JSONValue]:
    return {
        "request": {
            "items": [{"amount": 12.5}, {"amount": 0}],
            "nullable": None,
            "approved": False,
            "name": "Ada",
            "0": "numeric dictionary key",
            "literal": "${request.name}",
        },
        "variables": {"amount": 42, "enabled": True},
        "results": {"validation": {"accepted": True}},
    }


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("request.items.0.amount", 12.5),
        ("request.items.1.amount", 0),
        ("request.0", "numeric dictionary key"),
        ("request.nullable", None),
        ("request.approved", False),
        ("results.validation.accepted", True),
    ],
)
def test_paths_resolve_dictionary_keys_and_list_indices(
    context: dict[str, JSONValue], path: str, expected: JSONValue
) -> None:
    assert resolve_path(context, path) == expected


@pytest.mark.parametrize("path", ["request.missing", "request.nullable.child", "request.items.2"])
def test_missing_is_an_error_distinct_from_explicit_null(
    context: dict[str, JSONValue], path: str
) -> None:
    assert resolve_path(context, "request.nullable") is None
    with pytest.raises(SemanticsError) as error:
        resolve_path(context, path)
    assert error.value.code == "missing_path"
    assert error.value.path == path
    assert path in str(error.value)


@pytest.mark.parametrize(
    "path",
    [
        "",
        ".request",
        "request.",
        "request..name",
        "request.items.-1",
        "request.items.01",
        "request.items.1.0",
        "request.items.first",
        "request['name']",
        "request.name()",
    ],
)
def test_invalid_paths_and_indices_are_actionable(context: dict[str, JSONValue], path: str) -> None:
    with pytest.raises(SemanticsError, match="Path|path"):
        resolve_path(context, path)


def test_path_grammar_is_shared_by_templates_and_contracts() -> None:
    assert validate_path("results.risk-check.0.code") == ("results", "risk-check", "0", "code")
    with pytest.raises(SemanticsError, match="dot-separated"):
        validate_path("request.amount + 1")


def test_arbitrarily_large_index_is_reported_as_missing(context: dict[str, JSONValue]) -> None:
    with pytest.raises(SemanticsError, match="out-of-range index"):
        resolve_path(context, "request.items." + "9" * 5000)


@pytest.mark.parametrize(
    ("left", "operator", "right", "expected"),
    [
        (1, "==", 1.0, True),
        (True, "==", 1, False),
        (False, "!=", 0, True),
        (None, "==", None, True),
        (None, "==", False, False),
        ("1", "==", 1, False),
        ([1, {"a": True}], "==", [1.0, {"a": True}], True),
        ([1, {"a": True}], "==", [1, {"a": 1}], False),
        ({"a": 1, "b": 2}, "==", {"b": 2.0, "a": 1.0}, True),
        ([], "==", {}, False),
        ([1], "!=", [1, 2], True),
        (12.5, ">", 12, True),
        (12, ">", 12.5, False),
        (12, ">=", 12.0, True),
        (12, "<", 13.0, True),
        (12.5, "<=", 12, False),
        ("a", "<", "b", True),
        ("b", ">", "a", True),
        ("a", ">=", "a", True),
        ("b", "<=", "a", False),
        (1.0, "in", [2, 1], True),
        (True, "in", [1, 2], False),
        (1, "in", [True, False], False),
        ({"ok": True}, "in", [{"ok": 1}], False),
        ([1.0], "in", [[1]], True),
        (None, "in", [None], True),
        ("da", "in", "Ada", True),
        ("key", "in", {"key": False}, True),
        ("missing", "in", {"key": True}, False),
    ],
)
def test_decision_operators_have_strict_json_semantics(
    left: object, operator: str, right: object, expected: bool
) -> None:
    assert compare_values(left, operator, right) is expected


@pytest.mark.parametrize(
    ("left", "operator", "right", "message"),
    [
        (1, "contains", [1], "Unsupported decision operator"),
        (True, ">", 0, "excluding booleans"),
        (0, ">", False, "excluding booleans"),
        (1, "<", "2", "two finite numbers"),
        (None, "<=", 0, "two finite numbers"),
        ([1], ">=", [0], "two finite numbers"),
        (1, "in", "123", "string left operand"),
        (1, "in", {"1": True}, "string left operand"),
        (1, "in", None, "right operand"),
        (1, "in", 123, "right operand"),
        (float("nan"), "==", float("nan"), "finite JSON numbers"),
        (0, "<", float("inf"), "finite JSON numbers"),
        ([], "==", [{"nested": float("-inf")}], "finite JSON numbers"),
        ((1,), "==", [1], "unsupported JSON type"),
        ({1: "bad"}, "==", {}, "string dictionary keys"),
    ],
)
def test_incompatible_operands_fail_with_specific_errors(
    left: object, operator: str, right: object, message: str
) -> None:
    with pytest.raises(SemanticsError, match=message):
        compare_values(left, operator, right)


def test_routing_uses_strict_path_resolution(context: dict[str, JSONValue]) -> None:
    assert (
        route_decision(context, "request.items.0.amount", ">", 10, "approve", "finish") == "approve"
    )
    assert route_decision(context, "request.approved", "==", 0, "approve", "finish") == "finish"
    assert route_decision(context, "request.nullable", "==", None, "approve", "finish") == "approve"
    with pytest.raises(SemanticsError, match="missing dictionary key"):
        route_decision(context, "request.missing", "==", None, "approve", "finish")


@pytest.mark.parametrize(
    ("template", "expected"),
    [
        ("${variables.amount}", 42),
        ("${request.nullable}", None),
        ("${request.approved}", False),
        ("${request.items.0.amount}", 12.5),
        ("${request.literal}", "${request.name}"),
        ("hello ${request.name}", "hello Ada"),
        ("${request.name}/${variables.amount}", "Ada/42"),
        ("flag=${request.approved} enabled=${variables.enabled}", "flag=false enabled=true"),
        (
            "nullable=${request.nullable} amount=${request.items.0.amount}",
            "nullable=null amount=12.5",
        ),
        ("plain {text} $1", "plain {text} $1"),
        ("", ""),
    ],
)
def test_whole_templates_preserve_type_and_interpolation_renders_json_scalars(
    context: dict[str, JSONValue], template: str, expected: JSONValue
) -> None:
    resolved = resolve_template(template, context)
    assert resolved == expected
    assert type(resolved) is type(expected)


def test_recursive_templates_copy_context_values_without_aliasing(
    context: dict[str, JSONValue],
) -> None:
    original_context = deepcopy(context)
    template: dict[str, JSONValue] = {
        "payload": "${request}",
        "list": ["${request.items}", {"name": "${request.name}", "fixed": None}],
        "${request.name}": "literal key",
    }
    original_template = deepcopy(template)
    result = resolve_template(template, context)
    assert isinstance(result, dict)
    assert result["payload"] == context["request"]
    assert result["payload"] is not context["request"]
    assert result["${request.name}"] == "literal key"
    result_payload = cast(dict[str, JSONValue], result["payload"])
    result_items = cast(list[JSONValue], result_payload["items"])
    cast(dict[str, JSONValue], result_items[0])["amount"] = 999
    result_payload["name"] = "changed"
    assert context == original_context
    assert template == original_template


@pytest.mark.parametrize(
    "template",
    [
        "${}",
        "${request.name",
        "${request..name}",
        "${request['name']}",
        "${request.name + 1}",
        "${${request.name}}",
        "items=${request.items}",
        "payload ${request}",
        "${__import__('os').system('echo injected')}",
    ],
)
def test_malformed_or_nonscalar_interpolation_cannot_execute_code(
    context: dict[str, JSONValue], template: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("Template evaluation must never invoke eval or exec")

    monkeypatch.setattr("builtins.eval", forbidden)
    monkeypatch.setattr("builtins.exec", forbidden)
    with pytest.raises(SemanticsError):
        resolve_template(template, context)


def test_missing_template_reference_never_becomes_null(context: dict[str, JSONValue]) -> None:
    with pytest.raises(SemanticsError, match="missing dictionary key"):
        resolve_template("${request.unknown}", context)


@pytest.mark.parametrize("container_type", ["list", "dict", "mixed"])
def test_cyclic_python_containers_raise_semantics_error(container_type: str) -> None:
    cycle_list: list[JSONValue] = []
    cycle_dict: dict[str, JSONValue] = {}
    if container_type == "list":
        cycle_list.append(cycle_list)
        value: JSONValue = cycle_list
    elif container_type == "dict":
        cycle_dict["self"] = cycle_dict
        value = cycle_dict
    else:
        cycle_list.append(cycle_dict)
        cycle_dict["list"] = cycle_list
        value = cycle_list
    for action in (
        lambda: compare_values(value, "==", []),
        lambda: resolve_template(value, {}),
        lambda: resolve_template("${payload}", {"payload": value}),
        lambda: resolve_path({"payload": value}, "payload"),
    ):
        with pytest.raises(SemanticsError, match="container cycle") as error:
            action()
        assert error.value.code == "invalid_json"


def nested_lists(depth: int, leaf: JSONValue) -> JSONValue:
    value = leaf
    for _ in range(depth):
        value = [value]
    return value


def test_json_nesting_boundary_is_checked_before_python_recursion_limit() -> None:
    allowed = nested_lists(64, "leaf")
    assert compare_values(allowed, "==", allowed)
    assert resolve_template(allowed, {}) == allowed
    for rejected in (nested_lists(65, "leaf"), nested_lists(1500, "leaf")):
        with pytest.raises(SemanticsError, match="maximum JSON nesting depth 64"):
            compare_values(rejected, "==", [])
        with pytest.raises(SemanticsError, match="maximum JSON nesting depth 64"):
            resolve_template(rejected, {})


def test_expansion_checks_combined_depth_of_template_and_reference() -> None:
    context: dict[str, JSONValue] = {"payload": nested_lists(64, "leaf")}
    assert resolve_template("${payload}", context) == context["payload"]
    with pytest.raises(SemanticsError, match="Expanded template.*maximum JSON nesting depth"):
        resolve_template(["${payload}"], context)


def test_shared_container_references_are_copied_without_being_mistaken_for_cycles() -> None:
    shared: dict[str, JSONValue] = {"value": 1}
    value: list[JSONValue] = [shared, shared]
    assert compare_values(value, "==", [{"value": 1.0}, {"value": 1}])
    result = resolve_template(value, {})
    assert isinstance(result, list)
    assert result == value
    assert result[0] is not result[1]
    assert result[0] is not shared


def test_transition_constructor_uses_only_supplied_time() -> None:
    timestamp = datetime(2026, 9, 26, 12, 34, 56, tzinfo=timezone.utc)
    expected = {
        "step": "validate",
        "state": "COMPLETED",
        "detail": "validation",
        "workflow_time": "2026-09-26T12:34:56+00:00",
    }
    first = make_transition("validate", "COMPLETED", "validation", timestamp)
    assert first == expected
    assert make_transition("validate", "COMPLETED", "validation", timestamp.isoformat()) == first
    first["state"] = "changed"
    assert make_transition("validate", "COMPLETED", "validation", timestamp) == expected


@pytest.mark.parametrize("step_type", ["activity", "timer"])
def test_simple_transition_targets(step_type: str) -> None:
    assert transition_target({"type": step_type, "next": "finish"}) == "finish"
    assert transition_target({"type": step_type}) is None
    assert transition_target({"type": "end", "outcome": "REJECTED"}) is None


def test_decision_transition_targets_require_explicit_boolean() -> None:
    step: dict[str, object] = {"type": "decision", "on_true": "approve", "on_false": "finish"}
    assert transition_target(step, decision=True) == "approve"
    assert transition_target(step, decision=False) == "finish"
    with pytest.raises(SemanticsError, match="explicit boolean"):
        transition_target(step)
    with pytest.raises(SemanticsError, match="decision.on_false"):
        transition_target({"type": "decision", "on_true": "approve"}, decision=False)


def test_approval_transition_targets_require_supplied_outcome() -> None:
    step: dict[str, object] = {
        "type": "approval",
        "on_approved": "apply",
        "on_rejected": "reject",
        "on_timeout": "expire",
    }
    assert transition_target(step, approved=True) == "apply"
    assert transition_target(step, approved=False) == "reject"
    assert transition_target(step, timed_out=True) == "expire"
    assert transition_target({"type": "approval"}, timed_out=True) is None
    with pytest.raises(SemanticsError, match="explicit boolean"):
        transition_target(step)
    with pytest.raises(SemanticsError, match="both timed out and decided"):
        transition_target(step, approved=False, timed_out=True)


def test_unknown_step_and_malformed_target_raise_actionable_errors() -> None:
    with pytest.raises(SemanticsError, match="Unsupported step type"):
        transition_target({"type": "python"})
    with pytest.raises(SemanticsError, match="activity.next"):
        transition_target({"type": "activity", "next": 1})
    with pytest.raises(SemanticsError, match="Step type must be a string"):
        transition_target({"type": ["activity"]})
