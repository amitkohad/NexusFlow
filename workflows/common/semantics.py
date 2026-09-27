"""JSON-only paths, comparisons, templates, and transition records.

No helper reads a clock or performs I/O. Callers supply outcomes and workflow
timestamps; Temporal execution and lifecycle management remain runtime concerns.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping
from datetime import datetime
from typing import TypeAlias

JSONValue: TypeAlias = "None | bool | int | float | str | list[JSONValue] | dict[str, JSONValue]"
_PATH_SEGMENT = re.compile(r"(?:[A-Za-z_][A-Za-z0-9_-]*|0|[1-9][0-9]*)\Z")
_INDEX = re.compile(r"(?:0|[1-9][0-9]*)\Z")
_MAX_JSON_DEPTH = 64


class SemanticsError(ValueError):
    """An invalid definition expression or incompatible runtime JSON value."""

    def __init__(self, message: str, *, code: str, path: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.path = path


def validate_path(path: str) -> tuple[str, ...]:
    """Parse a nonempty dot path containing identifiers or unsigned indices."""
    if not isinstance(path, str) or not path:
        raise SemanticsError("Path must be a nonempty string", code="invalid_path")
    parts = tuple(path.split("."))
    for part in parts:
        if _PATH_SEGMENT.fullmatch(part) is None:
            raise SemanticsError(
                f"Invalid segment {part!r} in path {path!r}; use dot-separated identifiers "
                "or nonnegative indices without leading zeros",
                code="invalid_path",
                path=path,
            )
    return parts


def resolve_path(context: Mapping[str, JSONValue], path: str) -> JSONValue:
    """Return a present value; explicit null is valid, missing paths raise."""
    value: object = context
    for part in validate_path(path):
        if isinstance(value, Mapping):
            if part not in value:
                raise SemanticsError(
                    f"Path {path!r} is missing dictionary key {part!r}",
                    code="missing_path",
                    path=path,
                )
            value = value[part]
        elif isinstance(value, list):
            if _INDEX.fullmatch(part) is None:
                raise SemanticsError(
                    f"Path {path!r} requires a nonnegative list index at {part!r}",
                    code="invalid_path",
                    path=path,
                )
            # Bound the digit length before int conversion so arbitrarily large
            # JSON path indices still produce our actionable error.
            if len(part) > len(str(len(value))) or int(part) >= len(value):
                raise SemanticsError(
                    f"Path {path!r} has out-of-range index {part} for a list of "
                    f"length {len(value)}",
                    code="missing_path",
                    path=path,
                )
            value = value[int(part)]
        else:
            raise SemanticsError(
                f"Path {path!r} cannot traverse {part!r} through a scalar or null",
                code="missing_path",
                path=path,
            )
    # Validate values at the boundary without changing the reference returned.
    _copy_json(value, location=f"path {path!r}")
    return value  # type: ignore[return-value]


def _copy_json(
    value: object, *, location: str, depth: int = 0, ancestors: set[int] | None = None
) -> JSONValue:
    if value is None or isinstance(value, (bool, str)):
        return value
    if type(value) is int:
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise SemanticsError(
                f"{location} must contain finite JSON numbers", code="invalid_json"
            )
        return value
    if isinstance(value, (list, dict)):
        if depth >= _MAX_JSON_DEPTH:
            raise SemanticsError(
                f"{location} exceeds the maximum JSON nesting depth {_MAX_JSON_DEPTH}",
                code="invalid_json",
            )
        if ancestors is None:
            ancestors = set()
        identity = id(value)
        if identity in ancestors:
            raise SemanticsError(f"{location} contains a JSON container cycle", code="invalid_json")
        ancestors.add(identity)
        try:
            if isinstance(value, list):
                return [
                    _copy_json(
                        item, location=f"{location}[{index}]", depth=depth + 1, ancestors=ancestors
                    )
                    for index, item in enumerate(value)
                ]
            result: dict[str, JSONValue] = {}
            for key, item in value.items():
                if not isinstance(key, str):
                    raise SemanticsError(
                        f"{location} requires string dictionary keys", code="invalid_json"
                    )
                result[key] = _copy_json(
                    item, location=f"{location}.{key}", depth=depth + 1, ancestors=ancestors
                )
            return result
        finally:
            ancestors.remove(identity)
    raise SemanticsError(
        f"{location} contains unsupported JSON type {type(value).__name__}", code="invalid_json"
    )


def _equal(left: JSONValue, right: JSONValue) -> bool:
    if isinstance(left, bool) or isinstance(right, bool):
        return type(left) is type(right) and left == right
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return left == right
    if type(left) is not type(right):
        return False
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(
            _equal(a, b) for a, b in zip(left, right, strict=True)
        )
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(
            _equal(item, right[key]) for key, item in left.items()
        )
    return left == right


def compare_values(left: object, operator: str, right: object) -> bool:
    """Compare JSON values without Python's boolean/number coercion."""
    left_value = _copy_json(left, location="Decision left operand")
    right_value = _copy_json(right, location="Decision right operand")
    if operator in {"==", "!="}:
        equal = _equal(left_value, right_value)
        return equal if operator == "==" else not equal
    if operator == "in":
        if isinstance(right_value, list):
            return any(_equal(left_value, item) for item in right_value)
        if isinstance(right_value, (str, dict)):
            if not isinstance(left_value, str):
                raise SemanticsError(
                    "Decision 'in' requires a string left operand for a string or dictionary",
                    code="invalid_comparison",
                )
            return left_value in right_value
        raise SemanticsError(
            "Decision 'in' requires a list, string, or dictionary right operand",
            code="invalid_comparison",
        )
    if operator not in {">", ">=", "<", "<="}:
        raise SemanticsError(
            f"Unsupported decision operator {operator!r}; use ==, !=, >, >=, <, <=, or in",
            code="invalid_comparison",
        )
    if isinstance(left_value, str) and isinstance(right_value, str):
        return _order(left_value, operator, right_value)
    if (
        isinstance(left_value, (int, float))
        and not isinstance(left_value, bool)
        and isinstance(right_value, (int, float))
        and not isinstance(right_value, bool)
    ):
        return _order(left_value, operator, right_value)
    raise SemanticsError(
        f"Decision {operator!r} requires two finite numbers (excluding booleans) or two strings",
        code="invalid_comparison",
    )


def _order(left: int | float | str, operator: str, right: int | float | str) -> bool:
    # compare_values establishes compatible types before reaching this helper.
    if operator == ">":
        return left > right  # type: ignore[operator]
    if operator == ">=":
        return left >= right  # type: ignore[operator]
    if operator == "<":
        return left < right  # type: ignore[operator]
    return left <= right  # type: ignore[operator]


def route_decision(
    context: Mapping[str, JSONValue],
    field: str,
    operator: str,
    value: JSONValue,
    on_true: str,
    on_false: str,
) -> str:
    """Resolve a decision's field and select its explicit route."""
    _required_string(on_true, "on_true")
    _required_string(on_false, "on_false")
    return on_true if compare_values(resolve_path(context, field), operator, value) else on_false


def resolve_template(value: JSONValue, context: Mapping[str, JSONValue]) -> JSONValue:
    """Recursively replace ${path} references; never interpret expressions."""
    # Validate/copy once before descending, including list roots. References
    # can increase the resulting depth, so validate the expanded document too.
    template = _copy_json(value, location="Template")
    return _copy_json(_resolve_template(template, context), location="Expanded template")


def _resolve_template(value: JSONValue, context: Mapping[str, JSONValue]) -> JSONValue:
    if isinstance(value, list):
        return [_resolve_template(item, context) for item in value]
    if isinstance(value, dict):
        # Keys stay literal. Only values form the template language.
        return {key: _resolve_template(item, context) for key, item in value.items()}
    if not isinstance(value, str):
        return _copy_json(value, location="Template")
    cursor = 0
    rendered: list[str] = []
    while True:
        start = value.find("${", cursor)
        if start == -1:
            rendered.append(value[cursor:])
            return "".join(rendered)
        end = value.find("}", start + 2)
        if end == -1:
            raise SemanticsError(
                f"Template has an unclosed reference at position {start}", code="invalid_template"
            )
        path = value[start + 2 : end]
        resolved = resolve_path(context, path)
        if start == 0 and end == len(value) - 1:
            return _copy_json(resolved, location=f"Template reference {path!r}")
        if isinstance(resolved, (list, dict)):
            raise SemanticsError(
                f"Template reference {path!r} is not scalar; use a whole ${'{path}'} reference "
                "to preserve a list or dictionary",
                code="invalid_template",
                path=path,
            )
        rendered.append(value[cursor:start])
        rendered.append(
            resolved if isinstance(resolved, str) else json.dumps(resolved, allow_nan=False)
        )
        cursor = end + 1


def _required_string(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise SemanticsError(f"{field} must be a nonempty string", code="invalid_transition")
    return value


def make_transition(
    step: str, state: str, detail: str, workflow_time: datetime | str
) -> dict[str, str]:
    """Construct the prototype transition shape with an explicit timestamp."""
    _required_string(step, "step")
    _required_string(state, "state")
    if not isinstance(detail, str):
        raise SemanticsError("detail must be a string", code="invalid_transition")
    timestamp = workflow_time.isoformat() if isinstance(workflow_time, datetime) else workflow_time
    _required_string(timestamp, "workflow_time")
    return {"step": step, "state": state, "detail": detail, "workflow_time": timestamp}


def transition_target(
    step: Mapping[str, object],
    *,
    decision: bool | None = None,
    approved: bool | None = None,
    timed_out: bool = False,
) -> str | None:
    """Select a target from supplied outcomes, without managing execution state."""
    step_type = step.get("type")
    if not isinstance(step_type, str):
        raise SemanticsError("Step type must be a string", code="invalid_transition")
    if step_type in {"activity", "timer"}:
        return _target(step, "next")
    if step_type == "end":
        return None
    if step_type == "decision":
        if type(decision) is not bool:
            raise SemanticsError(
                "Decision routing requires an explicit boolean decision", code="invalid_transition"
            )
        return _target(step, "on_true" if decision else "on_false", required=True)
    if step_type == "approval":
        if type(timed_out) is not bool:
            raise SemanticsError("Approval timed_out must be a boolean", code="invalid_transition")
        if timed_out:
            if approved is not None:
                raise SemanticsError(
                    "Approval cannot be both timed out and decided", code="invalid_transition"
                )
            return _target(step, "on_timeout")
        if type(approved) is not bool:
            raise SemanticsError(
                "Approval routing requires an explicit boolean approved or timed_out=True",
                code="invalid_transition",
            )
        return _target(step, "on_approved" if approved else "on_rejected")
    raise SemanticsError(f"Unsupported step type {step_type!r}", code="invalid_transition")


def _target(step: Mapping[str, object], field: str, *, required: bool = False) -> str | None:
    target = step.get(field)
    if target is None and not required:
        return None
    return _required_string(target, f"{step.get('type')}.{field}")
