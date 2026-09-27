"""Definition validation without file access, environment reads, or network calls.

Issue paths use JSON Pointer escaping, with ``$`` identifying the document root.
Parsing checks the versioned contract; validation additionally checks graph and
capability policy. Version 1 requires an explicit end on every possible path and
does not permit cycles.
"""

from __future__ import annotations

import json
import math
from collections import deque
from collections.abc import Collection, Mapping
from typing import Any

from contracts import (
    ActivityStep,
    ApprovalStep,
    DecisionStep,
    DefinitionDocument,
    EndStep,
    TimerStep,
    ValidationIssue,
)
from nexusflow_common.errors import ValidationError as PlatformValidationError
from pydantic import ValidationError

DefinitionInput = Mapping[str, Any] | str | bytes


class DefinitionValidationError(PlatformValidationError, ValueError):
    """Actionable issues with stable codes and paths, safe for API translation."""

    def __init__(self, issues: Collection[ValidationIssue]) -> None:
        self.issues = tuple(issues)
        message = "; ".join(f"{i.path}: {i.message} [{i.code}]" for i in self.issues)
        super().__init__(message, code="definition_invalid", issues=self.issues)


def _issue(code: str, path: str, message: str) -> ValidationIssue:
    return ValidationIssue(code=code, path=path, message=message)


def _pointer(parts: Collection[str | int]) -> str:
    return "/" + "/".join(str(p).replace("~", "~0").replace("/", "~1") for p in parts)


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise DefinitionValidationError(
                [_issue("json.duplicate_key", "$", f"Duplicate JSON object key: {key!r}")]
            )
        result[key] = value
    return result


def _invalid_constant(value: str) -> Any:
    raise DefinitionValidationError(
        [_issue("json.non_finite_number", "$", f"JSON number must be finite: {value}")]
    )


def parse_definition(document: DefinitionInput) -> DefinitionDocument:
    """Parse JSON or a mapping into the strict definition contract.

    Unlike Python's default JSON reader, duplicate keys and non-finite constants
    are rejected. This function does not apply graph or deployment policy checks.
    """
    if isinstance(document, (str, bytes)):
        try:
            value = json.loads(
                document, object_pairs_hook=_unique_object, parse_constant=_invalid_constant
            )
        except DefinitionValidationError:
            raise
        except (ValueError, RecursionError) as exc:
            raise DefinitionValidationError(
                [_issue("json.invalid", "$", f"Cannot parse definition JSON: {exc}")]
            ) from exc
    elif isinstance(document, Mapping):
        value = dict(document)
    else:
        raise DefinitionValidationError(
            [_issue("definition.invalid_structure", "$", "Expected a JSON object or JSON text")]
        )

    try:
        return DefinitionDocument.model_validate(value)
    except RecursionError as exc:
        raise DefinitionValidationError(
            [
                _issue(
                    "definition.invalid_structure",
                    "$",
                    "Definition JSON is nested too deeply or contains cyclic data",
                )
            ]
        ) from exc
    except ValidationError as exc:
        issues = []
        step_variants = {"activity", "decision", "approval", "timer", "end"}
        for error in exc.errors(include_url=False, include_context=False, include_input=False):
            # Pydantic inserts the selected union variant after a step identifier;
            # it is not part of the actual definition document's path.
            location = list(error["loc"])
            if len(location) > 2 and location[0] == "steps" and location[2] in step_variants:
                location.pop(2)
            issues.append(
                _issue(
                    "definition.invalid_structure",
                    _pointer(location) if location else "$",
                    error["msg"],
                )
            )
        raise DefinitionValidationError(issues) from exc


def _edges(
    step: ActivityStep | DecisionStep | ApprovalStep | TimerStep | EndStep,
) -> dict[str, str]:
    if isinstance(step, (ActivityStep, TimerStep)):
        return {"next": step.next}
    if isinstance(step, DecisionStep):
        return {"on_true": step.on_true, "on_false": step.on_false}
    if isinstance(step, ApprovalStep):
        return {
            "on_approved": step.on_approved,
            "on_rejected": step.on_rejected,
            "on_timeout": step.on_timeout,
        }
    return {}


def _check_literal(step_id: str, step: DecisionStep) -> ValidationIssue | None:
    right = step.value
    if step.operator in {">", ">=", "<", "<="}:
        valid = (
            isinstance(right, str)
            or type(right) is int
            or (type(right) is float and math.isfinite(right))
        )
        expected = "Ordering requires a finite number or string literal (booleans are excluded)"
    elif step.operator == "in":
        valid = isinstance(right, (list, str, dict))
        expected = "Membership requires a list, string, or object literal"
    else:
        return None
    if valid:
        return None
    return _issue("decision.invalid_literal", _pointer(["steps", step_id, "value"]), expected)


def validate_definition(
    document: DefinitionDocument | DefinitionInput,
    *,
    allowed_capabilities: Collection[str] | None = None,
    max_steps: int = 500,
) -> DefinitionDocument:
    """Validate all possible graph branches using bounded iterative algorithms.

    Capability policy is supplied by the caller; omitting it makes validation
    independent of any deployed worker registry. Models are revalidated because
    their nested dictionaries may have changed since construction.
    """
    if type(max_steps) is not int or max_steps < 1:
        raise ValueError("max_steps must be a positive integer")
    try:
        value = (
            document.model_dump(mode="python")
            if isinstance(document, DefinitionDocument)
            else document
        )
    except (RecursionError, ValueError) as exc:
        raise DefinitionValidationError(
            [
                _issue(
                    "definition.invalid_structure",
                    "$",
                    "Definition JSON is nested too deeply or contains cyclic data",
                )
            ]
        ) from exc
    parsed = parse_definition(value)
    if len(parsed.steps) > max_steps:
        raise DefinitionValidationError(
            [_issue("definition.step_limit", "/steps", f"At most {max_steps} steps are allowed")]
        )

    issues: list[ValidationIssue] = []
    edges = {step_id: _edges(step) for step_id, step in parsed.steps.items()}
    allowed = set(allowed_capabilities) if allowed_capabilities is not None else None
    for step_id, step in parsed.steps.items():
        for field, target in edges[step_id].items():
            if target not in parsed.steps:
                issues.append(
                    _issue(
                        "graph.missing_target",
                        _pointer(["steps", step_id, field]),
                        f"Transition target {target!r} is not a declared step",
                    )
                )
        if isinstance(step, ActivityStep):
            if allowed is not None and step.capability not in allowed:
                issues.append(
                    _issue(
                        "capability.unsupported",
                        _pointer(["steps", step_id, "capability"]),
                        f"Capability {step.capability!r} is not in the supplied allowlist",
                    )
                )
            if (
                allowed is not None
                and step.compensation is not None
                and step.compensation.capability not in allowed
            ):
                issues.append(
                    _issue(
                        "capability.unsupported",
                        _pointer(["steps", step_id, "compensation", "capability"]),
                        f"Compensation capability {step.compensation.capability!r} is not allowed",
                    )
                )
        if isinstance(step, DecisionStep):
            literal_issue = _check_literal(step_id, step)
            if literal_issue is not None:
                issues.append(literal_issue)

    if parsed.start_at not in parsed.steps:
        issues.append(
            _issue("graph.invalid_start", "/start_at", "start_at must reference a declared step")
        )
        raise DefinitionValidationError(issues)

    adjacency = {
        node: set(target for target in targets.values() if target in parsed.steps)
        for node, targets in edges.items()
    }
    reached: set[str] = set()
    pending = [parsed.start_at]
    while pending:
        node = pending.pop()
        if node not in reached:
            reached.add(node)
            pending.extend(adjacency[node] - reached)
    for node in parsed.steps:
        if node not in reached:
            issues.append(
                _issue(
                    "graph.unreachable_step",
                    _pointer(["steps", node]),
                    "Step cannot be reached from start_at",
                )
            )

    # Kahn's algorithm detects cycles without Python recursion, including large
    # input graphs. Sorting diagnostics keeps issue ordering deterministic.
    indegree = dict.fromkeys(parsed.steps, 0)
    predecessors: dict[str, set[str]] = {node: set() for node in parsed.steps}
    for node, targets in adjacency.items():
        for target in targets:
            indegree[target] += 1
            predecessors[target].add(node)
    queue = deque(node for node, degree in indegree.items() if degree == 0)
    removed = 0
    while queue:
        node = queue.popleft()
        removed += 1
        for target in adjacency[node]:
            indegree[target] -= 1
            if indegree[target] == 0:
                queue.append(target)
    if removed < len(parsed.steps):
        blocked = sorted(node for node, degree in indegree.items() if degree > 0)
        issues.append(
            _issue(
                "graph.cycle",
                "/steps",
                f"Version 1 does not permit cycles; cycle or downstream steps: {', '.join(blocked)}",
            )
        )

    terminal = {node for node in reached if isinstance(parsed.steps[node], EndStep)}
    if not terminal:
        issues.append(
            _issue(
                "graph.missing_terminal", "/steps", "Declare an end step reachable from start_at"
            )
        )
    terminating = set(terminal)
    queue = deque(terminal)
    while queue:
        node = queue.popleft()
        for previous in predecessors[node] - terminating:
            terminating.add(previous)
            queue.append(previous)
    for node in parsed.steps:
        if node in reached and node not in terminating:
            issues.append(
                _issue(
                    "graph.non_terminating_path",
                    _pointer(["steps", node]),
                    "Step has no path to an explicit end step",
                )
            )

    if issues:
        raise DefinitionValidationError(issues)
    return parsed
