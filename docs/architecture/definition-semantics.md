# Definition semantics (v1)

`workflows/common/` provides pure functions for the Phase 2 contracts. They use
only the Python standard library and perform no network, file, environment,
Temporal, random, or clock access. Runtime callers must supply decision and
approval outcomes and workflow timestamps explicitly.

## Paths and missing values

`resolve_path(context, path)` walks dot-separated dictionary keys and numeric
list indices, for example `request.items.0.amount`. Each segment is either an
identifier matching `[A-Za-z_][A-Za-z0-9_-]*` or a canonical nonnegative integer
(`0` or a nonzero digit followed by digits). Negative indices, leading zeros,
empty segments, bracket indexing, and expressions are invalid. Numeric segments
remain string keys when traversing dictionaries. Lists require numeric segments.
`validate_path` exposes the same syntax check without requiring runtime data.

A present JSON `null` resolves to Python `None`. A missing key, out-of-range
index, or attempt to traverse a scalar raises `SemanticsError` with the failing
path. Missing data never silently compares equal to explicit null. The returned
path value is a reference to the context; callers should treat it as read-only.
Template expansion copies referenced lists and dictionaries.

## Decisions

`compare_values(left, operator, right)` accepts finite JSON values. NaN, infinity,
non-string dictionary keys, and non-JSON objects are rejected, including nested
values. Python containers containing cycles and documents with more than 64
nested containers are rejected with `SemanticsError`, rather than leaking a
recursion error. Shared references without cycles are valid. `route_decision`
resolves a field and returns the supplied `on_true` or
`on_false` target.

| Operator | Semantics |
| --- | --- |
| `==`, `!=` | Recursive JSON equality. Booleans differ from numbers, numeric `1` and `1.0` compare equal, lists are ordered, and dictionary key order is irrelevant. |
| `>`, `>=`, `<`, `<=` | Two finite numbers excluding booleans, or two strings using Python's deterministic Unicode lexicographic order. No conversion between strings and numbers. |
| `in` with a list | Membership using the same strict recursive equality. |
| `in` with a string | Substring membership with a string left operand. |
| `in` with a dictionary | Key membership with a string left operand. |

Unknown operators and incompatible operand types raise actionable
`SemanticsError`s. Definition validation can reject incompatible literal right
operands before execution; the runtime helper checks both resolved operands.

## Templates

`resolve_template(value, context)` recursively expands JSON values. Dictionary
keys stay literal. A whole string `${request.items}` preserves the referenced
JSON type and returns an independent copy. Interpolation such as
`Hello ${request.name}; approved=${request.approved}` produces a string. Strings
are inserted directly; null, booleans, and numbers use stable JSON rendering
(`null`, `false`, `42`, `12.5`). Lists and dictionaries are only allowed as whole
references and cannot be interpolated into surrounding text.
The template, referenced values, and fully expanded result use the same 64-level
JSON nesting bound.

References use the path grammar above. Empty, nested, or unclosed references,
missing values, bracket access, function calls, and arithmetic expressions are
rejected. Other text stays literal. Referenced strings are inserted once and
are never expanded again. There is no `eval`, `exec`, expression evaluator,
escape syntax, or secret lookup.

## Transitions

`make_transition(step, state, detail, workflow_time)` preserves the prototype's
`step`, `state`, `detail`, and `workflow_time` keys. A supplied `datetime` is
rendered with `isoformat`; a supplied nonempty timestamp string is retained.
The function never acquires time, infers state, or changes an execution.

`transition_target(step, decision=..., approved=..., timed_out=...)` selects
`next` for Activities/timers, `on_true`/`on_false` for decisions,
`on_approved`/`on_rejected`/`on_timeout` for approvals, and no target for an end
step. Decisions require a supplied boolean. Approvals require a supplied boolean
outcome or `timed_out=True`; contradictory timeout and approval outcomes are
rejected. Missing optional routes terminate by returning `None`, matching the
prototype's target behavior. Decision routes must be present. Graph validation
is responsible for confirming required definition routes and referenced steps.

These helpers do not schedule Activities, wait for signals, set business state,
record an audit event, run compensation, or implement cancellation.

## Runtime migration boundary

Phase 2 establishes and tests the v1 semantics independently of the existing
`app/workflows.py` interpreter. The legacy helpers remain in use there, so the
current `LightweightProcess` commands and recorded-history behavior are unchanged.
In particular, the prototype treats missing fields as null and uses Python's
boolean/number equality; the new contract deliberately rejects missing values
and uses strict JSON equality. Template expansion is new functionality.

Phase 4 (T023/T031) must integrate these helpers behind an explicit compatibility
strategy with Temporal workflow tests before changing decisions or recorded
commands for existing executions. Phase 2 tests verify the new pure semantics;
the existing Temporal baseline tests continue to protect the legacy interpreter.
