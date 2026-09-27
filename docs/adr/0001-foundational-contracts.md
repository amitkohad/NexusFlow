# ADR 0001: Versioned contracts and deterministic definition semantics

Status: Accepted for Phase 2. Date: 2026-09-26.

## Decision

Use Python 3.11+ and Pydantic 2 for closed, typed transport contracts. Unknown
fields, coerced scalar values, and non-JSON payloads are rejected. Generate the
Draft 2020-12 definition schema from the models and check it for drift during
lint. The schema describes syntax; SDK validation adds cross-field and graph
checks that JSON Schema alone does not enforce.

JSON payloads are finite, cycle-free, and limited to 64 nested containers.
Validation detaches payloads from caller data. This is a supported syntax bound,
not a maximum production payload size or an immutable persistence mechanism.

Version 1.0 supports activities, decisions, approvals, timers, and explicit end
steps. All routes must resolve, every step must be reachable, and cycles are
rejected. A loop construct needs an explicit execution/history policy before a
future schema version supports it. Capability policy is an allow-list supplied
by the caller, including compensation capabilities; it is not a built-in list
of demo actions. Payloads retain the existing sample's top-level request and
variables so the baseline can serve as a compatibility fixture.

Path, decision, template, and transition helpers are pure functions. Time is
supplied by the runtime; external I/O stays outside them. `${path}` templates
perform data substitution with no expression evaluation. Contracts and policies
declare shape and intent; Temporal executes retries, timers, and cancellation
when later workers consume these contracts.

The existing interpreter remains the Phase 1 baseline. Phase 4 will integrate
the foundation into the independently deployed runtime and capability workers.
This avoids changing recorded workflow commands during the contracts increment.

## Packaging and ownership

Keep the planned sources under `libs/contracts/src/contracts`,
`libs/workflow-sdk/src/workflow_sdk`, `libs/common/src/nexusflow_common`, and
`workflows/common`. For this foundation increment, Hatchling includes their import
packages in the existing `nexusflow` distribution. This avoids adding a workspace
release system before deployable service boundaries exist. Phase 4 worker
packaging must explicitly select its owned service and shared dependencies;
the current distribution is not a production worker image.

Pydantic frozen fields prevent reassignment but do not make nested dictionaries
immutable. Registry persistence must serialize, hash, and enforce immutable
promoted revisions in the governance phase. These models do not provide a
promotion store or authenticated task completion.

## Alternatives and consequences

Handwritten model and schema copies create drift. Plain dictionaries preserve the
prototype's weak validation. A general expression or cyclic graph engine would
add semantics with no defined business requirement or history policy. The
selected model provides actionable validation without replacing Temporal.

References: [Pydantic models](https://docs.pydantic.dev/latest/concepts/models/),
[JSON Schema 2020-12](https://json-schema.org/draft/2020-12/json-schema-core).
