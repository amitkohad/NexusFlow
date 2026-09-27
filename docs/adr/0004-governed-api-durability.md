# ADR 0004: Governed API start durability and business projections

- Status: Accepted for Phase 3
- Date: 2026-09-27
- Scope: T015–T021; builds on ADR 0002

## Decision

The FastAPI application injects repositories, a runtime adapter, and a verified
identity provider. Business operations are denied without a provider. Principals
establish actor, permissions, tenant, business domain, and application; request
context must match that scope. Local/test tokens are explicitly configured and
cannot serve as the deployed identity provider.

Store an execution reservation before contacting Temporal. PostgreSQL enforces a
unique key over scope, workflow type, and idempotency key. The reservation retains
a canonical request fingerprint, immutable definition document/version, business
ID, and start input. An identical retry uses that record; a changed request returns
409. Temporal rejects duplicate IDs even after completion. If a start response or
run binding is lost, the next identical request can recover the same run. No
database transaction stays open across a runtime RPC.

Registry approval/promotion and audit events share transactions. An environment's
active promotion is separate from the immutable revision. Resolving that slot
pins a start; a concurrent later promotion does not mutate a reserved execution.
The first increment supports validated, approved, and promoted revisions. Full
deprecation, dependency/version compatibility, and release governance remain
Phase 7.

The Temporal adapter submits the prototype by name. Registration permits its
five existing capabilities and rejects unsupported queue routing, contract
versions, compensation, templates, and versioned worker dependencies. The legacy
retry adapter now passes declared non-retryable exception types to Temporal.
The separate runtime and workers remain Phase 4; this increment does not claim
the full resilience policy implementation.

Public status and audit models exclude runtime IDs, queues, worker identities,
raw history, and capability details. Durable audit projections deduplicate by
transition ordinal. Snapshot updates compare workflow progress before observation
time and protect closed outcomes against late polls. Status observes the pinned
run. History attempts observation but serves existing business evidence with
`refreshed=false` if the runtime is unavailable or its retained run is gone.
Projection is observation driven; it is not a guaranteed complete asynchronous
audit export. Full audit/telemetry collection remains Phase 8.

## Consequences

There is no distributed atomic transaction between PostgreSQL and Temporal. A
pending reservation and duplicate-ID policy provide recovery on client retry.
Reservations must be retained as long as keys can be retried. If Temporal history
is deleted before a pending run binding is recovered, the engine cannot prove an
old ID existed; operational retention must exceed that recovery window. Automated
reconciliation, retention/export policy, and production SLOs require later work.

Approval signals authenticate the actor and require a waiting execution. They
do not implement exactly-once human-task completion, assignment-group policy, or
atomic signal/audit delivery; Phase 5 owns those guarantees. Cancellation is a
cooperative runtime request with persisted business reason. Request logs contain
method, path, status, and correlation, never bodies or bearer values. Request
bodies are bounded before validation and reject duplicate/non-finite JSON.
