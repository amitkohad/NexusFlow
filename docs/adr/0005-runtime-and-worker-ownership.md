# ADR 0005: Versioned orchestration and capability-owned worker services

- Status: Historical Phase 4 implementation decision; target deployment ownership superseded by ADR 0006
- Date: 2026-09-27
- Scope: T022–T031

The runtime and five capability-worker packages described here are the current
implemented baseline. The user subsequently selected independently deployable
workflow packages and generic executor pools. [ADR 0006](0006-workflow-packages-and-executor-pools.md)
records that accepted target; Phase 4A must implement its migration before new
package deployment is claimed. Existing queues and histories keep the bindings
recorded by this ADR during coexistence.

## Decision

Introduce `GovernedWorkflowV1` on `workflow-orchestration-tq`. It interprets a
pinned definition using the shared pure semantics and typed version 1 Activity
envelopes. External I/O belongs to capability workers. The immutable catalog maps
each capability/version to its registered Activity name and owned queue:

| Capability | Activity registration | Owner queue |
| --- | --- | --- |
| validate_request | validate_request.v1 | validation-tq |
| send_notification | send_notification.v1 | notification-tq |
| post_adjustment | post_adjustment.v1 | integration-tq |
| create_approval_task | create_approval_task.v1 | human-task-tq |
| risk_check | risk_check.v1 | sample-business-tq |
| record_rejection | record_rejection.v1 | sample-business-tq |

The runtime contains no generic dispatcher and registers no capability Activity.
Each worker package polls only its queue and imports no other worker. Shared
contracts, configuration/bootstrap, SDK validation, and pure workflow helpers
have separately buildable distributions. Individual service wheels do not depend
on the aggregate development distribution or package unrelated workers.

Use Temporal's [Pydantic data converter](https://github.com/temporalio/sdk-python#pydantic-support)
consistently in API, orchestration, and Activity worker clients. Activity payloads
carry trusted business context, workflow/run/step identity, contract version, and
an idempotency identity stable across attempts. Retry/timeout settings are applied
through SDK primitives. Worker validation failures are explicitly nonretryable;
the sample transient risk failure is retryable. Input templates and decisions use
the already-tested deterministic helpers rather than prototype coercion.

Approval steps call `human-task-worker` to create a reference before exposing a
wait. The runtime accepts the first strict boolean decision during that wait and
uses durable Temporal timers for expiry. Reference adapters are explicitly mock
implementations; persisted task lifecycle and exactly-once completion remain
Phase 5. Enterprise adapters and side-effect idempotency/remediation remain the
later service/resilience increments.

Migration `0002` persists runtime profile and queue with each execution reservation.
Pending retries use that binding across API rollout or rollback. The legacy class
and worker stay unchanged at their old paths for history compatibility and CLI
demonstrations. New API starts default to governed mode. Registration normalizes
owned queues, versions, and dependency metadata; unsupported routing/policy is
rejected before execution. Existing Phase 3 rows backfill their documented default
legacy queue; deployments that used a custom queue need an explicit migration
mapping for pending reservations.

Shared worker bootstrap provides liveness/readiness, safe structured lifecycle
logs, validated settings, and SDK graceful shutdown on SIGINT/SIGTERM. Readiness
drops before draining. Eager execution is disabled, so orchestration cannot
implicitly consume capability work. Full metrics/traces/security middleware
remain Phase 8.

## Compatibility and consequences

Runtime v1 graph size is capped at 500 steps; API limits may be lowered. Versioned
registrations and v1 catalog routes must not be rewritten while histories depend
on them. Replay tests cover compatible runtime restart and definition promotion
without rebinding running workflows. They do not replace deployment-specific
worker versioning and long-running rollout policies planned for Phase 6.

No production container/cloud deployment is included. Service wheel boundaries
and clean-environment install checks provide independent Python packaging now;
container and platform validation remain Phase 9. The reference human-task and
business adapters prove routing and correlation without introducing a second
durability engine or claiming real enterprise side effects. These Phase 4 service
wheels remain valid baseline artifacts; they do not satisfy the later requirement
to deploy a business workflow and all executable Activity dependencies as one
immutable package.
