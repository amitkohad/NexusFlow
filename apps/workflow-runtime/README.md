# Workflow runtime

The runtime service registers `GovernedWorkflowV1` on `workflow-orchestration-tq`.
It owns orchestration only; each capability Activity is handled by its independent
worker. Definitions and business context are captured in the start envelope and
stay fixed for the execution.

The interpreter uses shared deterministic path, decision, template, and transition
helpers from `workflows/common`. It supports Activities with explicit retry/timeout
policy, decisions, timers, approvals, cancellation, and terminal outcomes. Each
Activity uses a typed `ActivityRequest`/`ActivityResponse`, a versioned `.v1`
registration, and its catalog-owned queue. The client and workers use Temporal's
Pydantic data converter. Workflow code performs no external I/O.

Approval creates a task reference through `human-task-worker` before exposing
`WAITING_FOR_APPROVAL`. The first valid in-window decision is accepted by the
existing `approve` signal. Persisted task assignment, evidence, and exactly-once
completion are Phase 5 work. Compensation and remediation remain Phase 6.

Start this service separately from each capability worker:

```text
uv run --locked python -m workflow_runtime
```

`TEMPORAL_TASK_QUEUE`, if set, must be `workflow-orchestration-tq`. Namespace,
address, TLS, and graceful shutdown are shared settings. The API applies its
configured HTTP payload limit; full runtime history/payload budgets remain later work.
Version 1 validates graphs up to 500 steps. Local probe defaults are
`127.0.0.1:8080`; `NEXUSFLOW_PROBE_HOST` and `NEXUSFLOW_PROBE_PORT` configure them.
`/health` reports process health and `/ready` requires the running worker and
Temporal frontend. On shutdown, readiness drops before the SDK drains in-flight
work for `NEXUSFLOW_SHUTDOWN_GRACE_SECONDS` (default 30 seconds).

Build this service with `python -m build apps/workflow-runtime`, or build and
verify all independently installable services using
`uv run --locked python scripts/build_services.py --verify`.
Its wheel contains only `workflow_runtime` and depends on four shared library
wheels. It contains no API, legacy application, or capability worker package.
Container/deployment packaging remains Phase 9.

The old `LightweightProcess` class and prototype worker remain available at their
original paths for existing histories and CLI demonstrations. They are not
registered by this service. See the [local walkthrough](../../docs/development/runtime-workers.md)
and [rollout decision](../../docs/adr/0005-runtime-and-worker-ownership.md).
