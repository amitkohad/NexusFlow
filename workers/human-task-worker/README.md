# human-task-worker

Owns `create_approval_task.v1` on `human-task-tq`. The Activity accepts a typed
`ActivityRequest` and a bounded nonempty `input.assignee_group` (default
`approvers`). Its output includes `created: true`, `task_id`, and
`assignee_group`. The reference task ID is derived from workflow and step IDs,
so retries return the same task reference.

Phase 4 provides the routing and approval-wait integration. This reference
adapter does not persist tasks, assignments, forms, evidence, SLAs, or task
completion. Phase 5 supplies the durable human-task lifecycle. Invalid assignment
input raises a nonretryable `ValidationError`.

## Contracts and ownership

Every Activity is registered under its explicit `.v1` name and verifies its
capability envelope. `contract_version` is `1.0`. `ACTIVITIES`, `CONTRACTS`,
`SERVICE`, and `TASK_QUEUE` are exported by `human_task_worker`; `CONTRACTS` publishes
JSON input/output schemas and queue ownership through the shared catalog.
The worker imports no other worker package and polls only its owned queue.
Business context, actor, workflow/run IDs, and idempotency identity travel in the
typed envelope. No activity logs payloads, credentials, or authentication tokens.

## Local development

From the repository root, install the locked development environment with
`uv sync --locked`. Run this service in its own process:

```powershell
$env:TEMPORAL_ADDRESS = "localhost:7233"
$env:TEMPORAL_NAMESPACE = "default"
$env:TEMPORAL_TASK_QUEUE = "human-task-tq"
uv run --locked python -m human_task_worker
```

The shared bootstrap uses the Temporal Pydantic data converter, validated process
environment settings, and graceful shutdown. `NEXUSFLOW_SHUTDOWN_GRACE_SECONDS`
controls the bounded drain period (default 30 seconds).
`TEMPORAL_TASK_QUEUE`, when explicitly set, must equal `human-task-tq`; a mismatch
fails startup. Environment files are examples and are not automatically loaded.
`NEXUSFLOW_ENVIRONMENT=prod` requires a TLS-enabled Temporal connection.

Probes listen on `127.0.0.1:8084` by default. Set
`NEXUSFLOW_PROBE_HOST=0.0.0.0` for container probes and use
`NEXUSFLOW_PROBE_PORT` to select an explicit port. `GET /health` reports process
health; `GET /ready` returns 200 only while the SDK worker is running and the
Temporal frontend is healthy. Readiness becomes 503 before in-flight Activities
drain during shutdown, while health stays available until drain completes.

## Independent package

Build this worker independently from the repository root:

```text
python -m build --wheel workers/human-task-worker
```

The wheel contains only `human_task_worker`. Install it with the separately built
`nexusflow-contracts`, `nexusflow-common`, `nexusflow-workflow-sdk`, and
`nexusflow-workflows` wheels at version `0.1.0`, plus their locked dependencies.
It does not depend on the aggregate `nexusflow` development package or any other
worker distribution. Start the installed wheel with `python -m human_task_worker` or
`nexusflow-human-task-worker`. Image/container packaging is a later delivery task.

Contract tests are in `tests/contract/workers/test_worker_contracts.py`;
Temporal integration tests exercise actual independent queue registrations.
`tests/integration/test_worker_shutdown.py` verifies that in-flight typed
Activities finish within the configured SDK shutdown grace period.
