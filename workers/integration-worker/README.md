# integration-worker

Owns `post_adjustment.v1` on `integration-tq`. The Activity accepts a typed
`ActivityRequest` and returns `ActivityResponse.output = {posted: true, reference:
string}`. The mock receipt is derived from `idempotency_key`, so retry attempts
return the same reference.

This is a reference adapter: it does not post a real adjustment. Replace it with
an enterprise adapter that persists deduplication by the supplied idempotency key
and reconciles uncertain downstream outcomes before production use. A stable
mock reference is not a durable downstream transaction store.

## Contracts and ownership

Every Activity is registered under its explicit `.v1` name and verifies its
capability envelope. `contract_version` is `1.0`. `ACTIVITIES`, `CONTRACTS`,
`SERVICE`, and `TASK_QUEUE` are exported by `integration_worker`; `CONTRACTS` publishes
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
$env:TEMPORAL_TASK_QUEUE = "integration-tq"
uv run --locked python -m integration_worker
```

The shared bootstrap uses the Temporal Pydantic data converter, validated process
environment settings, and graceful shutdown. `NEXUSFLOW_SHUTDOWN_GRACE_SECONDS`
controls the bounded drain period (default 30 seconds).
`TEMPORAL_TASK_QUEUE`, when explicitly set, must equal `integration-tq`; a mismatch
fails startup. Environment files are examples and are not automatically loaded.
`NEXUSFLOW_ENVIRONMENT=prod` requires a TLS-enabled Temporal connection.

Probes listen on `127.0.0.1:8083` by default. Set
`NEXUSFLOW_PROBE_HOST=0.0.0.0` for container probes and use
`NEXUSFLOW_PROBE_PORT` to select an explicit port. `GET /health` reports process
health; `GET /ready` returns 200 only while the SDK worker is running and the
Temporal frontend is healthy. Readiness becomes 503 before in-flight Activities
drain during shutdown, while health stays available until drain completes.

## Independent package

Build this worker independently from the repository root:

```text
python -m build --wheel workers/integration-worker
```

The wheel contains only `integration_worker`. Install it with the separately built
`nexusflow-contracts`, `nexusflow-common`, `nexusflow-workflow-sdk`, and
`nexusflow-workflows` wheels at version `0.1.0`, plus their locked dependencies.
It does not depend on the aggregate `nexusflow` development package or any other
worker distribution. Start the installed wheel with `python -m integration_worker` or
`nexusflow-integration-worker`. Image/container packaging is a later delivery task.

Contract tests are in `tests/contract/workers/test_worker_contracts.py`;
Temporal integration tests exercise actual independent queue registrations.
`tests/integration/test_worker_shutdown.py` verifies that in-flight typed
Activities finish within the configured SDK shutdown grace period.
