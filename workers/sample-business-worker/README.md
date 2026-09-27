# sample-business-worker

Reference-only worker owning `risk_check.v1` and `record_rejection.v1` on
`sample-business-tq`. These mock capabilities demonstrate the sample business
flow; they are not production enterprise adapters.

`risk_check` requires a finite positive numeric `request.amount`, returning
`risk_score: 65, risk_band: HIGH` at amounts of 5000 or more and
`risk_score: 25, risk_band: LOW` below that. Setting the strict boolean
`request.simulate_transient_failure` to `true` produces a retryable
`TechnicalError` on the first Activity attempt and succeeds on the next attempt.
Invalid amount or flag values produce nonretryable `ValidationError` failures.
`record_rejection` returns `recorded: true` and
`reason: business approval rejected` without writing to an external system.

## Contracts and ownership

Every Activity is registered under its explicit `.v1` name and verifies its
capability envelope. `contract_version` is `1.0`. `ACTIVITIES`, `CONTRACTS`,
`SERVICE`, and `TASK_QUEUE` are exported by `sample_business_worker`; `CONTRACTS` publishes
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
$env:TEMPORAL_TASK_QUEUE = "sample-business-tq"
uv run --locked python -m sample_business_worker
```

The shared bootstrap uses the Temporal Pydantic data converter, validated process
environment settings, and graceful shutdown. `NEXUSFLOW_SHUTDOWN_GRACE_SECONDS`
controls the bounded drain period (default 30 seconds).
`TEMPORAL_TASK_QUEUE`, when explicitly set, must equal `sample-business-tq`; a mismatch
fails startup. Environment files are examples and are not automatically loaded.
`NEXUSFLOW_ENVIRONMENT=prod` requires a TLS-enabled Temporal connection.

Probes listen on `127.0.0.1:8085` by default. Set
`NEXUSFLOW_PROBE_HOST=0.0.0.0` for container probes and use
`NEXUSFLOW_PROBE_PORT` to select an explicit port. `GET /health` reports process
health; `GET /ready` returns 200 only while the SDK worker is running and the
Temporal frontend is healthy. Readiness becomes 503 before in-flight Activities
drain during shutdown, while health stays available until drain completes.

## Independent package

Build this worker independently from the repository root:

```text
python -m build --wheel workers/sample-business-worker
```

The wheel contains only `sample_business_worker`. Install it with the separately built
`nexusflow-contracts`, `nexusflow-common`, `nexusflow-workflow-sdk`, and
`nexusflow-workflows` wheels at version `0.1.0`, plus their locked dependencies.
It does not depend on the aggregate `nexusflow` development package or any other
worker distribution. Start the installed wheel with `python -m sample_business_worker` or
`nexusflow-sample-business-worker`. Image/container packaging is a later delivery task.

Contract tests are in `tests/contract/workers/test_worker_contracts.py`;
Temporal integration tests exercise actual independent queue registrations.
`tests/integration/test_worker_shutdown.py` verifies that in-flight typed
Activities finish within the configured SDK shutdown grace period.
