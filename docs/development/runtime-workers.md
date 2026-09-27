# Phase 4 runtime and independent workers

The governed API now starts `GovernedWorkflowV1`. Orchestration and capability
workers run as separate processes, poll only their owned queues, and can be
stopped or scaled independently. The five capability packages use mock/reference
adapters so the sample remains self-contained; they do not call real enterprise
notification, integration, or human-task services.

## Start the services

Run `uv sync --locked` from the repository root. Start a local Temporal frontend
with `temporal server start-dev`. In six separate terminals, clear a queue value
inherited from an earlier demo (`Remove-Item Env:TEMPORAL_TASK_QUEUE -ErrorAction SilentlyContinue`
in PowerShell), then run one command per terminal:

| Service | Command | Owned queue | Local probe port |
| --- | --- | --- | --- |
| Runtime | `uv run --locked python -m workflow_runtime` | `workflow-orchestration-tq` | 8080 |
| Validation | `uv run --locked python -m validation_worker` | `validation-tq` | 8081 |
| Notification | `uv run --locked python -m notification_worker` | `notification-tq` | 8082 |
| Integration | `uv run --locked python -m integration_worker` | `integration-tq` | 8083 |
| Human task | `uv run --locked python -m human_task_worker` | `human-task-tq` | 8084 |
| Sample business | `uv run --locked python -m sample_business_worker` | `sample-business-tq` | 8085 |

Each worker exports its own `.env.example`, package metadata, typed contracts,
and registrations. Environment files are references; loading is explicit.
`TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_TLS`, and
`NEXUSFLOW_ENVIRONMENT` apply to all services. `TEMPORAL_TASK_QUEUE`, if supplied,
must match that service's queue; startup rejects a different worker's queue.
Production settings require Temporal TLS; enterprise authentication and secret
provider integration remain later delivery work.

Probe defaults bind to loopback. Set `NEXUSFLOW_PROBE_HOST=0.0.0.0` in a container
and select `NEXUSFLOW_PROBE_PORT` when needed. `/health` checks process liveness;
`/ready` checks that the SDK worker is running and the Temporal frontend responds.
Ctrl+C/SIGTERM removes readiness before draining in-flight Activities. The SDK
uses `NEXUSFLOW_SHUTDOWN_GRACE_SECONDS` (default 30) as its bounded drain period.
Unfinished work after that period follows Temporal cancellation/retry behavior.

## Start through the API

In the API terminal, clear an inherited legacy `TEMPORAL_TASK_QUEUE`, set
`NEXUSFLOW_RUNTIME_PROFILE=governed` (the default), and apply database migrations:

```powershell
Remove-Item Env:TEMPORAL_TASK_QUEUE -ErrorAction SilentlyContinue
$env:NEXUSFLOW_RUNTIME_PROFILE = "governed"
$env:NEXUSFLOW_DATABASE_URL = "sqlite:///nexusflow.db"
uv run --locked python -m workflow_api.manage upgrade
```

Follow the [API walkthrough](workflow-api.md) for local identity configuration,
launching Uvicorn, registration, approval, promotion, start, status, and history.
Use its sample amount `1000` for completion without approval. For the high path,
submit amount `7500` with a fresh key and approve after `WAITING_FOR_APPROVAL`.
High-value execution invokes all five capability-owned queues before completing.

The API resolves missing Activity contract versions/queues during registration
and persists the matching dependencies. Definitions can explicitly specify the
supported `contract_version: "1.0"` and catalog-owned queue. Other versions or
queues are rejected. Input templates such as `${request.amount}` use the shared
deterministic semantics. Compensation remains rejected until Phase 6. The runtime
creates approval tasks internally; `create_approval_task` is reserved for approval
steps rather than ordinary Activity nodes.

## Rollout and package verification

Migration `0002` adds runtime profile and queue bindings to execution reservations.
Existing rows are backfilled as `legacy` on `lightweight-workflows`, preserving
the Phase 3 default. If a deployment used a custom legacy queue, establish that
queue for its pending reservations before cutover; the old schema did not store
it. Already-bound run status/history remain independent of worker queue changes.

New starts use the configured profile. Retrying an existing key reuses its
original ID, profile, queue, input, and revision even after rollout or rollback.
The backend uses the stored binding, so a pending legacy start can recover while
the current API serves governed starts. Keep the legacy worker available for
legacy executions that need it. A fresh legacy start against a revision requiring
governed features returns `409` before creating a reservation.

Compatible runtime restarts replay existing histories. Versioned Activity names,
contract versions, and v1 catalog routes must remain stable for those histories.
Incompatible orchestration changes require a new workflow type/version rather
than rewriting `GovernedWorkflowV1`. Production worker deployment/versioning and
long-running rollout policies remain Phase 6.

Build and independently install-check the six services and four shared libraries:

```text
uv run --locked python scripts/build_services.py --verify
```

Artifacts are written to `dist/services`. Each service wheel includes only its
own Python package, depends on the shared library wheels, and installs under the
committed runtime constraints. Verification creates clean temporary environments
and asserts that the API, legacy application, and other workers are absent.
The root `nexusflow` distribution aggregates packages for local development.
Deploy the individual service wheels; independent images/Helm remain Phase 9.

Run `uv run --locked python scripts/dev.py test` and
`uv run --locked python scripts/dev.py lint`. Real Temporal tests require a CLI
on PATH or `TEMPORAL_CLI_PATH`. PostgreSQL migration/concurrency checks run with
an explicitly configured `NEXUSFLOW_TEST_DATABASE_URL` and isolated schemas.
