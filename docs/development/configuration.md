# Development configuration and secrets

The governed API and six independent worker services use explicitly loaded typed
configuration. Follow the [runtime/worker walkthrough](runtime-workers.md) and
[API walkthrough](workflow-api.md) for current launch settings. The sections below
also retain the legacy prototype settings and shared foundation conventions.

## Legacy prototype worker settings

`app/worker.py` reads the following process environment variables when the
module is imported. Set them before starting the worker, then restart the
process after changing them.

| Variable | Default | Purpose |
| --- | --- | --- |
| `TEMPORAL_ADDRESS` | `localhost:7233` | Temporal frontend address passed to `Client.connect`. |
| `TEMPORAL_TASK_QUEUE` | `lightweight-workflows` | Queue polled by the prototype worker for its workflow and Activity. |

The current worker registers `LightweightProcess` and `execute_capability` in
one process. It does not read a configurable namespace, authentication,
TLS, database URL, or secret-provider setting. Adding those names to an
environment file would not configure the application.

The root `.env.example` contains governed API/runtime defaults. Each capability
worker has its own `.env.example` with its owned queue. The legacy prototype
requires `TEMPORAL_TASK_QUEUE=lightweight-workflows` if a governed queue was
exported earlier.
Copy it to `.env` for a local reference if useful. **The application does not
automatically load `.env`**: the file alone has no effect. Export settings in
the launching shell or use an explicit environment-file loader.

For PowerShell:

```powershell
$env:TEMPORAL_ADDRESS = "localhost:7233"
$env:TEMPORAL_TASK_QUEUE = "lightweight-workflows"
uv run --locked python -m app.worker
```

For Bash:

```bash
export TEMPORAL_ADDRESS=localhost:7233
export TEMPORAL_TASK_QUEUE=lightweight-workflows
uv run --locked python -m app.worker
```

Start the Temporal development server separately with
`temporal server start-dev`. Changing the worker address does not change the
Temporal CLI's connection settings. Configure CLI invocations separately when
using a different frontend.

## Demo configuration

The workflow start request must use the same queue the worker polls.
`scripts/demo.ps1` accepts `-TaskQueue` and `-SpecFile` parameters; it does not
read `TEMPORAL_TASK_QUEUE`. `scripts/demo.sh` reads `TEMPORAL_TASK_QUEUE`,
`SPEC_FILE`, `RUN_COUNT`, `QUERY_DELAY_SECONDS`, `INTERVAL_START_SECONDS`,
`INTERVAL_STEP_SECONDS`, and `PYTHON`. These are demo controls, not runtime
settings. The Bash demo starts multiple workflows with generated customer IDs
and amounts; the PowerShell demo starts one workflow using its supplied JSON.

Use synthetic customer and approval data for local development. The default
fixture in `examples/customer_adjustment.json` is a demonstration, with a high
amount and a deliberately transient risk-check failure.

## Configuration conventions

- Use Python 3.11 or newer and the committed `uv.lock` for reproducible
  development dependencies. Keep package and pytest/Ruff/mypy configuration in
  `pyproject.toml`; keep environment settings out of Python source changes.
- Keep `.env.example` safe to commit: implemented variable names, explanations,
  and non-secret local defaults only. Document the default and consuming
  service whenever adding a setting.
- Keep `.env` and `.env.*` files local; Git ignores them except for the safe
  `.env.example` template. Ignore rules do not remove previously tracked files
  or protect secrets stored under unrelated names.
- Runtime environment values override the current built-in defaults. There is
  no additional file precedence or configuration-validation layer yet.
- Coordinate queue changes between starters and workers. A worker polling a
  different queue will not pick up work submitted to the original queue.
- Add typed validation and fail-fast checks when the shared configuration
  contract is introduced. Phase 1 records behavior without changing it.

## Secret handling

- Do not commit passwords, tokens, private keys, service-account JSON keys,
  or real customer/evidence payloads. Keep them out of examples, workflow
  definitions, logs, test fixtures, command output, and dependency metadata.
- Local secrets belong in the local process environment or a local secret
  store. Avoid commands that echo their values or save them in shell history.
- Production configuration must use secret references and short-lived
  credentials through the platform's identity and secret-management design.
  The constitution prohibits permanent cloud credentials and service-account
  JSON keys.
- Do not put secrets directly in Temporal workflow inputs or Activity results:
  the prototype carries these values into durable history. Future adapters
  should resolve secret references inside Activities.
- If a secret is committed or exposed, revoke or rotate it and follow the
  organization's incident procedure. Removing the current file does not
  remove earlier Git revisions or Temporal history.

These are repository conventions, not implemented authentication, redaction,
encryption, secret-provider integration, or automated secret scanning.

Phase 3 implements a scoped API identity boundary, explicit local/test token
adapter, safe error responses and request logs, database migrations, and streamed
payload limits. Follow the [API configuration and walkthrough](workflow-api.md).
The prototype worker still consumes only its original two settings; enterprise
OIDC, secret-provider integration, and full telemetry remain later increments.

## Phase 2 shared settings

`nexusflow_common.config.load_settings()` explicitly loads supported process
environment variables into `WorkerSettings`. Pass a mapping to validate a supplied
configuration without reading the process environment. Importing this module does
not read settings or connect to Temporal. The prototype worker still reads only
its original two variables. The Phase 4 services consume shared settings through
the explicit worker bootstrap and reject a queue they do not own.

| Variable | Shared default | Constraint |
| --- | --- | --- |
| `NEXUSFLOW_ENVIRONMENT` | `local` | `local`, `dev`, `test`, or `prod` |
| `TEMPORAL_ADDRESS` | `localhost:7233` | Host/port, no URL scheme or embedded credentials |
| `TEMPORAL_NAMESPACE` | `default` | Nonblank namespace |
| `TEMPORAL_TASK_QUEUE` | `workflow-orchestration-tq` | Nonblank owned queue; prototype default remains `lightweight-workflows` |
| `TEMPORAL_TLS` | `false` | `true`, `false`, `1`, or `0`; required true for `prod` |
| `NEXUSFLOW_MAX_DEFINITION_STEPS` | `500` | Integer between 1 and 10,000 |
| `NEXUSFLOW_MAX_PAYLOAD_BYTES` | `1048576` | Integer between 1,024 and 16,777,216 |
| `NEXUSFLOW_SHUTDOWN_GRACE_SECONDS` | `30` | Positive integer |

These limits are development guardrails. The API enforces streamed HTTP payload
size and registration step limits. Runtime v1 has a fixed maximum of 500 steps;
the governed API rejects a larger configured limit. Independent workers use the
configured SDK graceful drain period. The prototype ignores those shared limits,
and a full runtime history/payload budget and heartbeat policy remain later work.
TLS validation does not implement authentication or credential resolution.

`nexusflow_common.errors` classifies explicitly declared validation, business,
authorization, configuration, technical, timeout, and cancellation failures.
Business/validation/authorization failures are non-retryable. Unknown exceptions
do not automatically become retryable; adapters must declare retry intent. The
classification output omits raw unclassified exception text and Pydantic input
values. Applying these policies to Temporal Activities belongs to the worker
and resilience phases.

## Decisions deferred beyond setup

| Decision | Implementation dependency |
| --- | --- |
| API framework and request/response modeling | Shared contracts and workflow API. |
| Control-plane and task persistence, migrations | Registry, execution metadata, human-task service. |
| Temporal hosting/version and connection security | Runtime deployment and platform infrastructure. |
| Identity provider, service identities, tenant/domain isolation | Authorization and security boundaries. |
| Human-task forms, evidence, delegation, SLA, retention | Human-task lifecycle and policy. |
| SLOs, capacity, RTO/RPO, payload/history limits, audit retention | Production sizing, operations, and resilience validation. |
| Alfresco inventory and compatibility scope | Migration planning and representative parity tests. |

Implementation choices are recorded in the plan and
[ADRs](../adr/0002-control-plane-and-identity.md). The table identifies the later
service dependencies; enterprise deployment values and production operating
targets still need their owners' inputs. Local defaults do not establish
production readiness.
