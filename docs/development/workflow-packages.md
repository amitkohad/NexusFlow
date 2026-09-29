# Workflow packages and generic executors

Phase 4A adds complete local deployment artifacts. A business package owns its
exact definitions, explicit Workflow and Activity registrations, runtime, and
locked dependencies. The reusable executor starts one replica of that installed
package. Shared Temporal, API, registry and database services remain separate.
Phase 5 Customer Adjustment 0.2.0 includes a durable task-creation Activity;
the [human-task service](../../apps/human-task-service/README.md) remains a
separate shared service and owns its task database/outbox.

## Build and install

```text
uv sync --locked
uv run --locked python scripts/build_workflow_packages.py --verify
```

The output directory is `dist/workflow-packages`. Each package produces a ZIP
wheelhouse and an external `.release.json` descriptor. Its artifact SHA-256 binds
the entire wheelhouse, including executable Activity code and third-party wheels.
The embedded manifest excludes the containing artifact digest. The ZIP includes
per-wheel checksums, its canonical manifest, and the exact dependency lock.
Artifacts are platform-specific; no OCI image is built in this phase.

Verification installs each package into its own temporary environment with
`--no-index`, checks manifest/registration/dependency closure, and exercises
installed code against a local Temporal server. Set `TEMPORAL_CLI_PATH` if the CLI
is not on PATH. The API, aggregate root, old workers, and other business package
are absent from those environments.

For a retained local install, verify the ZIP digest against its descriptor, expand
it into an empty directory, create a virtual environment and install from its
wheelhouse:

```text
uv venv .package-env
uv pip install --python .package-env/Scripts/python.exe --no-index --find-links release/wheelhouse nexusflow-customer-adjustment-package==0.2.0
```

On Unix use `.package-env/bin/python`. Before polling, the loader checks trusted
installed distribution entrypoints, exact dependency versions and named Temporal
registrations. Package configuration is operator-owned; task payloads never
select Python imports.

## Run replicas

Pool documents are environment configuration and are not bundled in the release
archive. Copy a repository example into operator configuration, or save the
following complete local example as `operator-pool.json` beside `.package-env`:

```json
{
  "pool_id": "customer-adjustment-mixed",
  "package_id": "customer-adjustment",
  "package_release_id": "customer-adjustment-0.2.0",
  "environment": "local",
  "temporal_namespace": "default",
  "role": "mixed",
  "worker_deployment_name": "nexusflow-customer-adjustment",
  "build_id": "customer-adjustment-0.2.0",
  "queue_bindings": {
    "workflow": "customer-adjustment-tq",
    "activities": "customer-adjustment-tq"
  },
  "replicas": 2,
  "min_replicas": 1,
  "max_replicas": 4,
  "workflow_task_slots": 10,
  "activity_task_slots": 10,
  "workflow_task_pollers": 2,
  "activity_task_pollers": 2,
  "shutdown_grace_seconds": 30
}
```

Start Temporal and the shared human-task service locally. Set
`NEXUSFLOW_HUMAN_TASK_SERVICE_URL` and its service-only
`NEXUSFLOW_HUMAN_TASK_SERVICE_TOKEN` in the executor environment, then run the
installed package using its Python and the operator pool document:

```text
.package-env/Scripts/python.exe -m workflow_executor --package customer-adjustment --pool operator-pool.json
```

On Unix use `.package-env/bin/python`. No repository checkout is needed at runtime
once the release is installed and operator configuration is supplied.

Each invocation starts one process. Run the same command again to add a replica,
choosing a different `NEXUSFLOW_PROBE_PORT` on a shared host. Each process has a
unique identity. Replicas share a stable queue and the same Worker Deployment
name and Build ID. Replica targets in the pool are desired deployment settings;
this CLI does not launch replicas or run an autoscaler.

Mixed Workflow/Activity execution is the default. Repository examples
`workflow-packages/customer-adjustment/pools/workflow.json` and
`workflow-packages/customer-adjustment/pools/activity.json` demonstrate split roles
using stable role queues from the
same artifact and deployment version. Each role retains the complete queue
mapping for destinations, while polling only its own registrations. Workflow
slots, Activity slots, poller limits and Activity rates are separate settings.

SIGINT/SIGTERM removes readiness before waiting for in-flight work to drain.
`/health` reports process liveness; `/ready` requires running SDK workers and
Temporal connectivity. Production serving pool minimum capacity is two replicas;
local single-replica development is allowed. GKE placement, controllers, scaling
and container images remain Phase 9.

Source development can run the root environment with `--development-source`.
This explicit local/test option bypasses separate distribution installation.
Installed release verification always uses the complete locked artifact.

## Govern release admission

Use API profile `NEXUSFLOW_RUNTIME_PROFILE=package` for new package starts. The
existing default `governed` profile remains available for the original runtime.
Apply Alembic migrations through `0004` before starting the updated API and
human-task service.

Operator endpoints require `packages:read`, `packages:write`, `packages:approve`
or `packages:deploy` as appropriate, with the authenticated business scope.

1. Register the manifest's exact definition documents through
   `POST /api/v1/workflows`. Approve their revisions and promote the business
   definition using the existing definition endpoints.
2. Register package ownership with `POST /api/v1/packages` (`package_id`, `name`,
   `owner`, `workflow_types`, and business scope).
3. Publish with `POST /api/v1/packages/{package_id}/releases`, carrying scope,
   `release` from the external descriptor and `manifest` from the artifact.
   Revisions must match registry hashes. Release IDs, versions and Build IDs are
   immutable. Deployment names and namespace/queue ownership cannot collide.
4. Approve using `POST /api/v1/package-releases/{release_id}/approve` with scope.
   Operators must validate replay compatibility before approving an upgrade;
   included old definitions and handlers alone cannot prove arbitrary code replay.
5. Configure capacity with `PUT /api/v1/executor-pools/{pool_id}`, carrying scope
   and a `pool` document. Read it under the release's `/executor-pools` endpoint.
   Observed readiness is server-owned and begins pending.
6. Start the configured executor processes. Promote using the release's
   `/promote` endpoint with scope, environment, namespace and `queue_bindings`.
   A non-null `ramp_percentage` selects Ramping routing. Promotion requires
   actual recent Temporal pollers on every role queue and confirms server routing.
7. `/reconcile` refreshes observed pool capacity and routing after an uncertain
   operation. A failed promotion leaves admission unconfirmed until reconciliation
   or a successful retry. Configuration never claims to deploy actual processes.
8. Start through `POST /api/v1/workflows/{workflow_type}/start` with the unchanged
   business request. The API freezes exact revision, intended release and queues
   before submitting to Temporal. Idempotent pending retries reuse that reservation.

Temporal initial Build ID is read from the first completed Workflow Task in the
original run's history. Current Build ID is reconciled separately. Business status
and history responses keep package infrastructure identifiers private.

## Version lifetime

`PackageWorkflowV1` uses Pinned behavior and an exact version override.
`PackageAutoUpgradeWorkflowV1` uses AutoUpgrade. Every Workflow and Activity queue
for a release belongs to its same modern Worker Deployment Version. Promotion
retains exact old revisions, registration contracts and stable queues.

AutoUpgrade initial eligible Current/Ramping builds are captured in the private
reservation. A later compatible code release may become Current without changing
the selected definition or initial provenance. The executor validates its installed
retained definition and complete captured Activity contracts before execution and
attests the actual loaded build to the runtime. The API accepts later observed
versions only when they are approved compatible releases. A bare payload or
signal cannot authorize code compatibility.

The `continue_execution` signal requests Continue-As-New at a completed-step
boundary. Approval/timer work finishes before continuation; results, decisions,
transitions and selected revision persist. Version policy is `inherit`. Pinned
`explicit_upgrade` is rejected before polling/admission: its exact start override
would need approved removal to allow a version move. Advanced upgrade/remediation
operations remain T044. The API follows only the original execution's run chain.

Retain old workers for pinned work, pending reservations, and retained queries.
Retirement requires Temporal drainage plus no routed, pending or nonterminal
execution needing that build. Metadata retirement never terminates worker processes.

See [migration mapping](../operations/package-migration.md) and
[Phase 4A verification](phase-4a-verification.md) for the original 0.1.0 package
and [Phase 5 verification](phase-5-verification.md) for durable task decisions.
