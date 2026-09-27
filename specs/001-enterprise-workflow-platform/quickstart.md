# Planning Quickstart

The current executable baseline is Phase 4. The accepted workflow-package topology
in [ADR 0006](../../docs/adr/0006-workflow-packages-and-executor-pools.md) is planned
work; this document keeps current checks distinct from its future acceptance path.

## Current Phase 4 validation

Follow the verified [runtime and worker walkthrough](../../docs/development/runtime-workers.md)
and [API walkthrough](../../docs/development/workflow-api.md) for exact launch commands.
They start a local Temporal server, the orchestration runtime, five capability
workers, and the governed API after applying business metadata migrations.
The Customer Adjustment low-value path completes through the API. The high-value
path waits for approval after creating a reference task and can be resumed with
the supported approval signal path. Persistent task API, claims, delegation,
business audit export, real enterprise side effects and production deployment
are later increments, not capabilities supplied by this quickstart.

Current development checks:

```text
uv sync --locked
uv run --locked python scripts/dev.py test
uv run --locked python scripts/dev.py lint
uv run --locked python scripts/build_services.py --verify
```

Real Temporal tests require the CLI on PATH or `TEMPORAL_CLI_PATH`. PostgreSQL
checks require an isolated `NEXUSFLOW_TEST_DATABASE_URL`. The current build script
verifies Phase 4 service wheels, not complete workflow-package images. Existing
runtime profiles and queues remain unchanged for their histories.

## Planned Phase 4A package acceptance

These are acceptance steps for T078–T090, not available launch commands:

1. Build one immutable Customer Adjustment package containing its pinned definition,
   runtime, complete Activity registrations, contracts and locked dependencies.
   Hash its installed content manifest canonically and publish a separate post-build
   artifact descriptor; OCI image digests are added in Phase 9.
2. Validate the manifest in a clean environment; reject omitted, extra/incompatible
   handlers, invalid bindings and dependencies before executor readiness.
3. Start shared Temporal and the API with registered package release metadata.
4. Start a generic executor configured with the installed package and its default
   mixed pool. Do not start the five capability-worker services for package runs.
5. Start through the same business API, verify trusted context and all package
   Activity calls, and preserve immutable definition/package reservation intent.
   Reconcile Temporal's confirmed initial version, including uncertain submissions;
   validate that every eligible current/ramping release supports that exact revision.
6. Add another compatible executor replica and exercise shutdown/downscale while
   work continues. Verify per-process concurrency separately from replica count.
7. Run a second package on separate queues and demonstrate independent capacity.
8. Run optional Workflow-only/Activity-only pools from the same image and release,
   where isolation warrants them; verify version-correlated Activity routing.
9. Exercise versioned old/new release coexistence, an open approval/timer, replay,
   promotion, pending start recovery and rollback. Declare and test the workflow's
   Pinned retention, Auto-Upgrade or Pinned-with-Continue-As-New upgrade policy;
   verify that a continuation changes release only when explicitly configured.
   An upgraded release must preserve the old exact definition/hash and its full
   handler/contract closure rather than silently replacing it with a newer revision.
10. Run legacy and Phase 4 executions alongside package runs without rewriting
    existing histories, registrations or queue bindings.

Persistent human-task lifecycle, real adapters, compensation and complete
observability are validated when their assigned later phases are implemented.

## Planned container and cloud validation

- Build one non-root image per workflow package and separate shared service images;
  promote the same immutable image digest across environments.
- Lint/render package and shared-service charts or selected Worker Controller
  resources, including version pools, role queues, probes, drain grace and capacity.
- Validate initial two-replica production serving-queue floors, failure-domain
  placement and measured autoscaling using pickup latency, slots and resources.
- Run Terraform formatting/validation and CI YAML/security checks for their artifacts.
- Verify the selected Temporal Server/SDK/controller combination and release routing.
- Adopt the preferred Worker Controller after support checks; when unavailable,
  verify an explicit equivalent lifecycle manager. Report desired configuration
  separately from observed serving replicas/readiness and reconciliation errors.

When an authorized GCP environment is available, provision infrastructure,
initialize/upgrade the external Temporal Cloud SQL schemas, install the shared
Temporal and control-plane services, deploy package release pools, and run smoke,
load, versioned rollback and recovery tests. Cloud availability, production sizing
and controller behavior remain unverified until that execution produces evidence.
