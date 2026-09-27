# Research Notes: Enterprise Workflow Platform

## Implemented baseline through Phase 4

The initial repository was a single `LightweightProcess` worker with a generic
demo Activity dispatcher. Phases 1–4 added locked dependencies, typed foundation
contracts, deterministic helpers, tests, governed API and persistence, and the
dedicated `GovernedWorkflowV1` runtime plus five capability-worker packages.
[ADR 0005](../../docs/adr/0005-runtime-and-worker-ownership.md) records that deployed
code model. Six service wheels and four shared libraries have independent build
checks; Phase 4 verification records 601 passing tests. Production containers,
Kubernetes, Terraform, CI/CD promotion, and persistent human-task management remain
unchecked implementation work.

Typed Activity envelopes already provide workflow/run/step/context correlation,
contract versions, and idempotency identities. That execution correlation does
not create a complete business-workflow release: the current runtime package
does not include its separately built executable Activity implementations.

## Accepted package architecture

On 2026-09-27 the user selected the deployment model in
[ADR 0006](../../docs/adr/0006-workflow-packages-and-executor-pools.md): one immutable
business workflow package contains its definition/code, runtime and all dependent
executable Activities. A generic executor loads the installed manifest and named
handlers before polling. Default pools execute both task types; optional role
pools retain the same image/release. Package-owned logical queues, release-aware
pool coexistence, and separate replica/concurrency controls replace mandatory
worker-per-capability production boundaries.

The immutable installed content manifest has a canonical hash; the separate
post-build release descriptor binds that hash to a wheel/package artifact digest
and the Phase 9 image digest. Environment queue/namespace resolution and desired
capacity are mutable audited configuration. Start reservations record intended
eligible release routing; Temporal's confirmed initial version is reconciled
separately, rather than assuming a database transaction atomically selects it.
Compatible code upgrades must retain support for the execution's exact governed
definition revision/hash and its complete handler/contract closure.

The API, registry, business PostgreSQL and persistent human-task service remain
shared control-plane services. Temporal remains self-hosted on GKE with external
Cloud SQL `temporal` and `temporal_visibility`; the hosting decision is unchanged.
Activity handlers in packages call shared services through explicit API contracts.

This is accepted design and future work, not a claim that the current Phase 4 code
implements packages. Phase 4A introduces the migration and tests; subsequent
service, container, platform and release tasks use the package boundary.

## Temporal guidance checked for this decision

| Guidance | Planning implication |
| --- | --- |
| [Worker deployment and performance](https://docs.temporal.io/best-practices/worker) | CI-built artifacts with injected configuration; logical workload isolation; initial two-worker queue floor; tune capacity using pickup latency, task slots and resources. |
| [Task Queues](https://docs.temporal.io/task-queue) | Queues are logical routing resources; compatible replicas register the handlers their queues serve. |
| [Worker Versioning](https://docs.temporal.io/production-deployment/worker-deployments/worker-versioning) | Use immutable Build IDs, coexistence/ramping, and declared Pinned or compatible upgrade behavior. |
| [Activity version behavior](https://docs.temporal.io/worker-versioning) | Keep package Activity pools in the same Worker Deployment Version for correlated executable releases. |
| [Safe deployments](https://docs.temporal.io/develop/safe-deployments) | Replay and compatibility validation protect histories; definition pinning alone is insufficient. |
| [Kubernetes Worker Controller](https://docs.temporal.io/production-deployment/worker-deployments/kubernetes-controller) | Preferred GKE lifecycle/scaling manager after verified Server/SDK/controller support; an explicit equivalent alternative is permitted when compatibility is unavailable. |

The per-workflow package boundary is a NexusFlow product decision compatible with
this guidance, not a packaging requirement imposed by Temporal. Initial replica
floors are not measured throughput guarantees. Long-lived pinned workflows can
require old release pools for months; upgrade/retention policy must be explicit.

## Spec Kit alignment

Constitution 1.1.0 captures package/release ownership without relaxing Workflow
determinism or external I/O isolation. The specification describes independently
testable outcomes; the plan describes target topology and coexistence; tasks retain
completed Phase 4 evidence and add unchecked Phase 4A tasks T078–T090 before Phase 5.
Planning artifacts and ADR updates do not authorize implementation code changes.

## Remaining inputs and risks

- Real downstream idempotency contracts are still needed for safe side-effect retries.
- Human-task identity, delegation, evidence, SLA and retention remain unspecified.
- Production SLOs, load, history/payload budgets, RTO/RPO and data classification need owners and measured acceptance evidence.
- The pinned development SDK/CLI do not select a production Server or prove Worker Versioning/controller interoperability.
- Package pool separation must account for different resource and permission needs while preserving one executable release.
- Legacy history and pending reservation inventories are required before retiring current queues or worker services.
- Alfresco inventory and unsupported process constructs remain unknown.
