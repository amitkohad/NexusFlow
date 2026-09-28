# Phase 4A verification

Date: 2026-09-27. Branch:
`codex/phase-4a-workflow-packages-generic-executors`, based on `feature/develop`
at `ed484f7`. The requested `feature/devlop` spelling did not exist locally or
remotely; the existing development branch was used.

## Delivered behavior

T078–T090 add immutable business workflow packages and a reusable executor.
Customer Adjustment contains its exact governed definition and every required
named Activity. The validation reference fixture demonstrates an independent
package and queue. Each local release archive includes the runtime, executor,
Activity library, contracts, deterministic helpers and all exact dependency
wheels. Its external descriptor hashes the whole containing artifact.

The executor supports mixed and split Workflow/Activity pools using the same
Worker Deployment Version. Replicas have distinct identities; task slots,
pollers, rate limits and graceful shutdown are independent capacity controls.
Installed content is verified before polling. The runtime keeps exact definitions
and Activity bindings, with Pinned or AutoUpgrade code routing and inherited
completed-step continuation. Incompatible live moves fail before new effects
while replay preserves historical commands.

Privileged API operations register, publish, approve, promote/ramp, reconcile,
configure pools and retire releases. New package-profile starts reserve immutable
intended provenance before submission, reconcile initial Build ID from historical
Workflow Task completion and track current Build ID separately. Actual observed
pool identities and Temporal queue membership substantiate readiness; desired
configuration never asserts that workers were launched.

Migration `0003` preserves existing runtime profiles/queues and refuses to remove
package provenance while execution records exist. Global deployment/build and
namespace/queue claims prevent conflicting owners. Package reservations and
retirement serialize through database locks. SQLite local sessions/readiness
share a reentrant engine lock; production PostgreSQL retains concurrent sessions.

## Executed checks

Environment: Windows, Python 3.12.2, Temporal Python SDK 1.33.0, Temporal CLI
1.9.1 / Server 1.32.0, and isolated PostgreSQL 16.

| Check | Result |
| --- | --- |
| Final full pytest including real Temporal and PostgreSQL | 802 passed in 157.61 seconds; one upstream deprecation warning |
| Original Phase 1–4 runtime/catalogue code | Unchanged |
| Ruff lint/format and mypy | Passed |
| Definition and manifest schema drift | Passed |
| Locked dependency sync/check and hashed runtime export | Passed |
| Complete package archives, clean offline install and isolated execution | Passed for both packages; two generic replicas each |
| Six original service wheels and independent clean installs | Passed |
| Aggregate wheel and source distribution | Passed; migration and new package modules included |

Verified release artifacts in `dist/workflow-packages/`:

| Package | Artifact | SHA-256 |
| --- | --- | --- |
| Customer Adjustment | `customer-adjustment-0.1.0-0e7241e7bf7a.zip` | `0e7241e7bf7abe25af60763af6695a474a46830037be49e3004b34fcc361fb15` |
| Validation Reference | `validation-reference-0.1.0-8f893dc91293.zip` | `8f893dc91293a9d73aa0f7239a76bf64bcde80b5b3baa580d8d69653b610e464` |

Each archive has a matching external `.release.json` descriptor. These are local
Windows/Python 3.12 artifacts; production images remain pending.

Real package checks include approval/rejection through all six Activities,
transient technical retry on attempt 2, nonretryable business failure, Activity
timeout, retry-stable idempotency, timer/approval cancellation, multi-replica
consumption, per-process slots, readiness removal and in-flight drain, split-role
version correlation, Current/Ramping routing and ramp clearing, pinned old/new
coexistence, compatible AutoUpgrade retaining the selected definition, rejection
of unadmitted initial routing and missing retained revisions, safe continuation,
worker restart and offline replay.

HTTP acceptance verifies actual two-replica pollers and private observed
provenance. Enhanced Temporal queue descriptions aggregate all partitions; default
queue descriptions cannot substantiate total replica counts. Unit checks dedupe
identities, exclude stale/wrong-build pollers and attribute observations to their
correct pool. Recovery tests cover uncertain starts after promotion and after
Continue-As-New, bounded continuation races, chain reuse rejection and sanitized
description errors.

Migration checks cover existing legacy/governed v1 bindings, blocked unsafe
downgrade, exact package provenance and three real PostgreSQL races: exclusive
queue owner, exclusive deployment owner, and reservation versus retirement.
Four SQLite stress cases cover file/memory stores and legacy/package profiles:
256 duplicate start requests and 96 concurrent readiness probes retain one
execution per key and unchanged bindings.

The suite retains one upstream Starlette TestClient/httpx deprecation warning.
No cloud deployment or package publication was performed. Build/test logs and
artifacts remain in ignored `dist/`; changes remain uncommitted for review.

## Scope boundaries

Integration, notification, task creation and business operations remain explicit
reference adapters. Persisted human-task lifecycle is Phase 5; downstream durable
deduplication, compensation, history budgets and advanced upgrade/remediation are
Phase 6. Pinned explicit Continue-As-New upgrades are rejected until approved
override removal is implemented in T044. Retained-query lifecycle still requires
operator retention of compatible workers.

OCI digests/images, Worker Controller lifecycle, GKE placement/autoscaling,
enterprise identity/telemetry and production server compatibility evidence remain
their later phases. Local desired replica/resource settings are not evidence of
Kubernetes deployment or measured production capacity.

See [local package setup](workflow-packages.md) and
[migration mapping](../operations/package-migration.md).
