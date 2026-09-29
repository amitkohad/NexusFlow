# Phase 5 verification

Date: 2026-09-28. Branch: `codex/phase-5-human-task-management`, created from
`feature/develop` at `951fcfdd292086870be4ca17d43012e552ad5662`.

## Delivered behavior

T032–T038 add a standalone shared human-task API with tenant/domain/application
scope, task creation and inbox, claim, approval/rejection, reassignment,
delegation, escalation and expiry. PostgreSQL migration `0004` adds task,
lifecycle-audit and signal-outbox tables. Row locks and unique keys arbitrate
concurrent creation/decisions. Terminal decisions commit one outbox event in the
same transaction; uncertain Temporal delivery is retried without reopening the
task. A definitive execution-chain mismatch blocks and audits the old event.

The Customer Adjustment 0.2.0 package owns the task-creation Activity and uses
the shared task service over its authenticated API. Generic executors still load
the whole package; the task service remains a separate deployment. The Activity
uses the first Temporal execution run ID in its retry-stable task key, and the
service retains original release provenance through compatible retries and
Continue-As-New. The dispatcher Describes the current workflow ID owner,
checks the first-run chain, and signals the exact current run. The package
Workflow validates task ID, task key and event ID, buffers a signal that arrives
before the create Activity returns, and accepts only the first terminal decision.
The task service alone decides expiry; a committed approval can resume after a
delivery outage crosses its deadline. A definitively closed or missing Workflow
parks a late outbox signal with an audited `workflow_closed` or
`workflow_missing` reason.
Direct approval through the governed workflow API is rejected for this package.

The new manifest mode is additive: it is omitted from canonical 0.1.0 hashes
when false, preserving historical release digests and replay behavior. The
validation-reference 0.2.0 fixture keeps the reference adapter. Old legacy,
governed-v1 and package 0.1.0 histories retain their original approval route.

## Executed checks

Environment: Windows, Python 3.12.2, Temporal Python SDK 1.33.0, Temporal CLI
1.9.1 / Server 1.32.0, and isolated PostgreSQL 16.

| Check | Result |
| --- | --- |
| Full pytest suite with real Temporal and PostgreSQL | 869 passed in 186.44 seconds; one upstream Starlette/httpx deprecation warning |
| Focused task/API/adapter/runtime/Temporal checks | 93 passed, including 7 real Temporal task flows |
| PostgreSQL task migration/concurrency | Passed, including concurrent create and opposing decisions |
| Ruff lint/format, mypy, definition/package schema drift | Passed |
| Locked dependency resolution | `uv lock --check` passed |
| Independent service wheel builds/clean installs | Passed for the new task service and six original services |
| Complete workflow packages, clean offline install, real Temporal execution | Both 0.2.0 packages passed with two generic replicas |
| Aggregate wheel and source distribution | Passed; wheel includes task service, task contracts and migration `0004` |

Verified local artifacts in `dist/workflow-packages/`:

| Package | Archive | Artifact SHA-256 |
| --- | --- | --- |
| Customer Adjustment | `customer-adjustment-0.2.0-c8f9e52445cd.zip` | `c8f9e52445cd7620f580bb7f24100189346c05dfc534293015f70d5ff82b40a0` |
| Validation Reference | `validation-reference-0.2.0-3134fde86472.zip` | `3134fde864724673293d98630bf410db4512ae1094afa9013a84e1d1216979e4` |

Each archive has an external immutable `.release.json` descriptor. The local
build logs are `dist/phase-5-services-build.log`,
`dist/phase-5-package-build.log` and `dist/phase-5-full-tests.log`.

Real Temporal cases cover approval/rejection and one terminal transition,
duplicate and conflicting signals, a signal before task-creation Activity
completion, SLA escalation and authoritative expiry, a committed approval
delivered after its deadline, an open task across executor restart and compatible
release rollout, and task correlation after Workflow ID reuse. Cancellation and
termination and missing-ID tests verify outbox classification. The reused-ID case
simulates an old signal accepted
before outbox acknowledgement; its delayed outbox event is blocked with
`DISPATCH_BLOCKED` audit while the new chain's task remains waiting and then
completes from its own approval. PostgreSQL and contract tests cover actor
scope, evidence, idempotent completion, lease/retry, SLA extension and races.

## Remaining scope

The standalone environment factory supplies separate local/test bearer tokens.
Production must inject a verified identity provider and run the same outbox/SLA
loop; enterprise identity, web inbox and broader audit/observability are later
phases. Package images, Kubernetes controller/autoscaling and cloud deployment
remain Phase 9. Side-effecting enterprise adapters, compensation and advanced
long-running upgrade/remediation are Phase 6 or later. Workflow cancellation
currently leaves an open inbox task for policy cleanup in Phase 6. No cloud
deployment or
package publication was performed. Changes remain uncommitted for review.

See [local setup](../../apps/human-task-service/README.md),
[package execution](workflow-packages.md) and the
[quickstart](../../specs/001-enterprise-workflow-platform/quickstart.md).
