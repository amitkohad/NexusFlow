# Phase 4 verification

Date: 2026-09-27. Branch: `codex/phase-4-runtime-independent-workers`, based on
`feature/develop` at `764e19e777eba2870865447838e1b0ab62df93ce`.

T022–T031 are complete. `GovernedWorkflowV1` runs on the dedicated orchestration
queue, with typed named Activities on five independently owned worker queues.
The governed API selects it by default. Shared deterministic helpers implement
template, routing, and transition semantics. The legacy workflow and its baseline
remain available at their original paths for compatible histories and demos.

## Executable results

Python 3.12.2 on Windows, Temporal SDK 1.33.0, Temporal CLI 1.9.1/server 1.32.0,
and PostgreSQL 16 in an isolated temporary test container. Dependency constraints
remain pinned by the committed `uv.lock` and runtime requirements.

| Check | Result |
| --- | --- |
| Full pytest suite | 601 passed |
| Existing Phase 1–3 baseline | 462 passed |
| New worker Activity contracts | 59 passed |
| New deterministic runtime unit tests | 20 passed |
| Worker bootstrap/settings/probe unit tests | 34 passed |
| Runtime transport/registration unit tests | 8 passed |
| Runtime binding/migration/recovery tests | 4 passed |
| New real Temporal/runtime/API scenarios | 13 passed |
| Real Temporal bootstrap shutdown/drain scenario | 1 passed |
| PostgreSQL migration/concurrency cases included in full suite | 6 passed |
| Ruff lint/format, mypy, generated schema drift | Passed |
| Locked dependency check/sync | Passed |
| Aggregate wheel and source build | Passed |
| Six independent service wheels/source distributions | Passed |
| Four independently built shared libraries | Passed |
| Six clean-environment service install/import checks | Passed |

The suite has one upstream Starlette TestClient/httpx deprecation warning.
The warning does not affect its passing results.

The high-value sample completes through validation, sample-business, human-task,
integration, and notification workers. Actual Temporal scheduled Activity events
prove their owned queues and versioned names; decoded typed payloads prove trusted
context propagation. Tests also cover approval/rejection/expiry, low-value template
execution, transient retry, nonretryable validation, Activity timeout, timer
cancellation, notification outage with independent validation still available,
runtime restart during approval, replay, and definition version pinning.

The bootstrap shutdown test drops readiness while keeping liveness, drains an
in-flight typed Activity, closes probes, and restores signal handlers. Migration
tests verify legacy data backfill, schema-aware readiness, downgrade/re-upgrade,
and pending start identity/profile/queue recovery across rollout and rollback.

`scripts/build_services.py --verify` builds service/shared artifacts from source
distributions and checks each service in its own temporary virtual environment
under the locked runtime constraints. The API, legacy `app`, and other workers
are absent. Artifacts are local under `dist/services`; no packages were published.
The root development distribution also includes API migrations. The temporary
PostgreSQL test container was removed after validation; no cloud deployment ran.

## Scope boundaries

Integration, notification, and task creation use explicit reference/mock
implementations. Human tasks currently have a stable reference for the runtime
wait; persistence, assignment, evidence, and exactly-once task completion remain
Phase 5. Compensation/remediation, downstream side-effect idempotency, full
payload/history budgets and production rollout/versioning remain Phase 6.
Enterprise identity/telemetry and independent containers/GKE remain Phases 8–9.

Migration `0002` backfills the documented Phase 3 default legacy queue. Deployments
using a custom legacy queue must map pending reservations to that prior queue
before cutover, since Phase 3 did not persist it. Version 1 queue/contract mappings
must remain immutable for replay; incompatible runtime changes need another
workflow type/version. These are compatible local restart tests, not production
rollout or cloud availability measurements.
