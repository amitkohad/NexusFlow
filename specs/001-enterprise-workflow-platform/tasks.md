# Tasks: Enterprise Workflow Platform

**Input**: Design documents from `specs/001-enterprise-workflow-platform/`
**Prerequisites**: [spec.md](spec.md), [plan.md](plan.md), [research.md](research.md), [data-model.md](data-model.md), [contracts/api.md](contracts/api.md), [ADR 0006](../../docs/adr/0006-workflow-packages-and-executor-pools.md)

**Architecture revision (2026-09-27)**: Target delivery now uses complete workflow packages and generic executor pools. Phases 1–4 and their checked tasks retain historical implementation evidence. Phase 4A (T078–T090) has its own implementation and verification checkpoint below; it is not covered by the original 601-test result. Later unchecked tasks consume the new interfaces. Task IDs T001–T077 are not renumbered.

## Task Format

`[ID] [P?] [Story] Description`

- `[P]` means the task can run in parallel with other tasks in the same phase when file ownership permits.
- Story labels map work to the independently testable user stories in `spec.md`.
- Every task names an expected path or artifact. These are planning paths; implementation must confirm the final structure before creating code.

## Phase 1: Setup

**Purpose**: Establish project conventions and record the current system before behavior changes.

- [x] T001 [P] Create `docs/refactoring-assessment.md` mapping current files and behavior to target services.
- [x] T002 [P] Add `pyproject.toml`, dependency pinning/lock strategy, formatter, linter, type-checker, and test configuration.
- [x] T003 [P] Add `.env.example`, configuration conventions, and secret-handling rules under `docs/development/`.
- [x] T004 [P] Add initial `Makefile` targets for `dev`, `test`, `lint`, `build`, and `docker-build`.
- [x] T005 [P] Add test directories for `tests/unit`, `tests/integration`, `tests/contract`, and `tests/e2e`.
- [x] T006 Add baseline tests for the existing `LightweightProcess` behavior in `tests/integration/test_lightweight_process.py`.

**Setup verification (2026-09-26)**: Locked dependency sync/check passed on Python
3.12.2/Windows with Temporal SDK 1.33.0 and CLI 1.9.1. The cross-platform runner
passed 9 tests (3 validator cases and 6 real Temporal scenarios), Ruff lint/format,
mypy, and wheel/source builds. Application edits are formatting only. GNU Make
is unavailable on this host; equivalent runner commands were executed directly.
The initial `docker-build` target explicitly exits with a deferred-packaging
message; Dockerfiles and image validation remain T056/T065.

## Phase 2: Foundational Contracts

**Purpose**: Blocking foundation for all user stories.

- [x] T007 [P] Define typed domain contracts in `libs/contracts/src/contracts/` for definitions, executions, tasks, Activities, errors, context, and audit events.
- [x] T008 [P] Add workflow definition JSON Schema under `workflows/definitions/schema/`.
- [x] T009 Implement definition parsing and graph validation in `libs/workflow-sdk/src/workflow_sdk/definitions/`.
- [x] T010 Implement deterministic path, decision, transition, and template semantics in `workflows/common/`.
- [x] T011 [P] Define retry, timeout, heartbeat, compensation, cancellation, idempotency, and failure policy models in `libs/contracts/`.
- [x] T012 [P] Add shared configuration and error taxonomy under `libs/common/`.
- [x] T013 Add unit tests for schema, graph validation, routing, path resolution, and error classification under `tests/unit/`.
- [x] T014 Record selected API framework, persistence, identity, Temporal hosting, and NFR decisions in `specs/001-enterprise-workflow-platform/plan.md` and ADRs.

**Foundation verification (2026-09-26)**: Implemented on
`codex/phase-2-foundational-contracts`, created from `feature/develop` at
`bc47f210ed74034e6e651882d8c714a6191a334e`. The full suite passed 261 tests,
including the existing Temporal baseline. Ruff lint/format, mypy, generated-schema
drift checks, locked dependency checks, wheel/source builds, archive inspection,
and wheel-only foundation imports/validation passed. The existing sample validates
as schema 1.0 with 10 steps. The legacy application and its queue were not changed.
ADRs record implementation direction and development guardrails; enterprise
OIDC deployment values and measured production SLO/recovery/retention targets
remain owner inputs for later service/deployment acceptance.

**Checkpoint**: Definitions are typed and validated; baseline behavior remains covered.

## Phase 3: User Story 1 - Governed Workflow API (P1)

**Goal**: Start and observe a versioned workflow without direct Temporal access.

**Independent Test**: Register/promote the sample definition, start through the API with an idempotency key, query status, and retrieve business history.

- [x] T015 [P] [US1] Define API request/response schemas in `specs/001-enterprise-workflow-platform/contracts/api.md` and `libs/contracts/`.
- [x] T016 [US1] Implement definition registry repository and lifecycle model under `apps/workflow-api/src/`.
- [x] T017 [US1] Implement execution metadata repository linking business executions to Temporal IDs under `apps/workflow-api/src/`.
- [x] T018 [US1] Implement versioned workflow endpoints in `apps/workflow-api/src/`.
- [x] T019 [US1] Add idempotent start, correlation, business reference, pagination, and problem responses.
- [x] T020 [P] [US1] Add API contract tests in `tests/contract/test_workflow_api.py`.
- [x] T021 [P] [US1] Add registry and API unit tests in `tests/unit/`.

**API verification (2026-09-27)**: Implemented on
`codex/phase-3-governed-workflow-api`, created from `feature/develop` at
`c3ade4cf9380299eaaec7f8f638576d319707996`. The full suite passed 462 tests,
including 49 HTTP contract tests, 6 real PostgreSQL migration/concurrency checks,
5 real Temporal API scenarios, and the preserved prototype baseline. Ruff
lint/format, mypy, schema drift, dependency lock/sync, wheel/source builds,
archive inspection, wheel-only API imports and packaged migration upgrade passed.
The API uses the existing worker through an adapter and rejects unsupported
runtime features. Enterprise OIDC, independent workers, full human-task policy,
and complete audit export remain in their assigned later phases. See
[verification](../../docs/development/phase-3-verification.md) and
[local walkthrough](../../docs/development/workflow-api.md).

**Checkpoint**: A client can start, query, and inspect a governed workflow through `/api/v1`.

## Phase 4: User Story 2 - Original Workflow Runtime and Independent Workers (P1)

**Historical scope**: T022–T031 are complete for the original capability-service design recorded in ADR 0005. They do not mark the revised workflow-package acceptance criteria complete. The following goal, tasks and verification record describe that delivered implementation.

**Goal**: Execute the sample flow across independently deployable workers.

**Independent Test**: Run the sample workflow and verify each Activity is consumed from its owned task queue.

- [x] T022 [US2] Create `apps/workflow-runtime/` with dedicated worker bootstrap and `workflow-orchestration-tq` registration.
- [x] T023 [US2] Move the deterministic runtime contract from `app/workflows.py` into `workflows/common/` and runtime-owned modules.
- [x] T024 [P] [US2] Create `workers/validation-worker/` with source, tests, config, README, dependency metadata, and queue registration.
- [x] T025 [P] [US2] Create `workers/notification-worker/` with source, tests, config, README, dependency metadata, and queue registration.
- [x] T026 [P] [US2] Create `workers/integration-worker/` with source, tests, config, README, dependency metadata, and queue registration.
- [x] T027 [P] [US2] Create `workers/human-task-worker/` with source, tests, config, README, dependency metadata, and queue registration.
- [x] T028 [P] [US2] Create `workers/sample-business-worker/` for reference-only business capabilities.
- [x] T029 [US2] Replace the production path of `execute_capability` with typed Activity contracts and queue routing.
- [x] T030 [P] [US2] Add worker Activity contract tests under `tests/contract/workers/`.
- [x] T031 [US2] Add Temporal runtime tests for deterministic routing, timers, retry, timeout, cancellation, and version compatibility.

**Runtime verification (2026-09-27)**: Implemented on
`codex/phase-4-runtime-independent-workers`, created from `feature/develop` at
`764e19e777eba2870865447838e1b0ab62df93ce`. The full suite passed 601 tests,
including 139 new Phase 4 cases and the 462-test existing baseline. Real Temporal
tests verify all five capability queues, typed context, retry/error/timeout
behavior, approval, cancellation, worker outage isolation, restart/replay, API
version pinning, and graceful shutdown. PostgreSQL migration/concurrency checks,
Ruff lint/format, mypy, schema drift and lock checks passed. Six service wheels
and four shared libraries build independently; each service installs and imports
in a clean environment without the API or other workers. Migration `0002` pins
runtime profile/queue for pending start recovery. The legacy runtime remains
unchanged for old histories; persistent tasks, compensation, enterprise adapters,
and cloud packaging remain their later increments. See
[verification](../../docs/development/phase-4-verification.md) and
[walkthrough](../../docs/development/runtime-workers.md).

**Checkpoint**: Workers are separate services and the sample flow no longer depends on one generic dispatcher.

## Phase 4A: User Story 2 - Workflow Packages and Generic Executors (P1)

**Goal**: Build each business workflow with all dependent executable Activities as one immutable package, load it through generic executors, and bind execution to approved release/queue/version policy.

**Independent Test**: Build/install the customer-adjustment package in a clean environment without the aggregate root or other worker services. Run its Workflow and every dependency through multiple generic executor replicas. Run a second minimal package from `tests/fixtures/workflow-packages/validation-reference/`, verify isolation and role-pool routing, drain replicas, and exercise compatible release rollover during an approval/timer. Preserve replay and pending-start recovery for existing legacy and governed v1 executions.

- [x] T078 [US2] Add `WorkflowPackage`, `WorkflowRelease`, `ExecutorPool`, manifest/handler/queue bindings and explicit version-policy contracts in `libs/contracts/src/contracts/packages.py`; publish a versioned manifest schema under `workflow-packages/schema/`. Separate the embedded canonical content manifest from a post-build immutable release descriptor binding its hash to artifact/image digests, without self-digest fields; distinguish immutable provenance from environment capacity and observed executing version. Declare exact supported definition revisions/closure, trusted installed entrypoints, dependencies, contracts and secret references.
- [x] T079 [US2] Implement manifest, dependency-closure, schema/hash and registration validation under `libs/workflow-sdk/src/workflow_sdk/packages/`; reject absent/incompatible handlers, mutable dependencies, unapproved entrypoints, incomplete Activity ownership and conflicting queue/version bindings before polling or promotion. Add focused contract/unit cases under `tests/contract/packages/` and `tests/unit/`.
- [x] T080 [P] [US2] Extract a reusable deterministic interpreter into `libs/workflow-sdk/src/workflow_sdk/runtime/` using `workflows/common/`; introduce a new package runtime type/profile with immutable release-resolved Activity bindings. Preserve `LightweightProcess`, `GovernedWorkflowV1`, their version 1 catalogue and command histories; no Workflow I/O or mutable deployment lookups.
- [x] T081 [P] [US2] Extract named typed validation, notification, integration, human-task creation and sample-business Activity implementations into reusable libraries under `libs/activities/`. Package-local registrations must carry trusted context, contract/release identity and retry-stable idempotency; retain reference/mock semantics and original services for history compatibility. Do not reinstate `execute_capability` or require external capability workers.
- [x] T082 [US2] Create `workflow-packages/customer-adjustment/` and a minimal second package fixture under `tests/fixtures/workflow-packages/validation-reference/`; include exact definition revision/code, any retained compatible revisions, runtime, all Activity handlers/contracts, content manifest, dependency lock and package metadata. Compute an external immutable release descriptor after artifact build; record the local artifact digest now and add the image digest in Phase 9. Extend `scripts/build_services.py` or a dedicated package build helper for isolated artifact/closure verification; Docker images remain T056/T065.
- [x] T083 [US2] Implement `apps/workflow-executor/` as one generic host that loads a trusted installed package manifest and registers explicit Workflow/Activity handlers. Support default mixed and optional Workflow-only/Activity-only roles from the same release artifact; validate owned queues and compatible registrations, preserve probes/draining and use unique instance identities with package/build/pool context.
- [x] T084 [US2] Extend validated executor capacity/configuration under `libs/common/` and `apps/workflow-executor/` for distinct Workflow/Activity slots, supported poller settings, downstream rate limits and shutdown grace. Define desired replicas/min/max/resources and role-specific settings in `ExecutorPool` configuration; local replica runs do not claim a Kubernetes autoscaler, which remains T063/T064.
- [x] T085 [US2] Extend `apps/workflow-api/`, repository migrations under `migrations/versions/`, and package control-plane contracts for immutable package/release registration, approved environment binding and executor desired/observed state. Resolve a compatible serving release before start and atomically freeze intended provenance/eligible routing policy in the database reservation. Reconcile the actual Temporal initial version using supported primitives without changing Pinned/Auto-Upgrade behavior or claiming a cross-system atomic commit; preserve idempotent recovery by stable identity. Keep desired versus observed reconciliation states separate, restrict deployment operations to privileged actors, and hide infrastructure handles in business responses.
- [x] T086 [US2] Connect package runtime and named Activity scheduling to approved per-package/per-role queue bindings in `libs/workflow-sdk/`, `apps/workflow-executor/` and API runtime transport. Record replay inputs without mutating current global v1 routes; all package Activity queues must be members of the same Temporal Worker Deployment Version.
- [x] T087 [US2] Implement modern Worker Versioning registration/routing and per-Workflow Pinned/Auto-Upgrade policy across executor and API adapters. Verify same-release Activity routing and local side-by-side versions; require replay compatibility plus included exact selected revision/handler closure for moves, retain old versions for assigned work, and define explicit Continue-As-New upgrade/retained-query behavior. Kubernetes rainbow lifecycle/controller rollout remains T062–T065 and advanced history/remediation policy remains T044.
- [x] T088 [US2] Add installed-package contract and real Temporal tests under `tests/contract/packages/`, `tests/integration/` and `tests/e2e/` proving complete dependency closure, mixed/role pools, two-package independence, multiple-replica consumption, configured slot limits, retry/timeout/cancellation, replica draining, version-aware Activities, open approval/timer rollover and trusted release correlation.
- [x] T089 [US2] Add migration/recovery/replay tests under `tests/integration/` and `tests/unit/` and an operator mapping plan in `docs/operations/package-migration.md`. Verify pending reservations keep their original release/queue, existing legacy/governed v1 histories never rebind, new package rollback affects eligible starts only, and compatible running-version moves preserve pinned definition and task identities.
- [x] T090 [US2] Update package/executor local setup and verification documents under `docs/development/`, `specs/001-enterprise-workflow-platform/quickstart.md` and package READMEs after implementation; record executed clean-install, real Temporal and migration checks. Keep production controller/autoscaling/cloud evidence explicitly pending until Phases 9–10.

**Acceptance gate**: US2's revised package/executor behavior is complete only after T078–T090 pass. The original Phase 4 result stays valid for its original scope. A documentation amendment alone does not complete any of these tasks.

**Phase 4A verification (2026-09-27)**: Implemented on
`codex/phase-4a-workflow-packages-generic-executors`, based on `feature/develop`
at `ed484f7`. The full suite passed 802 tests, including real Temporal execution
and isolated PostgreSQL migration/concurrency checks. Both complete package
archives passed clean offline installation and execution through two generic
replicas; all six Customer Adjustment Activities ran. Ruff, mypy, schema drift,
dependency lock checks and aggregate/service builds passed. Original legacy and
governed Workflow code/catalogue remain unchanged. See
[verification evidence](../../docs/development/phase-4a-verification.md),
[local setup](../../docs/development/workflow-packages.md) and
[migration mapping](../../docs/operations/package-migration.md). Images, controller
lifecycle, cloud autoscaling and advanced upgrade/remediation remain later phases.

## Phase 5: User Story 3 - Human Task Management (P1)

**Goal**: Manage human approval through a governed task API.

**Independent Test**: Create a task from the sample workflow, claim/approve or reject it, and verify exactly-once workflow resumption.

- [ ] T032 [P] [US3] Define HumanTask state transitions and persistence schema in `apps/human-task-service/`.
- [ ] T033 [US3] Implement task list/get/create/claim/complete/approve/reject endpoints.
- [ ] T034 [US3] Implement reassign/delegate/escalate/expire policy operations.
- [ ] T035 [US3] Implement durable task-service adapter Activities under `libs/activities/` and include them in dependent workflow packages; use each package's approved queue/version binding rather than a mandatory global `human-task-tq` worker.
- [ ] T036 [US3] Add authenticated actor, authorization, duplicate completion, evidence, SLA, and escalation handling.
- [ ] T037 [P] [US3] Add task contract tests in `tests/contract/test_human_tasks.py`.
- [ ] T038 [US3] Add package-executor Temporal tests for long waits, approval, rejection, timeout, escalation, duplicate signals and task correlation across compatible release changes.

**Checkpoint**: Human approval can remain open for days and resume the workflow safely.

## Phase 6: User Story 4 - Failure Handling and Operations (P1)

**Goal**: Make retries, timeouts, compensation, remediation, and long-running deployment safe.

**Independent Test**: Exercise retryable, non-retryable, timeout, compensation, cancellation, and manual resume scenarios.

- [ ] T039 [P] [US4] Implement retry/error policy evaluators in `libs/workflow-sdk/`.
- [ ] T040 [US4] Add idempotency and downstream correlation interfaces for side-effecting Activities.
- [ ] T041 [US4] Add compensation and remediation state handling to the reusable package runtime in `libs/workflow-sdk/`, with package-local compensating Activities and preserved historical runtimes.
- [ ] T042 [US4] Add operator-facing failure, retry, resume, and termination API operations.
- [ ] T043 [P] [US4] Add failure/resilience tests under `tests/integration/`.
- [ ] T044 [US4] Implement/verify advanced long-running history and upgrade policies in the package runtime and document them in `docs/operations/`: Pinned release retention, Auto-Upgrade patching/replay, explicit Continue-As-New upgrade boundaries, signal/task compatibility, rollback limits and retained-query support. Extend the initial T087 versioning foundation without assuming every wait can retire its old version.

## Phase 7: User Story 5 - Definition and Release Governance (P2)

**Goal**: Safely validate, approve, promote, deprecate and version definitions and their compatible immutable package releases.

**Independent Test**: Promote two versions and confirm running executions stay bound to their resolved version.

- [ ] T045 [US5] Extend definition and package-release lifecycle transitions, approvals, promotion metadata, immutable revisions and environment bindings under `apps/workflow-api/`; definition authoring remains independent while promotion verifies an approved serving executable release.
- [ ] T046 [US5] Enforce capability/entrypoint allow-lists, package-local handler/contract closure, manifest/content hashes and compatible runtime/release dependencies in `libs/workflow-sdk/` and `apps/workflow-api/`.
- [ ] T047 [US5] Add definition/release version compatibility and concurrent promotion tests under `tests/unit/` and `tests/integration/`; starts and pending retries keep their original bindings while only approved compatible code-version transitions may advance running execution.
- [ ] T048 [US5] Complete privileged definition/package/release/pool administration endpoints and OpenAPI documentation under `apps/workflow-api/`; separate desired configuration from confirmed serving state and preserve the business API's infrastructure abstraction.

## Phase 8: User Story 6 - Security, Audit, and Observability (P2)

**Goal**: Secure and correlate enterprise operations.

**Independent Test**: Execute authorized and unauthorized operations and inspect audit, logs, metrics, and traces.

- [ ] T049 [P] [US6] Add OIDC/OAuth2 and service-identity middleware under `libs/security/`.
- [ ] T050 [P] [US6] Add RBAC/ABAC policy interfaces and tenant/domain/application context.
- [ ] T051 [P] [US6] Add Secret Manager and Workload Identity configuration interfaces.
- [ ] T052 [US6] Implement business audit event persistence/export under `libs/observability/` and control-plane services.
- [ ] T053 [US6] Add structured logging and OpenTelemetry to API, package runtime/executor, task service and Activity libraries, including package/release/build/pool/role/unique-instance identifiers alongside existing business correlation.
- [ ] T054 [US6] Add `/health` and `/ready` endpoints and telemetry contract tests.
- [ ] T055 [US6] Document retention, redaction, data classification, dashboards, alerts, and SLO assumptions.

## Phase 9: User Story 7 - Containers, GKE, and Cloud SQL (P2)

**Goal**: Deploy shared platform services and independently scalable workflow-package release pools on GKE.

**Independent Test**: Build complete package/platform images, render/lint Helm/controller resources, validate Terraform, and verify per-version pools, serving replica floors, mixed/role topology, configured autoscaling and safe release retirement.

- [ ] T056 [P] [US7] Add optimized non-root Dockerfiles for shared API/task/admin services and a reusable executor image build that includes each workflow package's entire locked Workflow/Activity closure. Use one immutable package image for mixed or role-specific pools; no mandatory per-capability deployment image.
- [ ] T057 [P] [US7] Add `.dockerignore`, per-package artifact/image naming, immutable commit/digest and Temporal deployment/build identity conventions, and dependency-lock/provenance metadata.
- [ ] T058 [P] [US7] Add Terraform modules for network, GKE, Cloud SQL, Artifact Registry, Secret Manager, IAM, Workload Identity, and monitoring.
- [ ] T059 [US7] Add dev/test/prod Terraform environments without hard-coded reusable-module values.
- [ ] T060 [US7] Add Cloud SQL HA, private IP, backups, PITR, deletion protection, maintenance, monitoring, databases, and controlled Temporal schema jobs.
- [ ] T061 [US7] Add `platform/temporal/values/dev.yaml`, `test.yaml`, and `prod.yaml` using external Cloud SQL.
- [ ] T062 [P] [US7] Add Helm charts for shared API/task/admin services and reusable package/executor resources under `deploy/helm/`; select and verify Temporal Worker Controller compatibility, CRDs and ownership as the preferred versioned-pool lifecycle manager before adoption. If unsupported for the selected production combination, document and verify an equivalent version-aware adapter with the same lifecycle/scaling acceptance criteria.
- [ ] T063 [US7] Add lifecycle-manager-owned per-package/per-version executor Deployments and optional role pools, unique identities, ServiceAccounts, ConfigMaps/secret references, probes/resources, termination grace, disruption/topology controls and per-serving-queue replica floors. Add shared service ingress only where required; avoid conflicting controller/manual Deployment or HPA ownership.
- [ ] T064 [US7] Configure and verify per-active-version HPA/KEDA with the verified Worker Controller or equivalent lifecycle integration, using queue backlog/Schedule-to-Start, slot availability and resource/downstream metrics; expose min/max/stabilization targets, preserve draining versions and exclude open-workflow count as a standalone scaling signal. Document a tested wake-up mechanism before allowing scale-to-zero.
- [ ] T065 [US7] Add package-closure Docker build tests, Helm/controller schema/lint/template checks, Terraform fmt/validate and authorized GKE tests for version-aware scaling, draining, replay-compatible rollout and isolated package outage. Record static/local versus executed cloud evidence separately.

## Phase 10: User Story 8 - CI/CD and Release Promotion (P3)

**Goal**: Validate, build, scan, promote, and roll back immutable releases.

**Independent Test**: Execute PR validation, affected-image build, dev promotion, test promotion, and approved production promotion using one image digest.

- [ ] T066 [P] [US8] Add `.github/workflows/pr-validation.yml`.
- [ ] T067 [P] [US8] Add `.github/workflows/build-images.yml` with a transitive dependency-aware affected-package/platform-service matrix, closure/security scans, locked provenance and immutable digest outputs; shared runtime/Activity changes rebuild every affected package.
- [ ] T068 [P] [US8] Add `.github/workflows/deploy-dev.yml`, `deploy-test.yml` and `deploy-prod.yml` for the same tested package digest and release manifest, environment binding, controller-based version registration/ramping and production approval; do not rebuild or replace pinned older versions in place.
- [ ] T069 [P] [US8] Add `.github/workflows/terraform-plan.yml` and `terraform-apply.yml`.
- [ ] T070 [US8] Configure GitHub OIDC/WIF permissions, repository variables, environment approvals, and no-key policy.
- [ ] T071 [US8] Add package smoke/replay/serving-health gates, immutable promotion, version-aware rollback, release drainage and retained-query checks. Rollback changes eligible start/Auto-Upgrade routing only under declared policy; it never rewrites selected definitions, pending reservations or pinned historical releases.
- [ ] T072 [US8] Validate workflow YAML and CI/CD behavior without requiring a deployment.

## Phase 11: Polish and Documentation

- [ ] T073 [P] Update root `README.md` with platform, local, testing, deployment, GCP, Temporal, Cloud SQL, WIF, CI/CD, troubleshooting, and production guidance.
- [ ] T074 [P] Add architecture documents under `docs/architecture/` with Mermaid diagrams.
- [ ] T075 [P] Add/update ADRs for workflow-package ownership, generic executors, same-artifact role pools, queue/version routing, controller/scaling, GKE, Cloud SQL, WIF, definition/release governance and shared control-plane boundaries; retain superseded capability-worker decisions as historical evidence.
- [ ] T076 [P] Add migration documentation for Alfresco inventory, conversion, parity, dual-run, cutover, rollback, and long-running instances.
- [ ] T077 Add final verification checklist and record any unavailable cloud validation in `docs/operations/validation-report.md`.

## Dependencies and Execution Order

- Phases 1–4 are completed historical prerequisites. Their checked tasks are not renumbered or treated as package acceptance.
- Phase 4A is the completed package runtime foundation. T078 defines the contracts; T079 validates them. T080/T081 extract runtime and Activities. T082 composes packages; T083 loads them through generic executors. T084 defines executor capacity configuration.
- T085 can build release persistence/API against T078/T079 while host work proceeds. T086 integrates package, runtime and stored routing after T080/T082/T083/T085. T087 needs those bindings and executor registration. T088/T089 validate complete behavior and migration; T090 records their evidence.
- Phase 5 uses package-local adapters (T081/T082/T086). Shared task persistence can begin independently, but its runtime acceptance requires the Phase 4A gate.
- Phase 6 extends Phase 4A policies with side-effect safety, compensation, remediation and long-running/history operations.
- Phase 7 completes definition/release governance over T078/T079/T085; basic release validation cannot wait until Phase 7 to allow unvalidated starts.
- Phase 8 can build shared security/telemetry interfaces in parallel, with full acceptance requiring API, package/executor and task boundaries.
- Phase 9 uses stable package/pool configuration and version policy from Phase 4A. Terraform/shared platform design can proceed independently; production deployment requires selected compatible Temporal/Controller versions and measured operating targets.
- Phase 10 depends on package closure, deployment/controller artifacts and approved release state. It builds affected packages transitively and promotes immutable digests without rebuilding.
- Phase 11 documents the delivered implementation and evidence; historical documents remain labelled with their original scope.

## Parallel Opportunities

- T002–T005, T007/T008/T011/T012 and T020/T021 remain historical completed parallel work.
- T024–T028 were parallel work in the original Phase 4 architecture; no new capability deployment work is implied.
- In Phase 4A, T080/T081 can use separate runtime/Activity file ownership. Package fixtures and executor host can develop against agreed manifest contracts; integration waits for validated artifacts.
- T049–T051 and T058/T059 can proceed against stable security/platform interfaces while package integration progresses.
- T056/T057 and service/package charts may run in parallel after the release/pool model stabilizes; final controller/scaling validation remains sequential.
- Documentation can proceed independently, but verified implementation claims wait for executable results.

## Suggested MVP Boundary

Complete Phases 1–4A and Phase 5 for an API-started, package-deployed, human-approved customer-adjustment sample. The local MVP includes complete installed handler closure, generic mixed executors, configurable local replica/task capacity, approved release bindings and safe restart/migration evidence. Enterprise adapters remain explicit reference/mock implementations until their contracts are supplied.

Production delivery additionally requires Phases 6–10 for long-running/side-effect safety, governance/security/telemetry, immutable images, compatible controller-managed GKE pools and measured autoscaling/recovery. Local replica changes, documentation and static manifests do not establish production capacity or cloud acceptance.
