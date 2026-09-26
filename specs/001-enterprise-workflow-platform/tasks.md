# Tasks: Enterprise Workflow Platform

**Input**: Design documents from `specs/001-enterprise-workflow-platform/`
**Prerequisites**: [spec.md](spec.md), [plan.md](plan.md), [research.md](research.md), [data-model.md](data-model.md), [contracts/api.md](contracts/api.md)

## Task Format

`[ID] [P?] [Story] Description`

- `[P]` means the task can run in parallel with other tasks in the same phase when file ownership permits.
- Story labels map work to the independently testable user stories in `spec.md`.
- Every task names an expected path or artifact. These are planning paths; implementation must confirm the final structure before creating code.

## Phase 1: Setup

**Purpose**: Establish project conventions and record the current system before behavior changes.

- [ ] T001 [P] Create `docs/refactoring-assessment.md` mapping current files and behavior to target services.
- [ ] T002 [P] Add `pyproject.toml`, dependency pinning/lock strategy, formatter, linter, type-checker, and test configuration.
- [ ] T003 [P] Add `.env.example`, configuration conventions, and secret-handling rules under `docs/development/`.
- [ ] T004 [P] Add initial `Makefile` targets for `dev`, `test`, `lint`, `build`, and `docker-build`.
- [ ] T005 [P] Add test directories for `tests/unit`, `tests/integration`, `tests/contract`, and `tests/e2e`.
- [ ] T006 Add baseline tests for the existing `LightweightProcess` behavior in `tests/integration/test_lightweight_process.py`.

## Phase 2: Foundational Contracts

**Purpose**: Blocking foundation for all user stories.

- [ ] T007 [P] Define typed domain contracts in `libs/contracts/src/contracts/` for definitions, executions, tasks, Activities, errors, context, and audit events.
- [ ] T008 [P] Add workflow definition JSON Schema under `workflows/definitions/schema/`.
- [ ] T009 Implement definition parsing and graph validation in `libs/workflow-sdk/src/workflow_sdk/definitions/`.
- [ ] T010 Implement deterministic path, decision, transition, and template semantics in `workflows/common/`.
- [ ] T011 [P] Define retry, timeout, heartbeat, compensation, cancellation, idempotency, and failure policy models in `libs/contracts/`.
- [ ] T012 [P] Add shared configuration and error taxonomy under `libs/common/`.
- [ ] T013 Add unit tests for schema, graph validation, routing, path resolution, and error classification under `tests/unit/`.
- [ ] T014 Record selected API framework, persistence, identity, Temporal hosting, and NFR decisions in `specs/001-enterprise-workflow-platform/plan.md` and ADRs.

**Checkpoint**: Definitions are typed and validated; baseline behavior remains covered.

## Phase 3: User Story 1 - Governed Workflow API (P1)

**Goal**: Start and observe a versioned workflow without direct Temporal access.

**Independent Test**: Register/promote the sample definition, start through the API with an idempotency key, query status, and retrieve business history.

- [ ] T015 [P] [US1] Define API request/response schemas in `specs/001-enterprise-workflow-platform/contracts/api.md` and `libs/contracts/`.
- [ ] T016 [US1] Implement definition registry repository and lifecycle model under `apps/workflow-api/src/`.
- [ ] T017 [US1] Implement execution metadata repository linking business executions to Temporal IDs under `apps/workflow-api/src/`.
- [ ] T018 [US1] Implement versioned workflow endpoints in `apps/workflow-api/src/`.
- [ ] T019 [US1] Add idempotent start, correlation, business reference, pagination, and problem responses.
- [ ] T020 [P] [US1] Add API contract tests in `tests/contract/test_workflow_api.py`.
- [ ] T021 [P] [US1] Add registry and API unit tests in `tests/unit/`.

**Checkpoint**: A client can start, query, and inspect a governed workflow through `/api/v1`.

## Phase 4: User Story 2 - Workflow Runtime and Independent Workers (P1)

**Goal**: Execute the sample flow across independently deployable workers.

**Independent Test**: Run the sample workflow and verify each Activity is consumed from its owned task queue.

- [ ] T022 [US2] Create `apps/workflow-runtime/` with dedicated worker bootstrap and `workflow-orchestration-tq` registration.
- [ ] T023 [US2] Move the deterministic runtime contract from `app/workflows.py` into `workflows/common/` and runtime-owned modules.
- [ ] T024 [P] [US2] Create `workers/validation-worker/` with source, tests, config, README, dependency metadata, and queue registration.
- [ ] T025 [P] [US2] Create `workers/notification-worker/` with source, tests, config, README, dependency metadata, and queue registration.
- [ ] T026 [P] [US2] Create `workers/integration-worker/` with source, tests, config, README, dependency metadata, and queue registration.
- [ ] T027 [P] [US2] Create `workers/human-task-worker/` with source, tests, config, README, dependency metadata, and queue registration.
- [ ] T028 [P] [US2] Create `workers/sample-business-worker/` for reference-only business capabilities.
- [ ] T029 [US2] Replace the production path of `execute_capability` with typed Activity contracts and queue routing.
- [ ] T030 [P] [US2] Add worker Activity contract tests under `tests/contract/workers/`.
- [ ] T031 [US2] Add Temporal runtime tests for deterministic routing, timers, retry, timeout, cancellation, and version compatibility.

**Checkpoint**: Workers are separate services and the sample flow no longer depends on one generic dispatcher.

## Phase 5: User Story 3 - Human Task Management (P1)

**Goal**: Manage human approval through a governed task API.

**Independent Test**: Create a task from the sample workflow, claim/approve or reject it, and verify exactly-once workflow resumption.

- [ ] T032 [P] [US3] Define HumanTask state transitions and persistence schema in `apps/human-task-service/`.
- [ ] T033 [US3] Implement task list/get/create/claim/complete/approve/reject endpoints.
- [ ] T034 [US3] Implement reassign/delegate/escalate/expire policy operations.
- [ ] T035 [US3] Implement human-task worker Activities on `human-task-tq`.
- [ ] T036 [US3] Add authenticated actor, authorization, duplicate completion, evidence, SLA, and escalation handling.
- [ ] T037 [P] [US3] Add task contract tests in `tests/contract/test_human_tasks.py`.
- [ ] T038 [US3] Add Temporal tests for long waits, approval, rejection, timeout, escalation, and duplicate signals.

**Checkpoint**: Human approval can remain open for days and resume the workflow safely.

## Phase 6: User Story 4 - Failure Handling and Operations (P1)

**Goal**: Make retries, timeouts, compensation, remediation, and long-running deployment safe.

**Independent Test**: Exercise retryable, non-retryable, timeout, compensation, cancellation, and manual resume scenarios.

- [ ] T039 [P] [US4] Implement retry/error policy evaluators in `libs/workflow-sdk/`.
- [ ] T040 [US4] Add idempotency and downstream correlation interfaces for side-effecting Activities.
- [ ] T041 [US4] Add compensation and remediation state handling to `workflow-runtime`.
- [ ] T042 [US4] Add operator-facing failure, retry, resume, and termination API operations.
- [ ] T043 [P] [US4] Add failure/resilience tests under `tests/integration/`.
- [ ] T044 [US4] Document long-running workflow rollout, worker compatibility, continue-as-new/history, and rollback policy in `docs/operations/`.

## Phase 7: User Story 5 - Definition Governance (P2)

**Goal**: Safely validate, approve, promote, deprecate, and version definitions.

**Independent Test**: Promote two versions and confirm running executions stay bound to their resolved version.

- [ ] T045 [US5] Implement definition lifecycle transitions, approvals, promotion metadata, and immutable revisions.
- [ ] T046 [US5] Add dependency and capability allow-list validation.
- [ ] T047 [US5] Add definition version compatibility tests under `tests/unit/` and `tests/integration/`.
- [ ] T048 [US5] Add definition administration endpoints and OpenAPI documentation.

## Phase 8: User Story 6 - Security, Audit, and Observability (P2)

**Goal**: Secure and correlate enterprise operations.

**Independent Test**: Execute authorized and unauthorized operations and inspect audit, logs, metrics, and traces.

- [ ] T049 [P] [US6] Add OIDC/OAuth2 and service-identity middleware under `libs/security/`.
- [ ] T050 [P] [US6] Add RBAC/ABAC policy interfaces and tenant/domain/application context.
- [ ] T051 [P] [US6] Add Secret Manager and Workload Identity configuration interfaces.
- [ ] T052 [US6] Implement business audit event persistence/export under `libs/observability/` and control-plane services.
- [ ] T053 [US6] Add structured logging and OpenTelemetry instrumentation to API, runtime, task service, and workers.
- [ ] T054 [US6] Add `/health` and `/ready` endpoints and telemetry contract tests.
- [ ] T055 [US6] Document retention, redaction, data classification, dashboards, alerts, and SLO assumptions.

## Phase 9: User Story 7 - Containers, GKE, and Cloud SQL (P2)

**Goal**: Package and deploy every service independently on GKE.

**Independent Test**: Build images, render/lint Helm charts, validate Terraform, and inspect generated deployments.

- [ ] T056 [P] [US7] Add optimized non-root Dockerfiles for API, runtime, task service, admin, and each worker.
- [ ] T057 [P] [US7] Add `.dockerignore` and immutable image naming/tagging conventions.
- [ ] T058 [P] [US7] Add Terraform modules for network, GKE, Cloud SQL, Artifact Registry, Secret Manager, IAM, Workload Identity, and monitoring.
- [ ] T059 [US7] Add dev/test/prod Terraform environments without hard-coded reusable-module values.
- [ ] T060 [US7] Add Cloud SQL HA, private IP, backups, PITR, deletion protection, maintenance, monitoring, databases, and controlled Temporal schema jobs.
- [ ] T061 [US7] Add `platform/temporal/values/dev.yaml`, `test.yaml`, and `prod.yaml` using external Cloud SQL.
- [ ] T062 [P] [US7] Add Helm charts for `workflow-api`, `workflow-runtime`, `human-task-service`, admin, and workers.
- [ ] T063 [US7] Add independent Deployments, Services, ServiceAccounts, ConfigMaps, secret references, probes, resources, termination grace, disruption controls, topology rules, and HPAs.
- [ ] T064 [US7] Add queue-backlog and Schedule-to-Start metric integration points for future autoscaling.
- [ ] T065 [US7] Add Docker build tests, Helm lint/template validation, and Terraform fmt/validate checks.

## Phase 10: User Story 8 - CI/CD and Release Promotion (P3)

**Goal**: Validate, build, scan, promote, and roll back immutable releases.

**Independent Test**: Execute PR validation, affected-image build, dev promotion, test promotion, and approved production promotion using one image digest.

- [ ] T066 [P] [US8] Add `.github/workflows/pr-validation.yml`.
- [ ] T067 [P] [US8] Add `.github/workflows/build-images.yml` with changed-service matrix and digest outputs.
- [ ] T068 [P] [US8] Add `.github/workflows/deploy-dev.yml`, `deploy-test.yml`, and `deploy-prod.yml`.
- [ ] T069 [P] [US8] Add `.github/workflows/terraform-plan.yml` and `terraform-apply.yml`.
- [ ] T070 [US8] Configure GitHub OIDC/WIF permissions, repository variables, environment approvals, and no-key policy.
- [ ] T071 [US8] Add smoke, health validation, immutable promotion, and documented rollback steps.
- [ ] T072 [US8] Validate workflow YAML and CI/CD behavior without requiring a deployment.

## Phase 11: Polish and Documentation

- [ ] T073 [P] Update root `README.md` with platform, local, testing, deployment, GCP, Temporal, Cloud SQL, WIF, CI/CD, troubleshooting, and production guidance.
- [ ] T074 [P] Add architecture documents under `docs/architecture/` with Mermaid diagrams.
- [ ] T075 [P] Add ADRs for Temporal, worker-per-capability, task queues, GKE, Cloud SQL, WIF, definitions, and control-plane boundary.
- [ ] T076 [P] Add migration documentation for Alfresco inventory, conversion, parity, dual-run, cutover, rollback, and long-running instances.
- [ ] T077 Add final verification checklist and record any unavailable cloud validation in `docs/operations/validation-report.md`.

## Dependencies and Execution Order

- Phase 1 has no implementation dependency and records the current baseline.
- Phase 2 blocks all user stories because contracts, validation, configuration, and tests are shared foundations.
- User Stories 1, 2, and 3 depend on Phase 2 and can proceed in parallel after the contracts stabilize.
- User Story 4 depends on the runtime and worker contracts from User Story 2.
- User Story 5 depends on the definition model from Phase 2 and API foundation from User Story 1.
- User Story 6 can begin after shared contracts exist but requires API, workers, and task boundaries for full validation.
- User Story 7 depends on service boundaries and configuration contracts but infrastructure modules can begin in parallel with application work.
- User Story 8 depends on independently buildable services and deployment artifacts.
- Documentation and final validation depend on the corresponding implementation artifacts.

## Parallel Opportunities

- T002-T005 can run in parallel.
- T007, T008, T011, and T012 can run in parallel.
- T024-T028 can run in parallel once contracts stabilize.
- T049-T051 can run in parallel.
- T056-T062 can run in parallel by service/module ownership.
- T066-T070 can run in parallel after build and deployment conventions are fixed.

## Implementation Strategy

1. Complete Setup and Foundational phases.
2. Deliver User Story 1 as the first usable platform slice.
3. Add independent workflow and capability workers, then validate the sample flow.
4. Add human tasks and complete the approval path.
5. Add failure operations, governance, security, and observability.
6. Package and deploy locally before cloud infrastructure.
7. Add GKE/Cloud SQL and CI/CD artifacts.
8. Run the final static and executable acceptance checklist.

No application code is included in this task set; these are implementation instructions for a later coding phase.