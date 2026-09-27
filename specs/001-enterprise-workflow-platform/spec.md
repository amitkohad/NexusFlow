# Feature Specification: Enterprise Workflow Platform

**Feature Branch**: `001-enterprise-workflow-platform`
**Created**: 2026-09-26
**Status**: Updated target architecture; Phase 1–4 implementation retained
**Input**: Existing Temporal prototype and enterprise workflow platform requirements

## Problem Statement

NexusFlow started as a Temporal workflow interpreter with a hard-coded Activity dispatcher. Phases 1–4 now provide governed APIs, typed contracts, a dedicated runtime, and separately packaged capability workers. The platform needs to replace lightweight Alfresco Process Services capabilities with a governed, durable, auditable workflow platform without exposing Temporal implementation details to business applications.

The accepted deployment unit is now a **business workflow package**: one immutable artifact/image containing the exact governed definition revision, Workflow code/runtime, all dependent executable Activity implementations, explicit registrations, and locked dependencies. A generic executor loads the installed, trusted package manifest; it never obtains executable code from a task or caller-supplied definition. Each package owns a scalable executor pool. The default pool registers both Workflow and Activity handlers; optional role-specific pools use the same image and Build ID when resource, permission, or workload needs justify separation. This target replaces the mandatory shared runtime and capability-service deployment split implemented in Phase 4.

The shared API/registry, human-task persistence service, business database, and Temporal platform remain independently operated platform services. Packaging an Activity handler that calls one of those services does not package or duplicate that service's datastore. Phase 4A implements the package/executor bridge before the remaining feature phases; existing Phase 1–4 completion records describe the historical implementation rather than claiming this redesign already exists.

## User Scenarios and Testing

### User Story 1 - Start and observe a governed workflow (Priority: P1)

As a business application, I want to start an approved workflow definition through a versioned API and observe business status without using Temporal APIs.

**Why this priority**: This is the minimum platform boundary and validates that Temporal is hidden behind the enterprise API.

**Independent Test**: Promote the customer-adjustment definition and its compatible workflow-package release, start it through `/api/v1`, query status, and retrieve business history.

**Acceptance Scenarios**:

1. Given an approved definition, when a client starts it with tenant, business reference, correlation ID, and idempotency key, then the API returns a stable workflow ID and definition version.
2. Given a running workflow, when a client requests status, then the response contains business state, current step, version, and links to business audit data without requiring Temporal concepts.
3. Given a duplicate idempotency key, when the same start request is submitted, then the existing execution is returned and no duplicate workflow is created.

### User Story 2 - Deploy and execute an independent workflow package (Priority: P1)

As a platform operator, I want to deploy a business Workflow and all its dependent executable Activities as one immutable package, and configure generic executor capacity independently for that package.

**Why this priority**: Package closure, compatible release routing, and independent capacity are central to deployment safety and scaling.

**Independent Test**: Build and install the customer-adjustment package in a clean environment, launch its generic executor without separately deployed capability workers, and complete the flow. Run a second package concurrently and vary its replicas without affecting customer-adjustment capacity.

**Acceptance Scenarios**:

1. Given a package manifest, when the artifact is built and validated, then its exact definition revision and complete transitive set of Workflow/Activity registrations are included, and missing or incompatible dependencies reject the release.
2. Given the customer-adjustment image, when a generic executor starts in mixed mode, then it registers its named Workflow and dependent Activities on stable package-owned queues and executes without importing unrelated workflow packages or the API application.
3. Given two packages, when one package's executor replicas are scaled or unavailable, then the other package retains its separately configured queue capacity.
4. Given multiple compatible replicas polling a package queue, when executions run, then work is distributed safely and Activity side effects remain idempotent across retries.
5. Given role-specific Workflow and Activity pools, when deployed, then both use the same immutable image and Worker Deployment Version and route package Activities to that version's owned queues.
6. Given an executor termination signal, when it shuts down, then it stops polling, exposes draining readiness, and allows in-flight work to complete within the configured grace period; interrupted work remains recoverable by another compatible executor.
7. Given an untrusted manifest, duplicate registration, foreign queue binding, or incomplete dependency closure, when an executor starts, then it fails readiness before polling and reports an actionable configuration failure.

### User Story 3 - Manage human approval safely (Priority: P1)

As an authorized human operator, I want to find, claim, approve, reject, reassign, delegate, escalate, and complete a task without blocking an application thread.

**Independent Test**: Start a high-value customer adjustment, complete the approval through the task API, and verify workflow resumption and audit events.

**Acceptance Scenarios**:

1. Given a workflow reaches a human-task step, when the task is created, then it has a task ID, assignee/group, form/version, due date, SLA, and workflow correlation metadata.
2. Given an authorized approver, when the task is approved or rejected, then the workflow resumes exactly once with the recorded actor and outcome.
3. Given an overdue task, when the SLA expires, then escalation or expiration policy is applied and the workflow follows the configured route.

### User Story 4 - Operate failures and long-running workflows (Priority: P1)

As an operator, I want technical failures retried safely, business failures surfaced immediately, and long-running workflows recoverable without losing state.

**Independent Test**: Trigger a transient risk-check failure, a non-retryable validation error, an Activity timeout, and a workflow timeout; verify each follows its declared policy.

**Acceptance Scenarios**:

1. Given a retryable technical error, when an Activity fails, then exponential backoff and the configured attempt limit are applied.
2. Given a business validation error, when an Activity fails, then it is not blindly retried and the workflow exposes a remediation outcome.
3. Given a long-running approval, when workers are rolled forward compatibly, then the execution remains resumable.

### User Story 5 - Govern definitions and versions (Priority: P2)

As a workflow administrator, I want to validate, approve, promote, deprecate, and version definitions separately from package release governance, while ensuring each execution uses a release containing its exact approved revision.

**Independent Test**: Register two definition versions with matching validated package releases, promote one revision/release, start an execution, promote the next revision/release, and verify the existing execution remains bound to its resolved revision and compatible execution policy.

**Acceptance Scenarios**:

1. Given an invalid graph or unsupported capability, when a definition is submitted, then validation rejects it with actionable errors.
2. Given an approved version, when it is promoted to an environment, then the promotion actor, timestamp, dependencies, and immutable revision are recorded.
3. Given a newer definition version, when it is deployed, then running workflows do not automatically switch definitions.
4. Given a promoted definition without a compatible serving package release, when a new start is requested, then it is rejected without reserving an executable start against a different revision; governance promotion alone does not deploy code.

### User Story 6 - Audit and secure enterprise operations (Priority: P2)

As a security or operations user, I want business events, authorization decisions, correlated logs, metrics, and traces that do not expose secrets or duplicate raw Temporal history.

**Independent Test**: Execute a workflow and verify audit events, identity context, structured telemetry, redaction, and authorization behavior.

**Acceptance Scenarios**:

1. Given a workflow state transition, when it occurs, then a business audit event contains event ID, workflow/run IDs, actor, step, state change, correlation ID, and metadata.
2. Given an unauthorized task or workflow operation, when it is requested, then it is denied and the security event is recorded.
3. Given a log or trace, when it is emitted, then it contains workflow, run, worker, queue, Activity, correlation, and business identifiers where available.

### User Story 7 - Deploy and scale on GKE (Priority: P2)

As a platform team, I want independent workflow-package executor pools, separately operated platform services, external Cloud SQL persistence, and environment-specific GKE deployment artifacts.

**Independent Test**: Render and lint Helm charts, validate Terraform, build package and platform images independently, and verify executor-pool manifests contain probes, resources, ServiceAccounts, secret references, disruption controls, version-aware routing, and scaling controls.

**Acceptance Scenarios**:

1. Given a workflow-package image, when deployed, then its executor pools have explicit stable queue bindings, resources, health probes, graceful shutdown, and separately configured replica counts and per-process Workflow/Activity concurrency.
2. Given package-specific backlog and task pickup latency, when measured scaling thresholds are exceeded, then that package/release pool scales without changing another package's capacity; open Workflow count alone does not trigger scaling.
3. Given a production configuration, when Temporal is installed, then PostgreSQL is external Cloud SQL with `temporal` and `temporal_visibility` databases and no bundled database.
4. Given a production serving pool, when rendered, then at least two replicas poll each owned task queue and failure-domain spreading and disruption controls preserve capacity.
5. Given simultaneous compatible releases, when scaling or draining a release, then required workers remain available for its pinned executions; a release is not retired merely because it no longer receives new starts.

### User Story 8 - Promote immutable releases (Priority: P3)

As a release manager, I want CI/CD to build affected workflow packages and platform services, validate package closure and replay safety, and promote the same immutable image digest through environments using short-lived GCP credentials.

**Independent Test**: Run workflow validation on a pull request, build an affected image on merge, promote its digest through dev/test, and require approval for production.

**Acceptance Scenarios**:

1. Given a pull request, when validation runs, then it performs tests, static checks, security scans, Terraform validation, and Helm lint without deploying.
2. Given a merged change, when a package or shared library is affected, then every affected package image is rebuilt with its complete dependencies, scanned, tagged by commit SHA/digest, and pushed; unrelated packages are excluded.
3. Given a production promotion, when approval is granted, then the image digest tested in lower environments is deployed without rebuilding.
4. Given release A with an open approval/timer and candidate release B, when replay/compatibility checks pass and B is ramped through Temporal Worker Versioning, then new starts follow the approved routing while A's pinned execution remains resumable on its compatible version.
5. Given a long-lived Workflow, when a release is promoted, then its declared Pinned or Auto-Upgrade behavior controls code evolution and retained old-release capacity. A Pinned Workflow normally inherits its version through Continue-As-New; moving at that boundary requires an explicitly supported, approved upgrade mechanism. No policy silently changes its governed definition revision.

## Edge Cases

- Definition references a missing step, unreachable terminal state, invalid loop, incompatible decision value, or unapproved capability.
- Duplicate signal, duplicate task completion, duplicate start request, or stale task update.
- Activity timeout after the downstream operation succeeded but before the response was received.
- Worker deployment during an open human task or long-running timer.
- Secret unavailable, expired credentials, revoked authorization, or tenant mismatch.
- Oversized payload or workflow history approaching configured limits.
- Cloud SQL failover, Temporal schema upgrade, lost queue capacity, or partial deployment.
- Non-retryable business error, retry exhaustion, compensation failure, and operator resume.
- Missing transitive Activity implementation, duplicate named registration, incompatible contract, untrusted manifest, or a package digest whose declared revision does not match its contents.
- Package release and definition promotion diverge; no compatible release is serving; a pending idempotent reservation is retried after routing changes.
- Workflow and Activity pools run different image digests/Build IDs, or a foreign release polls the same queue without compatible registrations.
- Release retirement during an approval/timer; long-lived executions retain old code indefinitely; rollback leaves a required Activity version unavailable.

## Functional Requirements

- **FR-001**: The platform MUST expose versioned workflow and task APIs that do not require business consumers to call Temporal directly.
- **FR-002**: The platform MUST use Temporal for durable workflow execution and MUST keep external I/O in Activities.
- **FR-003**: Workflow definitions MUST be schema-validated, graph-validated, immutable after promotion, and independently versioned.
- **FR-004**: The runtime MUST support Activities, decisions, timers, human tasks, Signals/Updates, child workflows where needed, compensation, cancellation, and terminal outcomes.
- **FR-005**: Activities MUST support explicit retry, timeout, heartbeat, non-retryable error, idempotency, and correlation policies.
- **FR-006**: Business validation and authorization failures MUST NOT be blindly retried.
- **FR-007**: Each business workflow package MUST be independently buildable, deployable, releasable, and scalable as one immutable artifact/image that includes its exact governed definition revision, Workflow implementation/runtime, all dependent executable Activity implementations, explicit named registrations, and locked dependencies.
- **FR-008**: Generic executors MUST load only installed, trusted package manifests and register declared Workflow/Activity handlers. Validation, notification, integration, and human-task adapters MUST be reusable capability libraries included in a package where required, rather than mandatory separately deployed capability workers. Human-task persistence and external enterprise services remain separate platform services.
- **FR-009**: The human-task capability MUST support create, assign, claim, approve, reject, complete, reassign, delegate, escalate, and expire operations.
- **FR-010**: Human-task completion MUST resume the workflow exactly once and record the authenticated actor and evidence.
- **FR-011**: The platform MUST emit business audit events separate from raw Temporal history.
- **FR-012**: Audit and telemetry MUST support workflow ID, run ID, workflow type/version, Activity, worker, task queue, correlation ID, business reference, tenant, and domain.
- **FR-013**: Services MUST expose `/health` and `/ready` endpoints and structured logs.
- **FR-014**: Security MUST provide extension points for OIDC/OAuth2, RBAC/ABAC, service identities, tenant/domain authorization, and secret references.
- **FR-015**: Production cloud authentication MUST use Workload Identity Federation or equivalent short-lived credentials and MUST NOT require JSON service-account keys.
- **FR-016**: GKE deployments MUST provide resource settings, probes, graceful shutdown, ServiceAccounts, secret references, failure-domain spreading, disruption controls, and scaling per package/release executor pool. Each production serving pool MUST provide at least two replicas for every owned queue; replica counts and per-process task slots/concurrency MUST be independently configurable. Autoscaling MUST use measured backlog, task pickup latency, task slots, and resource signals rather than open Workflow count alone.
- **FR-017**: Temporal production persistence MUST use externally provisioned Cloud SQL PostgreSQL databases named `temporal` and `temporal_visibility`.
- **FR-018**: CI/CD MUST build affected workflow packages and platform services independently, validate transitive package closure and release compatibility, scan images, use immutable tags/digests, and promote the same digest without rebuilding. Changes to a shared Activity/runtime library MUST invalidate every dependent package build.
- **FR-019**: The platform MUST support local development with a local Temporal environment and a complete sample workflow test.
- **FR-020**: Documentation MUST describe architecture, worker ownership, deployment, security, operations, versioning, migration, and production assumptions.
- **FR-021**: An embedded package manifest MUST bind package identity/version, definition identity/version/content hash, explicitly included compatible definition revisions and their complete dependency closures, Workflow types, named Activity contracts and implementations, logical queues, executor roles, dependency lock, a precomputed stable Build ID, and a declared Workflow upgrade policy. After building, an external immutable release descriptor MUST record the manifest hash and final artifact digest; Phase 9 adds the container image digest under the same provenance. The manifest MUST NOT embed the final digest of its containing artifact/image. Validation MUST reject incomplete or mismatched registrations before the executor polls; Phase 4A local packages do not require a container image.
- **FR-022**: Executors MUST support a default mixed Workflow/Activity pool and optional Workflow-only or Activity-only pools using the same package image and Build ID. Queue ownership MUST be stable per package/pool/environment namespace; queues MUST NOT be created per execution, replica, or release merely to implement version routing. All replicas polling a queue MUST register the compatible handlers required by that pool.
- **FR-023**: Release routing MUST use Temporal Worker Deployment identity and Worker Version/Build ID with compatible current/ramping/retained versions. All dependent executable Activity handlers and their queues MUST participate in the same package Worker Deployment Version; separate pool roles do not create separate package releases. A bundled handler may call an independently operated platform or enterprise service through its declared external contract.
- **FR-024**: New starts MUST resolve an approved serving release containing the selected governed revision and atomically persist the private intended release/routing binding with the reservation in the business database. Temporal submission is a separate operation, reconciled through the stable execution ID: there is no database/Temporal distributed atomic commit. The implementation MUST distinguish intended initial release from Temporal-confirmed executing version, validate current/ramping selection against captured compatible release policy, and use a supported version override when concrete initial placement is required. Idempotent retries MUST NOT re-resolve revision or release provenance across promotions, rollout, or rollback; business consumers MUST NOT supply image, queue, Build ID, or executor placement.
- **FR-025**: Each Workflow type MUST declare Temporal Pinned or Auto-Upgrade behavior. Continue-As-New upgrade is a separate policy, not a third behavior; Pinned executions inherit their version by default until a supported explicit upgrade mechanism is authorized. Release checks MUST include appropriate history replay/patch compatibility and explicitly included exact compatible definition revisions plus their full handlers/dependency closures. An upgrade MUST be rejected if the new release cannot execute the selected old revision. Retirement MUST retain compatible workers for outstanding pinned executions; a release's newer primary revision MUST NOT silently replace existing execution content.
- **FR-026**: Package publication, environment release approval/promotion, routing/ramp changes, and retirement MUST be privileged, scoped, auditable platform operations separate from business Workflow start and definition governance. Production deployment tooling MUST evaluate Temporal's Kubernetes Worker Controller for managing versioned pools; any alternative MUST preserve equivalent version-aware rollout, scaling, and retirement guarantees.

## Key Entities

- **WorkflowDefinition**: Governed definition identity, version, schema, graph, lifecycle, dependencies, owner, tenant/domain, and promotion metadata.
- **WorkflowExecution**: Business execution identity, Temporal workflow/run linkage, resolved definition version, private package release/routing binding, state, correlation, business reference, and timestamps.
- **ActivityContract**: Capability action, input/output schema, logical package queue/role, retry/timeout policy, idempotency requirements, and implementation compatibility.
- **HumanTask**: Task identity, workflow/step linkage, assignment, form/version, status, SLA, outcome, actor, evidence, and escalation data.
- **AuditEvent**: Business event identity, execution linkage, event type, actor, state transition, correlation, tenant/domain, metadata, and retention class.
- **WorkflowPackage**: Governed package identity, workflow ownership, trusted manifest/registration boundary, and executor-role/queue declarations.
- **WorkflowRelease**: Immutable package version/image digest, exact bundled definition revision, dependency lock, complete registrations, Temporal deployment/build identity, upgrade policy, environment approval/routing, and retirement metadata.
- **ExecutorPool**: Package/release role, stable queue bindings, replica bounds, per-process concurrency, resource/identity configuration, scaling signals, and draining state.
- **DeploymentRelease**: Separately deployed platform-service version, image digest, environment, promotion approval, and rollback metadata; workflow execution release ownership belongs to WorkflowRelease.
- **TenantContext**: Tenant, business domain, application, authorization claims, correlation ID, and business reference.

## Success Criteria

- **SC-001**: A supported sample workflow can be started, observed, human-approved, retried, completed, and audited entirely through platform APIs and workers.
- **SC-002**: Every workflow package can be built, installed, tested, containerized, deployed, and scaled with its complete Workflow/Activity dependency closure and without separately deployed capability workers or unrelated workflow packages.
- **SC-003**: Invalid definitions are rejected before execution with actionable validation errors.
- **SC-004**: A transient technical failure is retried according to policy while business validation errors are not blindly retried.
- **SC-005**: Replay and integration tests prove a version-aware rollout/rollback with simultaneous releases, including an open approval/timer, preserves routing, Activity compatibility, and the pinned definition revision.
- **SC-006**: Static validation passes for Dockerfiles, Helm charts, Terraform, GitHub Actions, and secret scanning.
- **SC-007**: Production deployment artifacts reference external Cloud SQL persistence and short-lived cloud identity without committed credentials.
- **SC-008**: Operators can correlate a business execution across API response, worker logs, traces, metrics, audit events, and Temporal diagnostics.
- **SC-009**: Two package pools demonstrate independent capacity changes and availability; multiple compatible replicas complete work safely, and downscaling drains in-flight work within the configured grace period.
- **SC-010**: Manifest checks reject missing dependencies, mismatched definition hashes, incompatible registrations, and foreign queue/version bindings before polling. Production manifests provide at least two replicas per serving queue and documented measured scaling controls.

## Assumptions and Clarifications Needed

- Python, FastAPI, Pydantic contracts, and SQLAlchemy/PostgreSQL are the implemented foundation. The generic executor and package manifest are target requirements, not current runtime behavior.
- The hosting decision remains self-hosted Temporal on GKE with external Cloud SQL PostgreSQL; Temporal/SDK/Worker Controller versions and support for chosen Worker Versioning behavior must be verified and pinned before implementation. Production SLOs, scaling thresholds, and identity-provider integration require deployment decisions.
- Alfresco migration requires representative definitions and runtime data; this specification covers migration readiness, not automatic conversion of unknown processes.
- A visual designer, citizen-developer product, and agentic/AI capabilities are future extension points, not first-increment deliverables.
