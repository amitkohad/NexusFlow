# Feature Specification: Enterprise Workflow Platform

**Feature Branch**: `001-enterprise-workflow-platform`
**Created**: 2026-09-26
**Status**: Draft
**Input**: Existing Temporal prototype and enterprise workflow platform requirements

## Problem Statement

NexusFlow currently demonstrates a useful Temporal workflow interpreter, but consumers must invoke Temporal directly and all capabilities run in one worker through a hard-coded dispatcher. The platform needs to replace lightweight Alfresco Process Services capabilities with a governed, durable, auditable, and independently scalable workflow platform without exposing Temporal implementation details to business applications.

## User Scenarios and Testing

### User Story 1 - Start and observe a governed workflow (Priority: P1)

As a business application, I want to start an approved workflow definition through a versioned API and observe business status without using Temporal APIs.

**Why this priority**: This is the minimum platform boundary and validates that Temporal is hidden behind the enterprise API.

**Independent Test**: Promote the customer-adjustment definition, start it through `/api/v1`, query status, and retrieve business history.

**Acceptance Scenarios**:

1. Given an approved definition, when a client starts it with tenant, business reference, correlation ID, and idempotency key, then the API returns a stable workflow ID and definition version.
2. Given a running workflow, when a client requests status, then the response contains business state, current step, version, and links to business audit data without requiring Temporal concepts.
3. Given a duplicate idempotency key, when the same start request is submitted, then the existing execution is returned and no duplicate workflow is created.

### User Story 2 - Execute the sample workflow across independent workers (Priority: P1)

As a platform operator, I want workflow orchestration and capabilities to run in independently deployable worker services with explicit task queues.

**Why this priority**: Worker isolation is central to scaling, release safety, and failure isolation.

**Independent Test**: Run the customer-adjustment flow and verify validation, integration, notification, and human-task Activities are handled by their owning queues and services.

**Acceptance Scenarios**:

1. Given a sample execution, when an Activity is scheduled, then it is routed to the capability task queue defined by its contract.
2. Given a failure in notification-worker, when validation-worker is healthy, then validation work remains available and the services can be scaled independently.
3. Given a worker termination signal, when it shuts down, then it stops polling and allows in-flight work to complete within its termination grace period.

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

As a workflow administrator, I want to validate, approve, promote, deprecate, and version definitions independently from worker releases.

**Independent Test**: Register two definition versions, promote one, start an execution, promote the next, and verify the existing execution remains bound to its resolved version.

**Acceptance Scenarios**:

1. Given an invalid graph or unsupported capability, when a definition is submitted, then validation rejects it with actionable errors.
2. Given an approved version, when it is promoted to an environment, then the promotion actor, timestamp, dependencies, and immutable revision are recorded.
3. Given a newer definition version, when it is deployed, then running workflows do not automatically switch definitions.

### User Story 6 - Audit and secure enterprise operations (Priority: P2)

As a security or operations user, I want business events, authorization decisions, correlated logs, metrics, and traces that do not expose secrets or duplicate raw Temporal history.

**Independent Test**: Execute a workflow and verify audit events, identity context, structured telemetry, redaction, and authorization behavior.

**Acceptance Scenarios**:

1. Given a workflow state transition, when it occurs, then a business audit event contains event ID, workflow/run IDs, actor, step, state change, correlation ID, and metadata.
2. Given an unauthorized task or workflow operation, when it is requested, then it is denied and the security event is recorded.
3. Given a log or trace, when it is emitted, then it contains workflow, run, worker, queue, Activity, correlation, and business identifiers where available.

### User Story 7 - Deploy and scale on GKE (Priority: P2)

As a platform team, I want independently packaged services, external Cloud SQL persistence, and environment-specific GKE deployment artifacts.

**Independent Test**: Render and lint Helm charts, validate Terraform, build service images independently, and verify manifests contain probes, resources, ServiceAccounts, secret references, and HPAs.

**Acceptance Scenarios**:

1. Given a worker image, when deployed, then it has an independent Deployment, task queue configuration, resource settings, health probes, and graceful shutdown.
2. Given queue backlog for integration work, when scaling is enabled, then integration-worker can scale without scaling validation-worker.
3. Given a production configuration, when Temporal is installed, then PostgreSQL is external Cloud SQL with `temporal` and `temporal_visibility` databases and no bundled database.

### User Story 8 - Promote immutable releases (Priority: P3)

As a release manager, I want CI/CD to build affected services, scan them, and promote the same immutable image digest through environments using short-lived GCP credentials.

**Independent Test**: Run workflow validation on a pull request, build an affected image on merge, promote its digest through dev/test, and require approval for production.

**Acceptance Scenarios**:

1. Given a pull request, when validation runs, then it performs tests, static checks, security scans, Terraform validation, and Helm lint without deploying.
2. Given a merged change, when a service is affected, then only the affected image is built, scanned, tagged by commit SHA/digest, and pushed.
3. Given a production promotion, when approval is granted, then the image digest tested in lower environments is deployed without rebuilding.

## Edge Cases

- Definition references a missing step, unreachable terminal state, invalid loop, incompatible decision value, or unapproved capability.
- Duplicate signal, duplicate task completion, duplicate start request, or stale task update.
- Activity timeout after the downstream operation succeeded but before the response was received.
- Worker deployment during an open human task or long-running timer.
- Secret unavailable, expired credentials, revoked authorization, or tenant mismatch.
- Oversized payload or workflow history approaching configured limits.
- Cloud SQL failover, Temporal schema upgrade, lost queue capacity, or partial deployment.
- Non-retryable business error, retry exhaustion, compensation failure, and operator resume.

## Functional Requirements

- **FR-001**: The platform MUST expose versioned workflow and task APIs that do not require business consumers to call Temporal directly.
- **FR-002**: The platform MUST use Temporal for durable workflow execution and MUST keep external I/O in Activities.
- **FR-003**: Workflow definitions MUST be schema-validated, graph-validated, immutable after promotion, and independently versioned.
- **FR-004**: The runtime MUST support Activities, decisions, timers, human tasks, Signals/Updates, child workflows where needed, compensation, cancellation, and terminal outcomes.
- **FR-005**: Activities MUST support explicit retry, timeout, heartbeat, non-retryable error, idempotency, and correlation policies.
- **FR-006**: Business validation and authorization failures MUST NOT be blindly retried.
- **FR-007**: Each Temporal worker MUST be independently deployable and MUST poll only owned task queues.
- **FR-008**: The platform MUST provide validation, notification, integration, human-task, and workflow-runtime worker boundaries.
- **FR-009**: The human-task capability MUST support create, assign, claim, approve, reject, complete, reassign, delegate, escalate, and expire operations.
- **FR-010**: Human-task completion MUST resume the workflow exactly once and record the authenticated actor and evidence.
- **FR-011**: The platform MUST emit business audit events separate from raw Temporal history.
- **FR-012**: Audit and telemetry MUST support workflow ID, run ID, workflow type/version, Activity, worker, task queue, correlation ID, business reference, tenant, and domain.
- **FR-013**: Services MUST expose `/health` and `/ready` endpoints and structured logs.
- **FR-014**: Security MUST provide extension points for OIDC/OAuth2, RBAC/ABAC, service identities, tenant/domain authorization, and secret references.
- **FR-015**: Production cloud authentication MUST use Workload Identity Federation or equivalent short-lived credentials and MUST NOT require JSON service-account keys.
- **FR-016**: GKE deployments MUST provide resource settings, probes, graceful shutdown, ServiceAccounts, secret references, disruption controls where appropriate, and independent HPAs.
- **FR-017**: Temporal production persistence MUST use externally provisioned Cloud SQL PostgreSQL databases named `temporal` and `temporal_visibility`.
- **FR-018**: CI/CD MUST build affected services independently, scan images, use immutable tags/digests, and promote without rebuilding.
- **FR-019**: The platform MUST support local development with a local Temporal environment and a complete sample workflow test.
- **FR-020**: Documentation MUST describe architecture, worker ownership, deployment, security, operations, versioning, migration, and production assumptions.

## Key Entities

- **WorkflowDefinition**: Governed definition identity, version, schema, graph, lifecycle, dependencies, owner, tenant/domain, and promotion metadata.
- **WorkflowExecution**: Business execution identity, Temporal workflow/run linkage, resolved definition version, state, correlation, business reference, and timestamps.
- **ActivityContract**: Capability action, input/output schema, queue, retry/timeout policy, idempotency requirements, and worker version compatibility.
- **HumanTask**: Task identity, workflow/step linkage, assignment, form/version, status, SLA, outcome, actor, evidence, and escalation data.
- **AuditEvent**: Business event identity, execution linkage, event type, actor, state transition, correlation, tenant/domain, metadata, and retention class.
- **DeploymentRelease**: Service/worker version, image digest, definition dependencies, environment, promotion approval, and rollback metadata.
- **TenantContext**: Tenant, business domain, application, authorization claims, correlation ID, and business reference.

## Success Criteria

- **SC-001**: A supported sample workflow can be started, observed, human-approved, retried, completed, and audited entirely through platform APIs and workers.
- **SC-002**: Every deployable worker can be built, tested, containerized, deployed, and scaled without packaging unrelated workers.
- **SC-003**: Invalid definitions are rejected before execution with actionable validation errors.
- **SC-004**: A transient technical failure is retried according to policy while business validation errors are not blindly retried.
- **SC-005**: Running workflows remain compatible across a documented worker rollout and definition promotion.
- **SC-006**: Static validation passes for Dockerfiles, Helm charts, Terraform, GitHub Actions, and secret scanning.
- **SC-007**: Production deployment artifacts reference external Cloud SQL persistence and short-lived cloud identity without committed credentials.
- **SC-008**: Operators can correlate a business execution across API response, worker logs, traces, metrics, audit events, and Temporal diagnostics.

## Assumptions and Clarifications Needed

- Python remains the implementation language; FastAPI/Pydantic-style contracts are the recommended API approach but require confirmation during planning.
- The control-plane datastore, identity provider, UI scope, Temporal hosting/version, and production SLO values are not specified and must be decided before implementation.
- Alfresco migration requires representative definitions and runtime data; this specification covers migration readiness, not automatic conversion of unknown processes.
- A visual designer, citizen-developer product, and agentic/AI capabilities are future extension points, not first-increment deliverables.