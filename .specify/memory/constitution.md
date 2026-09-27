# NexusFlow Enterprise Workflow Platform Constitution

## Core Principles

### I. Temporal Is the Execution Substrate

Temporal is the durable orchestration engine. The platform MUST NOT recreate workflow durability, scheduling, retries, timers, history, or execution semantics. Temporal SDK details remain behind workflow-runtime and platform libraries; business consumers use the Enterprise Workflow API.

### II. Definitions Are Governed Contracts

Workflow definitions MUST be typed, schema-validated, semantically validated, immutable after promotion, and independently versioned from workflow code, worker releases, Activity contracts, and API contracts. Future visual authoring MUST compile to the same governed definition model.

### III. Workflow Packages Own Their Executable Dependencies

Each business workflow MUST be an independently testable, buildable, deployable,
and versioned package containing its governed definition or Workflow code, runtime,
all dependent executable Activity implementations, contracts, and exact dependency
manifest. A release is one immutable artifact/image. Shared Activity libraries MAY
be reused at build time; required implementations MUST be included in that package
rather than relying on separately released capability-worker deployments.

Workers are generic executor processes that load an installed, approved package at
startup and register its named handlers. They MUST NOT fetch arbitrary executable
code from task payloads or the definition registry. Each package owns its logical
task queues and executor pools; replicas sharing a queue and deployment version
MUST register compatible handlers. The default pool executes both Workflows and
Activities. Separate role pools MAY use the same release image when resource,
permission, or workload isolation warrants them. Unrelated business workflows
retain separate release and capacity ownership.

The installed content manifest and dependency lock are immutable inputs to the
build. A separate post-build release descriptor binds their canonical hashes to
the artifact digest and, once containerized, image digest; a manifest MUST NOT
require embedding the final digest of the image that contains it.

### IV. Deterministic Workflows, Isolated I/O

Workflow code MUST be deterministic and contain no external I/O. Enterprise calls,
database access, messaging, filesystem operations, secrets access, and
nondeterministic computation MUST occur in Activities. Packaging Workflow and
Activity code together MUST NOT move Activity I/O into Workflow execution. Package
loading and dependency validation occur outside the deterministic Workflow path.

### V. Testable Incremental Delivery

Each user story MUST have an independent acceptance test. Contract, workflow, integration, and end-to-end tests are required for shared boundaries. Tests SHOULD be written before implementation for new contracts and MUST protect long-running workflow compatibility.

### VI. Security and Audit by Default

Authentication, authorization, tenant/domain context, secret references, data classification, redaction, structured audit events, and correlated observability are platform capabilities, not optional application concerns. Permanent cloud credentials and service-account JSON keys are prohibited.

### VII. Observable and Recoverable Operations

Every deployable service MUST expose health/readiness signals, structured logs, metrics, and trace correlation. Failures MUST be classified, retried only when safe, and made visible for remediation, replay, resume, compensation, or manual intervention.

### VIII. Simplicity and Explicit Tradeoffs

Prefer existing Temporal, Kubernetes, GCP, and repository patterns over custom infrastructure. Any additional abstraction, service, queue, datastore, or namespace MUST have a documented ownership, scaling, security, and operational reason.

## Platform Constraints

- Target runtime: Python 3.11+ unless a later decision records a reason to change.
- Target cloud: Google Cloud Platform, GKE, Artifact Registry, Cloud SQL PostgreSQL, Secret Manager, IAM, Workload Identity Federation, Cloud Logging, Cloud Monitoring, and Cloud Trace.
- Temporal Server is shared platform infrastructure and uses externally provisioned Cloud SQL databases `temporal` and `temporal_visibility`.
- Environment-specific values belong in configuration, Helm values, Terraform environments, or secret references, never application source.
- Production images use immutable Git commit SHA or digest references and run as non-root users.
- Production package deployments use Temporal Worker Deployment Versions with
  immutable Build IDs and compatible handlers across all pools of that release.
  Activities belonging to a package MUST follow its correlated deployment version;
  unrelated shared worker releases are not implicit executable dependencies.
- Production serving queues initially have at least two executor replicas across
  failure domains. Replica count, Workflow/Activity task concurrency, poller tuning,
  and resource limits are distinct controls. Autoscaling MUST use measured pickup
  latency, backlog, task slots, and resource use rather than open workflow count alone.
- Production deployment requires graceful worker shutdown, compatible workflow
  evolution, version-aware rollout, and tested rollback. Each workflow type declares
  Temporal Pinned or Auto-Upgrade behavior and a long-running policy: retain pinned
  old releases, use replay-safe patching for Auto-Upgrade, or explicitly upgrade a
  Pinned workflow at tested Continue-As-New boundaries.
- A compatible code upgrade MUST preserve the execution's exact governed
  definition revision/hash and its complete handler/contract closure. A candidate
  release that supports another revision only is not a compatible upgrade.
- Existing workflow types, history-dependent queues, and registrations MUST NOT be
  rewritten during package migration. Legacy runtime support remains until its
  executions and pending reservations no longer require it.
- Temporal Worker Controller is the preferred Kubernetes lifecycle manager after
  its Server/SDK/controller support is verified. If compatibility is unavailable,
  an explicitly tested alternative MUST preserve equivalent version-aware routing,
  capacity, and retirement guarantees.

## Quality Gates

- No implementation task starts before the feature specification, technical plan, and task dependencies are reviewed.
- No production claim is made without executable validation or an explicitly documented unverified step.
- Required validation includes unit, Temporal workflow, contract, integration, and end-to-end tests as appropriate to risk.
- Infrastructure changes require Terraform formatting/validation and Helm lint/template validation.
- Pull requests require static analysis, dependency/security scanning, and container validation where practical.

## Governance

This constitution governs all platform design and implementation work. A deviation requires a documented rationale, rejected alternatives, risk, owner, and follow-up task in the implementation plan or an ADR. Amendments must update this file and identify affected specifications and tasks.

### Amendment 1.1.0

The user-authorized workflow-package deployment model replaces mandatory
worker-per-capability ownership with per-workflow release ownership and generic
executor pools. The I/O boundary and long-running compatibility requirements are
preserved. Affected artifacts are the enterprise workflow specification, plan,
tasks, data model, contracts, research, and quickstart, plus ADRs 0003, 0005, and
[0006](../../docs/adr/0006-workflow-packages-and-executor-pools.md). Phase 4 remains
the implemented baseline; the new package model is planned in Phase 4A and later
deployment increments, not claimed as implemented by this amendment.

**Version**: 1.1.0 | **Ratified**: 2026-09-26 | **Last Amended**: 2026-09-27
