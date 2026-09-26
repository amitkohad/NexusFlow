# NexusFlow Enterprise Workflow Platform Constitution

## Core Principles

### I. Temporal Is the Execution Substrate

Temporal is the durable orchestration engine. The platform MUST NOT recreate workflow durability, scheduling, retries, timers, history, or execution semantics. Temporal SDK details remain behind workflow-runtime and platform libraries; business consumers use the Enterprise Workflow API.

### II. Definitions Are Governed Contracts

Workflow definitions MUST be typed, schema-validated, semantically validated, immutable after promotion, and independently versioned from workflow code, worker releases, Activity contracts, and API contracts. Future visual authoring MUST compile to the same governed definition model.

### III. Workers Are Independently Deployable

Every Temporal worker MUST be self-contained, independently testable, buildable, deployable, scalable, configurable, observable, and versioned. A worker polls only task queues it owns. Unrelated workers MUST NOT be combined into one deployment or image.

### IV. Deterministic Workflows, Isolated I/O

Workflow code MUST be deterministic and contain no external I/O. Enterprise calls, database access, messaging, filesystem operations, secrets access, and nondeterministic computation MUST occur in Activities owned by capability workers.

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
- Production deployment requires graceful worker shutdown, compatible workflow evolution, rolling updates, and tested rollback.

## Quality Gates

- No implementation task starts before the feature specification, technical plan, and task dependencies are reviewed.
- No production claim is made without executable validation or an explicitly documented unverified step.
- Required validation includes unit, Temporal workflow, contract, integration, and end-to-end tests as appropriate to risk.
- Infrastructure changes require Terraform formatting/validation and Helm lint/template validation.
- Pull requests require static analysis, dependency/security scanning, and container validation where practical.

## Governance

This constitution governs all platform design and implementation work. A deviation requires a documented rationale, rejected alternatives, risk, owner, and follow-up task in the implementation plan or an ADR. Amendments must update this file and identify affected specifications and tasks.

**Version**: 1.0.0 | **Ratified**: 2026-09-26 | **Last Amended**: 2026-09-26