# Data Model: Enterprise Workflow Platform

This document describes the target model and current implementation. Phase 1–4
implemented definition and execution governance, including private
`runtime_profile` and `runtime_task_queue` bindings. Phase 4A added package,
release, pool and execution bindings in the business database. Phase 5 adds
human-task tables, scoped task contracts and a dedicated task API. Broader
cross-service audit capture remains a later phase.

## WorkflowDefinition

- `definition_id`
- `workflow_type`
- `version`
- `tenant`
- `business_domain`
- `application`
- `status`: draft, validated, approved, promoted, deprecated, retired
- `content_hash`
- `definition_document`
- `owner`
- `dependencies`
- `created_at`, `approved_at`, `promoted_at`
- `created_by`, `approved_by`, `promoted_by`

## WorkflowExecution

- `workflow_id`
- `run_id`
- `workflow_type`
- `definition_id`, `definition_version`
- `tenant`, `business_domain`, `application`
- `business_reference`
- `correlation_id`
- `idempotency_key`
- `state`
- `current_step`
- `started_at`, `updated_at`, `completed_at`
- `failure_code`, `failure_summary`
- private current bindings: `runtime_profile`, `runtime_task_queue`
- private target provenance: `package_id`, `intended_package_release_id`,
  `artifact_digest`, `image_digest` (introduced with container builds in Phase 9)
- private target definition binding: `definition_content_hash`
- private target intended routing: `temporal_namespace`, `worker_deployment_name`,
  `intended_initial_build_id`, `eligible_release_policy`, `workflow_task_queue`,
  `activity_queue_bindings`
- private target upgrade policy: `versioning_behavior`, `continue_as_new_policy`
- private target confirmed routing: `confirmed_initial_package_release_id`,
  `confirmed_initial_build_id`, `current_build_id`, `routing_updated_at`

The business database transaction atomically captures the selected definition
revision and intended approved release/routing decision before backend submission;
it does not atomically commit with Temporal. Temporal may choose the initial
current/ramping version after routing changes. Submission reconciliation uses the
stable execution ID to confirm the observed version against the captured eligible
release policy and exact revision/closure. Concrete required placement uses a
verified supported version override rather than assuming a reserved Build ID
controls server selection. A mismatch is reconciled safely or rejected, never
treated as confirmed merely because a database binding exists.

Idempotent recovery uses the stored revision and intended provenance even after a
promotion or rollback; it does not resolve them again. Confirmed initial placement
and subsequent observed versions are recorded separately. Auto-Upgrade or a
supported approved Continue-As-New upgrade may change the executing Build ID,
but only to an eligible release including that exact revision and full dependency
closure. Those transitions are audited. These fields are administrative data,
excluded from public business execution responses.

## HumanTask

- `task_id`
- `workflow_id`, `run_id`, `first_execution_run_id`, `step_id`
- `definition_version`
- `tenant`, `business_domain`, `application`
- `idempotency_key`, `request_fingerprint`, `version`
- `package_id`, `package_release_id`, `build_id`
- `assignee`, `assignee_group`
- `delegated_by`, `status`, `escalation_level`
- `form_schema_version`
- `payload_reference`
- `due_at`, `sla_deadline`
- `escalation_policy`
- `outcome`, `actor`, `evidence_reference`, `comment`
- `created_at`, `updated_at`, `claimed_at`, `completed_at`, `expired_at`

Phase 5 stores tasks in `human_tasks`, lifecycle events in
`human_task_audit_events`, and one terminal signal per task in
`human_task_outbox`. The outbox has lease/retry/delivered/blocked states.
PostgreSQL row locks and unique keys arbitrate concurrent decisions. A reused
Workflow ID starts a new task identity because the durable approval Activity
uses Temporal's first execution run ID in its idempotency key. The outbox also
retains that chain ID and sends only to a matching, exact run. The package
release captured on first task creation remains its provenance through a
compatible retry or Continue-As-New.

## AuditEvent

- `event_id`
- `event_type`
- `workflow_id`, `run_id`
- `business_reference`
- `tenant`, `business_domain`, `application`
- `step`
- `previous_state`, `new_state`
- `actor`
- `correlation_id`
- `timestamp`
- `metadata`
- `retention_class`

## ActivityContract

- `capability`
- `action`
- `contract_version`
- `logical_queue`, `executor_role`: package-owned routing declared in a manifest
- `input_schema`
- `output_schema`
- `retry_policy`
- `timeout_policy`
- `heartbeat_policy`
- `idempotency_required`
- `implementation_compatibility`

Phase 4's contract uses a global `task_queue` and `worker_compatibility` field.
Phase 4A evolves routing into package/pool bindings without changing retry,
timeout, heartbeat, correlation, or idempotency semantics. Reusable Activity
implementations may be shared source libraries, but each package contains the
required executable implementation and compatible contract version.

## WorkflowPackage (target, Phase 4A)

- `package_id`
- `name`, `owner`
- `tenant`, `business_domain`, `application`
- `workflow_types`: package-owned business types
- `manifest_schema_version`
- `trusted_registration_entrypoint`: installed code entrypoint, never arbitrary
  executable code or imports supplied by a business request
- `declared_executor_roles`: mixed by default; optional workflow/activity roles
- `logical_queue_ownership`
- `status`: active, deprecated, retired
- `created_by`, `created_at`

Suggested source locations are `workflow-packages/customer-adjustment/` for the
first business package, `apps/workflow-executor/` for the generic host, and
`libs/activities/` for reusable Activity implementations. Source reuse does not
create a separately deployed capability-service dependency.

## WorkflowRelease (target, Phase 4A)

- `package_release_id`, `package_id`, `release_version`
- `artifact_digest`, `image_digest` (absent until Phase 9 image build), `source_revision`
- `manifest_schema_version`, `manifest_hash`
- `definition_id`, `definition_version`, `definition_content_hash`
- `compatible_definition_revisions`: exact identity/version/hash of every
  additional included revision and its complete Workflow/Activity dependency
  closure; compatibility is not just a declared version range
- `workflow_registrations`: named executable type and installed entrypoint
- `activity_registrations`: named executable handler, capability/contract version,
  installed entrypoint, logical queue, and executor role
- `dependency_lock_hash`, `runtime_version`, `sdk_version`
- `worker_deployment_name`, `build_id`
- `workflow_upgrade_policy`: Temporal Pinned or Auto-Upgrade per Workflow type
- `continue_as_new_upgrade_policy`: separate explicit upgrade mechanism for
  permitted boundaries; Pinned inherits by default, never upgrades automatically
- `closure_validation_result`, `replay_validation_result`
- `published_by`, `published_at`
- `environment_bindings`: environment/Temporal namespace, resolved queue names,
  approval actor/time, current/ramping/retained/retired state, ramp percentage,
  rollout policy, rollback reference, and retirement evidence

The embedded manifest records definition content, registrations, dependency
locks, upgrade policies, and a stable Build ID computed before packaging. It does
not contain its own final artifact/image digest. After the build, an external
immutable release descriptor records the manifest hash and artifact digest.
Phase 4A supports local artifact releases; Phase 9 records a new immutable image
descriptor referencing that source/manifest/artifact provenance rather than
overwriting the original descriptor. Image existence is not a Phase 4A
prerequisite. Promotion never rebuilds an existing artifact or image.

Manifest and artifact contents are immutable. A release bundles its primary exact
governed revision and complete executable dependency closure, plus every exact
older revision and full closure it claims to support for upgrades. A new primary
revision alone does not prove old-execution compatibility. Missing old content or
handlers rejects an upgrade. Governance approval and promotion are separate from
code publication. Different bundled content requires a new immutable release.
Environment routing and promotion records change through audited privileged
operations while artifact digests remain unchanged across environments.

Workflow-only and Activity-only pools belonging to a release use the same image
digest, Worker Deployment identity, and Build ID. Queue names are stable for a
package/pool in its environment namespace; they are not synthesized per release,
replica, or execution. Temporal Worker Versioning differentiates compatible
release versions on those queues. A long-lived execution's policy determines
whether its original version remains available or a validated upgrade is allowed.

## ExecutorPool (target, Phase 4A)

- `pool_id`, `package_id`, `package_release_id`
- `environment`, `temporal_namespace`
- `role`: mixed, workflow, or activity
- `artifact_digest`, `image_digest` (Phase 9), `worker_deployment_name`, `build_id`
- `queue_bindings`: stable package-owned Workflow/Activity queues for the role
- `registration_set_hash`
- `replicas`, `min_replicas`, `max_replicas`
- `workflow_task_slots`, `activity_task_slots`, `poller_configuration`
- `resource_requests`, `resource_limits`
- `service_identity`, `secret_references`
- `probe_configuration`, `shutdown_grace`, `termination_grace`
- `disruption_policy`, `failure_domain_spread`
- `scaling_policy`: measured backlog, task pickup latency, available slots,
  CPU/memory thresholds, cooldowns, and scale bounds
- `desired_state`: serving, draining, retained, retired
- `observed_state`: pending, starting, ready, draining, retained, retired, failed
- `observed_replicas`, `observed_registration_set_hash`, `last_reconciled_at`
- `updated_by`, `updated_at`

Replica counts describe processes/pods; task slots describe per-process
concurrency and are configured independently. Every production serving queue has
at least two compatible polling replicas. Open Workflow count alone is not a
scaling signal because approvals and timers may wait without consuming executor
capacity. An old release stops receiving new starts before it drains, but pinned
executions still require compatible serving capacity until safely completed or
migrated. Retirement evidence verifies those obligations before removing a pool.
Desired configuration is not proof of serving capacity. Reconciliation must
confirm actual executors, registrations, queue readiness, and version routing
before an observed pool/release is eligible for starts.

## DeploymentRelease

Reserved for shared platform services such as the API and human-task persistence
service. Workflow-package release ownership is represented by WorkflowRelease.

- `service`
- `release_version`
- `image_digest`
- `worker_version`
- `activity_contract_versions`
- `environment`
- `definition_dependencies`
- `promoted_by`
- `promoted_at`
- `rollback_reference`

## Target invariants

- Release closure covers the exact selected definition and every transitive
  executable Workflow/Activity dependency; the generic executor validates a
  trusted installed manifest before polling.
- Any upgraded release explicitly includes that execution's exact older
  definition hash/content and full executable closure; a newer primary revision
  is insufficient compatibility evidence.
- All replicas on a pool's queue provide compatible named registrations; separate
  roles of one release use the same artifact and Temporal version identity.
- Business starts cannot select an image, queue, executor role, or Build ID.
  Routing is authorized platform configuration resolved against the selected
  governed revision.
- The same digest is promoted through environments without rebuilding; shared
  library changes require rebuilding every affected dependent package.
- Rollout/ramp/rollback preserve idempotent reservations and old-release
  availability for long waits. Upgrades never silently change definition content.
- Business control-plane PostgreSQL stores these governance records separately
  from Temporal's externally provisioned `temporal` and `temporal_visibility`
  persistence databases.
