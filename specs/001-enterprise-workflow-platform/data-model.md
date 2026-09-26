# Data Model: Enterprise Workflow Platform

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

## HumanTask

- `task_id`
- `workflow_id`, `run_id`, `step_id`
- `definition_version`
- `tenant`, `business_domain`, `application`
- `assignee`, `assignee_group`
- `status`
- `form_schema_version`
- `payload_reference`
- `due_at`, `sla_deadline`
- `escalation_policy`
- `outcome`, `actor`, `evidence_reference`
- `created_at`, `claimed_at`, `completed_at`, `expired_at`

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
- `task_queue`
- `input_schema`
- `output_schema`
- `retry_policy`
- `timeout_policy`
- `heartbeat_policy`
- `idempotency_required`
- `worker_compatibility`

## DeploymentRelease

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