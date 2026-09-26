# Planning Quickstart

This document describes the intended implementation validation path. It does not add or run application code.

## Local Flow

1. Start a local Temporal development server.
2. Start the workflow-runtime worker.
3. Start validation, integration, notification, and human-task workers independently.
4. Start workflow-api and human-task-service.
5. Register and promote the customer-adjustment definition.
6. Start a workflow through `/api/v1/workflows/customer-adjustment/start`.
7. Confirm validation and risk-check execution, including the transient retry scenario.
8. Retrieve the human task, approve it through the task API, and verify workflow resumption.
9. Verify business audit events, structured logs, final status, and notification result.

## Static Delivery Checks

- Run unit, contract, integration, and E2E tests.
- Build each service image independently.
- Run Helm lint/template checks for every release.
- Run `terraform fmt -check` and `terraform validate` per environment.
- Parse and validate GitHub Actions YAML.
- Scan source and images for secrets and vulnerabilities.

## Cloud Validation

When GCP credentials and approvals are available, provision the selected environment with Terraform, initialize/upgrade Temporal schemas in Cloud SQL, install Temporal and application Helm releases, run smoke/integration/E2E tests, inspect health and queue metrics, and validate rollback. Cloud steps remain unverified until executed in an authorized environment.