# API Contract Outline

All endpoints are versioned under `/api/v1`, require authenticated identity context, support correlation IDs, and return consistent problem details. Exact schemas are implementation tasks and must be generated as OpenAPI.

## Workflow Operations

- `POST /api/v1/workflows`: register or submit a definition revision.
- `POST /api/v1/workflows/{workflowType}/start`: start an approved definition using idempotency and business context.
- `GET /api/v1/workflows/{workflowId}`: retrieve execution metadata.
- `GET /api/v1/workflows/{workflowId}/status`: retrieve business status and current step.
- `GET /api/v1/workflows/{workflowId}/history`: retrieve business audit history, not raw Temporal history.
- `POST /api/v1/workflows/{workflowId}/signal`: send an authorized business signal/update.
- `POST /api/v1/workflows/{workflowId}/cancel`: request policy-controlled cancellation.
- `POST /api/v1/workflows/{workflowId}/terminate`: operator-only termination with reason.

## Task Operations

- `GET /api/v1/tasks`: list authorized tasks with filters and pagination.
- `GET /api/v1/tasks/{taskId}`: retrieve task, form, SLA, and assignment data.
- `POST /api/v1/tasks/{taskId}/claim`: claim an available task.
- `POST /api/v1/tasks/{taskId}/complete`: complete with outcome and evidence.
- `POST /api/v1/tasks/{taskId}/approve`: approve a task.
- `POST /api/v1/tasks/{taskId}/reject`: reject a task.
- `POST /api/v1/tasks/{taskId}/reassign`: reassign to a user/group.
- `POST /api/v1/tasks/{taskId}/delegate`: delegate under policy.
- `POST /api/v1/tasks/{taskId}/escalate`: trigger escalation.

## Platform Operations

- `GET /health`: process health.
- `GET /ready`: dependency/readiness state.
- `GET /api/v1/openapi.json`: generated API contract.

## Required Request Context

`tenant`, `business_domain`, `application`, `business_reference`, `correlation_id`, `idempotency_key`, and authenticated actor/service identity where applicable.