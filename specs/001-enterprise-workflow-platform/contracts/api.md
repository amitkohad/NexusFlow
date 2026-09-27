# Governed Workflow API Contract

Phase 3 provides the `/api/v1` business boundary. The executable schemas are
Pydantic contracts in `libs/contracts/src/contracts/api.py`; FastAPI generates
OpenAPI at `GET /api/v1/openapi.json`. This document records protocol semantics
and scope rather than duplicating the generated schema.

## Identity and scope

Every business operation requires `Authorization: Bearer <token>`. The API
application receives an identity provider that resolves the token to an
authenticated actor, tenant, business domain, application, and permissions.
Unconfigured authentication denies access. A local token provider is explicitly
configured through the application factory for development and tests; enterprise
OIDC validation is an injectable boundary and must be configured for deployment.

Scope is always enforced against the authenticated principal. A registration,
approval, promotion, or start body includes `tenant`, `business_domain`, and
`application`, which must match that principal. Listing and execution operations
derive their scope from authentication. A client cannot supply an actor,
authorization claims, or runtime handle in the body. Resources from other scopes
are hidden with `404`; insufficient permission for an operation returns `403`.

Permissions are `workflows:read`, `workflows:start`, `workflows:signal`,
`workflows:cancel`, `definitions:read`, `definitions:write`,
`definitions:approve`, and `definitions:promote`.

## Implemented Phase 3 operations

| Method and path | Permission | Request | Success response |
| --- | --- | --- | --- |
| `POST /api/v1/workflows` | `definitions:write` | `RegisterDefinitionRequest` | Registered `WorkflowDefinition` |
| `GET /api/v1/workflows` | `workflows:read` | Pagination query | `ExecutionPage` |
| `POST /api/v1/workflows/{workflow_type}/start` | `workflows:start` | `StartWorkflowRequest` | `WorkflowExecutionResponse` |
| `GET /api/v1/workflows/{workflow_id}` | `workflows:read` | None | `WorkflowExecutionResponse` |
| `GET /api/v1/workflows/{workflow_id}/status` | `workflows:read` | None | `WorkflowStatusResponse` |
| `GET /api/v1/workflows/{workflow_id}/history` | `workflows:read` | Pagination query | `HistoryPage` |
| `POST /api/v1/workflows/{workflow_id}/signal` | `workflows:signal` | `SignalWorkflowRequest` | `SignalResponse` |
| `POST /api/v1/workflows/{workflow_id}/cancel` | `workflows:cancel` | `CancelWorkflowRequest` | `CancelResponse` |
| `GET /api/v1/definitions` | `definitions:read` | Pagination query | `DefinitionPage` |
| `GET /api/v1/definitions/{workflow_type}/{version}` | `definitions:read` | None | `WorkflowDefinition` |
| `POST /api/v1/definitions/{workflow_type}/{version}/approve` | `definitions:approve` | `ApproveDefinitionRequest` | Approved `WorkflowDefinition` |
| `POST /api/v1/definitions/{workflow_type}/{version}/promote` | `definitions:promote` | `PromoteDefinitionRequest` | Promoted `WorkflowDefinition` |

`GET /health` reports process health. `GET /ready` checks configured dependencies
and returns an unavailable response when they cannot serve requests.

## Definition registration and promotion

`RegisterDefinitionRequest` requires business scope, `definition_id`,
`workflow_type`, `version`, `owner`, and a typed `definition_document`.
`dependencies` defaults to an empty array of `DefinitionDependency` objects.
The service computes the content hash and injects creation actor and timestamp;
the client cannot claim approval or lifecycle state. Registration runs schema
and graph validation. An invalid graph returns actionable validation issues.

The minimum Phase 3 lifecycle is registration/validation, approval, and promotion.
Approval records the authenticated actor and timestamp. Promotion takes an
`environment` in `local`, `dev`, `test`, or `prod` (default `local`) and records
the authenticated actor, timestamp, and active version for that scope and
environment. Revision changes cannot silently rewrite a registered version.
Promotion must target the environment configured for this API instance.
Full deprecation, retirement, and capability governance remain Phase 7 work.
Definition responses are administrative artifacts and include the governed
document and dependency routing; execution responses contain business data only.

Phase 3 uses the existing prototype runtime with the sample capabilities
`validate_request`, `risk_check`, `post_adjustment`, `send_notification`, and
`record_rejection`. Its registration profile rejects unknown capabilities,
nonempty dependency contracts, explicit Activity task queues or contract versions,
compensation, and Activity input templates. These fields are typed for later
runtime delivery, but registration must reject requirements the configured
runtime cannot honor. Independent queue routing and versioned worker contracts
arrive in Phase 4; compensation and its failure policies arrive in Phase 6.

## Start and idempotency

`StartWorkflowRequest` requires business scope, `business_reference`,
`correlation_id`, and `idempotency_key`. `definition_version` can be omitted to
resolve the promoted version for the API environment. `request` and `variables`
are JSON objects with independent empty defaults. The selected immutable revision
is pinned to the execution; later promotions do not change a running workflow.

A new start returns `202`. A repeated request for the same scoped workflow type
and idempotency key returns the existing stable workflow ID and pinned definition
version with `replayed: true` (`200` after the original submission is confirmed).
Reuse with different business input or an explicitly different version returns
`409`. Retrying without an explicit version continues to use the original pinned
revision even if promotion has changed. The stored start reservation and stable
runtime ID allow a retry after an uncertain backend response without creating a
second execution. A recovered backend submission can return `202` while its
submission is being reconciled.

## Business execution and history responses

`WorkflowStatusResponse` includes workflow ID and type, definition ID and version,
business scope, business reference, correlation ID, business execution state,
nullable current step, start/update/completion timestamps, nullable failure code
and summary, and `links` (`self`, `status`, `history`). Timestamps have an explicit
UTC offset. `WorkflowExecutionResponse` adds the `replayed` indicator, defaulting
to `false`.

Execution and history schemas exclude Temporal run IDs, task queues, worker
names, backend type, and raw engine history. `HistoryPage.items` contains
`BusinessAuditEvent` projections: event ID/type, business scope, workflow ID/type,
definition version, business reference, step, previous/new state, authenticated
actor, correlation ID, timestamp, and business metadata. The service sanitizes
failure summaries and emits only suitable business metadata. History is a stored
business projection refreshed by observation; Phase 3 does not promise complete
capture of every runtime transition between status polls. `HistoryPage.refreshed`
is `true` when that request observed the runtime. If the runtime is unavailable
or its execution is no longer retained, stored history remains readable with
`refreshed: false`; clients can distinguish the retained projection from a fresh
runtime observation.

Lists and history accept `limit` from 1 to 100 and a nonnegative `offset`.
Responses contain `items`, `limit`, `offset`, and nullable `next_offset`.
Pagination is deterministic over the stored projection; ongoing executions can
add history between requests.

## Signals and cancellation

`SignalWorkflowRequest` supports the existing sample approval signal:
`signal: "approve"` (default), required boolean `approved`, and optional `comment`
(default empty, at most 2000 characters). The signal actor comes from the
authenticated principal. A positive decision approves; `approved: false` rejects.
The acceptance response identifies the workflow and correlation ID. Acceptance
acknowledges dispatch, not completion of the workflow. The durable task lifecycle,
assignment, evidence, and task completion guarantees belong to Phase 5.

`CancelWorkflowRequest.reason` is nonempty, at most 2000 characters, and defaults
to `Client cancellation`. Cancellation requires its own permission and dispatches
cooperative runtime cancellation. The response acknowledges an accepted
cancellation request; clients observe its resulting state through the status URL.
The business audit event records the authenticated actor and cancellation reason.

## Validation, correlation, and problems

Models are closed (`extra: forbid`) and fields are frozen after validation.
String and boolean fields reject scalar coercion. Scope names and workflow types
are at most 128 characters; correlation IDs are at most 128; public identifiers,
business references, idempotency keys, and owners are at most 256; definition
versions are at most 64. Nonempty fields must contain a nonwhitespace character.
Workflow types, definition versions, and correlation IDs start with an ASCII
letter or digit and otherwise contain ASCII letters, digits, `_`, `.`, `:`, or `-`.
JSON payloads reject nonfinite numbers, cycles, non-JSON objects, and nesting
beyond the shared 64-level boundary. Request byte-size limits are enforced by the
API in addition to contract validation.

The API propagates a safe `X-Correlation-ID` and uses the start body's
`correlation_id` for the execution's business identity. Error bodies use
`application/problem+json` with `ProblemDetails`: `type` (default `about:blank`),
`title`, HTTP `status`, sanitized `detail`, request-path `instance`, stable `code`,
`correlation_id`, and optional `issues` (default empty). A validation issue
contains code, path, and a safe message without echoing confidential input.

| Status | Meaning |
| --- | --- |
| `401` | Missing or invalid authentication |
| `403` | Insufficient permission or request scope mismatch |
| `404` | Resource absent in the authorized scope |
| `409` | Immutable revision, lifecycle, or idempotency conflict |
| `413` | Request body exceeds the configured size limit |
| `422` | Contract, graph, or pagination validation failure |
| `500` | Unexpected service failure with a sanitized message |
| `503` | Required persistence or workflow backend unavailable |

Unexpected exceptions return sanitized problems without stack traces, backend
addresses, credentials, or raw exception messages.

## Later operations

Phase 5 adds `/api/v1/tasks` list/get/create/claim/complete/approve/reject and
reassign/delegate/escalate/expire operations. Phase 6 adds operator remediation,
resume, and `POST /api/v1/workflows/{workflow_id}/terminate` with a reason.
These operations are not part of the Phase 3 generated OpenAPI.
