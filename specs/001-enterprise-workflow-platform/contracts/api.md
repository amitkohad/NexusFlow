# Governed Workflow API Contract

Phase 3 provides the `/api/v1` business boundary. The executable schemas are
Pydantic contracts in `libs/contracts/src/contracts/api.py`; FastAPI generates
OpenAPI at `GET /api/v1/openapi.json`. This document records protocol semantics
and scope rather than duplicating the generated schema.

The operations and semantics marked implemented below describe the current
Phase 3–4 API. The package/release/executor operations in the target section are
Phase 4A planning contracts only: they are not implemented, published in OpenAPI,
or permission names accepted by the current identity provider. Updating this
document does not change current HTTP behavior.

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

## Implemented Phase 3–4 operations

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

The implemented Phase 4 profile defaults to the dedicated `GovernedWorkflowV1` runtime with the sample capabilities
`validate_request`, `risk_check`, `post_adjustment`, `send_notification`, and
`record_rejection`. Registration resolves and freezes catalog-owned Activity
queues and contract version `1.0`, publishes matching dependency metadata, and
supports deterministic input templates. Explicit unsupported versions, unowned
queues, or mismatched dependency sets are rejected. Approval creation routes to
the human-task worker internally. Compensation remains deferred to Phase 6.
The explicit `legacy` profile retains the prototype constraints: no dependency
contracts, explicit Activity routes/versions, or input templates. Fresh legacy
starts against a revision requiring governed features return `409`.

This fixed global queue catalogue is historical Phase 4 behavior. The Phase 4A
target replaces deployment ownership with workflow packages and manifest-resolved
pool queues. It must preserve immutable existing revisions and the routing of
running executions rather than rewriting their stored documents in place.

## Start and idempotency

`StartWorkflowRequest` requires business scope, `business_reference`,
`correlation_id`, and `idempotency_key`. `definition_version` can be omitted to
resolve the promoted version for the API environment. `request` and `variables`
are JSON objects with independent empty defaults. The selected immutable revision
is pinned to the execution; later promotions do not change a running workflow.
The internal runtime profile and orchestration queue are also pinned. An existing
key retains these bindings across rollout/rollback; runtime details stay out of
the public response.

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

Phase 4A adds the target package release governance and executor configuration
boundary described below. It does not add Temporal placement controls to public
business start requests or execution responses.

Phase 5 adds `/api/v1/tasks` list/get/create/claim/complete/approve/reject and
reassign/delegate/escalate/expire operations. Phase 6 adds operator remediation,
resume, and `POST /api/v1/workflows/{workflow_id}/terminate` with a reason.
These operations are not part of the Phase 3 generated OpenAPI.

## Target package and release operations (Phase 4A, not implemented)

A WorkflowPackage is the independently deployable business unit. Its embedded
manifest identifies the exact primary governed revision and explicitly included
compatible revision hashes/content, installed named Workflow/Activity
registrations, complete dependency closures, locked runtime/SDK dependencies,
logical queue ownership, executor roles, a precomputed stable Build ID, and
long-running Workflow upgrade policy. The final containing artifact/image digest
is not embedded in that manifest: an external immutable release descriptor records
the manifest hash and final artifact digest after building. Phase 4A can publish a
local artifact without an image; Phase 9 records an image digest under the same
provenance. The generic executor reads only its installed, trusted manifest.
The API never accepts Python source, dynamically executable payloads, or arbitrary
caller-supplied module imports as business definitions.

The following administrative paths are planned; final executable request/response
models and OpenAPI are introduced in Phase 4A after schema and SDK support checks.
All operations enforce authenticated business/package ownership and environment
permissions and produce audit events.

| Planned method and path | Planned permission | Semantics |
| --- | --- | --- |
| `POST /api/v1/workflow-packages` | `packages:write` | Register owned package identity and trusted manifest boundary |
| `GET /api/v1/workflow-packages` | `packages:read` | List authorized package governance records |
| `GET /api/v1/workflow-packages/{package_id}` | `packages:read` | Read owned package metadata |
| `POST /api/v1/workflow-packages/{package_id}/releases` | `releases:write` | Publish external immutable artifact descriptor/manifest metadata with closure and replay validation evidence; image metadata begins in Phase 9 |
| `GET /api/v1/workflow-packages/{package_id}/releases/{release_version}` | `releases:read` | Read artifact, registration, compatibility, and environment status |
| `POST /api/v1/workflow-packages/{package_id}/releases/{release_version}/approve` | `releases:approve` | Approve a validated release for the authorized environment |
| `POST /api/v1/workflow-packages/{package_id}/releases/{release_version}/promote` | `releases:promote` | Promote the same digest to approved serving/ramping routing; never rebuild |
| `POST /api/v1/workflow-packages/{package_id}/releases/{release_version}/ramp` | `releases:promote` | Adjust bounded current/ramping routing through version-aware deployment tooling |
| `POST /api/v1/workflow-packages/{package_id}/releases/{release_version}/retire` | `releases:retire` | Retire only after outstanding version obligations are satisfied |
| `GET /api/v1/workflow-packages/{package_id}/executor-pools` | `executors:read` | Observe authorized role/release pool configuration and serving/draining state |
| `PUT /api/v1/workflow-packages/{package_id}/executor-pools/{pool_id}` | `executors:configure` | Set validated desired capacity, resources, and scaling bounds through the deployment boundary |

Publishing governance metadata does not itself deploy a process. Deployment
tooling reconciles approved desired state, validates Temporal routing, and reports
serving readiness. API success must distinguish accepted reconciliation from a
confirmed serving release. Kubernetes Worker Controller evaluation belongs to
the deployment plan; privileged APIs must not claim capacity is ready merely
because desired state was stored.

Definition governance and release governance remain distinct. Definition
registration/approval/promotion approves immutable business content. Release
publication/promotion approves executable packaging and environment routing.
Each selected revision must have a serving release that bundles that exact
revision; promoting a definition does not automatically build or deploy its code.
Changing manifest contents under an existing release version or digest is a
conflict. Missing registrations, unowned queues, mismatched definition hashes,
incompatible contracts, or unverified dependency closure reject publication or
readiness before polling.

### Target start binding and compatibility

The business `StartWorkflowRequest` retains its existing fields. A new start
resolves the requested/promoted definition revision and an approved compatible
release according to the authorized environment's current/ramping policy.
The selected intended release must contain the exact revision and dependency
closure. The business database transaction that reserves the stable execution
also stores private intended package/release provenance, artifact digest, eligible
release policy, Worker Deployment/intended initial Build ID, namespace,
Workflow/Activity queue bindings, and upgrade policy. This transaction does not
atomically commit with Temporal. Temporal's current/ramping selection can change
between reservation and submission. Backend submission/reconciliation must record
the confirmed initial version separately, verify it against the captured eligible
policy and exact revision/closure, and use a supported version override when
concrete placement is required. A stored Build ID alone does not control server
routing or prove which version executed.

A retry uses the original revision and intended provenance even when definition
or release routing has changed, resolves uncertain submission by the stable
execution ID, and never silently selects a new revision/release. Unconfirmed or
incompatible observed routing is reconciled safely or rejected, not labelled
success merely because a reservation exists. No business request may select
private placement fields.

Target errors distinguish a revision with no approved compatible release
(`409`, planned `package_release_unavailable`) from a configured compatible
release whose required execution dependencies are unavailable (`503`). These
codes are planning semantics, not newly implemented responses. A failed release
resolution must not create a start reservation bound to some other revision.

Public business responses continue to expose the stable workflow ID, selected
definition version, state, correlation, and business links, excluding package
placement, image digests, queue names, Build IDs, executor roles, and raw Temporal
history. Privileged release/executor views expose the operational metadata needed
for rollout and capacity management.

### Target executor and rollout rules

- Mixed Workflow/Activity executors are the default. Workflow-only and
  Activity-only pools may be configured for isolation, but every role in a release
  uses the same immutable image and Worker Deployment Version/Build ID.
- Queue bindings are stable per package/pool/environment namespace and validated
  against ownership. New executions, replicas, and releases do not create queues
  solely for version routing. Compatible named handlers must exist on all
  replicas polling each role's queues.
- Replica bounds and per-process Workflow/Activity task concurrency are separate
  capacity settings. Each production serving queue has at least two compatible
  polling replicas. Scaling policies use measured backlog, pickup latency, slots,
  and resource signals; open Workflow count alone is insufficient.
- Temporal Worker Versioning supports simultaneous current/ramping/retained
  releases. Package Activity queues participate in the same deployment version.
  Rollback changes future authorized routing while preserving recorded starts and
  required compatible capacity for outstanding executions.
- Each Workflow type declares Temporal Pinned or Auto-Upgrade behavior.
  Continue-As-New upgrade is a separate policy, not a third enum; a Pinned
  execution inherits its version at that boundary by default until a supported
  explicit upgrade mechanism is approved. Replay/patch checks gate upgrades.
  Every eligible new release must include the selected old definition's exact
  hash/content and full handlers/dependency closure, not just a newer primary
  revision or claimed version range. Missing old content rejects an upgrade.
  Intended release provenance remains immutable while confirmed placement and
  approved code-version transitions are recorded separately.
- Retirement is guarded by evidence that pinned executions, open approvals,
  timers, and pending submissions can finish on retained compatible capacity or
  have undergone a validated authorized migration. Stopping new starts is not
  sufficient evidence that an old release can be removed.

The shared human-task persistence service remains outside the workflow package.
The package contains its executable Activity adapter and contract; Phase 5 defines
the task API and durable lifecycle. The same distinction applies to enterprise
systems called by packaged integration Activities.
