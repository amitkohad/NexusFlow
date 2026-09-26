# Refactoring assessment

Assessed on 2026-09-26 against the existing prototype and
`specs/001-enterprise-workflow-platform/`. Phase 1 follows the task list's
Setup scope, T001-T006: document the current system, establish tooling and
configuration conventions, and protect behavior before introducing services.

## Current system

The repository contains a Python Temporal prototype rather than a platform
API. A client starts `LightweightProcess` directly through Temporal and passes
the entire JSON definition as its input. One worker registers that workflow
and the `execute_capability` Activity on `lightweight-workflows`.

The interpreter maintains a request, variables, and accumulated results. It
executes activity, decision, approval, timer, and end steps. Workflow-side
waiting, transitions, and timestamps use Temporal APIs; simulated external
work remains inside the Activity. Status is available through the `status`
query, and approval arrives through the `approve` signal. There is no
definition registry, platform HTTP API, human-task persistence, authenticated
task actor, business audit store, or independent capability worker yet.

## Files and target ownership

Target paths are the planned boundaries, not services created during setup.

| Current file or behavior | Current responsibility | Planned owner and migration direction |
| --- | --- | --- |
| `app/workflows.py`: `LightweightProcess.run` | Interprets raw definitions and schedules all steps. | `apps/workflow-runtime/` worker bootstrap and runtime modules; deterministic interpreter semantics in `workflows/common/`. Preserve behavior before extraction. |
| `app/workflows.py`: validation, path lookup, comparison | Minimal input checks and inline routing helpers. | Definition models in `libs/contracts/`; parsing/graph validation in `libs/workflow-sdk/`; schema in `workflows/definitions/schema/`; deterministic routing in `workflows/common/`. |
| `app/workflows.py`: `status` and transitions | Direct Temporal query and in-memory business state. | Runtime state contract plus `apps/workflow-api/` execution metadata and business status/history endpoints. Business audit events have separate ownership in `libs/observability/` and control-plane services. |
| `app/workflows.py`: approval signal/wait | One mutable approval value and Temporal timeout. | `apps/human-task-service/` for task lifecycle/persistence; `workers/human-task-worker/` for task Activities; runtime for durable waiting and correlated resumption. |
| `app/activities.py`: `validate_request` and `risk_check` branches | Demo validation/risk logic, including transient failure simulation. | Owned typed Activity contracts and queues. Generic validation belongs to `workers/validation-worker/`; sample business-specific risk logic can remain reference-only in `workers/sample-business-worker/` until a real capability owner is selected. |
| `app/activities.py`: `post_adjustment` and `record_rejection` branches | Returns synthetic posting/rejection results. | Reference business behavior in `workers/sample-business-worker/`; actual external adapters in `workers/integration-worker/` when downstream contracts are known. |
| `app/activities.py`: `send_notification` branch | Returns a synthetic notification result. | `workers/notification-worker/` with its owned queue, input/output contract, and safe side-effect policy. |
| `app/worker.py` | Connects to Temporal and hosts all prototype work. | Dedicated `apps/workflow-runtime/` worker on `workflow-orchestration-tq` and independent worker bootstraps on owned queues. |
| `examples/customer_adjustment.json` | Raw compatibility fixture passed at start. | Preserve as the baseline fixture; introduce governed definitions and examples under `workflows/definitions/` and `workflows/examples/` in later phases. |
| `scripts/demo.sh`, `scripts/demo.ps1` | Direct CLI start/query/signal/result/history smoke demos. | Retain for prototype diagnostics; add governed API/task acceptance scripts after those services exist. |
| Original `requirements.txt` | Broad Temporal SDK dependency constraint. | Python package metadata, pinned dependency resolution, pytest, Ruff, mypy, and build conventions in Phase 1; requirements becomes a generated runtime export. Future services need independent dependency/build metadata. |
| `README.md`, `docs/technical-notes.md`, architecture SVG | Prototype walkthrough and target concepts. | Development/architecture/operations documents and later platform/migration guidance. |
| `.specify/memory/constitution.md`, feature specs | Principles, requirements, architecture, task order, acceptance criteria. | Continue as governance artifacts; record unresolved design selections before dependent implementation. |

## Behavior to preserve as the baseline

| Concern | Observed behavior |
| --- | --- |
| Definition validation | Requires `start_at` and `steps`, then checks that `start_at` references a step. No complete schema or graph validation. |
| Context | `request` and `variables` default to empty mappings; results accumulate by step ID. Each Activity receives capability, step ID, input, and the complete current context. |
| Activity execution | Calls `execute_capability` on the workflow's queue. Default start-to-close timeout is 30 seconds; default retry limit is 3 attempts with 1-second initial interval, 10-second maximum interval, and coefficient 2. |
| Decision routing | Resolves dot-separated dictionary paths; missing paths return `None`. Supports `==`, `!=`, `>`, `>=`, `<`, `<=`, and `in`; routes through `on_true` or `on_false`. |
| Approval | Enters `WAITING_FOR_APPROVAL`, exposes step/group, waits for a signal, records approved/approver/comment values, and follows configured approval/rejection/timeout routes. Default group is `approvers`; default timeout is 300 seconds. |
| Timer | Sleeps for integer seconds, defaulting to 1, then follows `next`. |
| End/fallthrough | End steps set their outcome, default `COMPLETED`. A path ending while state is `RUNNING` also completes. Successful loop exit clears `current_step`; an approval returning no route after rejection/timeout returns early with that terminal state. |
| Transitions and result | Transitions contain step, state, detail, and Temporal workflow time. Final output contains state, process ID, results, and transitions. Status also retains workflow name and current step. |
| Demo validation | Converts amount to float and rejects nonpositive amounts with `ValueError`. |
| Demo risk | Returns score 65 for amount at least 5000, otherwise 25. The sample routes to approval when score exceeds 50. An optional flag raises on the first Activity attempt. |
| Demo posting/notification | Returns a posting reference containing step ID and attempt; notification defaults to email. These branches do not contact real systems. |

The sample fixture sets amount to 7500 and enables transient risk failure, so
its expected successful path is validation, retried risk check, decision,
manager approval, posting, notification, and `COMPLETED`.

## Baseline acceptance coverage

Tests should execute the existing workflow through Temporal using isolated
workflow IDs and queues, then assert business state, results, and transition
order. Helper-only mocks do not establish durable workflow behavior.

- Low-value request: completes without entering approval; posting and
  notification results exist.
- High-value request: query exposes `WAITING_FOR_APPROVAL` and the configured
  group; approval records the supplied actor/comment and completes.
- Rejection: records rejection and reaches `REJECTED` without posting or
  notification.
- Approval timeout: follows the timeout route and reaches `TIMED_OUT`.
- Transient risk failure: retries the first attempt and eventually succeeds.
- Timer: waits through Temporal and records the completed timer transition.
- Invalid initial definition: characterize the three checks in the existing
  validator. Unsupported steps and full schema/graph rejection remain later
  foundation tests; ordinary workflow `ValueError` currently retries workflow
  tasks instead of closing the execution.

Baseline tests protect existing compatibility; they do not certify the later
platform requirements for security, exactly-once task completion, worker
isolation, or side-effect idempotency.

## Risks and later remediation

| Current limitation | Practical consequence | Planned follow-up |
| --- | --- | --- |
| Definitions are raw mutable input with shallow checks. | Missing targets, invalid operators/types, loops, unsupported capabilities, and unreachable terminal paths can fail or hang during execution. | Phase 2 typed contracts, schema/graph validation; later definition governance and capability allow-lists. |
| One workflow and generic dispatcher share one queue/process. | Capabilities cannot be released, scaled, or isolated independently. | Independent runtime/capability services and owned queues in User Story 2. |
| Activity failure policy does not classify business errors. | Nonpositive amount and unknown capability errors can be retried under the current policy. | Typed error taxonomy and non-retryable business failure policies. |
| Approval uses untrusted signal data and Boolean coercion. | Actor/evidence are not authenticated; a truthy string is treated as approval. | Governed task API, authorization, validated outcome contracts. |
| Approval state is reset on entering a task and overwritten by later signals. | Early approval can be lost; duplicate or stale signals lack task identity and deduplication. | Correlated task state transitions, duplicate/stale completion handling, exactly-once resumption semantics. |
| Activity payload contains the complete context. | Business data enters durable history repeatedly; large results and transitions increase payload/history size. | Data classification, references for large payloads, redaction/encryption design, history limits and continue-as-new policy. |
| Posting reference includes Activity attempt. | Retries can yield different identifiers; no real downstream idempotency guarantee exists. | Stable side-effect keys, downstream correlation, timeout-after-success tests. |
| Raw Temporal history is the available execution trail. | There is no governed business audit read model or retention/access policy. | Separate business audit persistence/export and platform history endpoints. |
| Runtime configuration is two environment variables only. | No explicit TLS/authentication/namespace, typed configuration, health/readiness, or production observability. | Shared configuration foundation, service identity/security, health/readiness and telemetry phases. |
| CLI scripts sleep a fixed interval before signaling. | Smoke demos do not reliably prove approval readiness and are not regression tests. | Baseline tests that observe workflow state before signaling; later API/task E2E tests. |

## Scope and deferred decisions

Phase 1 does not relocate interpreter code, change queue ownership, replace
Activity contracts, or fix approval/error semantics. Those changes require
their planned contracts and acceptance tests. The setup work provides a
reviewable baseline for later extraction.

The implementation plan's delivery-phase numbering differs from the task
list: its assessment/setup phase is named Phase 0. For this increment,
`tasks.md` Phase 1 T001-T006 is the scope reference.

API framework, control-plane/task datastore and migrations, identity provider,
Temporal hosting/version, tenant isolation, human-task policies, production
SLOs/capacity/RTO/RPO/retention, and Alfresco compatibility remain unresolved.
They do not prevent setup. Select and record them before the corresponding
implementation tasks; do not infer production choices from the prototype's
local configuration.
