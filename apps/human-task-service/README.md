# Human task service

This standalone service owns persistent task assignment, audit events and the
outbox that resumes workflows. Workflow packages contain only the HTTP Activity
adapter; they never embed this service or its database. Apply Alembic migration
`0004` to the same business PostgreSQL database used by the workflow control
plane. `TaskRepository.create_schema()` is for local tests only.

For local development, set the variables in `.env.example` in the process
environment, start Temporal and the business database, then run
`uv run --locked python -m human_task_service`. The two distinct tokens are
explicit local/test credentials: the service token permits task creation only,
and the actor token permits inbox, decision and management actions. Production
must inject a verified identity provider
through `create_app` and run an equivalent outbox/SLA dispatcher loop. Set the
package Activity adapter's `NEXUSFLOW_HUMAN_TASK_SERVICE_URL` to this service
and `NEXUSFLOW_HUMAN_TASK_SERVICE_TOKEN` to the same local credential.

The `tasks:create` permission is for the package Activity service identity.
Human actors require `tasks:read` for the inbox and `tasks:act` to claim or
decide a task. Group membership is an authenticated principal claim, never a
caller-supplied request field. `tasks:manage` permits reassignment, escalation
and expiry within the principal's tenant/domain/application scope. Request
bodies cannot select an actor. Assignments and outcome changes require row
locks and optionally `expected_version`; terminal decisions atomically add one
audited outbox event. Retries of the same create key preserve the first task
and run/release provenance across Continue-As-New and compatible upgrades.
The first execution run ID is captured separately and used as a Temporal chain
guard during dispatch. The dispatcher verifies the server's current chain and
signals its exact run. A later workflow that reuses the same workflow ID cannot
receive an old task's signal; an event for a different chain is retained as
`BLOCKED` with a `DISPATCH_BLOCKED` audit record for operator review. A
definitively closed original chain is likewise blocked with `workflow_closed`;
Continue-As-New handoff and transient delivery errors remain retryable.
If Temporal can no longer find the workflow ID, the event is blocked with
`workflow_missing`; other Describe errors remain retryable.

An SLA deadline before the task due time can escalate to a configured group.
`extend_seconds` opens one further SLA interval, capped by the original due
time; a second breach expires the task. Without an extension, an escalated task
remains available until the original due time. An actor cannot claim or decide
after either threshold has passed, even before the next sweeper iteration.

The background loop expires due tasks and retries outbox dispatch. A signal may
be sent again after an uncertain delivery, so it carries a stable event ID and
task ID. The Workflow validates task correlation and deduplicates the event;
the task service marks an outbox event delivered only after Temporal accepts
the signal. The task API returns the committed outcome before dispatch, and a
temporary Temporal outage does not roll the task back.

For durable package approvals, this service is the deadline authority. Its
transactional decision or expiry becomes the Workflow's terminal task signal;
the Workflow has no competing approval timer. A committed decision therefore
survives delayed signal delivery across the task deadline. Workflow
cancellation/termination currently leaves an open inbox task until a later
task policy transition; reconciling those orphaned tasks is Phase 6 work.
