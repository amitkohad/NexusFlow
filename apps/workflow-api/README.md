# Governed Workflow API

Phase 3 implements a FastAPI control plane for validated definitions and business
executions. PostgreSQL persists immutable revisions, active environment promotions,
idempotent start reservations, internal runtime identifiers, and business audit
events. SQLite is available for local development and tests.

The HTTP interface exposes business identifiers, states, definition versions, and
audit links. It does not require client access to Temporal. The API submits the
existing `LightweightProcess` by its registered name; it never imports a worker.
Phase 4 will extract the interpreter and capability workers.

Follow the [local walkthrough](../../docs/development/workflow-api.md) to migrate
the database, start the service, register/approve/promote a definition, and start
an execution. The [API contract](../../specs/001-enterprise-workflow-platform/contracts/api.md)
documents permissions, status codes, pagination, and compatibility limits.

`workflow_api.api.create_app(repository, backend, authenticator=...)` is the
injection boundary. Authentication denies access by default. The standalone
`workflow_api.main:create_app_from_env` factory supports explicitly configured
local/test bearer identities. Deployed identity providers implement
`Authenticator`; enterprise OIDC integration remains Phase 8.

Run database migrations separately from API startup:

```text
uv run --locked python -m workflow_api.manage upgrade
uv run --locked uvicorn workflow_api.main:create_app_from_env --factory --host 127.0.0.1 --port 8000
```

Use the committed lock and root development tools. The root distribution includes
this service and its migrations for this increment; independent containers and
release artifacts remain Phase 9.
