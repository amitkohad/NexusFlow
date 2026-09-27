# Local workflow API

The API can register, approve, and promote an immutable definition, start it with
an idempotency key, and read business status/history. This increment uses the
dedicated runtime and independent mock capability workers. Authentication and runtime connectivity
are explicit; no module import creates a database or connects to Temporal.

## Start local services

Install dependencies with `uv sync --locked`. Start `temporal server start-dev`
and the six processes in the [runtime/worker walkthrough](runtime-workers.md).
API and runtime default to `workflow-orchestration-tq`; capability workers own
their individual queues. Clear an inherited legacy `TEMPORAL_TASK_QUEUE` in the
API shell before using these defaults.

In an API terminal, set the configuration and migrate the local database. These
PowerShell commands generate a fresh local bearer token without printing it:

```powershell
$env:NEXUSFLOW_DATABASE_URL = "sqlite:///nexusflow.db"
$env:NEXUSFLOW_ENVIRONMENT = "local"
$env:NEXUSFLOW_RUNTIME_PROFILE = "governed"
$env:NEXUSFLOW_API_TOKEN = uv run --locked python -c "import secrets; print(secrets.token_urlsafe(32))"
uv run --locked python -m workflow_api.manage upgrade
```

For PostgreSQL, replace the database URL with a `postgresql+psycopg://` URL to the
separately provisioned business database. The database must not be a Temporal
persistence database. Apply migrations as a controlled deployment operation.
API startup never applies schema changes. The wheel includes the migration files.

Create the headers and launch the server from this shell, so its child process
inherits the token without displaying it or placing it in process arguments:

```powershell
$headers = @{ Authorization = "Bearer $env:NEXUSFLOW_API_TOKEN" }
$apiProcess = Start-Process -FilePath (Resolve-Path ".venv/Scripts/python.exe").Path -ArgumentList @("-m", "uvicorn", "workflow_api.main:create_app_from_env", "--factory", "--host", "127.0.0.1", "--port", "8000") -WindowStyle Hidden -PassThru -RedirectStandardOutput "workflow-api.log" -RedirectStandardError "workflow-api-error.log"
Invoke-RestMethod "http://127.0.0.1:8000/ready"
```

Run the following requests in this shell once the server is ready. Check the
local log files if startup fails. `/health` checks the process; `/ready` checks the migrated
business tables and Temporal frontend. Readiness does not prove a worker is polling.
Swagger is available at `/docs`; OpenAPI is at `/api/v1/openapi.json`.
Stop the API after the walkthrough with `Stop-Process -Id $apiProcess.Id`.

## Register and execute the sample

The configured local identity defaults to actor `local-developer`, tenant `demo`,
domain `customer-services`, and application `adjustments`, with all implemented
permissions. It is a development adapter; production must supply a verified
enterprise identity provider through its own application factory.

```powershell
$base = "http://127.0.0.1:8000/api/v1"
$scope = @{ tenant="demo"; business_domain="customer-services"; application="adjustments" }
$document = Get-Content -Raw examples/customer_adjustment.json | ConvertFrom-Json
$registration = $scope + @{
    definition_id="customer-adjustment"; workflow_type="customer-adjustment"
    version="1.0"; owner="customer-services"; definition_document=$document
}
Invoke-RestMethod "$base/workflows" -Method Post -Headers $headers -ContentType "application/json" -Body ($registration | ConvertTo-Json -Depth 64)
Invoke-RestMethod "$base/definitions/customer-adjustment/1.0/approve" -Method Post -Headers $headers -ContentType "application/json" -Body ($scope | ConvertTo-Json)
Invoke-RestMethod "$base/definitions/customer-adjustment/1.0/promote" -Method Post -Headers $headers -ContentType "application/json" -Body (($scope + @{environment="local"}) | ConvertTo-Json)
$start = $scope + @{
    business_reference="adjustment-1001"; correlation_id="adjustment-1001"
    idempotency_key="adjustment-1001"; request=@{amount=1000}; variables=@{}
}
$execution = Invoke-RestMethod "$base/workflows/customer-adjustment/start" -Method Post -Headers $headers -ContentType "application/json" -Body ($start | ConvertTo-Json -Depth 64)
Invoke-RestMethod "http://127.0.0.1:8000$($execution.links.status)" -Headers $headers
Invoke-RestMethod "http://127.0.0.1:8000$($execution.links.history)?limit=50" -Headers $headers
```

The low amount completes without approval. Resending the identical start returns
the same business execution. Changing a request while reusing its key returns
`409`. New starts resolve the actively promoted version; existing executions and
retries retain their frozen revision even after another version is promoted.
Per-start request/variables replace the demonstration values in the definition.

For approval, start another request with amount `7500` and a new business
reference, correlation ID, and idempotency key. Once status is
`WAITING_FOR_APPROVAL`, post `{"signal":"approve","approved":true,"comment":"Reviewed"}`
to the execution's `/signal` endpoint. The server supplies the authenticated
approver. Full task assignment and duplicate completion policies remain Phase 5.

## Settings and verification

| API setting | Default / requirement |
| --- | --- |
| `NEXUSFLOW_DATABASE_URL` | Required; `sqlite` for local tests, `postgresql+psycopg` for deployed storage |
| `NEXUSFLOW_API_TOKEN` | Optional; absent means deny all business calls; local/test token must be at least 32 characters |
| `NEXUSFLOW_API_ACTOR` | `local-developer` |
| `NEXUSFLOW_API_TENANT` | `demo` |
| `NEXUSFLOW_API_DOMAIN` | `customer-services` |
| `NEXUSFLOW_API_APPLICATION` | `adjustments` |
| `NEXUSFLOW_RUNTIME_PROFILE` | `governed`; explicit `legacy` for the prototype |
| `TEMPORAL_TASK_QUEUE` | API defaults to `workflow-orchestration-tq`; legacy profile defaults to `lightweight-workflows` |
| `NEXUSFLOW_MAX_PAYLOAD_BYTES` | Shared limit, applied to streamed HTTP bodies; default 1 MiB |
| `NEXUSFLOW_MAX_DEFINITION_STEPS` | Registration limit, default/v1 maximum 500; may be lowered |

Other Temporal settings come from the shared [configuration](configuration.md).
The local factory supports only local/test. A deployment factory must inject its
identity provider and connect its configured runtime. Static token authentication
is rejected in dev/prod. This does not implement enterprise OIDC or production
hosting.

Run `uv run --locked python scripts/dev.py test` and
`uv run --locked python scripts/dev.py lint`. Real Temporal tests require a CLI on
PATH or `TEMPORAL_CLI_PATH`. PostgreSQL tests run when
`NEXUSFLOW_TEST_DATABASE_URL` points to an explicitly chosen test database; they
create and remove only their own UUID-named schemas. For example:

```text
uv run --locked pytest tests/integration/test_workflow_postgres.py
```

The default suite skips those database tests when no test URL is configured.
No cloud deployment is needed for the Phase 3 acceptance path.
