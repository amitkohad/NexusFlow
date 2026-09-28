# NexusFlow workflow platform

Phase 4 adds a dedicated orchestration runtime and five independently packaged
capability workers. The governed API provides persistent definition approval and
promotion, idempotent starts, business status/history, and scoped authorization.
Follow the [runtime and workers walkthrough](docs/development/runtime-workers.md)
and [API walkthrough](docs/development/workflow-api.md). The CLI prototype below
remains available for legacy demonstrations and existing histories.

A small working prototype for replacing Alfresco Process Services-style lightweight workflows with an in-house framework built on Temporal.

The prototype intentionally separates **process definition** from **enterprise capability execution**:

- A process is supplied as JSON and interpreted by a generic Temporal Workflow (`LightweightProcess`).
- Business/system operations are invoked through one governed Activity dispatcher (`execute_capability`).
- Human approvals are modeled as Temporal Signals.
- Workflow status is exposed as a Temporal Query.
- Routing uses declarative decision steps.
- Temporal Activity retries demonstrate resilience to transient downstream failures.
- Temporal Event History provides the durable execution trail.

## What the demo proves

| Lightweight workflow need | Prototype mechanism |
|---|---|
| Sequential steps | Declarative `activity` steps |
| Human task / approval | `approval` step + Temporal Signal |
| Gateway / routing | `decision` step |
| Timer / wait | `timer` step |
| Integration with enterprise services | Capability dispatcher Activity |
| Retry / timeout | Activity RetryPolicy + timeouts |
| Long-running process state | Temporal durable Workflow state |
| Runtime status | Temporal Query `status` |
| Audit/execution history | Temporal Event History / Web UI |
| Versioned process definitions | Phase 3 registry, approval/promotion and execution version pinning |

## Prerequisites

- Python 3.11+
- Temporal CLI on PATH

Temporal CLI includes a development service. Start it with:

```bash
temporal server start-dev
```

The Temporal Web UI is available at `http://localhost:8233` when using the default dev-server settings.

## Setup

For the reproducible development environment and baseline checks, use
`uv sync --locked` and follow [development tooling](docs/development/tooling.md).
The pip instructions below install the generated runtime-only requirements;
development tools and tests are installed through uv.

### macOS/Linux

```bash
cd temporal-lightweight-workflow-prototype
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Windows PowerShell

```powershell
cd temporal-lightweight-workflow-prototype
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Run the prototype

Use three terminals.

### Terminal 1 — Temporal development server

```bash
temporal server start-dev
```

### Terminal 2 — Worker

```bash
python -m app.worker
```

Expected output:

```text
Worker connected to localhost:7233; polling task queue 'lightweight-workflows'
```

### Terminal 3 — CLI demo

macOS/Linux:

```bash
./scripts/demo.sh
```

Windows PowerShell:

```powershell
.\scripts\demo.ps1
```

The sample amount is deliberately high enough to route to a manager approval. The `risk_check` Activity also fails once on purpose so you can see Temporal retry it automatically.

## Manual CLI walkthrough

Start a workflow:

```bash
temporal workflow start \
  --workflow-id lw-1001 \
  --type LightweightProcess \
  --task-queue lightweight-workflows \
  --input-file examples/customer_adjustment.json
```

Query its live state:

```bash
temporal workflow query \
  --workflow-id lw-1001 \
  --name status
```

Approve the human task:

```bash
temporal workflow signal \
  --workflow-id lw-1001 \
  --name approve \
  --input '{"approved":true,"approver":"ops.manager","comment":"approved"}'
```

Reject instead:

```bash
temporal workflow signal \
  --workflow-id lw-1001 \
  --name approve \
  --input '{"approved":false,"approver":"ops.manager","comment":"insufficient evidence"}'
```

Wait for the final output:

```bash
temporal workflow result --workflow-id lw-1001
```

Inspect the durable history:

```bash
temporal workflow show --workflow-id lw-1001 --detailed
```

List executions:

```bash
temporal workflow list
```

Cancel an in-flight process:

```bash
temporal workflow cancel --workflow-id lw-1001
```

## Process definition model

The sample is a small state-machine DSL:

```json
{
  "start_at": "validate",
  "steps": {
    "validate": {"type":"activity","capability":"validate_request","next":"risk"},
    "route": {
      "type":"decision",
      "field":"results.risk.risk_score",
      "operator":"<=",
      "value":50,
      "on_true":"post",
      "on_false":"manager_approval"
    },
    "manager_approval": {
      "type":"approval",
      "assignee_group":"operations-managers",
      "on_approved":"post",
      "on_rejected":"rejected"
    }
  }
}
```

For a production framework, the JSON/YAML DSL should be generated by a visual process designer, validated against a schema, versioned, approved, and promoted through environments.

## Next increments after the prototype

Phase 4A now provides complete workflow-package artifacts and generic executors.
Customer Adjustment includes its definition, runtime and all six named Activities;
a second package fixture demonstrates independent queues and capacity. Build and
verify with `uv run --locked python scripts/build_workflow_packages.py --verify`.
Use the explicit `package` API profile for new release-bound starts. See
[package setup](docs/development/workflow-packages.md) and
[migration/rollback mapping](docs/operations/package-migration.md). Original
legacy and governed workers remain available for their existing histories.

Phase 2 foundations are available as shared libraries: typed contracts,
definition/schema validation, deterministic routing/template helpers, and explicit
configuration/error models. Validate the existing sample with:

```text
uv run --locked python -m workflow_sdk.definitions examples/customer_adjustment.json
```

See [development tooling](docs/development/tooling.md),
[definition semantics](docs/architecture/definition-semantics.md), and
[foundation decisions](docs/adr/0001-foundational-contracts.md). The prototype
worker retains its original execution path until the runtime extraction phase.

1. Implement persistent human-task management and its policies (Phase 5).
2. Extend the FastAPI control plane with later task, governance and operational policies.
3. Add a human-task service and web inbox so users never need Temporal CLI.
4. Replace the mock capability dispatcher with a plugin/adapter SDK for REST, gRPC, Kafka, files, databases, notifications, and enterprise APIs.
5. Add OIDC/RBAC, secrets integration, encryption/data converters, observability, SLOs and chargeback/showback.
6. Run workers on Kubernetes with separate task queues and autoscaling per workload class.
7. Add worker versioning, safe rollout, process-definition versioning and migration tooling.
8. Build an Alfresco migration factory: inventory -> classify -> convert -> regression-test -> dual-run -> cutover.

## Important production design principle

Do **not** expose Temporal concepts directly to citizen/process developers. Temporal should be the durable orchestration engine underneath a governed workflow product. The in-house framework owns process modeling, forms/tasks, identity, integrations, policy, versioning, audit views and operational experience.
