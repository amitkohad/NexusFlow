# Research Notes: Enterprise Workflow Platform

## Existing Repository Findings

- `app/workflows.py` contains the `LightweightProcess` Temporal workflow and inline interpreter.
- `app/activities.py` contains the `execute_capability` Activity with hard-coded demo branches.
- `app/worker.py` registers one workflow and one Activity on `lightweight-workflows`.
- `examples/customer_adjustment.json` is the compatibility fixture.
- `scripts/demo.sh` and `scripts/demo.ps1` are CLI smoke tests.
- `requirements.txt` contains only `temporalio>=1.13,<2`.
- No tests, API, persistence, package metadata, Dockerfiles, Helm, Terraform, or CI/CD exist yet.

## Spec Kit Alignment

This feature follows the Spec Kit sequence:

1. Constitution: project principles and quality gates.
2. Specify: user stories, requirements, entities, assumptions, and measurable outcomes.
3. Plan: technical context, structure, architecture, and delivery strategy.
4. Tasks: dependency-aware, independently testable implementation tasks.

The repository had no existing `.specify` directory, so this feature adds only the constitution and planning artifacts. Application implementation is intentionally deferred.

## Decisions Recommended for Planning

- Preserve Python and Temporal Python SDK to reduce migration risk.
- Use a versioned HTTP API with OpenAPI documentation and typed request/response models.
- Keep the workflow definition registry and task read model behind repository interfaces so persistence can be selected deliberately.
- Use PostgreSQL-compatible persistence for control-plane metadata where operational requirements permit, while keeping Temporal persistence separately managed.
- Start with the sample workflow and five demo capabilities as contract fixtures before adding broad adapter types.
- Treat queue ownership and worker boundaries as first-class contracts, not deployment-only concerns.

## Risks and Unknowns

- Real enterprise API idempotency guarantees are unknown; side-effect retries cannot be finalized until downstream contracts are available.
- Human-task identity, delegation policy, evidence requirements, and retention are unspecified.
- Production SLOs, throughput, history limits, RTO/RPO, and data classification are not provided.
- Alfresco process inventory and unsupported constructs are unknown.
- Temporal Cloud versus self-hosted Temporal on GKE requires an explicit decision; the stated target assumes self-hosted Temporal with Cloud SQL persistence.