# Package runtime migration and rollback

Migration `0003` adds package/release/environment/pool ownership tables and nullable
private execution provenance. It leaves `0002` runtime profiles and queues intact.

| Existing binding | Required executor during coexistence | New-start configuration |
| --- | --- | --- |
| `legacy`, `LightweightProcess`, `lightweight-workflows` | Original `app.worker` | Explicit `legacy` API profile |
| `governed`, `GovernedWorkflowV1`, `workflow-orchestration-tq` and owned v1 capability queues | Original runtime and five workers | Existing `governed` profile |
| `package`, `PackageWorkflowV1` or `PackageAutoUpgradeWorkflowV1`, package-owned queues | Generic executor loading exact release artifact | Explicit `package` profile and reconciled release |

Do not rename old registrations, mutate their capability catalogue, or route their
histories to the package interpreter. Leave original workers available until their
running work and required queries are no longer retained. Historical custom queues
missing from Phase 3 metadata still require the mapping described for migration
`0002`; migration `0003` does not infer those values.

## Cutover

1. Back up the business database, inventory pending/running executions by existing
   profile/queue, and preserve their original deployments.
2. Apply migrations through `0003`; verify readiness and unchanged old bindings.
3. Build and verify the complete package artifact. Register its exact definition
   revisions without global v1 queue normalization. Publish and approve its release.
   If an existing `(scope, workflow_type, version)` contains governed global queue
   names, changing it to package logical queues changes its content hash. Author a
   new definition version and include that revision in a new package release;
   never overwrite the existing registry row. The reference package's `1.0`
   revision assumes a fresh registry slot for the local demonstration.
4. Claim stable package queues and configure pools. Launch generic executors from
   the verified artifact; reconcile actual Temporal pollers and Current/Ramping
   routing. Namespace/deployment/queue ownership is exclusive to the package owner.
5. Enable package profile for new starts. Existing reservations select their stored
   runtime, release, namespace and queues regardless of the API's new default.
   An uncertain start never removes the reservation or substitutes a newer release.
6. Record actual initial Build ID from original history and subsequent observed
   versions separately. Review routing violations before admitting further work.

The business database and Temporal do not form an atomic transaction. Desired
routing is persisted first, then Temporal is changed and reconciled. A crash or
conflict can leave routing unconfirmed. Repeat promotion or reconciliation rather
than treating the desired release as serving evidence. Build eligibility captures
an initial routing window; it is not a claim of atomic routing across systems.

## Promotion and rollback

Keep deployment identity and logical queues stable across immutable releases.
Every upgrade must include exact old revisions and their complete Activity and
Workflow registration contracts. Run replay against representative retained
histories before approval, including open approval/timer and continuation histories.
Unit/static closure validation is necessary but does not prove arbitrary code
replay compatibility.

Pinned executions and pending pinned submissions stay on their original Build ID.
AutoUpgrade executions may advance to approved compatible installed code, while
their original selected definition, task identities and reservation remain intact.
An old release rollback must contain the complete Current revision closure. If it
does not, publish a new compatible rollback build carrying that closure; promotion
rejects revision or handler removal. Do not retire an old release merely because
new starts use a newer one.

Business signals/status/cancellation follow a Continue-As-New chain only after
verifying it belongs to the original reserved execution. Continuing at a completed
step inherits version policy and copies decisions/results/transition state.
Pinned explicit code upgrades require later approved override-removal operations;
Phase 4A rejects that policy.

The retirement API requires server drainage and rejects releases still Current,
Ramping, or needed by pending/nonterminal reservations. Retain workers for query
retention policy even after drainage; advanced retained-query lifecycle and
controller deletion are later work. Current metadata/configuration never asserts
that GKE scaling, deletion or scheduling happened.

For configuration rollback, retain the Phase 4A-compatible API and schema `0003`,
and select `governed` or `legacy` as the default for new starts. Existing package
executions still need package-aware status, signals and recovery. A pre-4A API
binary cannot read their runtime profile; deploying it over the same execution
inventory can break listing and status. A binary rollback therefore requires a
retained package-aware API for package operations and isolated routing/inventory
for the older API. Never downgrade away package provenance while package execution
rows exist. The migration deliberately refuses that destructive downgrade. An
empty package execution inventory permits schema downgrade/re-upgrade for local
tests.
