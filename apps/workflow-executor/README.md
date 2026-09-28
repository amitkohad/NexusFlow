# Generic workflow executor

The executor loads a workflow package published through the installed
`nexusflow.workflow_packages` distribution entrypoint group. It validates the
manifest and exact installed dependency versions before connecting or polling.
Business requests cannot choose import paths. The same artifact supports mixed,
Workflow-only and Activity-only roles, registering every handler for its role.

```text
python -m workflow_executor --package customer-adjustment --pool pool.json
```

`pool.json` is an approved `ExecutorPool` document. Each invocation launches one
process replica. Desired replicas, minimum/maximum bounds and resource settings
describe deployment capacity; this command does not create pods or an autoscaler.
Run additional identical invocations to add local replicas, choosing a distinct
`NEXUSFLOW_PROBE_PORT` per process on a shared host. Instance identities are unique
and include package, Build ID and pool. Queue names never vary by replica/build.

All role pools for a release use the same Worker Deployment name and Build ID.
The host uses modern `WorkerDeploymentConfig` and explicit per-Workflow Pinned or
Auto-Upgrade behavior. Activity eager execution is disabled. Workflow task slots,
Activity slots, both poller bounds and downstream rates are separate controls.
Selecting one Workflow poller disables the sticky cache to satisfy SDK limits;
two or more pollers enable the default cache. This does not change task slots.
SIGINT/SIGTERM removes readiness before the SDK drains in-flight work.

Environment connection settings are `TEMPORAL_ADDRESS`, `TEMPORAL_TLS`, and the
namespace/environment declared by the pool. Probe bind defaults to loopback; a
container can select `NEXUSFLOW_PROBE_HOST=0.0.0.0`. `/health` measures process
liveness and `/ready` checks running SDK workers and Temporal connectivity.
The accompanying `.env.example` lists capacity overrides. Capacity belongs to
deployment configuration and never changes the package content manifest.

For local aggregate source development only, add `--development-source` to use
the two repository fixtures without requiring separate distribution installs.
Installed artifacts always check locked dependencies. Source mode is rejected
outside local/test. Production containers, controller lifecycle and autoscaling
remain later infrastructure work.

Complete artifact verification also executes installed handlers against a local
Temporal server. Put the CLI on PATH or set `TEMPORAL_CLI_PATH`; verification does
not download a server binary or depend on external capability-worker processes.
