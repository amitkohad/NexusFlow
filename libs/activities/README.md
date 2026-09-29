# Package-local reference Activities

This library supplies six named, typed `.pkg.v1` Activities. Workflow artifacts
install the library with exact dependency versions and register the functions
explicitly in their trusted manifest. It has no dependency on capability-worker
services, the API, or the root development distribution.

The handlers retain reference behavior for notification and enterprise posting.
Approval creation is a durable adapter to the shared human-task service: set
`NEXUSFLOW_HUMAN_TASK_SERVICE_URL` and a `tasks:create` Bearer credential in
`NEXUSFLOW_HUMAN_TASK_SERVICE_TOKEN` on package Activity executors. The adapter
posts workflow context, frozen release provenance, assignment and timeout with a
retry-stable idempotency key, then returns the persisted task ID. It fails clearly
when service configuration is absent; it never invents a task reference.
The Activity stays registered on the package-owned queue and deploys with its
Workflow package rather than a global task worker.
Original `.v1` workers remain available for their existing histories.
