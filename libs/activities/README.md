# Package-local reference Activities

This library supplies six named, typed `.pkg.v1` Activities. Workflow artifacts
install the library with exact dependency versions and register the functions
explicitly in their trusted manifest. It has no dependency on capability-worker
services, the API, or the root development distribution.

The handlers retain Phase 4 reference behavior: notification and enterprise
posting are mock adapters; approval creation returns a stable reference without
a task datastore. Persistent task lifecycle remains Phase 5. Payloads carry the
workflow context, frozen release binding and retry-stable idempotency identity.
Original `.v1` workers remain available for their existing histories.
