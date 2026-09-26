# Test suites

- `unit/` characterizes isolated prototype validation and will hold domain-contract tests.
- `integration/` runs the current workflow and real Activities on an isolated local Temporal server.
- `contract/` is reserved for the versioned API and worker boundaries introduced in later phases.
- `e2e/` is reserved for complete platform acceptance scenarios introduced in later phases.

Run unit tests with `uv run --locked pytest tests/unit`. Integration tests require a locally
installed Temporal CLI. Put `temporal` on `PATH`, or set `TEMPORAL_CLI_PATH` to its
absolute executable path, then run `uv run --locked pytest tests/integration`.

The integration fixture starts and shuts down its own dev server, with a temporary
database and independently generated task queues and workflow IDs. It does not
connect to a shared server, start the application worker, or download a test server.
Approval waits and timers use short durations while exercising actual Temporal
timers. The server is shared only within this test module to keep startup overhead
small. Missing local CLI configuration is an actionable test failure.

These are Phase 1 characterization tests, not evidence that the planned enterprise
API, security, failure classification, or independent workers exist yet. Prototype
spec validation remains shallow; invalid specifications are checked directly in
unit tests because their `ValueError` otherwise causes repeated workflow-task
failures rather than a terminal workflow execution failure.
