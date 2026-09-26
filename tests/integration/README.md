# Temporal integration tests

`test_lightweight_process.py` protects the existing customer-adjustment behavior:
low-value completion, high-value approval, rejection, approval timeout, Activity
retry, durable timer, queried state, and transition history. The tests use the
repository's example definition and real `execute_capability` Activity.

Set `TEMPORAL_CLI_PATH` or install the Temporal CLI on `PATH`, then run
`uv run --locked pytest tests/integration`. The fixture owns its isolated local server and
cleans it up after the module. No external Temporal service is needed.
