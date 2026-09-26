# Local development and Phase 1 checks

Use Python 3.11+ and uv 0.7.7+ (with dependency-group support). Install the
Temporal CLI and ensure `temporal` is on PATH for development and integration
tests. The baseline was verified on Windows with Python 3.12.2. See
[configuration.md](configuration.md) for worker configuration.

## Install reproducibly

From the repository root:

```text
uv sync --locked
```

`pyproject.toml` owns package metadata, dependencies, and tool configuration.
`uv.lock` pins runtime, development, and transitive dependencies across supported
Python/platform combinations. Temporal SDK 1.33.0 is pinned to the existing local
baseline. The build backend is also pinned and installed in the development group.
`uv run --locked` fails when metadata and the lock disagree.

`requirements.txt` is the generated, hashed runtime-only compatibility export for
the existing pip setup. Do not edit it directly. For deliberate dependency updates:

```text
uv lock --upgrade-package PACKAGE
uv sync --locked
uv export --locked --no-dev --no-emit-project --output-file requirements.txt
```

Review the resulting lock/export diff and rerun checks. Never place index
credentials in project metadata or the lockfile. The lock is for dependency
reproducibility; it does not fix future OS, Python, or container versions.

## Commands

These commands work in PowerShell and POSIX shells without Make:

```text
uv run --locked python scripts/dev.py dev
uv run --locked python scripts/dev.py test
uv run --locked python scripts/dev.py lint
uv run --locked python scripts/dev.py format
uv run --locked python scripts/dev.py build
uv run --locked python scripts/dev.py docker-build
```

GNU Make users can run `make dev`, `make test`, `make lint`, `make format`,
`make build`, and `make docker-build`. Both entry points call the same runner.
`dev` starts the existing worker; start `temporal server start-dev` separately.
`lint` runs Ruff lint, Ruff format checks, and mypy; failed checks return nonzero.
`build` creates a wheel and source distribution in ignored `dist/`, using the
locked build tools without installing new build dependencies. No service
reorganization is performed in this phase.

`docker-build` deliberately exits with code 2 and explains that service Dockerfiles
are deferred to T056. It is an initial entry point, not a completed image build.

## Tests

```text
uv run --locked python scripts/dev.py test tests/unit
uv run --locked python scripts/dev.py test tests/integration
```

Pytest arguments are forwarded after `test`. The integration suite starts an
isolated Temporal development server through the SDK using the installed CLI,
owns its server lifecycle, and uses unique queues/execution IDs. It does not use
an existing development server. Missing CLI prerequisites fail the integration
suite with an actionable message instead of silently skipping it. Timeout/timer
fixtures use short waits while the shipped sample definition remains unchanged.

The unit suite characterizes current validation limits. Contract and E2E
directories reserve ownership for later API/worker/platform increments; no
coverage is claimed for services that do not exist yet. Integration tests use
Temporal's workflow sandbox and the actual prototype Activities.

If the CLI is outside PATH, set `TEMPORAL_CLI_PATH` to its absolute executable
path before running integration tests. This setting is consumed only by the
test fixture; it does not configure the worker's connection.

## Scope

Phase 1 means **Setup (T001–T006)** in `tasks.md`. The original delivery roadmap
calls this assessment/baseline Phase 0; its Phase 1 contracts/runtime foundation
maps to later task groups. API, persistence, identity, new runtime/worker services,
containers, and cloud choices remain later work.
