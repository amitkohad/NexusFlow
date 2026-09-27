# Phase 3 verification

Date: 2026-09-27. Branch: `codex/phase-3-governed-workflow-api`, based on
`feature/develop` at `c3ade4cf9380299eaaec7f8f638576d319707996`.

## Implemented acceptance

T015–T021 are complete. The sample can be registered, approved, promoted,
started, and observed through `/api/v1`. FastAPI generates business OpenAPI
schemas; scoped repositories persist immutable revisions, promotion slots,
idempotent start reservations, run bindings, and business history. Starts and
retries retain their resolved definition. Authenticated identity supplies the
actor and permissions. Unconfigured identity denies business calls.

The HTTP contract tests cover concurrent starts, mismatched idempotency payloads,
repository restart recovery, persistence failure after runtime acceptance,
scope isolation, permission denial, paging, correlation, business projections,
runtime outage/retention fallback, oversized or ambiguous JSON, sanitized
problems, and rejection of unsupported runtime features.

## Results

Python 3.12.2 on Windows, Temporal SDK 1.33.0, local Temporal CLI 1.9.1/server
1.32.0, PostgreSQL 16 in an isolated temporary test container. Runtime dependency
versions are pinned in `uv.lock`.

| Check | Result |
| --- | --- |
| Full pytest suite | 462 passed |
| HTTP API contract tests | 49 passed |
| API transport model tests | 64 passed |
| Repository unit tests | 21 passed |
| Temporal adapter unit tests | 31 passed |
| API configuration/startup tests | 25 passed |
| PostgreSQL migrations/concurrency tests | 6 passed |
| Real Temporal API acceptance tests | 5 passed |
| Existing tests, including Temporal baseline | 261 passed |
| Ruff lint and format, mypy | Passed |
| Generated definition schema drift | Passed |
| Locked dependency check/sync and runtime requirements export | Passed |
| Wheel and source distribution builds | Passed |
| Archive inclusion and wheel-only API import | Passed |
| Migration from packaged wheel and schema readiness | Passed |
| Git whitespace check | Passed |

The real Temporal tests cover sample completion, closed-run duplicate rejection,
authenticated approval, version pinning across promotion, cooperative cancellation,
rejection follow-up handling before actual closure, and a declared nonretryable
error stopping after one Activity attempt. PostgreSQL tests cover schema
upgrade/downgrade and metadata parity, concurrent reservations, conflicting keys,
promotion races, and deduplicated projections. Each database test creates/removes
only its own UUID-named schema; the temporary container was removed after testing.

The suite emits one upstream Starlette deprecation warning about its TestClient
httpx adapter. It does not affect the passing results. Test database checks skip
without `NEXUSFLOW_TEST_DATABASE_URL`; real Temporal tests require a local CLI.

## Remaining phase boundaries

The adapter uses the existing prototype and five mock capabilities. Queue routing,
versioned worker dependencies, compensation, and templates are rejected until the
dedicated runtime exists. The legacy retry bridge now honors declared
nonretryable exception types; full resilience policy remains Phase 6.

Local/test static bearer identities are explicit development configuration.
Enterprise OIDC/provider integration remains Phase 8. Approval signals do not
provide assignment policy or exactly-once human-task completion; Phase 5 owns
those guarantees. Audit history is a durable observation-driven business
projection with an explicit freshness indicator, not complete raw Temporal
history or asynchronous audit export. Independent containers and cloud validation
remain Phase 9. No deployment was performed.
