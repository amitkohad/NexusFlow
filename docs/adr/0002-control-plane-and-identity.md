# ADR 0002: Control-plane persistence, API, and identity boundary

Status: Accepted implementation direction; deployment identity details pending.
Date: 2026-09-26.

## Decision

Use FastAPI with Pydantic 2 request/response models for the Phase 3 versioned HTTP
API. Generate OpenAPI from the same models. This is a framework selection for
future implementation; no API service or FastAPI dependency is added in Phase 2.

Use PostgreSQL for definition revisions, execution metadata, human tasks,
idempotency records, and business audit. Use SQLAlchemy 2 repository adapters and
Alembic migrations when persistence is implemented. Repositories must make
idempotency keys and revision uniqueness database constraints, and lifecycle
updates must use transactions and concurrency checks. Control-plane data belongs
in a separately managed `nexusflow` database with its own service credentials;
Temporal retains its `temporal` and `temporal_visibility` databases. Do not query
Temporal's internal tables as a platform repository.

Use an external enterprise OpenID Connect/OAuth2 issuer. The provider is supplied
by deployment configuration; NexusFlow does not host an identity provider.
Future API middleware must validate access-token signature, issuer, audience,
expiry, and allowed algorithms, then construct the trusted tenant/domain/actor
context. Request payload claims alone must not grant access. APIs use access
tokens; an OIDC ID token is not a replacement API authorization token.

Tenant, business domain, and application are explicit record fields. Initial
repositories must scope reads/writes by trusted tenant context and enforce
RBAC/ABAC policy through service interfaces. Shared persistence is the initial
implementation model; stronger physical isolation requires a deployment decision
for the relevant workload. Identity middleware and enforcement remain Phase 8.

## Outstanding deployment inputs

The security/platform owners must supply the enterprise issuer URL, audiences,
claim mapping, service-identity flow, and tenant isolation policy before a
production deployment. No provider product, issuer URL, credential, or
authentication claim mapping is invented by this increment.

## Alternatives and consequences

Raw Temporal history has different retention and access requirements from
business records. An in-memory control-plane store would not meet durable
idempotency and task requirements. Selecting the existing Python ecosystem
reduces the number of implementation languages. A configured OIDC issuer permits
enterprise federation while keeping provider-specific dependencies at the edge.

References: [FastAPI features](https://fastapi.tiangolo.com/features/),
[SQLAlchemy 2](https://docs.sqlalchemy.org/en/20/orm/),
[OpenID Connect Core](https://openid.net/specs/openid-connect-core-1_0.html).
