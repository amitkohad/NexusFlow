# ADR 0003: Temporal hosting and operating assumptions

Status: Accepted hosting direction and development limits; production targets pending.
Date: 2026-09-26.

## Decision

Use self-hosted Temporal on GKE with external Cloud SQL PostgreSQL databases
`temporal` and `temporal_visibility`, following the constitution. Platform
infrastructure owns the service and schema upgrades. Use separate namespaces by
environment and owned task queues by worker. The local CLI server remains a test
dependency; its bundled server version is not a production release selection.
The Temporal SDK remains pinned to 1.33.0 for baseline compatibility. A production
server release must be selected and verified in the infrastructure increment.

Shared `WorkerSettings` loads configuration only when explicitly called outside
workflow code. It supports connection address, namespace, task queue, TLS,
definition-step/payload guardrails, and shutdown grace. Production connection
configuration requires TLS. TLS is a connection requirement, not an implemented
identity provider or complete production readiness check.

Use development defaults of 500 definition steps, 1 MiB payload budget, and
30-second shutdown grace. SDK validation enforces its supplied step bound. The
payload and shutdown values are settings for future service integration; the
prototype does not enforce them. They are adjustable guardrails, not measured
throughput, latency, availability, or recovery guarantees.

## Production acceptance inputs

| Input | Owner role | Required before |
| --- | --- | --- |
| Availability, start/pickup latency and throughput by workflow class | Product and platform | Capacity sizing and production acceptance |
| RTO/RPO, HA, failover and recovery exercises | Platform operations | Production infrastructure rollout |
| Maximum duration, history budget and continue-as-new policy | Runtime owners | Long-running production workflows |
| Data classification, audit/evidence retention and payload encryption | Security and data owners | Production persistence and audit release |
| Idempotency guarantees and timeout-after-success behavior | Capability owners | Enabling side-effect retries for real adapters |

Record measured targets and evidence when the owners provide them. No production
SLO value, retention period, or capacity is claimed by Phase 2.

## Alternatives and consequences

Managed Temporal remains an option requiring a constitution and infrastructure
decision. Bundled production PostgreSQL conflicts with the specified persistence
boundary. Local-only or inferred capacity figures cannot establish production
readiness, so deployment gates remain explicit while foundation work proceeds.
