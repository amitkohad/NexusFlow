# Technical notes for the target framework

## Separation of planes

**Control plane**
- Process definition registry and lifecycle
- Visual designer / DSL compiler
- Validation and policy guardrails
- API gateway and workflow management APIs
- Identity, authorization, tenant/domain controls
- Human task APIs and work queues
- Migration and administration

**Execution plane**
- Temporal Service
- Domain/capability worker pools
- Task queues that isolate workload classes
- Integration adapters
- Rules/decision execution
- Event and schedule starters

This separation prevents the low-code authoring surface from becoming tightly coupled to Temporal implementation details.

## Enterprise-grade scalability approach

- Partition workloads by domain, process criticality, latency class and integration type using task queues.
- Scale worker pools independently; use queue backlog, CPU/memory and worker-slot metrics.
- Keep workflow payloads small; store large business documents in system-of-record/document storage and pass references.
- Use child workflows / continue-as-new for very large or very long orchestration histories.
- Version process definitions independently from worker code.
- Use Temporal worker versioning for safe code rollout.
- Use HA persistence, multi-AZ Kubernetes, pod disruption budgets and tested disaster recovery.
- Apply rate limits/circuit breakers in adapters so downstream systems are protected during spikes.

## Recommended nonfunctional targets to define during productization

- Availability tiers by process criticality (for example 99.9 / 99.95 / 99.99 depending on use case)
- RTO/RPO for orchestration metadata and framework control plane
- Start latency and task pickup latency SLOs
- Throughput by process class, peak burst factors and queue-drain objectives
- Maximum supported process duration and workflow-history policy
- Audit-retention policy and evidence-export requirements
- Data classification, encryption and payload-redaction rules
- Operational ownership, on-call and release/change controls
