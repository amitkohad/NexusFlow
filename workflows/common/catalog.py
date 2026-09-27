"""Immutable v1 capability ownership, safe to import in workflow sandboxes.

New incompatible contracts need new versioned entries and runtime code. Never
rewrite v1 routing for an in-flight workflow.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType

RUNTIME_WORKFLOW_TYPE = "GovernedWorkflowV1"
RUNTIME_TASK_QUEUE = "workflow-orchestration-tq"
LEGACY_WORKFLOW_TYPE = "LightweightProcess"
LEGACY_TASK_QUEUE = "lightweight-workflows"


@dataclass(frozen=True)
class CapabilityRoute:
    capability: str
    activity_name: str
    contract_version: str
    task_queue: str
    worker: str


CAPABILITY_ROUTES = MappingProxyType(
    {
        capability: CapabilityRoute(capability, f"{capability}.v1", "1.0", queue, worker)
        for capability, queue, worker in (
            ("validate_request", "validation-tq", "validation-worker"),
            ("send_notification", "notification-tq", "notification-worker"),
            ("post_adjustment", "integration-tq", "integration-worker"),
            ("create_approval_task", "human-task-tq", "human-task-worker"),
            ("risk_check", "sample-business-tq", "sample-business-worker"),
            ("record_rejection", "sample-business-tq", "sample-business-worker"),
        )
    }
)


def resolve_capability(
    capability: str, contract_version: str | None = None, task_queue: str | None = None
) -> CapabilityRoute:
    route = CAPABILITY_ROUTES.get(capability)
    if route is None:
        raise ValueError("Capability is unavailable")
    if contract_version is not None and contract_version != route.contract_version:
        raise ValueError("Capability contract version is unavailable")
    if task_queue is not None and task_queue != route.task_queue:
        raise ValueError("Capability must use its owned task queue")
    return route
