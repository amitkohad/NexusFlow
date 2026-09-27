"""Independent integration-worker service registration."""

from workflow_sdk.activities import contract_for

from .activities import post_adjustment

SERVICE = "integration-worker"
TASK_QUEUE = "integration-tq"
ACTIVITIES = (post_adjustment,)
CONTRACTS = (contract_for("post_adjustment"),)

__all__ = ["ACTIVITIES", "CONTRACTS", "SERVICE", "TASK_QUEUE", "post_adjustment"]
