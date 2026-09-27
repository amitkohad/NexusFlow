"""Independent human-task-worker service registration."""

from workflow_sdk.activities import contract_for

from .activities import create_approval_task

SERVICE = "human-task-worker"
TASK_QUEUE = "human-task-tq"
ACTIVITIES = (create_approval_task,)
CONTRACTS = (contract_for("create_approval_task"),)

__all__ = ["ACTIVITIES", "CONTRACTS", "SERVICE", "TASK_QUEUE", "create_approval_task"]
