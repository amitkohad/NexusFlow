"""Independent notification-worker service registration."""

from workflow_sdk.activities import contract_for

from .activities import send_notification

SERVICE = "notification-worker"
TASK_QUEUE = "notification-tq"
ACTIVITIES = (send_notification,)
CONTRACTS = (contract_for("send_notification"),)

__all__ = ["ACTIVITIES", "CONTRACTS", "SERVICE", "TASK_QUEUE", "send_notification"]
