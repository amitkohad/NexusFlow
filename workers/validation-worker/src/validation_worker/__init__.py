"""Independent validation-worker service registration."""

from workflow_sdk.activities import contract_for

from .activities import validate_request

SERVICE = "validation-worker"
TASK_QUEUE = "validation-tq"
ACTIVITIES = (validate_request,)
CONTRACTS = (contract_for("validate_request"),)

__all__ = ["ACTIVITIES", "CONTRACTS", "SERVICE", "TASK_QUEUE", "validate_request"]
