"""Independent sample-business-worker service registration."""

from workflow_sdk.activities import contract_for

from .activities import record_rejection, risk_check

SERVICE = "sample-business-worker"
TASK_QUEUE = "sample-business-tq"
ACTIVITIES = (
    risk_check,
    record_rejection,
)
CONTRACTS = (
    contract_for("risk_check"),
    contract_for("record_rejection"),
)

__all__ = ["ACTIVITIES", "CONTRACTS", "SERVICE", "TASK_QUEUE", "risk_check", "record_rejection"]
