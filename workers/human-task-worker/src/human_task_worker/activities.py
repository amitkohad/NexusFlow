"""Reference task creation adapter for the sample approval wait.

This worker returns an idempotent task reference. Phase 5 supplies persisted task
state, assignment, forms, evidence, and task lifecycle operations.
"""

from __future__ import annotations

import hashlib
import json

from contracts import ActivityRequest, ActivityResponse
from temporalio import activity
from temporalio.exceptions import ApplicationError


@activity.defn(name="create_approval_task.v1")
async def create_approval_task(payload: ActivityRequest) -> ActivityResponse:
    if payload.capability != "create_approval_task" or payload.contract_version != "1.0":
        raise ApplicationError(
            "Activity envelope does not match the human-task contract",
            type="ValidationError",
            non_retryable=True,
        )
    assignee_group = payload.input.get("assignee_group", "approvers")
    if (
        not isinstance(assignee_group, str)
        or not assignee_group.strip()
        or len(assignee_group) > 256
    ):
        raise ApplicationError(
            "Assignee group must be a bounded nonempty string",
            type="ValidationError",
            non_retryable=True,
        )
    identity = json.dumps([payload.workflow_id, payload.step_id], separators=(",", ":"))
    task_id = "HT-" + hashlib.sha256(identity.encode()).hexdigest()[:24]
    return ActivityResponse(
        capability="create_approval_task",
        contract_version="1.0",
        output={"created": True, "task_id": task_id, "assignee_group": assignee_group},
    )
