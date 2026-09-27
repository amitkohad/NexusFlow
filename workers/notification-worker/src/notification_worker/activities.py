"""Reference notification adapter; no external notification is sent."""

from __future__ import annotations

from contracts import ActivityRequest, ActivityResponse
from temporalio import activity
from temporalio.exceptions import ApplicationError


@activity.defn(name="send_notification.v1")
async def send_notification(payload: ActivityRequest) -> ActivityResponse:
    if payload.capability != "send_notification" or payload.contract_version != "1.0":
        raise ApplicationError(
            "Activity envelope does not match the notification contract",
            type="ValidationError",
            non_retryable=True,
        )
    channel = payload.input.get("channel", "email")
    if not isinstance(channel, str) or not channel.strip() or len(channel) > 128:
        raise ApplicationError(
            "Notification channel must be a bounded nonempty string",
            type="ValidationError",
            non_retryable=True,
        )
    return ActivityResponse(
        capability="send_notification",
        contract_version="1.0",
        output={"sent": True, "channel": channel},
    )
