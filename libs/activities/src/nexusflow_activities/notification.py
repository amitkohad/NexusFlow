"""Reference notification adapter; no external notification is sent."""

from __future__ import annotations

from contracts import ActivityResponse, PackageActivityRequest
from temporalio import activity
from temporalio.exceptions import ApplicationError

from .envelopes import validate_envelope


@activity.defn(name="send_notification.pkg.v1")
async def send_notification(payload: PackageActivityRequest) -> ActivityResponse:
    validate_envelope(payload, "send_notification")
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
