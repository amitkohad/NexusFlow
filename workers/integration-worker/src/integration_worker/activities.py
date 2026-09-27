"""Reference posting adapter with a stable mock receipt across retries.

Replace this demonstration with an enterprise adapter that uses the supplied
idempotency key for durable downstream deduplication before production use.
"""

from __future__ import annotations

import hashlib

from contracts import ActivityRequest, ActivityResponse
from temporalio import activity
from temporalio.exceptions import ApplicationError


@activity.defn(name="post_adjustment.v1")
async def post_adjustment(payload: ActivityRequest) -> ActivityResponse:
    if payload.capability != "post_adjustment" or payload.contract_version != "1.0":
        raise ApplicationError(
            "Activity envelope does not match the integration contract",
            type="ValidationError",
            non_retryable=True,
        )
    receipt = hashlib.sha256(payload.idempotency_key.encode()).hexdigest()[:24].upper()
    return ActivityResponse(
        capability="post_adjustment",
        contract_version="1.0",
        output={"posted": True, "reference": f"ADJ-{receipt}"},
    )
