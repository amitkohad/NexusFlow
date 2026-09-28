"""Reference posting adapter with a stable mock receipt across retries.

Replace this demonstration with an enterprise adapter that uses the supplied
idempotency key for durable downstream deduplication before production use.
"""

from __future__ import annotations

import hashlib

from contracts import ActivityResponse, PackageActivityRequest
from temporalio import activity

from .envelopes import validate_envelope


@activity.defn(name="post_adjustment.pkg.v1")
async def post_adjustment(payload: PackageActivityRequest) -> ActivityResponse:
    validate_envelope(payload, "post_adjustment")
    receipt = hashlib.sha256(payload.idempotency_key.encode()).hexdigest()[:24].upper()
    return ActivityResponse(
        capability="post_adjustment",
        contract_version="1.0",
        output={"posted": True, "reference": f"ADJ-{receipt}"},
    )
