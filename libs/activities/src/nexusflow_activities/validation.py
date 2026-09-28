"""Typed request validation owned by the validation worker."""

from __future__ import annotations

import math

from contracts import ActivityResponse, PackageActivityRequest
from temporalio import activity
from temporalio.exceptions import ApplicationError

from .envelopes import validate_envelope


@activity.defn(name="validate_request.pkg.v1")
async def validate_request(payload: PackageActivityRequest) -> ActivityResponse:
    validate_envelope(payload, "validate_request")
    amount = payload.request.get("amount")
    if type(amount) not in (int, float) or not isinstance(amount, (int, float)):
        raise ApplicationError(
            "Amount must be a number", type="ValidationError", non_retryable=True
        )
    try:
        numeric_amount = float(amount)
    except OverflowError:
        raise ApplicationError(
            "Amount must be finite", type="ValidationError", non_retryable=True
        ) from None
    if not math.isfinite(numeric_amount) or numeric_amount <= 0:
        raise ApplicationError(
            "Amount must be finite and greater than zero",
            type="ValidationError",
            non_retryable=True,
        )
    return ActivityResponse(
        capability="validate_request",
        contract_version="1.0",
        output={"valid": True, "amount": numeric_amount},
    )
