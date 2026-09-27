"""Reference-only sample capabilities; never an enterprise business adapter."""

from __future__ import annotations

import math

from contracts import ActivityRequest, ActivityResponse
from temporalio import activity
from temporalio.exceptions import ApplicationError


def _validate_envelope(payload: ActivityRequest, capability: str) -> None:
    if payload.capability != capability or payload.contract_version != "1.0":
        raise ApplicationError(
            "Activity envelope does not match the sample capability contract",
            type="ValidationError",
            non_retryable=True,
        )


@activity.defn(name="risk_check.v1")
async def risk_check(payload: ActivityRequest) -> ActivityResponse:
    _validate_envelope(payload, "risk_check")
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
    simulate_failure = payload.request.get("simulate_transient_failure", False)
    if not isinstance(simulate_failure, bool):
        raise ApplicationError(
            "Transient failure flag must be a boolean", type="ValidationError", non_retryable=True
        )
    if simulate_failure and activity.info().attempt < 2:
        raise ApplicationError(
            "Simulated transient downstream error", type="TechnicalError", non_retryable=False
        )
    risk_score = 65 if numeric_amount >= 5000 else 25
    return ActivityResponse(
        capability="risk_check",
        contract_version="1.0",
        output={"risk_score": risk_score, "risk_band": "HIGH" if risk_score > 50 else "LOW"},
    )


@activity.defn(name="record_rejection.v1")
async def record_rejection(payload: ActivityRequest) -> ActivityResponse:
    _validate_envelope(payload, "record_rejection")
    return ActivityResponse(
        capability="record_rejection",
        contract_version="1.0",
        output={"recorded": True, "reason": "business approval rejected"},
    )
