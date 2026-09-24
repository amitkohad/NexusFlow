from __future__ import annotations

import asyncio
from typing import Any

from temporalio import activity


@activity.defn(name="execute_capability")
async def execute_capability(payload: dict[str, Any]) -> dict[str, Any]:
    """Prototype enterprise capability dispatcher.

    In the target framework, this becomes a governed adapter/plugin layer that
    calls REST/gRPC services, Kafka, databases, document stores, notifications,
    mainframe adapters, and other enterprise APIs.
    """
    capability = payload["capability"]
    context = payload.get("context", {})
    request = context.get("request", {})

    await asyncio.sleep(0.15)  # make CLI/UI progress visible in a demo

    if capability == "validate_request":
        amount = float(request.get("amount", 0))
        if amount <= 0:
            raise ValueError("amount must be greater than zero")
        return {"valid": True, "amount": amount}

    if capability == "risk_check":
        # Optional flag shows Temporal's built-in Activity retry behavior.
        if request.get("simulate_transient_failure") and activity.info().attempt < 2:
            raise RuntimeError("simulated transient downstream error")
        amount = float(request.get("amount", 0))
        risk_score = 65 if amount >= 5000 else 25
        return {"risk_score": risk_score, "risk_band": "HIGH" if risk_score > 50 else "LOW"}

    if capability == "post_adjustment":
        return {
            "posted": True,
            "reference": f"ADJ-{payload['step_id'].upper()}-{activity.info().attempt}",
        }

    if capability == "send_notification":
        return {
            "sent": True,
            "channel": payload.get("input", {}).get("channel", "email"),
        }

    if capability == "record_rejection":
        return {"recorded": True, "reason": "business approval rejected"}

    raise ValueError(f"Unknown capability: {capability}")
