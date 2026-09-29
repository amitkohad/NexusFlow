"""Named package Activities retain typed transport and reference adapter behavior."""

from __future__ import annotations

import hashlib
import importlib
from collections.abc import Callable
from typing import Any

import nexusflow_activities
import pytest
from contracts import PackageActivityRequest
from temporalio import activity
from temporalio.exceptions import ApplicationError

from tests.package_fixtures import source_package, start_request

pytestmark = pytest.mark.contract


@pytest.fixture(autouse=True)
def configured_reference_task_service(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exercise the named handler against an explicit external task reference."""

    def create_task(url: str, token: str, body: dict[str, Any]) -> str:
        assert url == "http://127.0.0.1:31999/api/v1/tasks"
        assert token == "contract-service-token"
        assert body["task_type"] == "approval"
        identity = f"{body['workflow_id']}:{body['step_id']}:{body['idempotency_key']}"
        return "HT-" + hashlib.sha256(identity.encode()).hexdigest()[:24]

    monkeypatch.setenv("NEXUSFLOW_HUMAN_TASK_SERVICE_URL", "http://127.0.0.1:31999")
    monkeypatch.setenv("NEXUSFLOW_HUMAN_TASK_SERVICE_TOKEN", "contract-service-token")
    handler_module = importlib.import_module("nexusflow_activities.human_tasks")
    monkeypatch.setattr(handler_module, "_post_task", create_task)


def payload(capability: str, **changes: Any) -> PackageActivityRequest:
    request = start_request(source_package())
    values = {
        "workflow_id": "workflow-1",
        "run_id": "run-1",
        "first_execution_run_id": "run-1",
        "step_id": "step-1",
        "capability": capability,
        "context": request.context,
        "release_binding": request.release_binding,
        "idempotency_key": "workflow-1:step-1:1.0",
        "input": {"assignee_group": "approvers", "timeout_seconds": 300}
        if capability == "create_approval_task"
        else {},
        "request": {"amount": 1000},
    }
    values.update(changes)
    return PackageActivityRequest.model_validate(values)


@pytest.mark.parametrize(
    "capability",
    [
        "validate_request",
        "send_notification",
        "post_adjustment",
        "create_approval_task",
        "risk_check",
        "record_rejection",
    ],
)
async def test_every_handler_is_explicit_typed_and_returns_matching_contract(
    capability: str,
) -> None:
    handler: Callable[..., Any] = getattr(nexusflow_activities, capability)
    registration = activity._Definition.from_callable(handler)
    assert registration is not None
    assert registration.name == capability + ".pkg.v1"
    assert registration.arg_types == [PackageActivityRequest]
    response = await handler(payload(capability))
    assert response.capability == capability
    assert response.contract_version == "1.0"
    assert response.output


@pytest.mark.parametrize(
    "capability",
    [
        "validate_request",
        "send_notification",
        "post_adjustment",
        "create_approval_task",
        "risk_check",
        "record_rejection",
    ],
)
async def test_handlers_reject_missing_release_closure(capability: str) -> None:
    request = payload(capability)
    request = request.model_copy(
        update={
            "release_binding": request.release_binding.model_copy(update={"activity_bindings": ()})
        }
    )
    with pytest.raises(ApplicationError) as failure:
        await getattr(nexusflow_activities, capability)(request)
    assert failure.value.type == "ValidationError"
    assert failure.value.non_retryable


@pytest.mark.parametrize("amount", [True, "100", 0, -1, 10**1000])
async def test_validation_rejects_invalid_amount_as_nonretryable(amount: Any) -> None:
    with pytest.raises(ApplicationError) as failure:
        await nexusflow_activities.validate_request(
            payload("validate_request", request={"amount": amount})
        )
    assert failure.value.non_retryable


async def test_context_revision_must_match_frozen_release() -> None:
    request = payload("validate_request")
    request = request.model_copy(
        update={"context": request.context.model_copy(update={"definition_version": "2.0"})}
    )
    with pytest.raises(ApplicationError, match="pinned package contract"):
        await nexusflow_activities.validate_request(request)


@pytest.mark.parametrize("capability", ["post_adjustment", "create_approval_task"])
async def test_reference_identity_survives_activity_retry_and_new_run(capability: str) -> None:
    first = payload(capability)
    retry = first.model_copy(update={"run_id": "another-run"})
    handler = getattr(nexusflow_activities, capability)
    assert (await handler(first)).output == (await handler(retry)).output


async def test_notification_validates_downstream_channel() -> None:
    with pytest.raises(ApplicationError) as failure:
        await nexusflow_activities.send_notification(
            payload("send_notification", input={"channel": " "})
        )
    assert failure.value.non_retryable
