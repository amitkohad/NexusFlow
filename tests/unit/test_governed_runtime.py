"""Pure preflight, approval-window, and typed Activity-boundary checks."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest
from contracts import (
    ActivityRequest,
    ActivityResponse,
    ActivityStep,
    DefinitionDocument,
    RetryPolicyModel,
    RuntimeContext,
)
from temporalio import workflow
from temporalio.exceptions import ApplicationError
from workflow_runtime.workflows import GovernedWorkflowV1, _prepare_definition, _retry_policy
from workflow_sdk.definitions import DefinitionValidationError

from workflows.common.catalog import resolve_capability


@pytest.fixture
def runtime_context() -> RuntimeContext:
    return RuntimeContext(
        tenant="tenant",
        business_domain="finance",
        application="adjustments",
        workflow_type="adjustment",
        definition_id="definition",
        definition_version="2.0",
        business_reference="business-reference",
        correlation_id="correlation",
        actor="verified-user",
    )


def test_sample_is_pinned_after_route_and_graph_validation() -> None:
    sample = Path(__file__).resolve().parents[2] / "examples/customer_adjustment.json"
    source = DefinitionDocument.model_validate(json.loads(sample.read_text(encoding="utf-8")))
    captured = _prepare_definition(source)
    source.request["amount"] = -1
    source.steps.clear()
    assert captured.request["amount"] == 7500
    assert captured.start_at == "validate"
    assert len(captured.steps) == 10


@pytest.mark.parametrize(
    "override",
    [
        {"task_queue": "another-owner"},
        {"contract_version": "2.0"},
        {"compensation": {"capability": "record_rejection"}},
        {"capability": "create_approval_task"},
        {"capability": "not_registered"},
    ],
)
def test_unsupported_routes_and_deferred_policies_fail_before_activity_execution(
    override: dict[str, Any],
) -> None:
    document = DefinitionDocument.model_validate(
        {
            "start_at": "run",
            "steps": {
                "run": {
                    "type": "activity",
                    "capability": "validate_request",
                    "next": "done",
                    **override,
                },
                "done": {"type": "end"},
            },
        }
    )
    with pytest.raises((ValueError, DefinitionValidationError)):
        _prepare_definition(document)


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"approved": "true", "approver": "actor"},
        {"approved": 1, "approver": "actor"},
        {"approved": None, "approver": "actor"},
        {"approved": True},
        {"approved": True, "approver": " "},
        {"approved": True, "approver": 123},
        {"approved": True, "approver": "actor", "comment": False},
    ],
)
def test_invalid_approval_signals_are_ignored_without_boolean_coercion(
    payload: dict[str, Any],
) -> None:
    instance = GovernedWorkflowV1()
    instance._approval_waiting = True
    instance.approve(payload)
    assert instance._approval is None


def test_only_first_in_window_approval_decision_is_accepted() -> None:
    instance = GovernedWorkflowV1()
    decision = {"approved": True, "approver": "first", "comment": "Verified"}
    instance.approve(decision)
    assert instance._approval is None
    instance._approval_waiting = True
    instance.approve(decision)
    decision["approver"] = "changed"
    instance.approve({"approved": False, "approver": "second"})
    assert instance._approval == {"approved": True, "approver": "first", "comment": "Verified"}
    instance._approval_waiting = False
    instance.approve({"approved": False, "approver": "late"})
    assert instance._approval == {"approved": True, "approver": "first", "comment": "Verified"}


def test_status_query_does_not_expose_mutable_internal_transitions_or_results() -> None:
    instance = GovernedWorkflowV1()
    instance._status["transitions"] = [{"step": "activity", "state": "STARTED"}]
    instance._status["results"] = {"activity": {"sent": True}}
    snapshot = instance.status()
    snapshot["transitions"].clear()
    snapshot["results"]["activity"]["sent"] = False
    assert instance.status()["transitions"] == [{"step": "activity", "state": "STARTED"}]
    assert instance.status()["results"] == {"activity": {"sent": True}}


def test_retry_contract_maps_finite_timeouts_and_non_retryable_error_types() -> None:
    policy = _retry_policy(
        RetryPolicyModel(
            maximum_attempts=5,
            initial_interval_seconds=0.5,
            maximum_interval_seconds=4,
            backoff_coefficient=1.5,
            non_retryable_error_types=("ValidationError", "BusinessError"),
        )
    )
    assert policy.maximum_attempts == 5
    assert policy.initial_interval == timedelta(seconds=0.5)
    assert policy.maximum_interval == timedelta(seconds=4)
    assert policy.backoff_coefficient == 1.5
    assert policy.non_retryable_error_types == ("ValidationError", "BusinessError")


async def test_invalid_start_payload_is_a_sanitized_non_retryable_workflow_failure() -> None:
    with pytest.raises(ApplicationError) as caught:
        await GovernedWorkflowV1().run({"runtime_version": "2.0", "secret": "must-not-leak"})
    assert caught.value.type == "ValidationError"
    assert caught.value.non_retryable is True
    assert "must-not-leak" not in str(caught.value)


async def test_activity_uses_typed_pinned_context_templates_and_owned_queue(
    monkeypatch: pytest.MonkeyPatch,
    runtime_context: RuntimeContext,
) -> None:
    instance = GovernedWorkflowV1()
    instance._context = runtime_context
    instance._workflow_id = "workflow-id"
    instance._run_id = "run-id"
    instance._request = {"amount": 1250, "destination": "customer"}
    instance._variables = {"reference": "BIZ-1"}
    response = ActivityResponse(capability="send_notification", output={"sent": True})
    execute = AsyncMock(return_value=response)
    monkeypatch.setattr(workflow, "execute_activity", execute)
    monkeypatch.setattr(workflow, "now", lambda: datetime(2026, 9, 27, tzinfo=timezone.utc))
    step = ActivityStep(
        capability="send_notification",
        timeout_seconds=12,
        next="done",
        input={
            "destination": "${request.destination}",
            "amount": "${request.amount}",
            "reference": "ref-${variables.reference}",
        },
    )
    assert await instance._activity("notify", step) == "done"
    call = execute.await_args
    assert call is not None
    args, kwargs = call
    assert args[0] == "send_notification.v1"
    envelope = args[1]
    assert isinstance(envelope, ActivityRequest)
    assert envelope.workflow_id == "workflow-id"
    assert envelope.run_id == "run-id"
    assert envelope.step_id == "notify"
    assert envelope.context == runtime_context
    assert envelope.context.definition_version == "2.0"
    assert envelope.idempotency_key == "workflow-id:notify:1.0"
    assert envelope.input == {"destination": "customer", "amount": 1250, "reference": "ref-BIZ-1"}
    assert kwargs["task_queue"] == "notification-tq"
    assert kwargs["result_type"] is ActivityResponse
    assert kwargs["start_to_close_timeout"] == timedelta(seconds=12)
    assert kwargs["cancellation_type"] == workflow.ActivityCancellationType.TRY_CANCEL
    response.output["sent"] = False
    assert instance._results["notify"] == {"sent": True}


async def test_worker_response_cannot_change_its_pinned_capability(
    monkeypatch: pytest.MonkeyPatch,
    runtime_context: RuntimeContext,
) -> None:
    instance = GovernedWorkflowV1()
    instance._context = runtime_context
    instance._workflow_id = "workflow"
    instance._run_id = "run"
    monkeypatch.setattr(
        workflow,
        "execute_activity",
        AsyncMock(return_value=ActivityResponse(capability="risk_check", output={"sent": True})),
    )
    with pytest.raises(ValueError, match="must match its pinned capability contract"):
        await instance._invoke(
            "notify", resolve_capability("send_notification"), {}, 30, RetryPolicyModel()
        )
