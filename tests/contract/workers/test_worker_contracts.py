"""Typed Activity boundaries and independently owned worker registrations."""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from dataclasses import replace
from pathlib import Path
from typing import Any

import human_task_worker
import integration_worker
import jsonschema
import notification_worker
import pytest
import sample_business_worker
import validation_worker
from contracts import ActivityRequest, ActivityResponse, RuntimeContext
from pydantic import ValidationError
from temporalio import activity
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment
from workflow_sdk.activities import contract_for

from workflows.common.catalog import resolve_capability

pytestmark = pytest.mark.contract

ActivityFunction = Callable[[ActivityRequest], Coroutine[Any, Any, ActivityResponse]]
ROOT = Path(__file__).resolve().parents[3]
CONTEXT = RuntimeContext(
    tenant="acme",
    business_domain="finance",
    application="adjustments",
    workflow_type="customer-adjustment",
    definition_id="definition-1",
    definition_version="1.0",
    business_reference="adjustment-1",
    correlation_id="correlation-1",
    actor="authenticated-service",
)
ACTIVITIES: tuple[tuple[str, ActivityFunction], ...] = (
    ("validate_request", validation_worker.validate_request),
    ("send_notification", notification_worker.send_notification),
    ("post_adjustment", integration_worker.post_adjustment),
    ("create_approval_task", human_task_worker.create_approval_task),
    ("risk_check", sample_business_worker.risk_check),
    ("record_rejection", sample_business_worker.record_rejection),
)


def request_for(
    capability: str,
    *,
    request: dict[str, Any] | None = None,
    inputs: dict[str, Any] | None = None,
    **overrides: Any,
) -> ActivityRequest:
    return ActivityRequest.model_validate(
        {
            "workflow_id": "workflow-1",
            "run_id": "run-1",
            "step_id": "sample_step",
            "capability": capability,
            "contract_version": "1.0",
            "context": CONTEXT,
            "idempotency_key": "workflow-1:sample_step:v1",
            "request": {"amount": 7500} if request is None else request,
            "input": {} if inputs is None else inputs,
            **overrides,
        }
    )


def test_workers_register_only_owned_versioned_activities() -> None:
    ownership = (
        (validation_worker, "validation-worker", "validation-tq", {"validate_request"}),
        (notification_worker, "notification-worker", "notification-tq", {"send_notification"}),
        (integration_worker, "integration-worker", "integration-tq", {"post_adjustment"}),
        (human_task_worker, "human-task-worker", "human-task-tq", {"create_approval_task"}),
        (
            sample_business_worker,
            "sample-business-worker",
            "sample-business-tq",
            {"risk_check", "record_rejection"},
        ),
    )
    all_names: list[str] = []
    for worker, service, queue, expected in ownership:
        assert worker.SERVICE == service
        assert worker.TASK_QUEUE == queue
        assert {contract.capability for contract in worker.CONTRACTS} == expected
        assert len(worker.ACTIVITIES) == len(expected)
        for function, contract in zip(worker.ACTIVITIES, worker.CONTRACTS, strict=True):
            definition = activity._Definition.from_callable(function)
            assert definition is not None
            assert definition.name == f"{contract.capability}.v1"
            assert definition.arg_types == [ActivityRequest]
            assert definition.ret_type is ActivityResponse
            assert contract.task_queue == queue
            assert contract.contract_version == "1.0"
            assert resolve_capability(contract.capability).worker == service
            assert definition.name is not None
            all_names.append(definition.name)
    assert len(all_names) == len(set(all_names)) == 6


@pytest.mark.parametrize(("capability", "function"), ACTIVITIES)
async def test_worker_contract_json_schemas_and_temporal_converter_roundtrip(
    capability: str, function: ActivityFunction
) -> None:
    request = request_for(capability)
    environment = ActivityEnvironment()
    response = await environment.run(function, request)
    assert response.capability == capability
    assert response.contract_version == "1.0"
    contract = contract_for(capability)
    jsonschema.Draft202012Validator.check_schema(contract.input_schema)
    jsonschema.Draft202012Validator.check_schema(contract.output_schema)
    jsonschema.validate(request.model_dump(mode="json"), contract.input_schema)
    jsonschema.validate(response.model_dump(mode="json"), contract.output_schema)
    payloads = await pydantic_data_converter.encode([request, response])
    decoded = await pydantic_data_converter.decode(payloads, [ActivityRequest, ActivityResponse])
    assert decoded == [request, response]


@pytest.mark.parametrize(("capability", "function"), ACTIVITIES)
async def test_worker_rejects_wrong_capability_without_retrying(
    capability: str, function: ActivityFunction
) -> None:
    with pytest.raises(ApplicationError) as raised:
        await ActivityEnvironment().run(function, request_for("different_capability"))
    assert raised.value.type == "ValidationError"
    assert raised.value.non_retryable
    assert "different_capability" not in str(raised.value)


@pytest.mark.parametrize("capability", [capability for capability, _ in ACTIVITIES])
def test_unsupported_versions_are_rejected_at_typed_boundary(capability: str) -> None:
    with pytest.raises(ValidationError):
        request_for(capability, contract_version="2.0")


@pytest.mark.parametrize("amount", [None, True, "7500", -1, 0, {}, [], 10**400])
@pytest.mark.parametrize(
    "function", [validation_worker.validate_request, sample_business_worker.risk_check]
)
async def test_bad_amount_is_a_nonretryable_business_validation_failure(
    amount: Any, function: ActivityFunction
) -> None:
    capability = (
        "validate_request" if function is validation_worker.validate_request else "risk_check"
    )
    with pytest.raises(ApplicationError) as raised:
        await ActivityEnvironment().run(
            function, request_for(capability, request={"amount": amount})
        )
    assert raised.value.type == "ValidationError"
    assert raised.value.non_retryable
    assert "authenticated-service" not in str(raised.value)


@pytest.mark.parametrize("amount", [0.5, 100, 7500.25])
async def test_validation_preserves_positive_numeric_amount(amount: int | float) -> None:
    response = await ActivityEnvironment().run(
        validation_worker.validate_request,
        request_for("validate_request", request={"amount": amount}),
    )
    assert response.output == {"valid": True, "amount": float(amount)}


@pytest.mark.parametrize(
    ("amount", "expected_score", "expected_band"), [(4999, 25, "LOW"), (5000, 65, "HIGH")]
)
async def test_risk_routing_boundary_is_preserved(
    amount: int, expected_score: int, expected_band: str
) -> None:
    response = await ActivityEnvironment().run(
        sample_business_worker.risk_check, request_for("risk_check", request={"amount": amount})
    )
    assert response.output == {"risk_score": expected_score, "risk_band": expected_band}


async def test_transient_risk_failure_retries_then_succeeds() -> None:
    request = request_for(
        "risk_check", request={"amount": 7500, "simulate_transient_failure": True}
    )
    environment = ActivityEnvironment()
    with pytest.raises(ApplicationError) as raised:
        await environment.run(sample_business_worker.risk_check, request)
    assert raised.value.type == "TechnicalError"
    assert raised.value.non_retryable is False
    environment.info = replace(environment.info, attempt=2)
    response = await environment.run(sample_business_worker.risk_check, request)
    assert response.output["risk_score"] == 65


@pytest.mark.parametrize("flag", ["true", 1, None, {}])
async def test_transient_simulation_flag_cannot_coerce_boolean(flag: Any) -> None:
    with pytest.raises(ApplicationError) as raised:
        await ActivityEnvironment().run(
            sample_business_worker.risk_check,
            request_for("risk_check", request={"amount": 100, "simulate_transient_failure": flag}),
        )
    assert raised.value.non_retryable


@pytest.mark.parametrize("channel", [None, True, "", "   ", "x" * 129])
async def test_notification_channel_validation_is_nonretryable(channel: Any) -> None:
    with pytest.raises(ApplicationError) as raised:
        await ActivityEnvironment().run(
            notification_worker.send_notification,
            request_for("send_notification", inputs={"channel": channel}),
        )
    assert raised.value.type == "ValidationError"
    assert raised.value.non_retryable


async def test_notification_preserves_selected_channel_and_default() -> None:
    environment = ActivityEnvironment()
    default = await environment.run(
        notification_worker.send_notification, request_for("send_notification")
    )
    selected = await environment.run(
        notification_worker.send_notification,
        request_for("send_notification", inputs={"channel": "sms"}),
    )
    assert default.output == {"sent": True, "channel": "email"}
    assert selected.output == {"sent": True, "channel": "sms"}


async def test_integration_receipt_is_stable_across_retries_and_unique_by_idempotency_key() -> None:
    environment = ActivityEnvironment()
    first = await environment.run(
        integration_worker.post_adjustment, request_for("post_adjustment")
    )
    environment.info = replace(environment.info, attempt=3)
    repeated = await environment.run(
        integration_worker.post_adjustment, request_for("post_adjustment")
    )
    different = await environment.run(
        integration_worker.post_adjustment,
        request_for("post_adjustment", idempotency_key="workflow-2:sample_step:v1"),
    )
    assert first.output == repeated.output
    assert first.output["posted"] is True
    assert str(first.output["reference"]).startswith("ADJ-")
    assert first.output["reference"] != different.output["reference"]


async def test_human_task_reference_is_stable_per_workflow_step_and_preserves_assignment() -> None:
    environment = ActivityEnvironment()
    request = request_for("create_approval_task", inputs={"assignee_group": "operations-managers"})
    first = await environment.run(human_task_worker.create_approval_task, request)
    environment.info = replace(environment.info, attempt=3)
    repeated = await environment.run(human_task_worker.create_approval_task, request)
    next_step = await environment.run(
        human_task_worker.create_approval_task,
        request_for("create_approval_task", step_id="second_approval"),
    )
    another_workflow = await environment.run(
        human_task_worker.create_approval_task,
        request_for("create_approval_task", workflow_id="workflow-2"),
    )
    assert first.output == repeated.output
    assert first.output["assignee_group"] == "operations-managers"
    assert (
        len(
            {
                first.output["task_id"],
                next_step.output["task_id"],
                another_workflow.output["task_id"],
            }
        )
        == 3
    )


@pytest.mark.parametrize("group", [None, True, "", "   ", "x" * 257])
async def test_human_task_group_validation_is_nonretryable(group: Any) -> None:
    with pytest.raises(ApplicationError) as raised:
        await ActivityEnvironment().run(
            human_task_worker.create_approval_task,
            request_for("create_approval_task", inputs={"assignee_group": group}),
        )
    assert raised.value.type == "ValidationError"
    assert raised.value.non_retryable


async def test_reference_rejection_output_preserves_business_outcome() -> None:
    response = await ActivityEnvironment().run(
        sample_business_worker.record_rejection, request_for("record_rejection")
    )
    assert response.output == {"recorded": True, "reason": "business approval rejected"}
