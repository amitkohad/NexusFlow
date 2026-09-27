"""Contract boundaries reject ambiguous payloads before runtime execution."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TypedDict

import pytest
from contracts import (
    MAX_JSON_DEPTH,
    ActivityContract,
    ActivityStep,
    ApprovalStep,
    AuditEvent,
    CancellationPolicyModel,
    CompensationPolicyModel,
    DecisionStep,
    DefinitionDocument,
    EndStep,
    ErrorCategory,
    ErrorDetail,
    FailurePolicyModel,
    HeartbeatPolicyModel,
    HumanTask,
    IdempotencyPolicyModel,
    RetryPolicyModel,
    TenantContext,
    TerminalOutcome,
    TimeoutPolicyModel,
    TimerStep,
    ValidationIssue,
    WorkflowDefinition,
    WorkflowExecution,
)
from pydantic import ValidationError

ROOT = Path(__file__).resolve().parents[2]
NOW = datetime(2026, 9, 26, tzinfo=timezone.utc)


class Context(TypedDict):
    tenant: str
    business_domain: str
    application: str


CONTEXT: Context = {"tenant": "acme", "business_domain": "finance", "application": "adjustments"}


def test_existing_sample_parses_as_typed_steps_and_roundtrips() -> None:
    source = json.loads((ROOT / "examples/customer_adjustment.json").read_text(encoding="utf-8"))
    definition = DefinitionDocument.model_validate(source)
    assert isinstance(definition.steps["validate"], ActivityStep)
    assert isinstance(definition.steps["route"], DecisionStep)
    assert isinstance(definition.steps["manager_approval"], ApprovalStep)
    assert isinstance(definition.steps["completed"], EndStep)
    assert definition.schema_version == "1.0"
    assert definition.request == source["request"]
    risk = definition.steps["risk"]
    rejected = definition.steps["rejected"]
    assert isinstance(risk, ActivityStep)
    assert isinstance(rejected, EndStep)
    assert risk.retry.maximum_interval_seconds == 10
    assert rejected.outcome == TerminalOutcome.REJECTED
    assert DefinitionDocument.model_validate_json(definition.model_dump_json()) == definition


@pytest.mark.parametrize(
    "step",
    [
        {"type": "activity", "capability": "validate"},
        {"type": "timer"},
        {"type": "decision", "field": "request.amount", "value": 1, "on_true": "done"},
        {"type": "decision", "field": "request.amount", "on_true": "done", "on_false": "done"},
        {"type": "approval", "on_approved": "done", "on_rejected": "done"},
        {"type": "end", "outcome": "UNKNOWN"},
        {"type": "script", "source": "print('unsupported')"},
        {"type": "end", "next": "done"},
    ],
)
def test_closed_step_union_requires_explicit_governed_routes(step: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        DefinitionDocument.model_validate({"start_at": "start", "steps": {"start": step}})


@pytest.mark.parametrize("value", [True, "10", 1.5, 0, -1])
def test_activity_timeouts_reject_coercion_and_nonpositive_values(value: Any) -> None:
    with pytest.raises(ValidationError):
        ActivityStep.model_validate(
            {"capability": "validate", "next": "done", "timeout_seconds": value}
        )


@pytest.mark.parametrize("value", [True, "1", -1, 0.5])
def test_timer_seconds_are_nonnegative_integers(value: Any) -> None:
    with pytest.raises(ValidationError):
        TimerStep.model_validate({"next": "done", "seconds": value})
    assert TimerStep(next="done", seconds=0).seconds == 0


@pytest.mark.parametrize("invalid", [float("inf"), float("nan"), (1, 2), {1: "bad"}, NOW])
def test_nested_payloads_are_finite_json_values(invalid: Any) -> None:
    with pytest.raises(ValidationError):
        ActivityStep.model_validate(
            {"capability": "validate", "next": "done", "input": {"nested": [invalid]}}
        )
    with pytest.raises(ValidationError):
        DecisionStep.model_validate(
            {"field": "request.value", "value": invalid, "on_true": "done", "on_false": "done"}
        )


def test_payload_cycles_and_excessive_nesting_fail_as_validation_errors() -> None:
    cyclic_dict: dict[str, Any] = {}
    cyclic_dict["self"] = cyclic_dict
    cyclic_list: list[Any] = []
    cyclic_list.append(cyclic_list)
    for cyclic in [cyclic_dict, cyclic_list]:
        with pytest.raises(ValidationError, match="must not contain cycles"):
            ActivityStep.model_validate(
                {"capability": "validate", "next": "done", "input": {"cyclic": cyclic}}
            )
    deep: Any = None
    for _ in range(MAX_JSON_DEPTH):
        deep = [deep]
    assert (
        DecisionStep(field="request.value", value=deep, on_true="done", on_false="done").value
        == deep
    )
    with pytest.raises(ValidationError, match=f"nesting must not exceed {MAX_JSON_DEPTH} levels"):
        DecisionStep(field="request.value", value=[deep], on_true="done", on_false="done")
    shared = {"value": True}
    activity = ActivityStep.model_validate(
        {"capability": "validate", "next": "done", "input": {"first": shared, "second": shared}}
    )
    assert activity.input == {"first": {"value": True}, "second": {"value": True}}
    shared["value"] = False
    assert activity.input["first"] == {"value": True}


def test_json_depth_boundary_serializes_definition_and_business_records() -> None:
    nested: Any = None
    for _ in range(MAX_JSON_DEPTH - 1):
        nested = [nested]
    payload = {"nested": nested}
    document = DefinitionDocument(
        start_at="done", steps={"done": EndStep()}, request=payload, variables=payload
    )
    governed = WorkflowDefinition(
        **CONTEXT,
        definition_id="D-1",
        workflow_type="adjustments",
        version="1.0",
        content_hash="a" * 64,
        definition_document=document,
        owner="finance-team",
        created_at=NOW,
        created_by="author@example.com",
    )
    event = AuditEvent(
        **CONTEXT,
        event_id="E-1",
        event_type="workflow.started",
        actor="user",
        correlation_id="C-1",
        timestamp=NOW,
        metadata=payload,
    )
    decision = DecisionStep(field="request.value", value=[nested], on_true="done", on_false="done")
    for record in [document, governed, event, decision]:
        assert record.model_dump(mode="json")
        encoded = record.model_dump_json()
        assert type(record).model_validate_json(encoded) == record
    with pytest.raises(ValidationError, match=f"nesting must not exceed {MAX_JSON_DEPTH} levels"):
        DefinitionDocument(start_at="done", steps={"done": EndStep()}, request={"nested": [nested]})


@pytest.mark.parametrize("invalid", ["bad.name", "", " a ", "1start"])
def test_definition_step_keys_are_identifiers(invalid: str) -> None:
    with pytest.raises(ValidationError):
        DefinitionDocument.model_validate(
            {"start_at": "start", "steps": {invalid: {"type": "end"}}}
        )


@pytest.mark.parametrize(
    "path", ["results.items.0.id", "request.customer_id", "0.value", "request.some-key"]
)
def test_paths_support_identifiers_and_canonical_list_indices(path: str) -> None:
    assert DecisionStep(field=path, value=None, on_true="done", on_false="done").field == path


@pytest.mark.parametrize(
    "path",
    ["", ".request", "request..amount", "request.01", "request.-1", "request[0]", "request. "],
)
def test_path_syntax_rejects_ambiguous_or_expression_segments(path: str) -> None:
    with pytest.raises(ValidationError):
        DecisionStep(field=path, value=None, on_true="done", on_false="done")


def test_contracts_are_closed_and_frozen_without_shared_payload_defaults() -> None:
    first = ActivityStep(capability="validate", next="done")
    second = ActivityStep(capability="validate", next="done")
    first.input["request_id"] = "R-1"
    assert second.input == {}
    with pytest.raises(ValidationError, match="frozen"):
        first.next = "other"
    with pytest.raises(ValidationError, match="extra_forbidden"):
        EndStep.model_validate({"unknown": 1})
    with pytest.raises(ValidationError):
        DefinitionDocument(start_at="done", steps={})


@pytest.mark.parametrize(
    "policy",
    [
        {"maximum_attempts": 0},
        {"maximum_attempts": True},
        {"initial_interval_seconds": "1"},
        {"initial_interval_seconds": 0},
        {"maximum_interval_seconds": float("inf")},
        {"backoff_coefficient": False},
        {"backoff_coefficient": 0.5},
        {"initial_interval_seconds": 20, "maximum_interval_seconds": 10},
    ],
)
def test_retry_policies_reject_unsafe_or_ambiguous_configuration(policy: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        RetryPolicyModel.model_validate(policy)


def test_policy_defaults_and_json_roundtrip() -> None:
    retry = RetryPolicyModel()
    assert (
        retry.maximum_attempts,
        retry.initial_interval_seconds,
        retry.maximum_interval_seconds,
        retry.backoff_coefficient,
    ) == (3, 1, 10, 2)
    assert "AuthorizationError" in retry.non_retryable_error_types
    policies = [
        retry,
        TimeoutPolicyModel(),
        HeartbeatPolicyModel(timeout_seconds=0.5),
        CompensationPolicyModel(capability="undo_adjustment"),
        CancellationPolicyModel(),
        IdempotencyPolicyModel(key_path="request.request_id"),
        FailurePolicyModel(),
    ]
    for policy in policies:
        assert type(policy).model_validate_json(policy.model_dump_json()) == policy
    assert ErrorCategory.BUSINESS in FailurePolicyModel().non_retryable_categories
    with pytest.raises(ValidationError):
        IdempotencyPolicyModel.model_validate({"required": "true"})
    with pytest.raises(ValidationError):
        TimeoutPolicyModel.model_validate({"schedule_to_start_seconds": -1})


def test_activity_contract_requires_consistent_idempotency_and_versioned_queue() -> None:
    values = {
        "capability": "post_adjustment",
        "action": "post",
        "contract_version": "1.0",
        "task_queue": "integration-tq",
        "input_schema": {"type": "object"},
        "output_schema": {"type": "object"},
        "worker_compatibility": ["1.x"],
    }
    contract = ActivityContract.model_validate(values)
    assert contract.idempotency_required
    assert ActivityContract.model_validate_json(contract.model_dump_json()) == contract
    with pytest.raises(ValidationError, match="idempotency_required"):
        ActivityContract.model_validate({**values, "idempotency_required": False})
    non_idempotent = ActivityContract.model_validate(
        {**values, "idempotency_required": False, "idempotency_policy": {"required": False}}
    )
    assert not non_idempotent.idempotency_required


@pytest.mark.parametrize(
    "category",
    [
        ErrorCategory.VALIDATION,
        ErrorCategory.BUSINESS,
        ErrorCategory.AUTHORIZATION,
        ErrorCategory.CONFIGURATION,
        ErrorCategory.CANCELLATION,
    ],
)
def test_nonretryable_error_categories_cannot_claim_retryability(category: ErrorCategory) -> None:
    with pytest.raises(ValidationError, match="must not be retryable"):
        ErrorDetail(code="INVALID", category=category, message="Invalid operation", retryable=True)


def test_error_details_preserve_actionable_issue_without_untyped_fields() -> None:
    error = ErrorDetail(
        code="INVALID_DEFINITION",
        category=ErrorCategory.VALIDATION,
        message="Definition could not be validated",
        issues=(
            ValidationIssue(
                code="MISSING_TARGET", path="steps.start.next", message="Target does not exist"
            ),
        ),
    )
    assert not error.retryable
    assert ErrorDetail.model_validate_json(error.model_dump_json()) == error
    assert ErrorDetail(
        code="DOWNSTREAM_UNAVAILABLE",
        category=ErrorCategory.TECHNICAL,
        message="Downstream unavailable",
        retryable=True,
    ).retryable


def test_business_records_keep_context_version_actor_and_offsets() -> None:
    definition = WorkflowDefinition(
        **CONTEXT,
        definition_id="D-1",
        workflow_type="adjustments",
        version="1.0",
        content_hash="a" * 64,
        definition_document=DefinitionDocument(start_at="done", steps={"done": EndStep()}),
        owner="finance-team",
        created_at=NOW,
        created_by="author@example.com",
    )
    execution = WorkflowExecution(
        **CONTEXT,
        workflow_id="W-1",
        run_id="R-1",
        workflow_type="adjustments",
        definition_id="D-1",
        definition_version="1.0",
        business_reference="A-1",
        correlation_id="C-1",
        idempotency_key="I-1",
        started_at=NOW,
        updated_at=NOW,
    )
    task = HumanTask(
        **CONTEXT,
        task_id="T-1",
        workflow_id="W-1",
        run_id="R-1",
        step_id="approval",
        definition_version="1.0",
        correlation_id="C-1",
        created_at=NOW,
    )
    context = TenantContext(**CONTEXT, correlation_id="C-1", actor="user@example.com")
    event = AuditEvent(
        **CONTEXT,
        event_id="E-1",
        event_type="workflow.started",
        actor="user@example.com",
        correlation_id="C-1",
        timestamp=NOW,
    )
    for record in [definition, execution, task, context, event]:
        assert type(record).model_validate_json(record.model_dump_json()) == record
        assert record.tenant == "acme"
    with pytest.raises(ValidationError):
        WorkflowDefinition.model_validate(
            {**definition.model_dump(), "content_hash": "not-a-sha256"}
        )


@pytest.mark.parametrize(
    "timestamp", [datetime(2026, 9, 26), "2026-09-26T12:00:00", 1780000000, True]
)
def test_audit_timestamps_require_explicit_offset_and_reject_epoch_coercion(timestamp: Any) -> None:
    with pytest.raises(ValidationError):
        AuditEvent.model_validate(
            {
                **CONTEXT,
                "event_id": "E-1",
                "event_type": "workflow.started",
                "actor": "user",
                "correlation_id": "C-1",
                "timestamp": timestamp,
            }
        )
