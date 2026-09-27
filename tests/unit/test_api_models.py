"""Public HTTP contracts preserve business scope and reject impersonation."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, TypedDict

import pytest
from contracts import (
    MAX_JSON_DEPTH,
    ApproveDefinitionRequest,
    BusinessAuditEvent,
    CancelWorkflowRequest,
    DefinitionDocument,
    EndStep,
    ExecutionPage,
    HistoryPage,
    ProblemDetails,
    PromoteDefinitionRequest,
    RegisterDefinitionRequest,
    SignalWorkflowRequest,
    StartWorkflowRequest,
    WorkflowExecutionResponse,
    WorkflowLinks,
    WorkflowStatusResponse,
)
from pydantic import ValidationError


class Context(TypedDict):
    tenant: str
    business_domain: str
    application: str


CONTEXT: Context = {"tenant": "acme", "business_domain": "finance", "application": "adjustments"}
NOW = datetime(2026, 9, 27, tzinfo=timezone.utc)


def start_payload() -> dict[str, Any]:
    return {
        **CONTEXT,
        "business_reference": "adjustment-1",
        "correlation_id": "correlation-1",
        "idempotency_key": "start-1",
    }


def execution_payload() -> dict[str, Any]:
    return {
        **CONTEXT,
        "workflow_id": "workflow-1",
        "workflow_type": "customer-adjustment",
        "definition_id": "definition-1",
        "definition_version": "1.0",
        "state": "RUNNING",
        "business_reference": "adjustment-1",
        "correlation_id": "correlation-1",
        "started_at": NOW,
        "updated_at": NOW,
        "links": {
            "self": "/api/v1/workflows/workflow-1",
            "status": "/api/v1/workflows/workflow-1/status",
            "history": "/api/v1/workflows/workflow-1/history",
        },
    }


def test_client_can_roundtrip_start_and_business_status_without_runtime_identifiers() -> None:
    start = StartWorkflowRequest.model_validate(start_payload())
    assert start.definition_version is None
    assert start.request == {}
    assert start.variables == {}
    response = WorkflowExecutionResponse.model_validate(execution_payload())
    assert not response.replayed
    assert response.definition_version == "1.0"
    assert response.state == "RUNNING"
    assert response.current_step is None
    assert response.links.status.endswith("/status")
    for record in (start, response, WorkflowStatusResponse.model_validate(execution_payload())):
        assert type(record).model_validate_json(record.model_dump_json()) == record
    assert "run_id" not in response.model_dump()
    assert "task_queue" not in response.model_dump()
    assert "backend_type" not in response.model_dump()


@pytest.mark.parametrize("field", ["actor", "authorization_claims", "run_id", "task_queue"])
def test_start_cannot_claim_identity_or_runtime_settings(field: str) -> None:
    with pytest.raises(ValidationError, match="extra_forbidden"):
        StartWorkflowRequest.model_validate({**start_payload(), field: "spoofed"})


@pytest.mark.parametrize("field", ["run_id", "task_queue", "backend_type", "history"])
def test_execution_response_rejects_runtime_and_raw_history_fields(field: str) -> None:
    with pytest.raises(ValidationError, match="extra_forbidden"):
        WorkflowExecutionResponse.model_validate({**execution_payload(), field: "internal"})


@pytest.mark.parametrize("value", [0, True, "", "   ", None])
@pytest.mark.parametrize(
    "field", ["tenant", "correlation_id", "business_reference", "idempotency_key"]
)
def test_start_identifiers_do_not_coerce_or_accept_empty_claims(field: str, value: Any) -> None:
    with pytest.raises(ValidationError):
        StartWorkflowRequest.model_validate({**start_payload(), field: value})


@pytest.mark.parametrize(
    ("field", "maximum"),
    [
        ("tenant", 128),
        ("business_domain", 128),
        ("application", 128),
        ("correlation_id", 128),
        ("business_reference", 256),
        ("idempotency_key", 256),
        ("definition_version", 64),
    ],
)
def test_start_identifiers_have_bounded_storage_size(field: str, maximum: int) -> None:
    assert StartWorkflowRequest.model_validate({**start_payload(), field: "x" * maximum})
    with pytest.raises(ValidationError):
        StartWorkflowRequest.model_validate({**start_payload(), field: "x" * (maximum + 1)})


@pytest.mark.parametrize("value", ["space here", "path/value", "?query", "\nunsafe"])
def test_correlation_and_definition_versions_are_safe_url_components(value: str) -> None:
    for field in ("correlation_id", "definition_version"):
        with pytest.raises(ValidationError):
            StartWorkflowRequest.model_validate({**start_payload(), field: value})


def test_start_payload_defaults_are_independent_and_json_depth_is_bounded() -> None:
    first = StartWorkflowRequest.model_validate(start_payload())
    second = StartWorkflowRequest.model_validate(start_payload())
    first.request["amount"] = 2500
    assert second.request == {}
    deep: Any = None
    for _ in range(MAX_JSON_DEPTH):
        deep = [deep]
    with pytest.raises(ValidationError, match="nesting"):
        StartWorkflowRequest.model_validate({**start_payload(), "request": {"value": deep}})
    with pytest.raises(ValidationError):
        StartWorkflowRequest.model_validate({**start_payload(), "variables": {"value": NOW}})


def test_registration_keeps_document_version_and_rejects_lifecycle_spoofing() -> None:
    request = RegisterDefinitionRequest(
        **CONTEXT,
        definition_id="definition-1",
        workflow_type="customer-adjustment",
        version="1.0",
        owner="finance-team",
        definition_document=DefinitionDocument(start_at="done", steps={"done": EndStep()}),
    )
    assert RegisterDefinitionRequest.model_validate_json(request.model_dump_json()) == request
    for field in ("status", "approved_by", "created_by", "content_hash"):
        with pytest.raises(ValidationError, match="extra_forbidden"):
            RegisterDefinitionRequest.model_validate({**request.model_dump(), field: "spoofed"})
    assert ApproveDefinitionRequest.model_validate(CONTEXT).tenant == "acme"
    assert PromoteDefinitionRequest.model_validate(CONTEXT).environment == "local"
    with pytest.raises(ValidationError):
        PromoteDefinitionRequest.model_validate({**CONTEXT, "environment": "uncontrolled"})


@pytest.mark.parametrize("value", ["true", 1, 0, None])
def test_signal_requires_explicit_boolean_decision(value: Any) -> None:
    with pytest.raises(ValidationError):
        SignalWorkflowRequest.model_validate({"approved": value})


@pytest.mark.parametrize("field", ["actor", "tenant", "business_domain", "application", "run_id"])
def test_signal_actor_and_scope_must_come_from_authentication(field: str) -> None:
    with pytest.raises(ValidationError, match="extra_forbidden"):
        SignalWorkflowRequest.model_validate({"approved": True, field: "spoofed"})


def test_signal_and_cancellation_require_bounded_explicit_intent() -> None:
    assert SignalWorkflowRequest(approved=False).signal == "approve"
    assert CancelWorkflowRequest().reason == "Client cancellation"
    with pytest.raises(ValidationError):
        SignalWorkflowRequest.model_validate({"signal": "terminate", "approved": True})
    with pytest.raises(ValidationError):
        SignalWorkflowRequest(approved=True, comment="x" * 2001)
    for reason in ("", "   ", "x" * 2001):
        with pytest.raises(ValidationError):
            CancelWorkflowRequest(reason=reason)


def test_business_history_projects_actor_and_transition_without_runtime_data() -> None:
    event = BusinessAuditEvent(
        **CONTEXT,
        event_id="event-1",
        event_type="workflow.started",
        workflow_id="workflow-1",
        actor="client-1",
        correlation_id="correlation-1",
        timestamp=NOW,
        previous_state="CREATED",
        new_state="RUNNING",
    )
    history = HistoryPage(items=(event,), limit=25, offset=0)
    assert history.refreshed is True
    assert HistoryPage.model_validate_json(history.model_dump_json()) == history
    for field in ("run_id", "task_queue", "worker", "activity"):
        assert field not in event.model_dump()
        with pytest.raises(ValidationError, match="extra_forbidden"):
            BusinessAuditEvent.model_validate({**event.model_dump(), field: "internal"})


@pytest.mark.parametrize("limit", [True, "25", 0, 101, 1.5])
def test_pagination_rejects_ambiguous_and_unbounded_limits(limit: Any) -> None:
    with pytest.raises(ValidationError):
        HistoryPage.model_validate({"items": [], "limit": limit, "offset": 0})


@pytest.mark.parametrize("offset", [True, "0", -1, 1.5])
def test_pagination_requires_nonnegative_integer_offsets(offset: Any) -> None:
    with pytest.raises(ValidationError):
        HistoryPage.model_validate({"items": [], "limit": 25, "offset": offset})
    with pytest.raises(ValidationError):
        ExecutionPage.model_validate({"items": [], "limit": 25, "offset": 0, "next_offset": offset})


def test_problem_details_are_closed_bounded_and_have_business_correlation() -> None:
    problem = ProblemDetails(
        title="Forbidden",
        status=403,
        detail="Operation is not permitted",
        instance="/api/v1/workflows/workflow-1",
        code="FORBIDDEN",
        correlation_id="correlation-1",
    )
    assert problem.type == "about:blank"
    assert problem.issues == ()
    assert ProblemDetails.model_validate_json(problem.model_dump_json()) == problem
    for status in (399, 600, "403", True):
        with pytest.raises(ValidationError):
            ProblemDetails.model_validate({**problem.model_dump(), "status": status})
    with pytest.raises(ValidationError):
        ProblemDetails.model_validate(
            {**problem.model_dump(), "exception": "private backend message"}
        )


def test_models_are_frozen_and_response_links_are_required() -> None:
    response = WorkflowExecutionResponse.model_validate(execution_payload())
    with pytest.raises(ValidationError, match="frozen"):
        response.workflow_id = "different-workflow"
    with pytest.raises(ValidationError):
        WorkflowLinks.model_validate({"status": "/status", "history": "/history"})
