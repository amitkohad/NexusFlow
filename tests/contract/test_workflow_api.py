"""HTTP acceptance contracts for governed starts and business observation."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from contracts import DefinitionDocument, ExecutionState, JsonObject
from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient, Response
from sqlalchemy.exc import SQLAlchemyError
from workflow_api.api import create_app
from workflow_api.backend import BackendNotFound, BackendUnavailable, BusinessSnapshot
from workflow_api.repository import ExecutionRecord, Scope, WorkflowRepository
from workflow_api.security import Principal, StaticTokenAuthenticator

pytestmark = pytest.mark.contract

CONTEXT = {"tenant": "acme", "business_domain": "finance", "application": "adjustments"}
ALL_PERMISSIONS = frozenset(
    {
        "definitions:read",
        "definitions:write",
        "definitions:approve",
        "definitions:promote",
        "workflows:read",
        "workflows:start",
        "workflows:signal",
        "workflows:cancel",
    }
)
AUTH = {"Authorization": "Bearer owner-token"}


@dataclass
class FakeBackend:
    starts: list[dict[str, Any]] = field(default_factory=list)
    signals: list[dict[str, Any]] = field(default_factory=list)
    cancellations: list[tuple[str, str]] = field(default_factory=list)
    statuses: dict[str, BusinessSnapshot] = field(default_factory=dict)
    unavailable: bool = False
    fail_once: bool = False
    unexpected_error: bool = False
    not_found: bool = False

    async def start(
        self,
        workflow_id: str,
        workflow_type: str,
        definition: DefinitionDocument,
        request: JsonObject,
        variables: JsonObject,
    ) -> str:
        if self.unexpected_error:
            raise RuntimeError("password=private-secret host=internal.example")
        self.starts.append(
            {
                "workflow_id": workflow_id,
                "workflow_type": workflow_type,
                "definition": definition,
                "request": request,
                "variables": variables,
            }
        )
        if self.fail_once:
            self.fail_once = False
            raise BackendUnavailable()
        if self.unavailable:
            raise BackendUnavailable()
        self.statuses.setdefault(workflow_id, BusinessSnapshot(ExecutionState.RUNNING))
        return f"private-run-{workflow_id}"

    async def status(self, workflow_id: str, run_id: str) -> BusinessSnapshot:
        if self.unavailable:
            raise BackendUnavailable()
        if self.not_found:
            raise BackendNotFound()
        return self.statuses[workflow_id]

    async def signal(
        self, workflow_id: str, run_id: str, signalname: str, payload: dict[str, Any]
    ) -> None:
        self.signals.append(
            {"workflow_id": workflow_id, "run_id": run_id, "signal": signalname, "payload": payload}
        )

    async def cancel(self, workflow_id: str, run_id: str) -> None:
        self.cancellations.append((workflow_id, run_id))

    async def ready(self) -> bool:
        return not self.unavailable


@dataclass
class Harness:
    client: TestClient
    backend: FakeBackend
    repository: WorkflowRepository
    authenticator: StaticTokenAuthenticator


@pytest.fixture
def harness(tmp_path: Path) -> Iterator[Harness]:
    repository = WorkflowRepository(f"sqlite:///{(tmp_path / 'api.db').as_posix()}")
    repository.create_schema()
    backend = FakeBackend()
    authenticator = StaticTokenAuthenticator(
        {
            "owner-token": Principal("owner-service", **CONTEXT, permissions=ALL_PERMISSIONS),
            "reader-token": Principal(
                "reader-service", **CONTEXT, permissions=frozenset({"workflows:read"})
            ),
            "other-token": Principal(
                "other-service", "other-tenant", "finance", "adjustments", ALL_PERMISSIONS
            ),
        }
    )
    with TestClient(
        create_app(repository, backend, authenticator=authenticator, environment="local"),
        raise_server_exceptions=False,
    ) as client:
        yield Harness(client, backend, repository, authenticator)
    repository.close()


def definition_payload(version: str = "1.0") -> dict[str, Any]:
    return {
        **CONTEXT,
        "definition_id": "definition-adjustment",
        "workflow_type": "customer-adjustment",
        "version": version,
        "owner": "finance-team",
        "definition_document": {
            "start_at": "approval",
            "steps": {
                "approval": {
                    "type": "approval",
                    "on_approved": "done",
                    "on_rejected": "rejected",
                    "on_timeout": "timeout",
                },
                "done": {"type": "end"},
                "rejected": {"type": "end", "outcome": "REJECTED"},
                "timeout": {"type": "end", "outcome": "TIMED_OUT"},
            },
        },
    }


def start_payload(key: str = "start-1") -> dict[str, Any]:
    return {
        **CONTEXT,
        "business_reference": "adjustment-1",
        "correlation_id": "business-correlation-1",
        "idempotency_key": key,
        "request": {"customer_id": "customer-1", "amount": 2500},
        "variables": {"source": "api"},
    }


def promote(harness: Harness, version: str = "1.0") -> None:
    registered = harness.client.post(
        "/api/v1/workflows", json=definition_payload(version), headers=AUTH
    )
    assert registered.status_code == 201, registered.text
    assert registered.json()["created_by"] == "owner-service"
    approved = harness.client.post(
        f"/api/v1/definitions/customer-adjustment/{version}/approve", json=CONTEXT, headers=AUTH
    )
    assert approved.status_code == 200, approved.text
    assert approved.json()["approved_by"] == "owner-service"
    promoted = harness.client.post(
        f"/api/v1/definitions/customer-adjustment/{version}/promote", json=CONTEXT, headers=AUTH
    )
    assert promoted.status_code == 200, promoted.text
    assert promoted.json()["status"] == "promoted"
    assert promoted.json()["promoted_by"] == "owner-service"


def start(harness: Harness, key: str = "start-1") -> Response:
    return harness.client.post(
        "/api/v1/workflows/customer-adjustment/start", json=start_payload(key), headers=AUTH
    )


def assert_problem(response: Response, status: int) -> dict[str, Any]:
    assert response.status_code == status, response.text
    assert response.headers["content-type"].startswith("application/problem+json")
    problem: dict[str, Any] = response.json()
    assert problem["status"] == status
    assert problem["code"]
    assert problem["correlation_id"]
    assert problem["instance"].startswith("/")
    assert "private-secret" not in response.text
    assert "internal.example" not in response.text
    return problem


def assert_private_fields_absent(value: Any) -> None:
    if isinstance(value, dict):
        assert not {"run_id", "task_queue", "worker", "backend_type", "raw_history"}.intersection(
            value
        )
        for child in value.values():
            assert_private_fields_absent(child)
    elif isinstance(value, list):
        for child in value:
            assert_private_fields_absent(child)


def test_register_promote_start_observe_and_business_history(harness: Harness) -> None:
    promote(harness)
    response = start(harness)
    assert response.status_code == 202, response.text
    execution = response.json()
    workflow_id = execution["workflow_id"]
    assert execution["definition_version"] == "1.0"
    assert execution["business_reference"] == "adjustment-1"
    assert execution["correlation_id"] == "business-correlation-1"
    assert len(harness.backend.starts) == 1
    assert harness.backend.starts[0]["request"]["amount"] == 2500
    assert harness.backend.starts[0]["variables"]["source"] == "api"
    harness.backend.statuses[workflow_id] = BusinessSnapshot(
        ExecutionState.WAITING_FOR_APPROVAL,
        current_step="approval",
        transitions=({"step": "approval", "state": "WAITING_FOR_APPROVAL", "detail": "Waiting"},),
    )
    status = harness.client.get(execution["links"]["status"], headers=AUTH)
    assert status.status_code == 200, status.text
    assert status.json()["state"] == "WAITING_FOR_APPROVAL"
    assert status.json()["current_step"] == "approval"
    detail = harness.client.get(execution["links"]["self"], headers=AUTH)
    assert detail.status_code == 200
    assert detail.json()["workflow_id"] == workflow_id
    history = harness.client.get(execution["links"]["history"], headers=AUTH)
    assert history.status_code == 200, history.text
    assert history.json()["items"]
    assert any(event["new_state"] == "WAITING_FOR_APPROVAL" for event in history.json()["items"])
    for body in (execution, status.json(), detail.json(), history.json()):
        assert_private_fields_absent(body)
        assert "private-run-" not in str(body)


@pytest.mark.parametrize("token", [None, "invalid-token"])
def test_missing_or_invalid_identity_is_rejected(harness: Harness, token: str | None) -> None:
    headers = {} if token is None else {"Authorization": f"Bearer {token}"}
    assert_problem(harness.client.get("/api/v1/workflows", headers=headers), 401)
    assert harness.backend.starts == []


def test_unconfigured_identity_denies_access(tmp_path: Path) -> None:
    repository = WorkflowRepository(f"sqlite:///{(tmp_path / 'deny.db').as_posix()}")
    repository.create_schema()
    with TestClient(create_app(repository, FakeBackend()), raise_server_exceptions=False) as client:
        assert_problem(client.get("/api/v1/workflows", headers=AUTH), 401)
    repository.close()


def test_permissions_are_required_before_starting_or_registering(harness: Harness) -> None:
    reader = {"Authorization": "Bearer reader-token"}
    assert_problem(
        harness.client.post("/api/v1/workflows", json=definition_payload(), headers=reader), 403
    )
    assert_problem(
        harness.client.post(
            "/api/v1/workflows/customer-adjustment/start", json=start_payload(), headers=reader
        ),
        403,
    )
    assert harness.backend.starts == []


@pytest.mark.parametrize("field", ["tenant", "business_domain", "application"])
def test_body_scope_cannot_override_identity(harness: Harness, field: str) -> None:
    body = {**definition_payload(), field: "other-scope"}
    assert_problem(harness.client.post("/api/v1/workflows", json=body, headers=AUTH), 403)
    assert harness.client.get("/api/v1/definitions", headers=AUTH).json()["items"] == []


def test_cross_tenant_execution_and_registry_are_hidden(harness: Harness) -> None:
    promote(harness)
    execution = start(harness).json()
    other = {"Authorization": "Bearer other-token"}
    for link in execution["links"].values():
        assert_problem(harness.client.get(link, headers=other), 404)
    assert_problem(
        harness.client.get("/api/v1/definitions/customer-adjustment/1.0", headers=other), 404
    )
    assert harness.client.get("/api/v1/workflows", headers=other).json()["items"] == []
    assert harness.client.get("/api/v1/definitions", headers=other).json()["items"] == []


def test_unapproved_definition_cannot_be_promoted_or_started(harness: Harness) -> None:
    registered = harness.client.post("/api/v1/workflows", json=definition_payload(), headers=AUTH)
    assert registered.status_code == 201
    assert_problem(
        harness.client.post(
            "/api/v1/definitions/customer-adjustment/1.0/promote", json=CONTEXT, headers=AUTH
        ),
        409,
    )
    assert_problem(
        harness.client.post(
            "/api/v1/workflows/customer-adjustment/start",
            json={**start_payload(), "definition_version": "1.0"},
            headers=AUTH,
        ),
        409,
    )
    assert harness.backend.starts == []


def test_invalid_graph_returns_actionable_safe_issues(harness: Harness) -> None:
    body = definition_payload()
    body["definition_document"]["steps"]["approval"]["on_approved"] = "missing"
    problem = assert_problem(harness.client.post("/api/v1/workflows", json=body, headers=AUTH), 422)
    assert problem["issues"]
    assert any("on_approved" in issue["path"] for issue in problem["issues"])
    assert harness.backend.starts == []


def test_registered_revision_cannot_be_replaced_in_place(harness: Harness) -> None:
    promote(harness)
    replacement = definition_payload()
    replacement["definition_document"]["request"] = {"different": True}
    assert_problem(harness.client.post("/api/v1/workflows", json=replacement, headers=AUTH), 409)
    stored = harness.client.get("/api/v1/definitions/customer-adjustment/1.0", headers=AUTH)
    assert stored.json()["definition_document"]["request"] == {}


@pytest.mark.parametrize(
    "unsupported",
    [
        {"task_queue": "custom-queue"},
        {"contract_version": "1.0"},
        {"compensation": {"capability": "post_adjustment"}},
        {"input": {"amount": "${request.amount}"}},
        {"capability": "unavailable_capability"},
        {"dependency": True},
    ],
)
def test_registration_rejects_features_unavailable_in_current_runtime(
    harness: Harness, unsupported: dict[str, Any]
) -> None:
    body = definition_payload()
    overrides = dict(unsupported)
    if overrides.pop("dependency", False):
        body["dependencies"] = [
            {
                "capability": "validate_request",
                "contract_version": "1.0",
                "task_queue": "custom-queue",
            }
        ]
    body["definition_document"] = {
        "start_at": "validate",
        "steps": {
            "validate": {
                "type": "activity",
                "capability": "validate_request",
                "next": "done",
                **overrides,
            },
            "done": {"type": "end"},
        },
    }
    assert_problem(harness.client.post("/api/v1/workflows", json=body, headers=AUTH), 422)
    assert harness.client.get("/api/v1/definitions", headers=AUTH).json()["items"] == []
    assert harness.backend.starts == []


def test_duplicate_start_is_stable_and_does_not_dispatch_twice(harness: Harness) -> None:
    promote(harness)
    first = start(harness)
    second = start(harness)
    assert first.status_code == 202
    assert second.status_code == 200, second.text
    assert second.json()["workflow_id"] == first.json()["workflow_id"]
    assert second.json()["replayed"] is True
    assert len(harness.backend.starts) == 1


def test_duplicate_after_promotion_remains_pinned_to_original_version(harness: Harness) -> None:
    promote(harness, "1.0")
    original = start(harness).json()
    promote(harness, "2.0")
    replayed = start(harness)
    fresh = start(harness, "start-2")
    assert replayed.status_code == 200, replayed.text
    assert replayed.json()["workflow_id"] == original["workflow_id"]
    assert replayed.json()["definition_version"] == "1.0"
    assert fresh.status_code == 202, fresh.text
    assert fresh.json()["definition_version"] == "2.0"
    assert len(harness.backend.starts) == 2


@pytest.mark.parametrize(
    "change",
    [
        {"business_reference": "different-reference"},
        {"request": {"amount": 500}},
        {"variables": {"source": "different"}},
        {"definition_version": "2.0"},
    ],
)
def test_reuse_with_different_business_input_conflicts(
    harness: Harness, change: dict[str, Any]
) -> None:
    promote(harness)
    assert start(harness).status_code == 202
    promote(harness, "2.0")
    assert_problem(
        harness.client.post(
            "/api/v1/workflows/customer-adjustment/start",
            json={**start_payload(), **change},
            headers=AUTH,
        ),
        409,
    )
    assert len(harness.backend.starts) == 1


def test_uncertain_start_retry_reuses_reserved_workflow_id(harness: Harness) -> None:
    promote(harness)
    harness.backend.fail_once = True
    assert_problem(start(harness), 503)
    recovered = start(harness)
    assert recovered.status_code == 202, recovered.text
    assert len(harness.backend.starts) == 2
    assert harness.backend.starts[0]["workflow_id"] == harness.backend.starts[1]["workflow_id"]
    assert start(harness).status_code == 200
    assert len(harness.backend.starts) == 2


async def test_concurrent_duplicate_starts_resolve_to_one_execution(harness: Harness) -> None:
    promote(harness)
    async with AsyncClient(
        transport=ASGITransport(app=harness.client.app), base_url="http://testserver"
    ) as client:
        responses = await asyncio.gather(
            *(
                client.post(
                    "/api/v1/workflows/customer-adjustment/start",
                    json=start_payload(),
                    headers=AUTH,
                )
                for _ in range(6)
            )
        )
    assert all(response.status_code in {200, 202} for response in responses), [
        response.text for response in responses
    ]
    assert len({response.json()["workflow_id"] for response in responses}) == 1
    assert len({attempt["workflow_id"] for attempt in harness.backend.starts}) == 1
    executions = harness.client.get("/api/v1/workflows", headers=AUTH).json()
    assert len(executions["items"]) == 1


def test_restart_retains_promotions_and_idempotency_records(harness: Harness) -> None:
    promote(harness)
    original = start(harness).json()
    restarted_repository = WorkflowRepository(str(harness.repository.engine.url))
    try:
        with TestClient(
            create_app(
                restarted_repository,
                harness.backend,
                authenticator=harness.authenticator,
                environment="local",
            ),
            raise_server_exceptions=False,
        ) as client:
            repeated = client.post(
                "/api/v1/workflows/customer-adjustment/start", json=start_payload(), headers=AUTH
            )
            assert repeated.status_code == 200, repeated.text
            assert repeated.json()["workflow_id"] == original["workflow_id"]
            assert repeated.json()["definition_version"] == "1.0"
            assert repeated.json()["replayed"] is True
            assert client.get(original["links"]["self"], headers=AUTH).status_code == 200
            definitions = client.get("/api/v1/definitions", headers=AUTH).json()
            assert definitions["items"][0]["status"] == "promoted"
    finally:
        restarted_repository.close()
    assert len(harness.backend.starts) == 1


def test_persistence_failure_after_backend_acceptance_recovers_same_id(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    promote(harness)
    original_mark_started = harness.repository.mark_started
    failed = False

    def fail_first_mark(
        scope: Scope, workflow_id: str, run_id: str, now: datetime
    ) -> ExecutionRecord:
        nonlocal failed
        if not failed:
            failed = True
            raise SQLAlchemyError("password=private-secret host=internal.example")
        return original_mark_started(scope, workflow_id, run_id, now)

    monkeypatch.setattr(harness.repository, "mark_started", fail_first_mark)
    assert_problem(start(harness), 503)
    recovered = start(harness)
    assert recovered.status_code == 202, recovered.text
    assert recovered.json()["workflow_id"] == harness.backend.starts[0]["workflow_id"]
    assert len({attempt["workflow_id"] for attempt in harness.backend.starts}) == 1
    assert start(harness).status_code == 200
    assert len(harness.backend.starts) == 2


def test_business_history_paginates_and_repeated_polling_does_not_duplicate(
    harness: Harness,
) -> None:
    promote(harness)
    execution = start(harness).json()
    harness.backend.statuses[execution["workflow_id"]] = BusinessSnapshot(
        ExecutionState.WAITING_FOR_APPROVAL,
        "approval",
        ({"step": "approval", "state": "WAITING_FOR_APPROVAL", "detail": "Waiting"},),
    )
    path = execution["links"]["history"]
    first = harness.client.get(path, headers=AUTH).json()
    repeated = harness.client.get(path, headers=AUTH).json()
    assert [event["event_id"] for event in first["items"]] == [
        event["event_id"] for event in repeated["items"]
    ]
    page = harness.client.get(path, params={"limit": 1, "offset": 0}, headers=AUTH).json()
    assert len(page["items"]) == 1
    assert page["next_offset"] == 1
    next_page = harness.client.get(path, params={"limit": 1, "offset": 1}, headers=AUTH).json()
    assert page["items"][0]["event_id"] != next_page["items"][0]["event_id"]
    assert_private_fields_absent(first)


@pytest.mark.parametrize("failure", ["unavailable", "not_found"])
def test_durable_history_remains_readable_when_runtime_cannot_refresh(
    harness: Harness, failure: str
) -> None:
    promote(harness)
    execution = start(harness).json()
    history_path = execution["links"]["history"]
    original = harness.client.get(history_path, headers=AUTH)
    assert original.status_code == 200
    assert original.json()["refreshed"] is True
    setattr(harness.backend, failure, True)
    cached = harness.client.get(history_path, headers=AUTH)
    assert cached.status_code == 200, cached.text
    assert cached.json()["refreshed"] is False
    assert cached.json()["items"] == original.json()["items"]
    assert_private_fields_absent(cached.json())


@pytest.mark.parametrize("query", [{"limit": 0}, {"limit": 101}, {"offset": -1}, {"limit": "bad"}])
def test_invalid_pagination_returns_problem(harness: Harness, query: dict[str, Any]) -> None:
    assert_problem(harness.client.get("/api/v1/workflows", params=query, headers=AUTH), 422)


def test_execution_lists_are_scoped_and_paginated(harness: Harness) -> None:
    promote(harness)
    first = start(harness, "start-1").json()
    second = start(harness, "start-2").json()
    page = harness.client.get("/api/v1/workflows", params={"limit": 1}, headers=AUTH)
    assert page.status_code == 200
    assert page.json()["items"][0]["workflow_id"] == first["workflow_id"]
    assert page.json()["next_offset"] == 1
    following = harness.client.get(
        "/api/v1/workflows", params={"limit": 1, "offset": 1}, headers=AUTH
    )
    assert following.json()["items"][0]["workflow_id"] == second["workflow_id"]
    assert following.json()["next_offset"] is None
    assert_private_fields_absent(page.json())


def test_signal_actor_is_injected_and_cancellation_is_audited(harness: Harness) -> None:
    promote(harness)
    execution = start(harness).json()
    workflow_id = execution["workflow_id"]
    harness.backend.statuses[workflow_id] = BusinessSnapshot(
        ExecutionState.WAITING_FOR_APPROVAL, "approval"
    )
    signaled = harness.client.post(
        f"/api/v1/workflows/{workflow_id}/signal",
        json={"approved": True, "comment": "Reviewed"},
        headers=AUTH,
    )
    assert signaled.status_code == 202, signaled.text
    assert signaled.json()["accepted"] is True
    assert harness.backend.signals[0]["payload"]["approver"] == "owner-service"
    assert harness.backend.signals[0]["payload"]["approved"] is True
    cancelled = harness.client.post(
        f"/api/v1/workflows/{workflow_id}/cancel", json={"reason": "No longer needed"}, headers=AUTH
    )
    assert cancelled.status_code == 202, cancelled.text
    assert len(harness.backend.cancellations) == 1
    history = harness.client.get(execution["links"]["history"], headers=AUTH).json()
    assert any(
        event["actor"] == "owner-service" and "signal" in event["event_type"]
        for event in history["items"]
    )
    assert any(
        event["actor"] == "owner-service" and "cancel" in event["event_type"]
        for event in history["items"]
    )
    assert_private_fields_absent(history)


def test_signal_body_cannot_spoof_actor_or_boolean(harness: Harness) -> None:
    promote(harness)
    workflow_id = start(harness).json()["workflow_id"]
    for body in ({"approved": True, "actor": "spoofed"}, {"approved": "true"}):
        assert_problem(
            harness.client.post(f"/api/v1/workflows/{workflow_id}/signal", json=body, headers=AUTH),
            422,
        )
    assert harness.backend.signals == []


def test_business_correlation_is_returned_and_header_mismatch_rejected(harness: Harness) -> None:
    promote(harness)
    matched = harness.client.post(
        "/api/v1/workflows/customer-adjustment/start",
        json=start_payload(),
        headers={**AUTH, "X-Correlation-ID": "business-correlation-1"},
    )
    assert matched.status_code == 202, matched.text
    assert matched.headers["X-Correlation-ID"] == "business-correlation-1"
    assert_problem(
        harness.client.post(
            "/api/v1/workflows/customer-adjustment/start",
            json=start_payload("different"),
            headers={**AUTH, "X-Correlation-ID": "different-correlation"},
        ),
        400,
    )
    assert len(harness.backend.starts) == 1


@pytest.mark.parametrize("invalid", ["contains space", "invalid/slash", "x" * 129])
def test_invalid_correlation_headers_return_safe_problem(harness: Harness, invalid: str) -> None:
    assert_problem(
        harness.client.get("/api/v1/workflows", headers={**AUTH, "X-Correlation-ID": invalid}), 400
    )


@pytest.mark.parametrize(
    "raw",
    [
        '{"tenant":"acme","tenant":"other"}',
        '{"request":{"value":NaN}}',
        '{"request":{"value":Infinity}}',
        '{"request":{"value":-Infinity}}',
    ],
)
def test_ambiguous_or_nonfinite_json_is_rejected_before_validation(
    harness: Harness, raw: str
) -> None:
    assert_problem(
        harness.client.post(
            "/api/v1/workflows/customer-adjustment/start",
            content=raw,
            headers={**AUTH, "Content-Type": "application/json"},
        ),
        422,
    )
    assert harness.backend.starts == []


def test_oversized_request_is_rejected_without_dispatch(harness: Harness) -> None:
    response = harness.client.post(
        "/api/v1/workflows/customer-adjustment/start",
        json={**start_payload(), "request": {"large": "x" * (1024 * 1024)}},
        headers=AUTH,
    )
    assert_problem(response, 413)
    assert harness.backend.starts == []


def test_backend_failure_and_unexpected_exception_are_sanitized(harness: Harness) -> None:
    promote(harness)
    harness.backend.unavailable = True
    assert_problem(start(harness), 503)
    assert harness.client.get("/health").status_code == 200
    assert harness.client.get("/ready").status_code == 503
    harness.backend.unavailable = False
    harness.backend.unexpected_error = True
    problem = assert_problem(start(harness, "new-start"), 500)
    assert "RuntimeError" not in problem["detail"]


def test_generated_openapi_documents_business_schemas_and_authenticated_operations(
    harness: Harness,
) -> None:
    response = harness.client.get("/api/v1/openapi.json")
    assert response.status_code == 200
    schema = response.json()
    assert schema["paths"]["/api/v1/workflows/{workflow_type}/start"]["post"]["security"]
    for name in ("WorkflowExecutionResponse", "WorkflowStatusResponse", "BusinessAuditEvent"):
        properties = schema["components"]["schemas"][name]["properties"]
        assert not {"run_id", "task_queue", "worker", "backend_type"}.intersection(properties)
    assert "ProblemDetails" in schema["components"]["schemas"]
    assert "SignalWorkflowRequest" in schema["components"]["schemas"]
    assert "/api/v1/tasks" not in schema["paths"]
