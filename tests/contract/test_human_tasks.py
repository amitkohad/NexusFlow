"""Human-task HTTP contracts: scoped actors, idempotency, evidence, and durable decisions."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from human_task_service.api import create_app
from human_task_service.models import TaskAuditRow, TaskOutboxRow
from human_task_service.repository import TaskRepository
from human_task_service.security import StaticTokenAuthenticator, TaskPrincipal
from sqlalchemy import select

pytestmark = pytest.mark.contract

SCOPE = {"tenant": "demo", "business_domain": "customer-services", "application": "adjustments"}
SERVICE_TOKEN = "service-token-00000000000000000000"
APPROVER_TOKEN = "approver-token-000000000000000000"
OTHER_GROUP_TOKEN = "other-group-token-00000000000000"
MANAGER_TOKEN = "manager-token-000000000000000000"
FOREIGN_TOKEN = "foreign-token-000000000000000000"


@dataclass
class RecordingDispatcher:
    signals: list[tuple[str, str, str, dict[str, Any]]] = field(default_factory=list)
    fail_next: bool = False

    async def signal(
        self, workflow_id: str, run_id: str, signal_name: str, payload: dict[str, Any]
    ) -> None:
        if self.fail_next:
            self.fail_next = False
            raise RuntimeError("temporary Temporal outage")
        self.signals.append((workflow_id, run_id, signal_name, payload))


@dataclass
class Harness:
    client: TestClient
    app: FastAPI
    repository: TaskRepository
    dispatcher: RecordingDispatcher


@pytest.fixture
def harness(tmp_path: Path) -> Iterator[Harness]:
    repository = TaskRepository(f"sqlite:///{(tmp_path / 'human-tasks.db').as_posix()}")
    repository.create_schema()
    dispatcher = RecordingDispatcher()
    permissions = frozenset({"tasks:create", "tasks:read", "tasks:act", "tasks:manage"})
    principals = {
        SERVICE_TOKEN: TaskPrincipal(
            subject="package-adapter",
            permissions=frozenset({"tasks:create", "tasks:read"}),
            groups=frozenset(),
            **SCOPE,
        ),
        APPROVER_TOKEN: TaskPrincipal(
            subject="alice",
            permissions=frozenset({"tasks:read", "tasks:act"}),
            groups=frozenset({"operations-managers"}),
            **SCOPE,
        ),
        OTHER_GROUP_TOKEN: TaskPrincipal(
            subject="bob",
            permissions=frozenset({"tasks:read", "tasks:act"}),
            groups=frozenset({"unrelated-group"}),
            **SCOPE,
        ),
        MANAGER_TOKEN: TaskPrincipal(
            subject="manager",
            permissions=permissions,
            groups=frozenset({"operations-managers"}),
            **SCOPE,
        ),
        FOREIGN_TOKEN: TaskPrincipal(
            subject="foreign",
            tenant="other-tenant",
            business_domain="customer-services",
            application="adjustments",
            permissions=permissions,
            groups=frozenset({"operations-managers"}),
        ),
    }
    app = create_app(
        repository,
        dispatcher,
        authenticator=StaticTokenAuthenticator(principals),
        environment="test",
    )
    with TestClient(app, raise_server_exceptions=False) as client:
        yield Harness(client, app, repository, dispatcher)
    repository.close()


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _create_body(key: str | None = None, **changes: Any) -> dict[str, Any]:
    workflow_id = "workflow-" + uuid4().hex
    body: dict[str, Any] = {
        **SCOPE,
        "workflow_id": workflow_id,
        "run_id": "run-1",
        "first_execution_run_id": "run-1",
        "step_id": "manager_approval",
        "definition_version": "1.0",
        "correlation_id": "customer-case-1",
        "business_reference": "CASE-1",
        "idempotency_key": key or f"{workflow_id}:manager_approval:1.0",
        "task_type": "approval",
        "assignee_group": "operations-managers",
        "timeout_seconds": 300,
        "package_id": "customer-adjustment",
        "package_release_id": "customer-adjustment-0.2.0",
        "build_id": "customer-adjustment-0.2.0",
    }
    body.update(changes)
    return body


def _create(harness: Harness, body: dict[str, Any] | None = None) -> dict[str, Any]:
    response = harness.client.post(
        "/api/v1/tasks", json=body or _create_body(), headers=_auth(SERVICE_TOKEN)
    )
    assert response.status_code == 201, response.text
    return response.json()


def _post(harness: Harness, task_id: str, action: str, token: str, body: dict[str, Any]) -> Any:
    return harness.client.post(f"/api/v1/tasks/{task_id}/{action}", json=body, headers=_auth(token))


def test_scoped_create_is_durable_and_idempotent(harness: Harness) -> None:
    body = _create_body()
    created = _create(harness, body)
    assert created["task_id"]
    assert created["workflow_id"] == body["workflow_id"]
    assert created["step_id"] == body["step_id"]
    assert created["first_execution_run_id"] == body["first_execution_run_id"]
    assert created["assignee_group"] == body["assignee_group"]
    assert created["package_release_id"] == body["package_release_id"]
    replay = harness.client.post("/api/v1/tasks", json=body, headers=_auth(SERVICE_TOKEN))
    assert replay.status_code in {200, 201}, replay.text
    assert replay.json()["task_id"] == created["task_id"]
    assert replay.json()["replayed"] is True
    compatible_retry = harness.client.post(
        "/api/v1/tasks",
        json={
            **body,
            "run_id": "run-2",
            "package_release_id": "customer-adjustment-0.3.0",
            "build_id": "customer-adjustment-0.3.0",
        },
        headers=_auth(SERVICE_TOKEN),
    )
    assert compatible_retry.status_code in {200, 201}, compatible_retry.text
    assert compatible_retry.json()["task_id"] == created["task_id"]
    assert compatible_retry.json()["run_id"] == "run-1"
    assert compatible_retry.json()["package_release_id"] == body["package_release_id"]
    detail = harness.client.get(
        f"/api/v1/tasks/{created['task_id']}", headers=_auth(APPROVER_TOKEN)
    )
    assert detail.status_code == 200, detail.text
    assert detail.json()["task_id"] == created["task_id"]
    listed = harness.client.get("/api/v1/tasks", headers=_auth(APPROVER_TOKEN))
    assert listed.status_code == 200, listed.text
    assert created["task_id"] in listed.text
    invisible = harness.client.get(
        f"/api/v1/tasks/{created['task_id']}", headers=_auth(FOREIGN_TOKEN)
    )
    assert invisible.status_code == 404, invisible.text
    assert (
        created["task_id"]
        not in harness.client.get("/api/v1/tasks", headers=_auth(FOREIGN_TOKEN)).text
    )
    changed = harness.client.post(
        "/api/v1/tasks",
        json={**body, "assignee_group": "different-group"},
        headers=_auth(SERVICE_TOKEN),
    )
    assert changed.status_code == 409, changed.text


@pytest.mark.parametrize("decision, approved", [("approve", True), ("reject", False)])
def test_authorized_decision_commits_once_with_actor_and_evidence(
    harness: Harness, decision: str, approved: bool
) -> None:
    task = _create(harness)
    task_id = task["task_id"]
    assert _post(harness, task_id, "claim", OTHER_GROUP_TOKEN, {}).status_code == 403
    assert _post(harness, task_id, "claim", APPROVER_TOKEN, {}).status_code == 200
    evidence = {"evidence_reference": "doc://approval-proof", "comment": "Reviewed"}
    response = _post(harness, task_id, decision, APPROVER_TOKEN, evidence)
    assert response.status_code == 200, response.text
    assert response.json()["actor"] == "alice"
    assert response.json()["evidence_reference"] == evidence["evidence_reference"]
    repeated = _post(harness, task_id, decision, APPROVER_TOKEN, evidence)
    assert repeated.status_code == 200, repeated.text
    assert repeated.json()["replayed"] is True
    assert (
        _post(
            harness,
            task_id,
            decision,
            APPROVER_TOKEN,
            {**evidence, "evidence_reference": "doc://different-proof"},
        ).status_code
        == 409
    )
    opposite = "reject" if decision == "approve" else "approve"
    assert _post(harness, task_id, opposite, APPROVER_TOKEN, {}).status_code == 409
    assert _post(harness, task_id, "claim", APPROVER_TOKEN, {}).status_code == 409
    assert (
        harness.client.get(f"/api/v1/tasks/{task_id}", headers=_auth(FOREIGN_TOKEN)).status_code
        == 404
    )
    assert harness.app.state.task_service is not None
    # Re-dispatch is safe even if an earlier HTTP completion already delivered it.
    asyncio.run(harness.app.state.task_service.dispatch_pending())
    assert len(harness.dispatcher.signals) == 1
    workflow_id, run_id, signal_name, payload = harness.dispatcher.signals[0]
    assert (workflow_id, run_id, signal_name) == (task["workflow_id"], task["run_id"], "approve")
    assert payload["task_id"] == task_id
    assert payload["event_id"]
    assert payload["approved"] is approved
    assert payload["approver"] == "alice"
    assert payload["evidence_reference"] == evidence["evidence_reference"]
    with harness.repository.sessions() as session:
        audit = session.scalars(select(TaskAuditRow).where(TaskAuditRow.task_id == task_id)).all()
        outbox = session.scalars(
            select(TaskOutboxRow).where(TaskOutboxRow.task_id == task_id)
        ).all()
    terminal_event = "APPROVED" if approved else "REJECTED"
    assert [event.event_type for event in audit] == ["CREATED", "CLAIMED", terminal_event]
    assert len(outbox) == 1


def test_untrusted_actor_and_scope_cannot_complete_task(harness: Harness) -> None:
    task = _create(harness)
    task_id = task["task_id"]
    assert _post(harness, task_id, "approve", SERVICE_TOKEN, {}).status_code == 403
    assert _post(harness, task_id, "approve", FOREIGN_TOKEN, {}).status_code == 404
    assert _post(harness, task_id, "approve", OTHER_GROUP_TOKEN, {}).status_code == 403
    assert harness.client.get(f"/api/v1/tasks/{task_id}").status_code == 401
    assert harness.client.post("/api/v1/tasks", json=_create_body()).status_code == 401


def test_completed_decision_retries_dispatch_without_reopening_task(harness: Harness) -> None:
    task = _create(harness)
    task_id = task["task_id"]
    assert _post(harness, task_id, "claim", APPROVER_TOKEN, {}).status_code == 200
    harness.dispatcher.fail_next = True
    completed = _post(
        harness,
        task_id,
        "complete",
        APPROVER_TOKEN,
        {"outcome": "approved", "evidence_reference": "doc://proof"},
    )
    assert completed.status_code == 200, completed.text
    assert not harness.dispatcher.signals
    assert _post(harness, task_id, "reject", APPROVER_TOKEN, {}).status_code == 409
    asyncio.run(harness.app.state.task_service.dispatch_pending())
    assert not harness.dispatcher.signals
    time.sleep(2.1)
    asyncio.run(harness.app.state.task_service.dispatch_pending())
    assert len(harness.dispatcher.signals) == 1
    assert harness.dispatcher.signals[0][3]["approved"] is True


def test_assignment_policy_and_sla_expiration_preserve_audited_lifecycle(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task = _create(harness)
    task_id = task["task_id"]
    assert task["due_at"] and task["sla_deadline"]
    assert (
        _post(
            harness,
            task_id,
            "reassign",
            APPROVER_TOKEN,
            {"assignee_group": "unrelated-group", "reason": "staffing"},
        ).status_code
        == 403
    )
    reassigned = _post(
        harness,
        task_id,
        "reassign",
        MANAGER_TOKEN,
        {"assignee_group": "unrelated-group", "reason": "staffing"},
    )
    assert reassigned.status_code == 200, reassigned.text
    assert reassigned.json()["assignee_group"] == "unrelated-group"
    assert _post(harness, task_id, "claim", APPROVER_TOKEN, {}).status_code == 403
    assert _post(harness, task_id, "claim", OTHER_GROUP_TOKEN, {}).status_code == 200
    delegated = _post(
        harness,
        task_id,
        "delegate",
        OTHER_GROUP_TOKEN,
        {"assignee": "alice", "reason": "handoff"},
    )
    assert delegated.status_code == 200, delegated.text
    assert delegated.json()["assignee"] == "alice"
    assert _post(harness, task_id, "claim", APPROVER_TOKEN, {}).status_code == 200

    overdue = _create(harness)
    overdue_id = overdue["task_id"]
    escalated = _post(
        harness,
        overdue_id,
        "escalate",
        MANAGER_TOKEN,
        {"assignee_group": "urgent-managers", "reason": "SLA breach"},
    )
    assert escalated.status_code == 200, escalated.text
    assert escalated.json()["assignee_group"] == "urgent-managers"
    assert escalated.json()["escalation_level"] == 1
    simulated_now = datetime.now(UTC) + timedelta(days=1)
    changed = harness.app.state.task_service.process_due_tasks(now=simulated_now)
    assert changed
    expired = harness.client.get(f"/api/v1/tasks/{overdue_id}", headers=_auth(MANAGER_TOKEN))
    assert expired.status_code == 200, expired.text
    assert expired.json()["status"] == "EXPIRED"
    assert _post(harness, overdue_id, "approve", APPROVER_TOKEN, {}).status_code == 409
    monkeypatch.setattr("human_task_service.repository.utcnow", lambda: simulated_now)
    asyncio.run(harness.app.state.task_service.dispatch_pending())
    expiration_signals = [
        entry
        for entry in harness.dispatcher.signals
        if entry[2] == "task_expired" and entry[3]["task_id"] == overdue_id
    ]
    assert len(expiration_signals) == 1
    assert expiration_signals[0][3]["event_id"]
    asyncio.run(harness.app.state.task_service.dispatch_pending())
    assert (
        len(
            [
                entry
                for entry in harness.dispatcher.signals
                if entry[2] == "task_expired" and entry[3]["task_id"] == overdue_id
            ]
        )
        == 1
    )


def test_create_contract_rejects_scope_forgery_and_unbounded_fields(harness: Harness) -> None:
    foreign_scope = harness.client.post(
        "/api/v1/tasks",
        json={**_create_body(), "tenant": "other-tenant"},
        headers=_auth(SERVICE_TOKEN),
    )
    assert foreign_scope.status_code == 403, foreign_scope.text
    invalid = harness.client.post(
        "/api/v1/tasks",
        json=_create_body(timeout_seconds=0),
        headers=_auth(SERVICE_TOKEN),
    )
    assert invalid.status_code == 422, invalid.text
    invalid = harness.client.post(
        "/api/v1/tasks",
        json=_create_body(assignee_group="x" * 1000),
        headers=_auth(SERVICE_TOKEN),
    )
    assert invalid.status_code == 422, invalid.text


def test_privileged_expire_is_idempotent_and_emits_one_timeout_signal(harness: Harness) -> None:
    task = _create(harness)
    task_id = task["task_id"]
    assert _post(harness, task_id, "expire", APPROVER_TOKEN, {}).status_code == 403
    expired = _post(
        harness,
        task_id,
        "expire",
        MANAGER_TOKEN,
        {"reason": "Business deadline cancelled approval"},
    )
    assert expired.status_code == 200, expired.text
    assert expired.json()["status"] == "EXPIRED"
    repeated = _post(harness, task_id, "expire", MANAGER_TOKEN, {})
    assert repeated.status_code == 200 and repeated.json()["replayed"] is True
    asyncio.run(harness.app.state.task_service.dispatch_pending())
    assert [
        (signal, payload["task_id"]) for _, _, signal, payload in harness.dispatcher.signals
    ] == [("task_expired", task_id)]


def test_sla_escalates_once_then_expires_at_extended_deadline(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    deadline = datetime.now(UTC) + timedelta(seconds=1)
    task = _create(
        harness,
        _create_body(
            timeout_seconds=120,
            sla_deadline=deadline.isoformat(),
            escalation_policy={
                "action": "escalate",
                "target_group": "senior-approvers",
                "extend_seconds": 60,
            },
        ),
    )
    task_id = task["task_id"]
    first_sweep = deadline + timedelta(seconds=1)
    assert harness.app.state.task_service.process_due_tasks(now=first_sweep) == 1
    escalated = harness.client.get(f"/api/v1/tasks/{task_id}", headers=_auth(MANAGER_TOKEN))
    assert escalated.status_code == 200, escalated.text
    assert escalated.json()["status"] == "ASSIGNED"
    assert escalated.json()["assignee_group"] == "senior-approvers"
    assert escalated.json()["escalation_level"] == 1
    next_sla = datetime.fromisoformat(escalated.json()["sla_deadline"])
    assert first_sweep < next_sla <= first_sweep + timedelta(seconds=60)
    second_sweep = next_sla + timedelta(seconds=1)
    assert harness.app.state.task_service.process_due_tasks(now=second_sweep) == 1
    expired = harness.client.get(f"/api/v1/tasks/{task_id}", headers=_auth(MANAGER_TOKEN))
    assert expired.status_code == 200, expired.text
    assert expired.json()["status"] == "EXPIRED"
    monkeypatch.setattr("human_task_service.repository.utcnow", lambda: second_sweep)
    asyncio.run(harness.app.state.task_service.dispatch_pending())
    assert [(name, payload["task_id"]) for _, _, name, payload in harness.dispatcher.signals] == [
        ("task_expired", task_id)
    ]
