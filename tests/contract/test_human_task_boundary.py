"""Boundary regressions for task shape and permanently blocked delivery."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, NoReturn, cast

import pytest
from fastapi.testclient import TestClient
from human_task_service.api import create_app
from human_task_service.dispatch import (
    TaskWorkflowClosed,
    TaskWorkflowMissing,
    TemporalTaskDispatcher,
)
from human_task_service.models import TaskAuditRow, TaskOutboxRow, TaskRow
from human_task_service.repository import TaskRepository
from human_task_service.security import StaticTokenAuthenticator, TaskPrincipal
from sqlalchemy import select
from temporalio.client import Client
from temporalio.service import RPCError, RPCStatusCode

pytestmark = pytest.mark.contract
SERVICE_TOKEN = "service-token-00000000000000000000"
ACTOR_TOKEN = "actor-token-000000000000000000000"
SCOPE = {"tenant": "demo", "business_domain": "finance", "application": "adjustments"}


class DefinitiveFailureDispatcher:
    def __init__(self, error_type: type[Exception] = TaskWorkflowClosed) -> None:
        self.calls = 0
        self.error_type = error_type

    async def signal(
        self,
        workflow_id: str,
        first_execution_run_id: str,
        signal_name: str,
        payload: dict[str, Any],
    ) -> None:
        self.calls += 1
        raise self.error_type("Workflow execution is unavailable")


class DescribeFailureHandle:
    def __init__(self, status: RPCStatusCode) -> None:
        self.status = status

    async def describe(self) -> NoReturn:
        raise RPCError("Describe failed", self.status, b"")


class DescribeFailureClient:
    def __init__(self, status: RPCStatusCode) -> None:
        self.handle = DescribeFailureHandle(status)

    def get_workflow_handle(self, workflow_id: str) -> DescribeFailureHandle:
        return self.handle


def _body(step_id: str = "manager_approval") -> dict[str, Any]:
    return {
        **SCOPE,
        "workflow_id": "workflow-1",
        "run_id": "run-1",
        "first_execution_run_id": "run-1",
        "step_id": step_id,
        "definition_version": "1.0",
        "correlation_id": "case-1",
        "business_reference": "CASE-1",
        "idempotency_key": "workflow-1:manager_approval:1.0",
        "assignee_group": "approvers",
        "timeout_seconds": 300,
        "package_id": "customer-adjustment",
        "package_release_id": "customer-adjustment-0.2.0",
        "build_id": "customer-adjustment-0.2.0",
    }


def test_invalid_step_is_rejected_before_persistence(tmp_path: Path) -> None:
    repository = TaskRepository(f"sqlite:///{(tmp_path / 'invalid-step.db').as_posix()}")
    repository.create_schema()
    app = create_app(
        repository,
        DefinitiveFailureDispatcher(),
        authenticator=StaticTokenAuthenticator(
            {
                SERVICE_TOKEN: TaskPrincipal(
                    subject="package-activity",
                    tenant=SCOPE["tenant"],
                    business_domain=SCOPE["business_domain"],
                    application=SCOPE["application"],
                    permissions=frozenset({"tasks:create"}),
                )
            }
        ),
        environment="test",
    )
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/tasks",
            json=_body("invalid step"),
            headers={"Authorization": f"Bearer {SERVICE_TOKEN}"},
        )
    assert response.status_code == 422
    with repository.sessions() as session:
        assert session.scalars(select(TaskRow)).all() == []
    repository.close()


@pytest.mark.parametrize(
    "error_type,reason",
    [
        (TaskWorkflowClosed, "workflow_closed"),
        (TaskWorkflowMissing, "workflow_missing"),
    ],
)
def test_definitive_delivery_failure_blocks_outbox_once_with_audit(
    tmp_path: Path, error_type: type[Exception], reason: str
) -> None:
    repository = TaskRepository(f"sqlite:///{(tmp_path / 'blocked-chain.db').as_posix()}")
    repository.create_schema()
    dispatcher = DefinitiveFailureDispatcher(error_type)
    app = create_app(
        repository,
        dispatcher,
        authenticator=StaticTokenAuthenticator(
            {
                SERVICE_TOKEN: TaskPrincipal(
                    subject="package-activity",
                    tenant=SCOPE["tenant"],
                    business_domain=SCOPE["business_domain"],
                    application=SCOPE["application"],
                    permissions=frozenset({"tasks:create"}),
                ),
                ACTOR_TOKEN: TaskPrincipal(
                    subject="alice",
                    tenant=SCOPE["tenant"],
                    business_domain=SCOPE["business_domain"],
                    application=SCOPE["application"],
                    permissions=frozenset({"tasks:read", "tasks:act"}),
                    groups=frozenset({"approvers"}),
                ),
            }
        ),
        environment="test",
    )
    with TestClient(app) as client:
        created = client.post(
            "/api/v1/tasks",
            json=_body(),
            headers={"Authorization": f"Bearer {SERVICE_TOKEN}"},
        )
        assert created.status_code == 201
        task_id = created.json()["task_id"]
        decided = client.post(
            f"/api/v1/tasks/{task_id}/approve",
            json={},
            headers={"Authorization": f"Bearer {ACTOR_TOKEN}"},
        )
        assert decided.status_code == 200
    assert asyncio.run(app.state.task_service.dispatch_pending()) == 0
    assert asyncio.run(app.state.task_service.dispatch_pending()) == 0
    assert dispatcher.calls == 1
    with repository.sessions() as session:
        outbox = session.scalar(select(TaskOutboxRow).where(TaskOutboxRow.task_id == task_id))
        audit = session.scalars(
            select(TaskAuditRow)
            .where(TaskAuditRow.task_id == task_id)
            .order_by(TaskAuditRow.created_at, TaskAuditRow.event_id)
        ).all()
    assert outbox is not None and outbox.status == "BLOCKED"
    assert audit[-1].event_type == "DISPATCH_BLOCKED"
    assert audit[-1].reason == reason
    repository.close()


def test_only_describe_not_found_is_permanent() -> None:
    missing = TemporalTaskDispatcher(cast(Client, DescribeFailureClient(RPCStatusCode.NOT_FOUND)))
    with pytest.raises(TaskWorkflowMissing):
        asyncio.run(missing.signal("workflow-1", "run-1", "approve", {}))
    unavailable = TemporalTaskDispatcher(
        cast(Client, DescribeFailureClient(RPCStatusCode.UNAVAILABLE))
    )
    with pytest.raises(RPCError) as caught:
        asyncio.run(unavailable.signal("workflow-1", "run-1", "approve", {}))
    assert caught.value.status == RPCStatusCode.UNAVAILABLE
