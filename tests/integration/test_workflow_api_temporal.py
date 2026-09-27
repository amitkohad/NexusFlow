"""Governed HTTP operations against SQLite and the real Temporal prototype.

Tests use one isolated Temporal CLI server, a unique worker queue per test, and a
temporary local database. API and worker run on the same asyncio event loop.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import ActivityError, RetryState
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from workflow_api.api import create_app
from workflow_api.backend import TemporalBackend
from workflow_api.repository import Scope, WorkflowRepository
from workflow_api.security import Principal, StaticTokenAuthenticator

from app.activities import execute_capability
from app.workflows import LightweightProcess

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="module")]

SCOPE = {"tenant": "integration-tenant", "business_domain": "finance", "application": "adjustments"}
TOKEN = "integration-test-token"
ACTOR = "manager.integration"
WORKFLOW_TYPE = "customer-adjustment"
ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class ApiFixture:
    http: httpx.AsyncClient
    repository: WorkflowRepository
    backend: TemporalBackend


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def api_temporal_environment() -> AsyncIterator[WorkflowEnvironment]:
    cli_path = os.environ.get("TEMPORAL_CLI_PATH") or shutil.which("temporal")
    if not cli_path or not Path(cli_path).is_file():
        pytest.fail(
            "Workflow API integration tests require a local Temporal CLI. Add temporal to "
            "PATH or set TEMPORAL_CLI_PATH to the executable's absolute path."
        )
    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=cli_path, dev_server_log_level="error", ui=False
    ) as environment:
        yield environment


@pytest_asyncio.fixture(loop_scope="module")
async def governed_api(
    api_temporal_environment: WorkflowEnvironment, tmp_path: Path
) -> AsyncIterator[ApiFixture]:
    repository = WorkflowRepository(f"sqlite:///{(tmp_path / 'api.sqlite').as_posix()}")
    repository.create_schema()
    queue = f"workflow-api-{uuid4().hex}"
    backend = TemporalBackend(api_temporal_environment.client, task_queue=queue)
    authenticator = StaticTokenAuthenticator(
        {
            TOKEN: Principal(
                subject=ACTOR,
                **SCOPE,
                permissions=frozenset(
                    {
                        "definitions:write",
                        "definitions:read",
                        "definitions:approve",
                        "definitions:promote",
                        "workflows:start",
                        "workflows:read",
                        "workflows:signal",
                        "workflows:cancel",
                    }
                ),
            ),
        }
    )
    application = create_app(repository, backend, authenticator=authenticator, environment="local")
    try:
        async with Worker(
            api_temporal_environment.client,
            task_queue=queue,
            workflows=[LightweightProcess],
            activities=[execute_capability],
        ):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=application),
                base_url="http://workflow-api.local",
                headers={"Authorization": f"Bearer {TOKEN}"},
            ) as http:
                yield ApiFixture(http, repository, backend)
    finally:
        repository.close()


@pytest.fixture
def sample_definition() -> dict[str, Any]:
    return json.loads((ROOT / "examples/customer_adjustment.json").read_text(encoding="utf-8"))


async def register_and_promote(
    http: httpx.AsyncClient, definition: dict[str, Any], version: str
) -> None:
    response = await http.post(
        "/api/v1/workflows",
        json={
            **SCOPE,
            "definition_id": "customer-adjustment-definition",
            "workflow_type": WORKFLOW_TYPE,
            "version": version,
            "owner": ACTOR,
            "definition_document": definition,
        },
    )
    assert response.status_code == 201, response.text
    assert response.json()["status"] == "validated"
    approved = await http.post(f"/api/v1/definitions/{WORKFLOW_TYPE}/{version}/approve", json=SCOPE)
    assert approved.status_code == 200, approved.text
    assert approved.json()["status"] == "approved"
    promoted = await http.post(
        f"/api/v1/definitions/{WORKFLOW_TYPE}/{version}/promote",
        json={**SCOPE, "environment": "local"},
    )
    assert promoted.status_code == 200, promoted.text
    assert promoted.json()["status"] == "promoted"


def start_request(amount: int = 1000, *, key: str = "sample-key") -> dict[str, Any]:
    return {
        **SCOPE,
        "business_reference": f"adjustment-{key}",
        "correlation_id": f"correlation-{key}",
        "idempotency_key": key,
        "request": {"amount": amount, "simulate_transient_failure": False},
        "variables": {"requested_by": "integration-client"},
    }


async def start(http: httpx.AsyncClient, request: dict[str, Any]) -> dict[str, Any]:
    response = await http.post(f"/api/v1/workflows/{WORKFLOW_TYPE}/start", json=request)
    assert response.status_code == 202, response.text
    return response.json()


async def wait_for_state(http: httpx.AsyncClient, workflow_id: str, state: str) -> dict[str, Any]:
    async with asyncio.timeout(15):
        while True:
            response = await http.get(f"/api/v1/workflows/{workflow_id}/status")
            assert response.status_code == 200, response.text
            status = response.json()
            if status["state"] == state:
                return status
            await asyncio.sleep(0.05)


async def test_http_start_duplicate_closed_run_status_and_business_history(
    governed_api: ApiFixture, sample_definition: dict[str, Any]
) -> None:
    http = governed_api.http
    await register_and_promote(http, sample_definition, "1.0")
    request = start_request()
    execution = await start(http, request)
    workflow_id = execution["workflow_id"]
    status = await wait_for_state(http, workflow_id, "COMPLETED")
    assert status["definition_version"] == "1.0"
    assert status["business_reference"] == request["business_reference"]
    assert status["correlation_id"] == request["correlation_id"]
    assert status["current_step"] is None

    record = governed_api.repository.get_execution(Scope(**SCOPE), workflow_id)
    assert record.run_id
    # Exercise both HTTP idempotency and the SDK's closed-run duplicate policy.
    repeated = await http.post(f"/api/v1/workflows/{WORKFLOW_TYPE}/start", json=request)
    assert repeated.status_code == 200, repeated.text
    assert repeated.json()["workflow_id"] == workflow_id
    assert repeated.json()["replayed"] is True
    duplicate_run = await governed_api.backend.start(
        workflow_id, WORKFLOW_TYPE, record.definition_document, record.request, record.variables
    )
    assert duplicate_run == record.run_id

    history = await http.get(f"/api/v1/workflows/{workflow_id}/history", params={"limit": 100})
    assert history.status_code == 200, history.text
    events = history.json()["items"]
    assert events
    assert any(event["step"] == "route" and event["new_state"] == "ROUTED" for event in events)
    assert any(
        event["step"] == "completed" and event["new_state"] == "COMPLETED" for event in events
    )
    assert all(event["workflow_id"] == workflow_id for event in events)
    assert all(event["correlation_id"] == request["correlation_id"] for event in events)
    serialized = json.dumps({"execution": execution, "status": status, "history": history.json()})
    assert "run_id" not in serialized
    assert "event_id" in serialized
    assert "workflow_execution" not in serialized
    assert "activity_task_scheduled_event_attributes" not in serialized
    assert (await http.get("/ready")).status_code == 200


async def test_approval_uses_authenticated_actor_and_execution_remains_version_pinned(
    governed_api: ApiFixture, sample_definition: dict[str, Any]
) -> None:
    http = governed_api.http
    await register_and_promote(http, sample_definition, "1.0")
    execution = await start(http, start_request(amount=7500, key="approval"))
    workflow_id = execution["workflow_id"]
    waiting = await wait_for_state(http, workflow_id, "WAITING_FOR_APPROVAL")
    assert waiting["current_step"] == "manager_approval"
    assert waiting["definition_version"] == "1.0"

    # Promoting another immutable revision changes future starts, while the
    # already-running execution keeps its original document and version.
    next_definition = {
        "start_at": "rejected",
        "steps": {"rejected": {"type": "end", "outcome": "REJECTED"}},
    }
    await register_and_promote(http, next_definition, "2.0")
    second = await start(http, start_request(key="new-version"))
    second_status = await wait_for_state(http, second["workflow_id"], "REJECTED")
    assert second_status["definition_version"] == "2.0"

    signalled = await http.post(
        f"/api/v1/workflows/{workflow_id}/signal",
        json={"signal": "approve", "approved": True, "comment": "Verified in integration test"},
    )
    assert signalled.status_code == 202, signalled.text
    completed = await wait_for_state(http, workflow_id, "COMPLETED")
    assert completed["definition_version"] == "1.0"
    record = governed_api.repository.get_execution(Scope(**SCOPE), workflow_id)
    assert record.run_id
    result = await governed_api.backend.client.get_workflow_handle(
        workflow_id, run_id=record.run_id
    ).result()
    assert result["results"]["manager_approval"]["approver"] == ACTOR
    assert result["results"]["manager_approval"]["comment"] == "Verified in integration test"
    history = await http.get(f"/api/v1/workflows/{workflow_id}/history", params={"limit": 100})
    assert history.status_code == 200, history.text
    approvals = [event for event in history.json()["items"] if event["new_state"] == "APPROVED"]
    assert approvals
    assert any(
        event["actor"] == ACTOR and "signal" in event["event_type"]
        for event in history.json()["items"]
    )


async def test_http_cancellation_reaches_real_temporal_execution(governed_api: ApiFixture) -> None:
    http = governed_api.http
    definition = {
        "start_at": "wait",
        "steps": {
            "wait": {"type": "timer", "seconds": 300, "next": "done"},
            "done": {"type": "end"},
        },
    }
    await register_and_promote(http, definition, "1.0")
    execution = await start(http, start_request(key="cancellation"))
    workflow_id = execution["workflow_id"]
    await wait_for_state(http, workflow_id, "RUNNING")
    cancelled = await http.post(
        f"/api/v1/workflows/{workflow_id}/cancel", json={"reason": "Integration cancellation"}
    )
    assert cancelled.status_code == 202, cancelled.text
    assert (await wait_for_state(http, workflow_id, "CANCELLED"))["completed_at"]


async def test_rejection_handler_stays_running_until_its_configured_end(
    governed_api: ApiFixture,
) -> None:
    http = governed_api.http
    definition = {
        "start_at": "approval",
        "steps": {
            "approval": {
                "type": "approval",
                "timeout_seconds": 30,
                "on_approved": "completed",
                "on_rejected": "handler",
                "on_timeout": "completed",
            },
            "handler": {"type": "timer", "seconds": 2, "next": "completed"},
            "completed": {"type": "end", "outcome": "COMPLETED"},
        },
    }
    await register_and_promote(http, definition, "1.0")
    execution = await start(http, start_request(key="rejection-handler"))
    workflow_id = execution["workflow_id"]
    await wait_for_state(http, workflow_id, "WAITING_FOR_APPROVAL")
    signalled = await http.post(
        f"/api/v1/workflows/{workflow_id}/signal",
        json={"signal": "approve", "approved": False, "comment": "Run rejection handler"},
    )
    assert signalled.status_code == 202, signalled.text
    async with asyncio.timeout(10):
        while True:
            response = await http.get(f"/api/v1/workflows/{workflow_id}/status")
            assert response.status_code == 200, response.text
            status = response.json()
            if status["current_step"] == "handler":
                assert status["state"] == "RUNNING"
                assert status["completed_at"] is None
                break
            await asyncio.sleep(0.025)
    completed = await wait_for_state(http, workflow_id, "COMPLETED")
    assert completed["completed_at"]


async def test_governed_non_retryable_error_policy_stops_activity_after_one_attempt(
    governed_api: ApiFixture,
) -> None:
    http = governed_api.http
    definition = {
        "start_at": "validate",
        "steps": {
            "validate": {
                "type": "activity",
                "capability": "validate_request",
                "next": "done",
                "retry": {"maximum_attempts": 3, "non_retryable_error_types": ["ValueError"]},
            },
            "done": {"type": "end"},
        },
    }
    await register_and_promote(http, definition, "1.0")
    execution = await start(http, start_request(amount=0, key="non-retryable"))
    workflow_id = execution["workflow_id"]
    status = await wait_for_state(http, workflow_id, "FAILED")
    assert status["failure_code"] == "workflow_failed"
    assert status["failure_summary"] == "Workflow execution failed"
    assert "ValueError" not in json.dumps(status)

    record = governed_api.repository.get_execution(Scope(**SCOPE), workflow_id)
    assert record.run_id
    handle = governed_api.backend.client.get_workflow_handle(workflow_id, run_id=record.run_id)
    with pytest.raises(WorkflowFailureError) as caught:
        await handle.result(follow_runs=False)
    failure = caught.value.cause
    assert isinstance(failure, ActivityError)
    assert failure.retry_state == RetryState.NON_RETRYABLE_FAILURE
    history = await handle.fetch_history()
    attempts = [
        event.activity_task_started_event_attributes.attempt
        for event in history.events
        if event.HasField("activity_task_started_event_attributes")
    ]
    failures = [
        event.activity_task_failed_event_attributes.retry_state
        for event in history.events
        if event.HasField("activity_task_failed_event_attributes")
    ]
    assert attempts == [1]
    assert failures == [RetryState.NON_RETRYABLE_FAILURE]
