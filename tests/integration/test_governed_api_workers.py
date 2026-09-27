"""HTTP acceptance through the governed runtime and separate capability queues."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypedDict

import httpx
import pytest
import pytest_asyncio
from contracts import ActivityRequest
from temporalio.testing import WorkflowEnvironment
from workflow_api.api import create_app
from workflow_api.backend import TemporalBackend
from workflow_api.repository import Scope, WorkflowRepository
from workflow_api.security import Principal, StaticTokenAuthenticator

from tests.integration.test_governed_runtime import (
    HUMAN_TASK_QUEUE,
    INTEGRATION_QUEUE,
    NOTIFICATION_QUEUE,
    ORCHESTRATION_QUEUE,
    SAMPLE_BUSINESS_QUEUE,
    VALIDATION_QUEUE,
    running_workers,
    sample_document,
)
from tests.integration.test_governed_runtime import governed_environment as governed_environment

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="module")]


class ScopeFields(TypedDict):
    tenant: str
    business_domain: str
    application: str


SCOPE: ScopeFields = {
    "tenant": "governed-api-tenant",
    "business_domain": "finance",
    "application": "adjustments",
}
TOKEN = "governed-integration-token"
ACTOR = "governed.manager"
WORKFLOW_TYPE = "customer-adjustment"


@dataclass(frozen=True)
class GovernedApi:
    http: httpx.AsyncClient
    repository: WorkflowRepository
    backend: TemporalBackend


@pytest_asyncio.fixture(loop_scope="module")
async def governed_http(
    governed_environment: WorkflowEnvironment,
    tmp_path: Path,
) -> AsyncIterator[GovernedApi]:
    repository = WorkflowRepository(f"sqlite:///{(tmp_path / 'governed-api.sqlite').as_posix()}")
    repository.create_schema()
    backend = TemporalBackend(governed_environment.client, task_queue=ORCHESTRATION_QUEUE)
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
            )
        }
    )
    application = create_app(repository, backend, authenticator=authenticator, environment="local")
    try:
        async with running_workers(governed_environment.client):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=application),
                base_url="http://governed-api.local",
                headers={"Authorization": f"Bearer {TOKEN}"},
            ) as http:
                yield GovernedApi(http, repository, backend)
    finally:
        repository.close()


async def register_and_promote(
    http: httpx.AsyncClient, document: dict[str, Any], version: str
) -> None:
    registered = await http.post(
        "/api/v1/workflows",
        json={
            **SCOPE,
            "definition_id": "governed-adjustment",
            "workflow_type": WORKFLOW_TYPE,
            "version": version,
            "owner": ACTOR,
            "definition_document": document,
        },
    )
    assert registered.status_code == 201, registered.text
    approved = await http.post(f"/api/v1/definitions/{WORKFLOW_TYPE}/{version}/approve", json=SCOPE)
    assert approved.status_code == 200, approved.text
    promoted = await http.post(
        f"/api/v1/definitions/{WORKFLOW_TYPE}/{version}/promote",
        json={
            **SCOPE,
            "environment": "local",
        },
    )
    assert promoted.status_code == 200, promoted.text


def start_request(key: str, *, amount: int = 1000) -> dict[str, Any]:
    return {
        **SCOPE,
        "business_reference": f"adjustment-{key}",
        "correlation_id": f"trace-{key}",
        "idempotency_key": key,
        "request": {"amount": amount, "simulate_transient_failure": False},
        "variables": {"channel": "email"},
    }


async def start(http: httpx.AsyncClient, request: dict[str, Any]) -> dict[str, Any]:
    response = await http.post(f"/api/v1/workflows/{WORKFLOW_TYPE}/start", json=request)
    assert response.status_code == 202, response.text
    return response.json()


async def wait_for_state(http: httpx.AsyncClient, workflow_id: str, state: str) -> dict[str, Any]:
    async with asyncio.timeout(20):
        while True:
            response = await http.get(f"/api/v1/workflows/{workflow_id}/status")
            assert response.status_code == 200, response.text
            status = response.json()
            if status["state"] == state:
                return status
            await asyncio.sleep(0.025)


async def test_api_sample_crosses_all_owned_queues_and_stays_bound_to_promoted_revision(
    governed_http: GovernedApi,
) -> None:
    http = governed_http.http
    await register_and_promote(http, sample_document(), "1.0")
    request = start_request("approval", amount=7500)
    execution = await start(http, request)
    workflow_id = execution["workflow_id"]
    waiting = await wait_for_state(http, workflow_id, "WAITING_FOR_APPROVAL")
    assert waiting["current_step"] == "manager_approval"

    await register_and_promote(
        http,
        {
            "start_at": "rejected",
            "steps": {
                "rejected": {"type": "end", "outcome": "REJECTED"},
            },
        },
        "2.0",
    )
    newer = await start(http, start_request("newer"))
    assert (await wait_for_state(http, newer["workflow_id"], "REJECTED"))[
        "definition_version"
    ] == "2.0"
    repeated = await http.post(f"/api/v1/workflows/{WORKFLOW_TYPE}/start", json=request)
    assert repeated.status_code == 200, repeated.text
    assert repeated.json()["workflow_id"] == workflow_id
    assert repeated.json()["definition_version"] == "1.0"

    accepted = await http.post(
        f"/api/v1/workflows/{workflow_id}/signal",
        json={
            "approved": True,
            "comment": "Approved through governed HTTP",
        },
    )
    assert accepted.status_code == 202, accepted.text
    completed = await wait_for_state(http, workflow_id, "COMPLETED")
    assert completed["definition_version"] == "1.0"

    record = governed_http.repository.get_execution(Scope(**SCOPE), workflow_id)
    assert record.run_id is not None
    handle = governed_http.backend.client.get_workflow_handle(workflow_id, run_id=record.run_id)
    result = await handle.result()
    assert result["results"]["manager_approval"]["approver"] == ACTOR
    history = await handle.fetch_history()
    scheduled = [
        event.activity_task_scheduled_event_attributes
        for event in history.events
        if event.HasField("activity_task_scheduled_event_attributes")
    ]
    assert {(event.activity_type.name, event.task_queue.name) for event in scheduled} == {
        ("validate_request.v1", VALIDATION_QUEUE),
        ("risk_check.v1", SAMPLE_BUSINESS_QUEUE),
        ("create_approval_task.v1", HUMAN_TASK_QUEUE),
        ("post_adjustment.v1", INTEGRATION_QUEUE),
        ("send_notification.v1", NOTIFICATION_QUEUE),
    }
    for event in scheduled:
        decoded = await governed_http.backend.client.data_converter.decode(
            event.input.payloads,
            [ActivityRequest],
        )
        payload = decoded[0]
        assert isinstance(payload, ActivityRequest)
        assert payload.workflow_id == workflow_id
        assert payload.context.definition_version == "1.0"
        assert payload.context.actor == ACTOR
        assert payload.context.correlation_id == request["correlation_id"]
        assert payload.request["amount"] == 7500
        assert payload.idempotency_key
    business_history = await http.get(
        f"/api/v1/workflows/{workflow_id}/history", params={"limit": 100}
    )
    assert business_history.status_code == 200, business_history.text
    serialized = json.dumps(business_history.json())
    assert "run_id" not in serialized
    assert "task_queue" not in serialized
    assert "workflow-orchestration-tq" not in serialized
    assert (await http.get("/ready")).status_code == 200


async def test_api_accepts_typed_queue_version_binding_and_runtime_templates(
    governed_http: GovernedApi,
) -> None:
    http = governed_http.http
    document = sample_document(1000)
    document["steps"]["notify"].update(
        {
            "task_queue": NOTIFICATION_QUEUE,
            "contract_version": "1.0",
            "input": {"channel": "${variables.channel}"},
        }
    )
    await register_and_promote(http, document, "1.0")
    execution = await start(http, start_request("template"))
    workflow_id = execution["workflow_id"]
    assert (await wait_for_state(http, workflow_id, "COMPLETED"))["definition_version"] == "1.0"
    record = governed_http.repository.get_execution(Scope(**SCOPE), workflow_id)
    assert record.run_id is not None
    result = await governed_http.backend.client.get_workflow_handle(
        workflow_id, run_id=record.run_id
    ).result()
    assert result["results"]["notify"] == {"sent": True, "channel": "email"}
    assert "manager_approval" not in result["results"]


async def test_api_cancellation_reaches_the_new_runtime_and_audits_reason(
    governed_http: GovernedApi,
) -> None:
    http = governed_http.http
    document = {
        "start_at": "wait",
        "steps": {
            "wait": {"type": "timer", "seconds": 300, "next": "done"},
            "done": {"type": "end"},
        },
    }
    await register_and_promote(http, document, "1.0")
    execution = await start(http, start_request("cancel"))
    workflow_id = execution["workflow_id"]
    await wait_for_state(http, workflow_id, "RUNNING")
    canceled = await http.post(
        f"/api/v1/workflows/{workflow_id}/cancel", json={"reason": "Test client cancellation"}
    )
    assert canceled.status_code == 202, canceled.text
    assert (await wait_for_state(http, workflow_id, "CANCELLED"))["completed_at"]
    history = await http.get(f"/api/v1/workflows/{workflow_id}/history", params={"limit": 100})
    assert history.status_code == 200, history.text
    cancellation = next(
        event
        for event in history.json()["items"]
        if event["event_type"] == "execution.cancel_requested"
    )
    assert cancellation["actor"] == ACTOR
    assert cancellation["metadata"]["reason"] == "Test client cancellation"
