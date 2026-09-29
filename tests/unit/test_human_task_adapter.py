"""Package Activity contract with the durable task-service HTTP boundary."""

from __future__ import annotations

import json
from email.message import Message
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request

import pytest
from contracts import PackageActivityRequest
from contracts.human_tasks import CreateTaskRequest
from nexusflow_activities import create_approval_task, human_tasks
from temporalio.exceptions import ApplicationError

from tests.package_fixtures import source_package, start_request


def _payload(**changes: Any) -> PackageActivityRequest:
    start = start_request(source_package())
    values: dict[str, Any] = {
        "workflow_id": "workflow-1",
        "run_id": "run-1",
        "step_id": "manager_approval",
        "capability": "create_approval_task",
        "context": start.context,
        "release_binding": start.release_binding,
        "idempotency_key": "run-1:manager_approval:1.0",
        "input": {"assignee_group": "operations-managers", "timeout_seconds": 300},
    }
    values.update(changes)
    return PackageActivityRequest.model_validate(values)


class _Response:
    status = 201

    def __init__(self, body: dict[str, Any] | None = None, *, replayed: bool = False) -> None:
        self.body = body or {}
        self.replayed = replayed

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self, _limit: int) -> bytes:
        return json.dumps(
            {
                "task_id": "persisted-123",
                "replayed": self.replayed,
                "run_id": "original-run",
                **{field: self.body[field] for field in human_tasks._TASK_ECHO_FIELDS},
                "package_release_id": self.body["package_release_id"],
                "build_id": self.body["build_id"],
            }
        ).encode()


@pytest.fixture
def configured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NEXUSFLOW_HUMAN_TASK_SERVICE_URL", "http://127.0.0.1:8765")
    monkeypatch.setenv("NEXUSFLOW_HUMAN_TASK_SERVICE_TOKEN", "service-token")


async def test_create_posts_frozen_provenance_and_returns_durable_task(
    monkeypatch: pytest.MonkeyPatch, configured: None
) -> None:
    calls: list[tuple[Request, int]] = []

    def fake_urlopen(request: Request, timeout: int) -> _Response:
        calls.append((request, timeout))
        assert isinstance(request.data, bytes)
        return _Response(json.loads(request.data))

    monkeypatch.setattr(human_tasks, "_open_request", fake_urlopen)
    first = _payload(first_execution_run_id="run-1")
    result = await create_approval_task(first)
    assert result.output == {
        "created": True,
        "durable": True,
        "task_id": "persisted-123",
        "assignee_group": "operations-managers",
    }
    retry = first.model_copy(update={"run_id": "continued-run"})
    assert (await create_approval_task(retry)).output == result.output
    request, timeout = calls[0]
    assert timeout == 5
    assert request.full_url == "http://127.0.0.1:8765/api/v1/tasks"
    assert request.get_method() == "POST"
    assert request.get_header("Authorization") == "Bearer service-token"
    assert request.get_header("Content-type") == "application/json"
    assert isinstance(request.data, bytes)
    body = json.loads(request.data)
    assert CreateTaskRequest.model_validate(body).idempotency_key == first.idempotency_key
    assert body == {
        "tenant": first.context.tenant,
        "business_domain": first.context.business_domain,
        "application": first.context.application,
        "workflow_id": first.workflow_id,
        "run_id": first.run_id,
        "first_execution_run_id": first.run_id,
        "step_id": first.step_id,
        "definition_version": first.context.definition_version,
        "correlation_id": first.context.correlation_id,
        "business_reference": first.context.business_reference,
        "idempotency_key": first.idempotency_key,
        "task_type": "approval",
        "assignee_group": "operations-managers",
        "timeout_seconds": 300,
        "package_id": first.release_binding.package_id,
        "package_release_id": first.release_binding.package_release_id,
        "build_id": first.release_binding.build_id,
    }
    assert isinstance(calls[1][0].data, bytes)
    retry_body = json.loads(calls[1][0].data)
    assert retry_body["run_id"] == "continued-run"
    assert retry_body["first_execution_run_id"] == "run-1"
    assert retry_body["idempotency_key"] == body["idempotency_key"]


@pytest.mark.parametrize(
    "field",
    [
        "tenant",
        "workflow_id",
        "first_execution_run_id",
        "step_id",
        "idempotency_key",
        "package_release_id",
        "build_id",
    ],
)
async def test_mismatched_task_identity_or_release_is_rejected(
    monkeypatch: pytest.MonkeyPatch, configured: None, field: str
) -> None:
    def fake_urlopen(request: Request, timeout: int) -> _Response:
        assert isinstance(request.data, bytes)
        response = json.loads(request.data)
        response[field] = "wrong"
        return _Response(response)

    monkeypatch.setattr(human_tasks, "_open_request", fake_urlopen)
    with pytest.raises(ApplicationError) as error:
        await create_approval_task(_payload())
    assert error.value.type == "ContractError"
    assert error.value.non_retryable


@pytest.mark.parametrize("replayed", [False, True])
async def test_compatible_release_retry_requires_explicit_task_replay(
    monkeypatch: pytest.MonkeyPatch, configured: None, replayed: bool
) -> None:
    def fake_urlopen(request: Request, timeout: int) -> _Response:
        assert isinstance(request.data, bytes)
        response = json.loads(request.data)
        response["package_release_id"] = "customer-adjustment-0.2.0"
        response["build_id"] = "customer-adjustment-0.2.0"
        return _Response(response, replayed=replayed)

    monkeypatch.setattr(human_tasks, "_open_request", fake_urlopen)
    current = _payload(first_execution_run_id="run-1")
    newer_binding = current.release_binding.model_copy(
        update={
            "package_release_id": "customer-adjustment-0.3.0",
            "package_version": "0.3.0",
            "build_id": "customer-adjustment-0.3.0",
        }
    )
    retried = current.model_copy(
        update={"run_id": "continued-run", "release_binding": newer_binding}
    )
    if replayed:
        result = await create_approval_task(retried)
        assert result.output["durable"] is True
        assert result.output["task_id"] == "persisted-123"
    else:
        with pytest.raises(ApplicationError) as error:
            await create_approval_task(retried)
        assert error.value.type == "ContractError"
        assert error.value.non_retryable


async def test_replayed_task_still_requires_matching_chain_identity(
    monkeypatch: pytest.MonkeyPatch, configured: None
) -> None:
    def fake_urlopen(request: Request, timeout: int) -> _Response:
        assert isinstance(request.data, bytes)
        response = json.loads(request.data)
        response["first_execution_run_id"] = "unrelated-chain"
        response["package_release_id"] = "customer-adjustment-0.2.0"
        response["build_id"] = "customer-adjustment-0.2.0"
        return _Response(response, replayed=True)

    monkeypatch.setattr(human_tasks, "_open_request", fake_urlopen)
    with pytest.raises(ApplicationError) as error:
        await create_approval_task(_payload())
    assert error.value.type == "ContractError"
    assert error.value.non_retryable


@pytest.mark.parametrize(
    "environment",
    [
        {"NEXUSFLOW_HUMAN_TASK_SERVICE_URL": "", "NEXUSFLOW_HUMAN_TASK_SERVICE_TOKEN": "x"},
        {
            "NEXUSFLOW_HUMAN_TASK_SERVICE_URL": "http://localhost",
            "NEXUSFLOW_HUMAN_TASK_SERVICE_TOKEN": "",
        },
        {
            "NEXUSFLOW_HUMAN_TASK_SERVICE_URL": "file:///tmp/task",
            "NEXUSFLOW_HUMAN_TASK_SERVICE_TOKEN": "x",
        },
        {
            "NEXUSFLOW_HUMAN_TASK_SERVICE_URL": "http://user:pass@localhost",
            "NEXUSFLOW_HUMAN_TASK_SERVICE_TOKEN": "x",
        },
        {
            "NEXUSFLOW_HUMAN_TASK_SERVICE_URL": "http://tasks.example",
            "NEXUSFLOW_HUMAN_TASK_SERVICE_TOKEN": "x",
        },
        {
            "NEXUSFLOW_HUMAN_TASK_SERVICE_URL": "http://[broken",
            "NEXUSFLOW_HUMAN_TASK_SERVICE_TOKEN": "x",
        },
    ],
)
async def test_missing_or_invalid_service_configuration_fails_before_io(
    monkeypatch: pytest.MonkeyPatch, environment: dict[str, str]
) -> None:
    for key, value in environment.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(human_tasks, "_open_request", lambda *_args, **_kwargs: pytest.fail("IO"))
    with pytest.raises(ApplicationError) as error:
        await create_approval_task(_payload())
    assert error.value.type == "ConfigurationError"
    assert error.value.non_retryable


@pytest.mark.parametrize(
    "input_document",
    [
        {},
        {"assignee_group": " ", "timeout_seconds": 300},
        {"assignee_group": "approvers", "timeout_seconds": True},
        {"assignee_group": "approvers", "timeout_seconds": 0},
        {"assignee_group": "approvers", "timeout_seconds": 31_536_001},
    ],
)
async def test_invalid_task_input_is_nonretryable(input_document: dict[str, object]) -> None:
    with pytest.raises(ApplicationError) as error:
        await create_approval_task(_payload(input=input_document))
    assert error.value.type == "ValidationError"
    assert error.value.non_retryable


@pytest.mark.parametrize(
    ("code", "expected_type", "non_retryable"),
    [
        (400, "ValidationError", True),
        (401, "AuthorizationError", True),
        (403, "AuthorizationError", True),
        (409, "ValidationError", True),
        (422, "ValidationError", True),
        (302, "ContractError", True),
        (408, "TechnicalError", False),
        (429, "TechnicalError", False),
        (503, "TechnicalError", False),
    ],
)
def test_http_failures_have_bounded_retry_classification(
    monkeypatch: pytest.MonkeyPatch, code: int, expected_type: str, non_retryable: bool
) -> None:
    def failing(_request: Request, timeout: int) -> _Response:
        raise HTTPError(
            "https://tasks.example/api/v1/tasks", code, "secret details", Message(), None
        )

    monkeypatch.setattr(human_tasks, "_open_request", failing)
    with pytest.raises(ApplicationError) as error:
        human_tasks._post_task("https://tasks.example/api/v1/tasks", "secret-token", {})
    assert error.value.type == expected_type
    assert bool(error.value.non_retryable) is non_retryable
    assert "secret" not in str(error.value)


def test_network_failure_is_retryable_without_leaking_connection_details(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def failing(_request: Request, timeout: int) -> _Response:
        raise URLError("internal-host-and-secret")

    monkeypatch.setattr(human_tasks, "_open_request", failing)
    with pytest.raises(ApplicationError) as error:
        human_tasks._post_task("https://tasks.example/api/v1/tasks", "secret-token", {})
    assert error.value.type == "TechnicalError"
    assert not error.value.non_retryable
    assert "internal-host-and-secret" not in str(error.value)


@pytest.mark.parametrize("content", [b"{}", b"not JSON", b'{"task_id":" "}'])
def test_success_without_task_reference_is_nonretryable_contract_error(
    monkeypatch: pytest.MonkeyPatch, content: bytes
) -> None:
    class Response(_Response):
        def read(self, _limit: int) -> bytes:
            return content

    def fake_urlopen(_request: Request, timeout: int) -> Response:
        return Response()

    monkeypatch.setattr(human_tasks, "_open_request", fake_urlopen)
    with pytest.raises(ApplicationError) as error:
        human_tasks._post_task("https://tasks.example/api/v1/tasks", "token", {})
    assert error.value.type == "ContractError"
    assert error.value.non_retryable


def test_real_http_redirect_never_follows_or_forwards_token() -> None:
    requests: list[tuple[str, str | None]] = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            self.rfile.read(int(self.headers.get("Content-Length", "0")))
            requests.append((self.path, self.headers.get("Authorization")))
            self.send_response(302)
            self.send_header("Location", "/redirected")
            self.end_headers()

        def do_GET(self) -> None:
            requests.append((self.path, self.headers.get("Authorization")))
            self.send_response(200)
            self.end_headers()

        def log_message(self, _format: str, *_args: object) -> None:
            return None

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with pytest.raises(ApplicationError) as error:
            human_tasks._post_task(
                f"http://127.0.0.1:{server.server_port}/api/v1/tasks", "secret-token", {}
            )
        assert error.value.type == "ContractError"
        assert error.value.non_retryable
        assert requests == [("/api/v1/tasks", "Bearer secret-token")]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
