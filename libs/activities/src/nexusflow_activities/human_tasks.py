"""Durable package-local approval adapter for the shared human-task service.

The Workflow owns the Activity route and its frozen package release binding. The
service owns task persistence and deduplicates the retry-stable idempotency key.
"""

from __future__ import annotations

import asyncio
import ipaddress
import json
import os
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from contracts import ActivityResponse, PackageActivityRequest
from temporalio import activity
from temporalio.exceptions import ApplicationError

from .envelopes import validate_envelope

_SERVICE_URL_ENV = "NEXUSFLOW_HUMAN_TASK_SERVICE_URL"
_SERVICE_TOKEN_ENV = "NEXUSFLOW_HUMAN_TASK_SERVICE_TOKEN"
_HTTP_TIMEOUT_SECONDS = 5
_MAX_RESPONSE_BYTES = 64 * 1024
_MAX_APPROVAL_SECONDS = 365 * 24 * 60 * 60
_TASK_ECHO_FIELDS = (
    "tenant",
    "business_domain",
    "application",
    "workflow_id",
    "first_execution_run_id",
    "step_id",
    "definition_version",
    "correlation_id",
    "business_reference",
    "idempotency_key",
    "task_type",
    "package_id",
)


class _NoRedirects(HTTPRedirectHandler):
    def redirect_request(
        self, req: Request, fp: Any, code: int, msg: str, headers: Any, newurl: str
    ) -> None:
        return None


def _open_request(request: Request, timeout: int) -> Any:
    # urllib's default opener follows redirects and could forward the service token.
    return build_opener(_NoRedirects()).open(request, timeout=timeout)


def _is_loopback(hostname: str) -> bool:
    if hostname == "localhost":
        return True
    try:
        return ipaddress.ip_address(hostname).is_loopback
    except ValueError:
        return False


def _service_configuration() -> tuple[str, str]:
    base_url = os.environ.get(_SERVICE_URL_ENV, "").strip()
    token = os.environ.get(_SERVICE_TOKEN_ENV, "")
    try:
        parts = urlsplit(base_url)
        _ = parts.port
    except ValueError:
        raise ApplicationError(
            "Human-task service URL/token configuration is invalid or missing",
            type="ConfigurationError",
            non_retryable=True,
        ) from None
    if (
        parts.scheme not in {"http", "https"}
        or not parts.netloc
        or not parts.hostname
        or (parts.scheme == "http" and not _is_loopback(parts.hostname))
        or parts.username is not None
        or parts.password is not None
        or parts.path not in {"", "/"}
        or parts.query
        or parts.fragment
        or any(character.isspace() for character in base_url)
        or not token
        or len(token) > 4096
        or token != token.strip()
        or any(character in token for character in "\r\n\x00")
    ):
        raise ApplicationError(
            "Human-task service URL/token configuration is invalid or missing",
            type="ConfigurationError",
            non_retryable=True,
        )
    return base_url.rstrip("/") + "/api/v1/tasks", token


def _validate_input(payload: PackageActivityRequest) -> tuple[str, int]:
    assignee_group = payload.input.get("assignee_group")
    timeout_seconds = payload.input.get("timeout_seconds")
    if (
        not isinstance(assignee_group, str)
        or not assignee_group.strip()
        or len(assignee_group) > 256
        or type(timeout_seconds) is not int
        or timeout_seconds < 1
        or timeout_seconds > _MAX_APPROVAL_SECONDS
        or len(payload.idempotency_key) > 256
    ):
        raise ApplicationError(
            "Approval task input or idempotency key is invalid",
            type="ValidationError",
            non_retryable=True,
        )
    return assignee_group, timeout_seconds


def _post_task(url: str, token: str, body: dict[str, Any]) -> str:
    request = Request(
        url,
        data=json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with _open_request(request, timeout=_HTTP_TIMEOUT_SECONDS) as response:
            if response.status not in {200, 201}:
                raise ApplicationError(
                    "Human-task service returned an unexpected success status",
                    type="ContractError",
                    non_retryable=True,
                )
            content = response.read(_MAX_RESPONSE_BYTES + 1)
    except HTTPError as exc:
        if 300 <= exc.code < 400:
            raise ApplicationError(
                "Human-task service redirected task creation",
                type="ContractError",
                non_retryable=True,
            ) from None
        if exc.code in {408, 429} or exc.code >= 500:
            raise ApplicationError(
                "Human-task service is temporarily unavailable",
                type="TechnicalError",
            ) from None
        if exc.code in {401, 403}:
            raise ApplicationError(
                "Human-task service denied task creation",
                type="AuthorizationError",
                non_retryable=True,
            ) from None
        raise ApplicationError(
            "Human-task service rejected task creation",
            type="ValidationError",
            non_retryable=True,
        ) from None
    except (URLError, TimeoutError, OSError):
        raise ApplicationError(
            "Human-task service is temporarily unavailable",
            type="TechnicalError",
        ) from None
    if len(content) > _MAX_RESPONSE_BYTES:
        raise ApplicationError(
            "Human-task service response is too large",
            type="ContractError",
            non_retryable=True,
        )
    try:
        response_body = json.loads(content)
    except (json.JSONDecodeError, UnicodeDecodeError):
        response_body = None
    task_id = response_body.get("task_id") if isinstance(response_body, dict) else None
    if not isinstance(task_id, str) or not task_id.strip() or len(task_id) > 256:
        raise ApplicationError(
            "Human-task service returned no durable task ID",
            type="ContractError",
            non_retryable=True,
        )
    # The persisted task may be reassigned and a duplicate retry may have a new
    # run_id or execute on a newer compatible release. Its immutable chain and
    # business identity must still agree. An old release is valid only when the
    # service explicitly says it replayed the original task creation.
    if any(response_body.get(field) != body[field] for field in _TASK_ECHO_FIELDS):
        raise ApplicationError(
            "Human-task service returned mismatched task provenance",
            type="ContractError",
            non_retryable=True,
        )
    original_release = response_body.get("package_release_id")
    original_build = response_body.get("build_id")
    if (
        not isinstance(original_release, str)
        or not original_release.strip()
        or len(original_release) > 128
        or not isinstance(original_build, str)
        or not original_build.strip()
        or len(original_build) > 128
        or (
            response_body.get("replayed") is not True
            and (
                original_release != body["package_release_id"] or original_build != body["build_id"]
            )
        )
    ):
        raise ApplicationError(
            "Human-task service returned mismatched task release provenance",
            type="ContractError",
            non_retryable=True,
        )
    return task_id


@activity.defn(name="create_approval_task.pkg.v1")
async def create_approval_task(payload: PackageActivityRequest) -> ActivityResponse:
    validate_envelope(payload, "create_approval_task")
    assignee_group, timeout_seconds = _validate_input(payload)
    url, token = _service_configuration()
    context = payload.context
    binding = payload.release_binding
    body = {
        "tenant": context.tenant,
        "business_domain": context.business_domain,
        "application": context.application,
        "workflow_id": payload.workflow_id,
        "run_id": payload.run_id,
        "first_execution_run_id": payload.first_execution_run_id or payload.run_id,
        "step_id": payload.step_id,
        "definition_version": context.definition_version,
        "correlation_id": context.correlation_id,
        "business_reference": context.business_reference,
        "idempotency_key": payload.idempotency_key,
        "task_type": "approval",
        "assignee_group": assignee_group,
        "timeout_seconds": timeout_seconds,
        "package_id": binding.package_id,
        "package_release_id": binding.package_release_id,
        "build_id": binding.build_id,
    }
    task_id = await asyncio.to_thread(_post_task, url, token, body)
    return ActivityResponse(
        capability="create_approval_task",
        contract_version="1.0",
        output={
            "created": True,
            "durable": True,
            "task_id": task_id,
            "assignee_group": assignee_group,
        },
    )
