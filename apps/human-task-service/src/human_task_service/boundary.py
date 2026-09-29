"""Bound HTTP bodies, reject ambiguous JSON, attach safe request correlation."""

from __future__ import annotations

import json
import re
from http import HTTPStatus
from uuid import uuid4

from contracts import ProblemDetails
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

CORRELATION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")


def problem_response(
    status: int, code: str, detail: str, path: str, correlation_id: str
) -> JSONResponse:
    problem = ProblemDetails(
        title=HTTPStatus(status).phrase,
        status=status,
        detail=detail,
        instance=path,
        code=code,
        correlation_id=correlation_id,
    )
    return JSONResponse(
        problem.model_dump(mode="json"),
        status_code=status,
        media_type="application/problem+json",
    )


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON member")
        result[key] = value
    return result


def _invalid_constant(value: str) -> None:
    raise ValueError("Non-finite JSON number")


class RequestBoundary:
    def __init__(self, app: ASGIApp, max_request_bytes: int = 1024 * 1024) -> None:
        if max_request_bytes <= 0:
            raise ValueError("max_request_bytes must be positive")
        self.app = app
        self.max_request_bytes = max_request_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = [
            value.decode("latin-1")
            for name, value in scope.get("headers", [])
            if name.lower() == b"x-correlation-id"
        ]
        correlation_id = headers[0] if len(headers) == 1 else uuid4().hex
        valid = len(headers) <= 1 and bool(CORRELATION.fullmatch(correlation_id))
        if not valid:
            correlation_id = uuid4().hex
        scope.setdefault("state", {})["correlation_id"] = correlation_id

        async def correlated_send(message: Message) -> None:
            if message["type"] == "http.response.start":
                message = dict(message)
                message["headers"] = [
                    (name, value)
                    for name, value in message.get("headers", [])
                    if name.lower() != b"x-correlation-id"
                ] + [(b"x-correlation-id", correlation_id.encode("ascii"))]
            await send(message)

        async def reject(status: int, code: str, detail: str) -> None:
            await problem_response(status, code, detail, scope["path"], correlation_id)(
                scope, receive, correlated_send
            )

        if not valid:
            await reject(400, "invalid_correlation_id", "Correlation header has an invalid format")
            return
        body = bytearray()
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            body.extend(message.get("body", b""))
            if len(body) > self.max_request_bytes:
                await reject(413, "payload_too_large", "Request exceeds the configured size limit")
                return
            if not message.get("more_body", False):
                break
        if body:
            try:
                json.loads(body, object_pairs_hook=_unique_object, parse_constant=_invalid_constant)
            except (ValueError, UnicodeError, RecursionError):
                await reject(
                    422, "invalid_json", "Request must contain finite JSON with unique members"
                )
                return
        consumed = False

        async def replay_receive() -> Message:
            nonlocal consumed
            if not consumed:
                consumed = True
                return {"type": "http.request", "body": bytes(body), "more_body": False}
            return await receive()

        await self.app(scope, replay_receive, correlated_send)
