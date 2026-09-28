"""Repeated local SQLite API bursts retain one execution and provenance per key."""

from __future__ import annotations

import asyncio
from typing import Literal

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import Engine
from workflow_api.repository import WorkflowRepository

from tests.contract.test_workflow_api import AUTH as LEGACY_AUTH
from tests.contract.test_workflow_api import Harness, start_payload
from tests.contract.test_workflow_api import harness as harness
from tests.contract.test_workflow_api import promote as promote_legacy
from tests.unit.test_package_api import (
    AUTH,
    SCOPE,
    PackageHarness,
    configure_pool,
    govern_definition,
    promote,
    publish_fixture,
    start_request,
)
from tests.unit.test_package_api import (
    package_harness as package_harness,
)


@pytest.fixture(params=["file", "memory"], autouse=True)
def sqlite_storage(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    if request.param == "memory":
        original = WorkflowRepository.__init__

        def initialize(self: WorkflowRepository, engine_or_url: Engine | str) -> None:
            selected = (
                "sqlite://"
                if isinstance(engine_or_url, str) and engine_or_url.startswith("sqlite")
                else engine_or_url
            )
            original(self, selected)

        monkeypatch.setattr(WorkflowRepository, "__init__", initialize)


@pytest.mark.asyncio
@pytest.mark.parametrize("profile", ["legacy", "package"])
async def test_repeated_parallel_duplicate_start_bursts(
    profile: Literal["legacy", "package"],
    harness: Harness,
    package_harness: PackageHarness,
) -> None:
    if profile == "legacy":
        promote_legacy(harness)
        app = harness.client.app
        workflow_type = "customer-adjustment"
        auth = LEGACY_AUTH
    else:
        manifest = publish_fixture(package_harness)
        govern_definition(package_harness, manifest)
        assert configure_pool(package_harness, manifest).status_code == 200
        assert promote(package_harness, manifest).status_code == 200
        app = package_harness.client.app
        workflow_type = "validation_reference"
        auth = AUTH
    workflow_ids: set[str] = set()
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://sqlite-test"
    ) as client:
        for burst in range(8):
            key = f"parallel-{profile}-{burst}"
            payload = start_payload(key) if profile == "legacy" else start_request(key)
            combined = await asyncio.gather(
                *(
                    client.post(
                        f"/api/v1/workflows/{workflow_type}/start", json=payload, headers=auth
                    )
                    for _ in range(8)
                ),
                *(client.get("/ready") for _ in range(3)),
            )
            responses = combined[:8]
            assert all(response.status_code == 200 for response in combined[8:]), [
                response.text for response in combined[8:]
            ]
            assert all(response.status_code in {200, 202} for response in responses), [
                response.text for response in responses
            ]
            burst_ids = {response.json()["workflow_id"] for response in responses}
            assert len(burst_ids) == 1
            workflow_ids.update(burst_ids)
            if profile == "package":
                stored = package_harness.repository.get_by_idempotency(SCOPE, workflow_type, key)
                assert stored and stored.package_binding
                assert stored.package_binding.build_id == manifest.build_id
                attempts = [
                    request
                    for workflow_id, request in package_harness.backend.package_starts
                    if workflow_id == stored.workflow_id
                ]
                assert attempts and all(
                    request.release_binding == stored.package_binding for request in attempts
                )
        listed = await client.get("/api/v1/workflows", headers=auth)
        assert listed.status_code == 200
        assert len(listed.json()["items"]) == 8
    assert len(workflow_ids) == 8
