"""Actual SDK shutdown drains a typed Activity and removes worker readiness."""

from __future__ import annotations

import asyncio
import os
import shutil
import signal
import socket
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from contracts import (
    ActivityRequest,
    ActivityResponse,
    ActivityStep,
    DefinitionDocument,
    EndStep,
    RuntimeContext,
    RuntimeStartRequest,
)
from nexusflow_common.worker import load_worker_settings, run_worker
from temporalio import activity
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from workflow_runtime import GovernedWorkflowV1

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="module")]


async def wait_for_probe(client: httpx.AsyncClient, expected_status: int) -> None:
    async with asyncio.timeout(10):
        while True:
            try:
                response = await client.get("/ready")
                if response.status_code == expected_status:
                    return
            except httpx.ConnectError:
                pass
            await asyncio.sleep(0.05)


async def test_worker_shutdown_drains_inflight_activity_before_closing_probes() -> None:
    cli_path = os.environ.get("TEMPORAL_CLI_PATH") or shutil.which("temporal")
    if not cli_path or not Path(cli_path).is_file():
        pytest.fail(
            "Worker shutdown integration requires a local Temporal CLI; it downloads no binaries"
        )
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        probe_port = reservation.getsockname()[1]
    started = asyncio.Event()
    release = asyncio.Event()
    completed = asyncio.Event()
    stop = asyncio.Event()
    previous_signals = {name: signal.getsignal(name) for name in (signal.SIGINT, signal.SIGTERM)}

    @activity.defn(name="validate_request.v1")
    async def delayed_validation(payload: ActivityRequest) -> ActivityResponse:
        assert isinstance(payload, ActivityRequest)
        started.set()
        await release.wait()
        completed.set()
        return ActivityResponse(
            capability="validate_request",
            output={"valid": True, "amount": payload.request["amount"]},
        )

    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=cli_path,
        dev_server_log_level="error",
        ui=False,
        data_converter=pydantic_data_converter,
    ) as environment:
        settings = load_worker_settings(
            "validation-worker",
            "validation-tq",
            {
                "TEMPORAL_ADDRESS": environment.client.service_client.config.target_host,
                "NEXUSFLOW_PROBE_PORT": str(probe_port),
                "NEXUSFLOW_SHUTDOWN_GRACE_SECONDS": "5",
            },
        )
        running = asyncio.create_task(
            run_worker(
                "validation-worker",
                "validation-tq",
                activities=[delayed_validation],
                settings=settings,
                shutdown_event=stop,
            )
        )
        try:
            async with httpx.AsyncClient(
                base_url=f"http://127.0.0.1:{probe_port}", trust_env=False
            ) as probes:
                await wait_for_probe(probes, 200)
                async with Worker(
                    environment.client,
                    task_queue="workflow-orchestration-tq",
                    workflows=[GovernedWorkflowV1],
                    disable_eager_activity_execution=True,
                ):
                    context = RuntimeContext(
                        tenant="acme",
                        business_domain="finance",
                        application="adjustments",
                        workflow_type="customer-adjustment",
                        definition_id="definition-1",
                        definition_version="1.0",
                        business_reference="adjustment-1",
                        correlation_id="correlation-1",
                        actor="authenticated-service",
                    )
                    request = RuntimeStartRequest(
                        context=context,
                        definition_document=DefinitionDocument(
                            start_at="validate",
                            steps={
                                "validate": ActivityStep(
                                    capability="validate_request", next="done"
                                ),
                                "done": EndStep(),
                            },
                        ),
                        request={"amount": 100},
                    )
                    handle = await environment.client.start_workflow(
                        GovernedWorkflowV1.run,
                        request.model_dump(mode="json"),
                        id=f"shutdown-{uuid4().hex}",
                        task_queue="workflow-orchestration-tq",
                        execution_timeout=timedelta(seconds=20),
                    )
                    await asyncio.wait_for(started.wait(), timeout=10)
                    stop.set()
                    await wait_for_probe(probes, 503)
                    assert not running.done()
                    assert (await probes.get("/health")).status_code == 200
                    release.set()
                    await asyncio.wait_for(running, timeout=10)
                    assert completed.is_set()
                    result = await asyncio.wait_for(handle.result(), timeout=10)
                    assert result["state"] == "COMPLETED"
                    assert result["results"]["validate"]["valid"] is True
                    with pytest.raises(httpx.ConnectError):
                        await probes.get("/health")
        finally:
            stop.set()
            release.set()
            if not running.done():
                await asyncio.wait_for(running, timeout=10)
    assert {name: signal.getsignal(name) for name in previous_signals} == previous_signals
