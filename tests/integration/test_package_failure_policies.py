"""Real package Activity retry, nonretryable failure and timeout behavior."""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from datetime import timedelta
from typing import Any, Literal
from uuid import uuid4

import pytest
from contracts import ActivityResponse, PackageActivityRequest
from temporalio import activity
from temporalio.client import WorkflowFailureError
from temporalio.common import PinnedVersioningOverride, WorkerDeploymentVersion
from temporalio.exceptions import ActivityError, ApplicationError, TimeoutError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker, WorkerDeploymentConfig
from workflow_sdk.runtime import PackageWorkflowV1

from tests.integration.test_package_runtime import package_environment, payload, wait_for_version

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="module")]
# Import the isolated server fixture; it is managed by pytest in this module.
__all__ = ["package_environment"]


@pytest.mark.parametrize("mode", ["retry", "business_error", "timeout"])
async def test_package_activity_failure_policy(
    package_environment: WorkflowEnvironment, mode: Literal["retry", "business_error", "timeout"]
) -> None:
    deployment = "failure-policy-" + uuid4().hex
    attempts: list[tuple[int, str]] = []

    @activity.defn(name="validate_request.pkg.v1")
    async def validation(request: PackageActivityRequest) -> ActivityResponse:
        attempt = activity.info().attempt
        attempts.append((attempt, request.idempotency_key))
        if mode == "business_error":
            raise ApplicationError("Rejected input", type="BusinessError", non_retryable=True)
        if mode == "retry" and attempt == 1:
            raise ApplicationError("Transient service failure", type="TechnicalError")
        if mode == "timeout":
            await asyncio.sleep(10)
        return ActivityResponse(capability="validate_request", output={"attempt": attempt})

    document: dict[str, Any] = {
        "start_at": "validate",
        "steps": {
            "validate": {
                "type": "activity",
                "capability": "validate_request",
                "timeout_seconds": 1,
                "retry": {
                    "maximum_attempts": 1 if mode == "timeout" else 3,
                    "initial_interval_seconds": 1,
                },
                "next": "done",
            },
            "done": {"type": "end"},
        },
    }
    start = payload(deployment, document, eligible=("build-1",))
    config = WorkerDeploymentConfig(
        version=WorkerDeploymentVersion(deployment, "build-1"), use_worker_versioning=True
    )
    async with AsyncExitStack() as stack:
        await stack.enter_async_context(
            Worker(
                package_environment.client,
                task_queue=deployment + "-workflow",
                workflows=[PackageWorkflowV1],
                deployment_config=config,
            )
        )
        await stack.enter_async_context(
            Worker(
                package_environment.client,
                task_queue=deployment + "-activity",
                activities=[validation],
                deployment_config=config,
                graceful_shutdown_timeout=timedelta(seconds=0),
            )
        )
        await wait_for_version(package_environment.client, deployment, "build-1")
        handle = await package_environment.client.start_workflow(
            "PackageWorkflowV1",
            start,
            id="failure-" + uuid4().hex,
            task_queue=deployment + "-workflow",
            versioning_override=PinnedVersioningOverride(
                WorkerDeploymentVersion(deployment, "build-1")
            ),
        )
        if mode == "retry":
            result = await asyncio.wait_for(handle.result(), 15)
            assert result["results"]["validate"]["attempt"] == 2
            assert [attempt for attempt, _ in attempts] == [1, 2]
            assert len({key for _, key in attempts}) == 1
        else:
            with pytest.raises(WorkflowFailureError) as failure:
                await asyncio.wait_for(handle.result(), 15)
            assert isinstance(failure.value.cause, ActivityError)
            if mode == "timeout":
                assert isinstance(failure.value.cause.cause, TimeoutError)
            else:
                assert isinstance(failure.value.cause.cause, ApplicationError)
                assert failure.value.cause.cause.type == "BusinessError"
            assert [attempt for attempt, _ in attempts] == [1]
        history = await handle.fetch_history()
        completed = any(
            event.HasField("workflow_execution_completed_event_attributes")
            for event in history.events
        )
        assert completed == (mode == "retry")
