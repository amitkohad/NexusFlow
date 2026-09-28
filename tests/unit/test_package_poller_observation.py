"""Release capacity counts aggregate partitions and filter actual version pollers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock

import pytest
from temporalio.api.deployment.v1 import WorkerDeploymentOptions
from temporalio.api.enums.v1 import DescribeTaskQueueMode, TaskQueueType
from temporalio.api.taskqueue.v1 import PollerInfo, TaskQueueTypeInfo, TaskQueueVersionInfo
from temporalio.api.workflowservice.v1 import (
    DescribeTaskQueueResponse,
    DescribeWorkerDeploymentVersionResponse,
)
from temporalio.client import Client
from workflow_api.package_backend import PackageTemporalBackend

from tests.package_fixtures import source_package


@pytest.mark.parametrize("activity_available", [True, False])
async def test_observation_uses_partition_fanout_and_exact_fresh_deployment_identity(
    activity_available: bool,
) -> None:
    manifest = source_package().manifest
    queue = "customer-adjustment-tq"
    now = datetime.now(timezone.utc)

    def poller(
        identity: str,
        *,
        build: str | None = None,
        deployment: str | None = None,
        stale: bool = False,
    ) -> PollerInfo:
        result = PollerInfo(
            identity=identity,
            deployment_options=WorkerDeploymentOptions(
                deployment_name=deployment or manifest.worker_deployment_name,
                build_id=build or manifest.build_id,
            ),
        )
        result.last_access_time.FromDatetime(now - timedelta(seconds=120 if stale else 1))
        return result

    correct = [poller("replica-1"), poller("replica-2")]
    excluded = [
        poller("wrong-build", build="other-build"),
        poller("wrong-deployment", deployment="other-deployment"),
        poller("old-replica", stale=True),
        poller(""),
    ]
    deployment = DescribeWorkerDeploymentVersionResponse()
    deployment.worker_deployment_version_info.task_queue_infos.add(name=queue, type=1)
    deployment.worker_deployment_version_info.task_queue_infos.add(name=queue, type=2)
    snapshot = DescribeTaskQueueResponse(
        # Legacy/root-partition pollers cannot substantiate global replica count.
        pollers=[correct[0]],
        versions_info={
            "active-version": TaskQueueVersionInfo(
                types_info={
                    1: TaskQueueTypeInfo(pollers=correct + excluded),
                    2: TaskQueueTypeInfo(
                        pollers=(correct if activity_available else []) + excluded
                    ),
                }
            ),
            "duplicate-partition-result": TaskQueueVersionInfo(
                types_info={
                    1: TaskQueueTypeInfo(pollers=[correct[0]]),
                    2: TaskQueueTypeInfo(pollers=[correct[0]] if activity_available else []),
                }
            ),
        },
    )
    service = SimpleNamespace(
        describe_worker_deployment_version=AsyncMock(return_value=deployment),
        describe_task_queue=AsyncMock(return_value=snapshot),
    )
    client = SimpleNamespace(namespace="default", workflow_service=service)
    observed = await PackageTemporalBackend(cast(Client, client)).inspect_release(
        manifest,
        "default",
        {"workflow": queue, "activities": queue},
    )
    assert observed["ready"] is activity_available
    counts = {item["task_type"]: item["replicas"] for item in observed["queues"]}
    assert counts == {1: 2, 2: 2 if activity_available else 0}
    assert observed["queues"][0]["identities"] == ["replica-1", "replica-2"]
    assert observed["queues"][1]["identities"] == (
        ["replica-1", "replica-2"] if activity_available else []
    )
    for call in service.describe_task_queue.await_args_list:
        request = call.args[0]
        assert request.api_mode == DescribeTaskQueueMode.DESCRIBE_TASK_QUEUE_MODE_ENHANCED
        assert request.task_queue_type == TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW
        assert len(request.task_queue_types) == 1
        assert list(request.versions.build_ids) == [
            f"{manifest.worker_deployment_name}:{manifest.build_id}"
        ]
        assert request.report_pollers is True
