"""Recovery remains anchored to the original run despite continuation races."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, patch

import pytest
from contracts import ExecutionState
from temporalio.client import Client
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode
from workflow_api.backend import (
    BackendNotFound,
    BackendUnavailable,
    BusinessSnapshot,
    TemporalBackend,
)
from workflow_api.package_backend import PackageTemporalBackend

from tests.package_fixtures import source_package, start_request


def description(run_id: str, first: str = "original") -> Any:
    return SimpleNamespace(
        run_id=run_id,
        raw_description=SimpleNamespace(
            workflow_execution_info=SimpleNamespace(first_run_id=first)
        ),
    )


def fake_client(*latest: Any) -> Any:
    original = SimpleNamespace(describe=AsyncMock(return_value=description("original")))
    current = SimpleNamespace(describe=AsyncMock(side_effect=list(latest)))
    return SimpleNamespace(
        namespace="default",
        get_workflow_handle=lambda workflow_id, **kwargs: (
            original if kwargs.get("run_id") else current
        ),
    )


async def test_status_retries_a_continuation_race_without_freezing_manual_intervention() -> None:
    client = fake_client(description("run-2"), description("run-3"))
    backend = PackageTemporalBackend(cast(Client, client))
    raced = BusinessSnapshot(
        state=ExecutionState.MANUAL_INTERVENTION, failure_code="runtime_run_changed"
    )
    completed = BusinessSnapshot(state=ExecutionState.COMPLETED)
    with patch.object(
        TemporalBackend, "status", AsyncMock(side_effect=[raced, completed])
    ) as status:
        run_id, snapshot = await backend.status_package("workflow", "original")
    assert run_id == "run-3" and snapshot.state == ExecutionState.COMPLETED
    assert [call.args for call in status.await_args_list] == [
        ("workflow", "run-2"),
        ("workflow", "run-3"),
    ]


async def test_repeated_continuation_race_is_retryable_and_never_persisted_as_business_failure() -> (
    None
):
    backend = PackageTemporalBackend(
        cast(Client, fake_client(*(description(f"run-{n}") for n in range(3))))
    )
    with patch.object(
        TemporalBackend,
        "status",
        AsyncMock(
            return_value=BusinessSnapshot(
                state=ExecutionState.MANUAL_INTERVENTION, failure_code="runtime_run_changed"
            )
        ),
    ):
        with pytest.raises(BackendUnavailable):
            await backend.status_package("workflow", "original")


async def test_status_rejects_an_unrelated_workflow_id_reuse() -> None:
    backend = PackageTemporalBackend(
        cast(Client, fake_client(description("reused", first="different-chain")))
    )
    with patch.object(TemporalBackend, "status", AsyncMock()) as status:
        with pytest.raises(BackendUnavailable):
            await backend.status_package("workflow", "original")
        status.assert_not_awaited()


@pytest.mark.parametrize(
    "code, expected",
    [(RPCStatusCode.NOT_FOUND, BackendNotFound), (RPCStatusCode.UNAVAILABLE, BackendUnavailable)],
)
async def test_chain_description_failures_are_sanitized(
    code: RPCStatusCode, expected: type[Exception]
) -> None:
    backend = PackageTemporalBackend(
        cast(Client, fake_client(RPCError("private server details", code, b"")))
    )
    with pytest.raises(expected) as caught:
        await backend.status_package("workflow", "original")
    assert "private" not in str(caught.value)


async def test_duplicate_uncertain_start_recovers_first_run_after_continuation() -> None:
    package = source_package("validation-reference")
    start = start_request(package)
    client = fake_client(description("continued-run"))
    client.start_workflow = AsyncMock(
        side_effect=WorkflowAlreadyStartedError(
            "workflow", "PackageWorkflowV1", run_id="continued-run"
        )
    )
    assert (
        await PackageTemporalBackend(cast(Client, client)).start_package("workflow", start)
        == "original"
    )
