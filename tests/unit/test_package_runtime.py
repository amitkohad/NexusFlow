"""Release capture, safe continuation and modern versioning runtime contracts."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Literal
from unittest.mock import AsyncMock, Mock

import pytest
from contracts import (
    ActivityResponse,
    ApprovalStep,
    DefinitionDocument,
    PackageActivityRequest,
    PackageRuntimeStartRequest,
)
from temporalio import workflow
from temporalio.common import VersioningBehavior, WorkerDeploymentVersion
from temporalio.exceptions import ApplicationError
from workflow_sdk.packages.validation import definition_hash
from workflow_sdk.runtime import PackageAutoUpgradeWorkflowV1, PackageWorkflowV1
from workflow_sdk.runtime.interpreter import _prepare_start

from tests.package_fixtures import source_package, start_request


def package_start(
    document: dict[str, Any] | None = None,
    *,
    behavior: Literal["pinned", "auto_upgrade"] = "pinned",
) -> PackageRuntimeStartRequest:
    definition = DefinitionDocument.model_validate(
        document
        or {
            "start_at": "validate",
            "steps": {
                "validate": {
                    "type": "activity",
                    "capability": "validate_request",
                    "next": "done",
                    "input": {"amount": "${request.amount}"},
                },
                "done": {"type": "end"},
            },
        }
    )
    return PackageRuntimeStartRequest.model_validate(
        {
            "context": {
                "tenant": "tenant",
                "business_domain": "finance",
                "application": "adjustments",
                "workflow_type": "adjustment",
                "definition_id": "adjustment-definition",
                "definition_version": "1.0",
                "business_reference": "BIZ-1",
                "correlation_id": "correlation",
                "actor": "verified-user",
            },
            "definition_document": definition,
            "request": {"amount": 1250},
            "release_binding": {
                "package_id": "adjustments",
                "package_release_id": "release-1",
                "package_version": "1.0",
                "build_id": "build-1",
                "manifest_hash": "a" * 64,
                "artifact_digest": "sha256:" + "b" * 64,
                "worker_deployment_name": "adjustments",
                "temporal_namespace": "default",
                "workflow_task_queue": "adjustment-workflows",
                "definition_id": "adjustment-definition",
                "definition_version": "1.0",
                "definition_content_hash": definition_hash(definition),
                "activity_bindings": [
                    {
                        "capability": "validate_request",
                        "activity_name": "validate_request.pkg.v1",
                        "logical_queue": "validation",
                        "task_queue": "release-validation",
                    }
                ],
                "eligible_build_ids": ["build-1", "build-2"],
                "versioning_behavior": behavior,
            },
        }
    )


def approval_start(*, task_api_required: bool) -> PackageRuntimeStartRequest:
    raw = package_start(
        {
            "start_at": "approval",
            "steps": {
                "approval": {
                    "type": "approval",
                    "assignee_group": "managers",
                    "timeout_seconds": 60,
                    "on_approved": "done",
                    "on_rejected": "rejected",
                    "on_timeout": "timed_out",
                },
                "done": {"type": "end"},
                "rejected": {"type": "end", "outcome": "REJECTED"},
                "timed_out": {"type": "end", "outcome": "TIMED_OUT"},
            },
        }
    ).model_dump(mode="json")
    raw["release_binding"]["activity_bindings"] = [
        {
            "capability": "create_approval_task",
            "activity_name": "create_approval_task.pkg.v1",
            "logical_queue": "human_tasks",
            "task_queue": "release-validation",
        }
    ]
    raw["release_binding"]["task_api_required"] = task_api_required
    return PackageRuntimeStartRequest.model_validate(raw)


def mock_workflow_info(
    monkeypatch: pytest.MonkeyPatch,
    *,
    build: str = "build-1",
    deployment: str = "adjustments",
    namespace: str = "default",
    queue: str = "adjustment-workflows",
    versioned: bool = True,
) -> None:
    actual = WorkerDeploymentVersion(deployment, build) if versioned else None
    monkeypatch.setattr(
        workflow,
        "info",
        lambda: SimpleNamespace(
            workflow_id="workflow-1",
            run_id="run-1",
            namespace=namespace,
            task_queue=queue,
            get_current_deployment_version=lambda: actual,
        ),
    )
    monkeypatch.setattr(workflow, "now", lambda: datetime(2026, 9, 27, tzinfo=timezone.utc))


def test_workflow_modes_and_handlers_are_explicit() -> None:
    for runtime, behavior in (
        (PackageWorkflowV1, VersioningBehavior.PINNED),
        (PackageAutoUpgradeWorkflowV1, VersioningBehavior.AUTO_UPGRADE),
    ):
        definition = workflow._Definition.must_from_class(runtime)
        assert definition.versioning_behavior == behavior
        assert set(definition.signals) == {"approve", "task_expired", "continue_execution"}
        assert set(definition.queries) == {"status"}


def test_preflight_returns_an_independent_definition_capture() -> None:
    source = package_start()
    captured = _prepare_start(source, "pinned")
    source.definition_document.steps.clear()
    assert set(captured.steps) == {"validate", "done"}


@pytest.mark.parametrize(
    "changes",
    [
        {"definition_content_hash": "c" * 64},
        {"definition_version": "2.0"},
        {"definition_id": "another-definition"},
        {"versioning_behavior": "auto_upgrade"},
        {"continue_as_new_policy": "explicit_upgrade"},
        {"activity_bindings": []},
    ],
)
def test_mismatched_release_or_incomplete_closure_is_rejected(changes: dict[str, Any]) -> None:
    raw = package_start().model_dump(mode="json")
    raw["release_binding"].update(changes)
    start = PackageRuntimeStartRequest.model_validate(raw)
    with pytest.raises(ValueError):
        _prepare_start(start, "pinned")


@pytest.mark.parametrize(
    "change",
    [
        {"task_queue": "other-package-queue"},
        {"contract_version": "2.0"},
        {"compensation": {"capability": "validate_request"}},
        {"capability": "create_approval_task"},
    ],
)
def test_activity_cannot_escape_the_bound_handler_or_enable_deferred_policy(
    change: dict[str, Any],
) -> None:
    raw = package_start().model_dump(mode="json")
    raw["definition_document"]["steps"]["validate"].update(change)
    definition = DefinitionDocument.model_validate(raw["definition_document"])
    raw["release_binding"]["definition_content_hash"] = definition_hash(definition)
    with pytest.raises(ValueError):
        _prepare_start(PackageRuntimeStartRequest.model_validate(raw), "pinned")


def test_binding_does_not_admit_unrelated_capabilities() -> None:
    raw = package_start().model_dump(mode="json")
    raw["release_binding"]["activity_bindings"].append(
        {
            "capability": "send_notification",
            "activity_name": "send_notification.pkg.v1",
            "logical_queue": "notifications",
            "task_queue": "notification-queue",
        }
    )
    with pytest.raises(ValueError, match="exactly the required handler closure"):
        _prepare_start(PackageRuntimeStartRequest.model_validate(raw), "pinned")


@pytest.mark.parametrize(
    "changes",
    [
        {"build": "unadmitted-build"},
        {"deployment": "another-package"},
        {"namespace": "other-namespace"},
        {"versioned": False},
        {"build": "build-2"},
    ],
)
async def test_pinned_routing_mismatch_fails_before_activity(
    monkeypatch: pytest.MonkeyPatch, changes: dict[str, Any]
) -> None:
    mock_workflow_info(monkeypatch, **changes)
    execute = AsyncMock()
    monkeypatch.setattr(workflow, "execute_activity", execute)
    with pytest.raises(ApplicationError) as caught:
        await PackageWorkflowV1().run(package_start().model_dump(mode="json"))
    assert caught.value.type == "ValidationError"
    assert caught.value.non_retryable
    execute.assert_not_awaited()


async def test_auto_upgrade_uses_captured_binding_and_definition_on_an_eligible_new_build(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_workflow_info(monkeypatch, build="build-2")
    response = ActivityResponse(capability="validate_request", output={"valid": True})
    execute = AsyncMock(return_value=response)
    monkeypatch.setattr(workflow, "execute_activity", execute)
    start = package_start(behavior="auto_upgrade")
    result = await PackageAutoUpgradeWorkflowV1().run(start.model_dump(mode="json"))
    assert result["state"] == "COMPLETED"
    assert result["actual_build_id"] == "build-2"
    assert result["release_binding"]["build_id"] == "build-1"
    assert result["definition_version"] == "1.0"
    call = execute.await_args
    assert call is not None
    args, kwargs = call
    assert args[0] == "validate_request.pkg.v1"
    assert kwargs["task_queue"] == "release-validation"
    assert kwargs["result_type"] is ActivityResponse
    envelope = args[1]
    assert isinstance(envelope, PackageActivityRequest)
    assert envelope.release_binding == start.release_binding
    assert envelope.input == {"amount": 1250}
    assert envelope.idempotency_key == "workflow-1:validate:1.0"


async def test_replay_worker_queue_does_not_replace_captured_activity_routing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_workflow_info(monkeypatch, queue="replay-synthetic")
    execute = AsyncMock(return_value=ActivityResponse(capability="validate_request", output={}))
    monkeypatch.setattr(workflow, "execute_activity", execute)
    result = await PackageWorkflowV1().run(package_start().model_dump(mode="json"))
    assert result["state"] == "COMPLETED"
    call = execute.await_args
    assert call is not None
    assert call.kwargs["task_queue"] == "release-validation"


async def test_activity_response_cannot_change_the_bound_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_workflow_info(monkeypatch)
    monkeypatch.setattr(
        workflow,
        "execute_activity",
        AsyncMock(return_value=ActivityResponse(capability="send_notification", output={})),
    )
    with pytest.raises(ApplicationError) as caught:
        await PackageWorkflowV1().run(package_start().model_dump(mode="json"))
    assert caught.value.type == "ValidationError"


async def test_trusted_installed_closure_attestation_admits_later_auto_upgrade(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_workflow_info(monkeypatch, build="build-1")

    async def completed_activity(*args: Any, **kwargs: Any) -> ActivityResponse:
        mock_workflow_info(monkeypatch, build="build-3")
        return ActivityResponse(capability="validate_request", output={})

    execute = AsyncMock(side_effect=completed_activity)
    monkeypatch.setattr(workflow, "execute_activity", execute)
    instance = PackageAutoUpgradeWorkflowV1()
    instance.authorize_loaded_release("build-3")
    start = package_start(behavior="auto_upgrade")
    result = await instance.run(start.model_dump(mode="json"))
    assert result["actual_build_id"] == "build-3"
    assert result["release_binding"]["eligible_build_ids"] == ["build-1", "build-2"]
    assert result["release_binding"]["build_id"] == "build-1"
    execute.assert_awaited_once()


async def test_trusted_attestation_cannot_expand_a_new_runs_initial_admission_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_workflow_info(monkeypatch, build="build-3")
    execute = AsyncMock()
    monkeypatch.setattr(workflow, "execute_activity", execute)
    instance = PackageAutoUpgradeWorkflowV1()
    instance.authorize_loaded_release("build-3")
    with pytest.raises(ApplicationError) as caught:
        await instance.run(package_start(behavior="auto_upgrade").model_dump(mode="json"))
    assert caught.value.type == "ValidationError"
    execute.assert_not_awaited()


@pytest.mark.parametrize("behavior", ["pinned", "auto_upgrade"])
async def test_attestation_cannot_authorize_a_different_actual_build_or_change_a_pin(
    monkeypatch: pytest.MonkeyPatch,
    behavior: Literal["pinned", "auto_upgrade"],
) -> None:
    mock_workflow_info(monkeypatch, build="build-3")
    instance = PackageWorkflowV1() if behavior == "pinned" else PackageAutoUpgradeWorkflowV1()
    instance.authorize_loaded_release("build-3" if behavior == "pinned" else "another-build")
    with pytest.raises(ApplicationError) as caught:
        await instance.run(package_start(behavior=behavior).model_dump(mode="json"))
    assert caught.value.type == "ValidationError"


async def test_host_rejection_preserves_prior_build_replay_then_fails_its_actual_build(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    start = package_start(behavior="auto_upgrade")
    instance = PackageAutoUpgradeWorkflowV1()
    instance._binding = start.release_binding
    instance.reject_loaded_release("build-2")
    mock_workflow_info(monkeypatch, build="build-1")
    instance._check_deployment()
    mock_workflow_info(monkeypatch, build="build-2")
    with pytest.raises(ApplicationError) as caught:
        instance._check_deployment()
    assert caught.value.type == "PackageCompatibilityError"
    assert caught.value.non_retryable


async def test_host_rejection_is_a_terminal_sanitized_failure_before_activity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_workflow_info(monkeypatch)
    execute = AsyncMock()
    monkeypatch.setattr(workflow, "execute_activity", execute)
    instance = PackageAutoUpgradeWorkflowV1()
    instance.reject_loaded_release("build-1")
    with pytest.raises(ApplicationError) as caught:
        await instance.run(package_start(behavior="auto_upgrade").model_dump(mode="json"))
    assert caught.value.type == "PackageCompatibilityError"
    assert caught.value.non_retryable
    execute.assert_not_awaited()


def test_continuation_preserves_release_and_prior_business_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_workflow_info(monkeypatch)
    instance = PackageWorkflowV1()
    start = package_start()
    instance._start = start
    instance._binding = start.release_binding
    instance._results = {"validate": {"valid": True}}
    instance._status["transitions"] = [{"step": "validate", "state": "COMPLETED"}]

    class TestContinuation(workflow.ContinueAsNewError):
        pass

    continue_as_new = Mock(side_effect=TestContinuation)
    monkeypatch.setattr(workflow, "continue_as_new", continue_as_new)
    with pytest.raises(workflow.ContinueAsNewError):
        instance._continue("done")
    call = continue_as_new.call_args
    assert call is not None
    assert not call.kwargs
    continued = PackageRuntimeStartRequest.model_validate(call.args[0])
    assert continued.release_binding == start.release_binding
    assert continued.definition_document == start.definition_document
    assert continued.request == start.request
    state = continued.continuation
    assert state is not None
    assert state.next_step == "done"
    assert state.continuation_count == 1
    assert state.results == {"validate": {"valid": True}}
    assert state.transitions[0] == {"step": "validate", "state": "COMPLETED"}
    assert state.transitions[-1]["state"] == "CONTINUED_AS_NEW"


def test_continuation_signal_cannot_request_an_unapproved_upgrade() -> None:
    instance = PackageWorkflowV1()
    instance.continue_execution({"build_id": "unadmitted"})
    assert not instance._continue_requested
    instance.continue_execution({})
    assert instance._continue_requested


def test_continuation_cannot_resume_an_undeclared_step() -> None:
    raw = package_start().model_dump(mode="json")
    raw["continuation"] = {"next_step": "unknown"}
    with pytest.raises(ValueError, match="Continuation step"):
        _prepare_start(PackageRuntimeStartRequest.model_validate(raw), "pinned")


@pytest.mark.parametrize("approved", [True, False])
def test_only_first_strict_in_window_approval_is_accepted(approved: bool) -> None:
    instance = PackageWorkflowV1()
    instance.approve({"approved": approved, "approver": "early"})
    assert instance._approval is None
    instance._approval_waiting = True
    instance.approve({"approved": "true", "approver": "invalid"})
    assert instance._approval is None
    instance.approve({"approved": approved, "approver": "first"})
    instance.approve({"approved": not approved, "approver": "later"})
    assert instance._approval == {"approved": approved, "approver": "first", "comment": ""}


def test_durable_task_decision_buffers_early_event_and_requires_correlation() -> None:
    instance = PackageWorkflowV1()
    instance._active_task_key = "current-task-key"
    for index in range(8):
        instance.approve(
            {
                "task_id": f"old-task-{index}",
                "event_id": f"old-event-{index}",
                "idempotency_key": "previous-task-key",
                "approved": False,
                "approver": "old-actor",
            }
        )
    assert instance._pending_task_events == {}
    early = {
        "task_id": "task-1",
        "event_id": "event-1",
        "idempotency_key": "current-task-key",
        "approved": True,
        "approver": "manager",
        "evidence_reference": "evidence-1",
    }
    instance.approve(early)
    assert instance._approval is None
    assert instance._pending_task_events["task-1"]["event_id"] == "event-1"
    instance.approve({**early, "event_id": "event-2", "approved": False})
    assert instance._pending_task_events["task-1"]["event_id"] == "event-1"

    instance._approval_waiting = True
    instance._durable_task_id = "task-1"
    instance.approve({"approved": False, "approver": "bypass"})
    instance.approve({**early, "task_id": "other-task"})
    instance.approve({**early, "idempotency_key": "previous-task-key"})
    instance.approve({**early, "event_id": ""})
    assert instance._approval is None
    instance.approve(early)
    instance.approve({**early, "event_id": "event-3", "approved": False})
    assert instance._approval == {
        "approved": True,
        "approver": "manager",
        "comment": "",
        "task_id": "task-1",
        "event_id": "event-1",
        "idempotency_key": "current-task-key",
        "evidence_reference": "evidence-1",
    }


def test_durable_task_expiration_is_correlated_and_first_terminal_event_wins() -> None:
    instance = PackageWorkflowV1()
    instance._approval_waiting = True
    instance._durable_task_id = "task-1"
    instance._active_task_key = "current-task-key"
    event = {"task_id": "task-1", "idempotency_key": "current-task-key"}
    instance.task_expired({**event, "task_id": "other-task", "event_id": "event-wrong"})
    instance.task_expired({**event, "event_id": ""})
    assert instance._approval is None
    instance.task_expired({**event, "event_id": "event-expired"})
    instance.approve(
        {
            **event,
            "event_id": "event-approved",
            "approved": True,
            "approver": "late-approver",
        }
    )
    assert instance._approval == {
        "task_id": "task-1",
        "event_id": "event-expired",
        "idempotency_key": "current-task-key",
        "expired": True,
    }


async def test_durable_binding_requires_durable_task_activity_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    start = start_request(source_package())
    instance = PackageWorkflowV1()
    instance._binding = start.release_binding
    instance._routes = {
        route.capability: route for route in start.release_binding.activity_bindings
    }
    monkeypatch.setattr(instance, "_invoke", AsyncMock(return_value={"task_id": "synthetic-task"}))
    step = start.definition_document.steps["manager_approval"]
    assert isinstance(step, ApprovalStep)
    with pytest.raises(ApplicationError, match="durable human task") as failure:
        await instance._wait_for_approval("manager_approval", step)
    assert failure.value.type == "TaskContractError"
    assert failure.value.non_retryable


async def test_durable_response_remains_correlated_under_a_false_historical_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_workflow_info(monkeypatch)
    instance = PackageWorkflowV1()

    async def create_task(
        _name: str, request: PackageActivityRequest, **_options: Any
    ) -> ActivityResponse:
        instance.approve(
            {
                "task_id": "task-1",
                "event_id": "early-event",
                "idempotency_key": request.idempotency_key,
                "approved": True,
                "approver": "manager",
            }
        )
        return ActivityResponse(
            capability="create_approval_task",
            output={"task_id": "task-1", "durable": True},
        )

    async def wait_for_signal(predicate: Any, **_options: Any) -> None:
        assert predicate()

    monkeypatch.setattr(workflow, "execute_activity", create_task)
    monkeypatch.setattr(workflow, "wait_condition", wait_for_signal)
    result = await instance.run(approval_start(task_api_required=False).model_dump(mode="json"))
    assert result["state"] == "COMPLETED"
    assert result["results"]["approval"]["idempotency_key"] == "workflow-1:approval:1.0"


async def test_durable_binding_reports_missing_durable_activity_output_from_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_workflow_info(monkeypatch)
    monkeypatch.setattr(
        workflow,
        "execute_activity",
        AsyncMock(
            return_value=ActivityResponse(
                capability="create_approval_task", output={"task_id": "synthetic-task"}
            )
        ),
    )
    with pytest.raises(ApplicationError, match="durable human task") as failure:
        await PackageWorkflowV1().run(
            approval_start(task_api_required=True).model_dump(mode="json")
        )
    assert failure.value.type == "TaskContractError"
    assert failure.value.non_retryable


async def test_invalid_transport_is_sanitized_and_non_retryable() -> None:
    with pytest.raises(ApplicationError) as caught:
        await PackageWorkflowV1().run({"secret": "must-not-leak"})
    assert caught.value.type == "ValidationError"
    assert caught.value.non_retryable
    assert "must-not-leak" not in str(caught.value)
