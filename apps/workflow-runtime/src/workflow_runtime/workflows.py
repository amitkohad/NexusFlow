"""Immutable v1 routing and deterministic interpretation of governed definitions."""

from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import timedelta
from typing import Any, Literal, cast

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError, is_cancelled_exception

# Domain validation performs no external I/O. Passing these modules through
# avoids reinitializing Pydantic's model internals inside each workflow sandbox.
with workflow.unsafe.imports_passed_through():
    from contracts import (
        ActivityRequest,
        ActivityResponse,
        ActivityStep,
        ApprovalStep,
        DecisionStep,
        DefinitionDocument,
        EndStep,
        JsonObject,
        RetryPolicyModel,
        RuntimeContext,
        RuntimeStartRequest,
        TimerStep,
    )
    from pydantic import ValidationError
    from workflow_sdk.definitions import DefinitionValidationError, validate_definition

from workflows.common import (
    JSONValue,
    SemanticsError,
    make_transition,
    resolve_template,
    route_decision,
    transition_target,
)
from workflows.common.catalog import CAPABILITY_ROUTES, CapabilityRoute, resolve_capability

_ACTIVITY_CAPABILITIES = frozenset(CAPABILITY_ROUTES) - {"create_approval_task"}


def _prepare_definition(document: DefinitionDocument) -> DefinitionDocument:
    """Reject unsupported policies and routes before scheduling any side effect."""
    definition = validate_definition(document, allowed_capabilities=_ACTIVITY_CAPABILITIES)
    for step in definition.steps.values():
        if isinstance(step, ActivityStep):
            if step.compensation is not None:
                raise ValueError("Compensation requires the failure-management runtime")
            resolve_capability(step.capability, step.contract_version, step.task_queue)
    return definition.model_copy(deep=True)


def _retry_policy(policy: RetryPolicyModel) -> RetryPolicy:
    return RetryPolicy(
        maximum_attempts=policy.maximum_attempts,
        initial_interval=timedelta(seconds=policy.initial_interval_seconds),
        maximum_interval=timedelta(seconds=policy.maximum_interval_seconds),
        backoff_coefficient=policy.backoff_coefficient,
        non_retryable_error_types=policy.non_retryable_error_types,
    )


def _approval_decision(payload: object) -> JsonObject | None:
    if not isinstance(payload, dict) or type(payload.get("approved")) is not bool:
        return None
    actor = payload.get("approver")
    comment = payload.get("comment", "")
    if not isinstance(actor, str) or not actor.strip() or not isinstance(comment, str):
        return None
    return {"approved": payload["approved"], "approver": actor, "comment": comment}


@workflow.defn(name="GovernedWorkflowV1")
class GovernedWorkflowV1:
    """Execute one pinned definition through its capability-owned task queues.

    The workflow never reads configuration or calls an external service directly.
    Approval-task persistence and lifecycle enforcement are supplied in Phase 5;
    this version creates a task reference and accepts the first valid in-window
    decision through the existing business signal.
    """

    def __init__(self) -> None:
        self._status: dict[str, Any] = {
            "state": "CREATED",
            "current_step": None,
            "transitions": [],
            "results": {},
        }
        self._approval: JsonObject | None = None
        self._approval_waiting = False
        self._context: RuntimeContext | None = None
        self._request: JsonObject = {}
        self._variables: JsonObject = {}
        self._results: JsonObject = {}
        self._workflow_id = ""
        self._run_id = ""

    @workflow.signal(name="approve")
    def approve(self, payload: dict[str, Any]) -> None:
        if not self._approval_waiting or self._approval is not None:
            return
        decision = _approval_decision(payload)
        if decision is not None:
            self._approval = decision

    @workflow.query(name="status")
    def status(self) -> dict[str, Any]:
        return deepcopy(self._status)

    @workflow.run
    async def run(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            start = RuntimeStartRequest.model_validate(payload).model_copy(deep=True)
            definition = _prepare_definition(start.definition_document)
            self._context = start.context
            self._request = start.request
            self._variables = start.variables
            self._workflow_id = workflow.info().workflow_id
            self._run_id = workflow.info().run_id
            self._status.update(
                state="RUNNING",
                process_id=self._workflow_id,
                workflow_name=start.context.workflow_type,
                definition_id=start.context.definition_id,
                definition_version=start.context.definition_version,
                runtime_version=start.runtime_version,
                results=self._results,
            )

            current: str | None = definition.start_at
            while current is not None:
                step = definition.steps[current]
                self._status["current_step"] = current
                self._record(current, "STARTED", step.type)
                if isinstance(step, ActivityStep):
                    current = await self._activity(current, step)
                elif isinstance(step, DecisionStep):
                    target = route_decision(
                        self._expression_context(),
                        step.field,
                        step.operator,
                        step.value,
                        step.on_true,
                        step.on_false,
                    )
                    self._record(current, "ROUTED", f"next={target}")
                    current = target
                elif isinstance(step, ApprovalStep):
                    current = await self._wait_for_approval(current, step)
                elif isinstance(step, TimerStep):
                    await workflow.sleep(timedelta(seconds=step.seconds))
                    self._record(current, "COMPLETED", f"slept {step.seconds}s")
                    current = transition_target(step.model_dump(mode="python"))
                elif isinstance(step, EndStep):
                    self._status["state"] = step.outcome.value
                    self._record(current, step.outcome.value, "end")
                    current = None

            self._status["current_step"] = None
            return self.status()
        except (Exception, asyncio.CancelledError) as exc:
            self._approval_waiting = False
            self._status.pop("approval", None)
            cancelled = is_cancelled_exception(exc)
            self._status["state"] = "CANCELLED" if cancelled else "FAILED"
            current = self._status["current_step"]
            if current is not None:
                self._record(current, self._status["state"], "cancelled" if cancelled else "failed")
            self._status["current_step"] = None
            if cancelled or isinstance(exc, ActivityError):
                raise
            if isinstance(
                exc, (DefinitionValidationError, ValidationError, SemanticsError, ValueError)
            ):
                raise ApplicationError(
                    "Workflow definition or input is invalid",
                    type="ValidationError",
                    non_retryable=True,
                ) from None
            raise ApplicationError(
                "Workflow execution failed", type="WorkflowError", non_retryable=True
            ) from None

    def _expression_context(self) -> dict[str, JSONValue]:
        return cast(
            dict[str, JSONValue],
            {"request": self._request, "variables": self._variables, "results": self._results},
        )

    def _record(self, step: str, state: str, detail: str) -> None:
        self._status["transitions"].append(make_transition(step, state, detail, workflow.now()))

    async def _invoke(
        self,
        step_id: str,
        route: CapabilityRoute,
        input_document: JsonObject,
        timeout_seconds: int,
        retry: RetryPolicyModel,
    ) -> JsonObject:
        assert self._context is not None
        request = ActivityRequest(
            workflow_id=self._workflow_id,
            run_id=self._run_id,
            step_id=step_id,
            capability=route.capability,
            contract_version=cast(Literal["1.0"], route.contract_version),
            context=self._context,
            idempotency_key=f"{self._workflow_id}:{step_id}:{route.contract_version}",
            input=deepcopy(input_document),
            request=deepcopy(self._request),
            variables=deepcopy(self._variables),
            results=deepcopy(self._results),
        )
        response = await workflow.execute_activity(
            route.activity_name,
            request,
            result_type=ActivityResponse,
            task_queue=route.task_queue,
            start_to_close_timeout=timedelta(seconds=timeout_seconds),
            retry_policy=_retry_policy(retry),
            cancellation_type=workflow.ActivityCancellationType.TRY_CANCEL,
        )
        validated = ActivityResponse.model_validate(response)
        if (
            validated.capability != route.capability
            or validated.contract_version != route.contract_version
        ):
            raise ValueError("Activity response must match its pinned capability contract")
        return deepcopy(validated.output)

    async def _activity(self, step_id: str, step: ActivityStep) -> str | None:
        route = resolve_capability(step.capability, step.contract_version, step.task_queue)
        expanded = resolve_template(step.input, self._expression_context())
        if not isinstance(expanded, dict):
            raise ValueError("Activity input must resolve to an object")
        output = await self._invoke(step_id, route, expanded, step.timeout_seconds, step.retry)
        self._results[step_id] = output
        self._record(step_id, "COMPLETED", step.capability)
        return transition_target(step.model_dump(mode="python"))

    async def _wait_for_approval(self, step_id: str, step: ApprovalStep) -> str | None:
        self._approval = None
        self._approval_waiting = False
        task = await self._invoke(
            step_id,
            resolve_capability("create_approval_task"),
            {"assignee_group": step.assignee_group, "timeout_seconds": step.timeout_seconds},
            30,
            RetryPolicyModel(),
        )
        task_id = task.get("task_id")
        if not isinstance(task_id, str) or not task_id.strip():
            raise ValueError("Approval Activity must return a task reference")
        self._results[step_id] = deepcopy(task)
        self._status.update(
            state="WAITING_FOR_APPROVAL",
            approval={
                "step_id": step_id,
                "assignee_group": step.assignee_group,
                "task_id": task_id,
            },
        )
        self._approval_waiting = True
        try:
            await workflow.wait_condition(
                lambda: self._approval is not None, timeout=timedelta(seconds=step.timeout_seconds)
            )
        except asyncio.TimeoutError:
            self._results[step_id] = {**task, "status": "TIMED_OUT"}
            self._status["state"] = "RUNNING"
            self._record(step_id, "TIMED_OUT", f"after {step.timeout_seconds}s")
            return transition_target(step.model_dump(mode="python"), timed_out=True)
        finally:
            self._approval_waiting = False
            self._status.pop("approval", None)
        assert self._approval is not None
        self._results[step_id] = {**task, **self._approval}
        self._status["state"] = "RUNNING"
        approved = self._approval["approved"] is True
        self._record(
            step_id, "APPROVED" if approved else "REJECTED", str(self._approval["approver"])
        )
        return transition_target(step.model_dump(mode="python"), approved=approved)
