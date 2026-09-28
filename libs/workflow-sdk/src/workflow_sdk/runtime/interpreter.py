"""Release-bound package interpreter using modern Worker Deployment versioning."""

from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import timedelta
from typing import Any, ClassVar, Literal, cast

from temporalio import workflow
from temporalio.common import RetryPolicy, VersioningBehavior
from temporalio.exceptions import ActivityError, ApplicationError, is_cancelled_exception

# Domain validation performs no external I/O. Passing these modules through
# avoids reinitializing Pydantic's model internals inside each workflow sandbox.
with workflow.unsafe.imports_passed_through():
    from contracts import (
        ActivityResponse,
        ActivityStep,
        ApprovalStep,
        DecisionStep,
        DefinitionDocument,
        EndStep,
        JsonObject,
        PackageActivityBinding,
        PackageActivityRequest,
        PackageContinuationState,
        PackageExecutionBinding,
        PackageRuntimeStartRequest,
        RetryPolicyModel,
        RuntimeContext,
        TimerStep,
    )
    from pydantic import ValidationError

    from workflow_sdk.definitions import DefinitionValidationError, validate_definition
    from workflow_sdk.packages.validation import definition_hash

from workflows.common import (
    JSONValue,
    SemanticsError,
    make_transition,
    resolve_template,
    route_decision,
    transition_target,
)


def _prepare_start(
    start: PackageRuntimeStartRequest,
    behavior: Literal["pinned", "auto_upgrade"],
) -> DefinitionDocument:
    """Check the captured exact revision and its complete handler closure."""
    binding = start.release_binding
    if binding.versioning_behavior != behavior:
        raise ValueError("Workflow type must match the captured versioning behavior")
    if binding.continue_as_new_policy != "inherit":
        raise ValueError("Explicit continuation upgrades require approved override removal")
    if (
        start.context.definition_id != binding.definition_id
        or start.context.definition_version != binding.definition_version
        or definition_hash(start.definition_document) != binding.definition_content_hash
    ):
        raise ValueError("Definition content and context must match the captured release revision")
    routes = {item.capability: item for item in binding.activity_bindings}
    definition = validate_definition(
        start.definition_document,
        allowed_capabilities=set(routes) - {"create_approval_task"},
    )
    required: set[str] = set()
    for step in definition.steps.values():
        if isinstance(step, ActivityStep):
            if step.compensation is not None:
                raise ValueError("Compensation requires the failure-management runtime")
            route = routes[step.capability]
            if step.contract_version not in (None, route.contract_version):
                raise ValueError("Activity contract must match its captured binding")
            if step.task_queue not in (None, route.task_queue, route.logical_queue):
                raise ValueError("Activity queue must match its captured binding")
            required.add(step.capability)
        elif isinstance(step, ApprovalStep):
            required.add("create_approval_task")
    if required != set(routes):
        raise ValueError("Activity bindings must contain exactly the required handler closure")
    if start.continuation is not None and start.continuation.next_step not in definition.steps:
        raise ValueError("Continuation step must belong to the captured definition revision")
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


class _PackageInterpreter:
    """No filesystem, network, configuration reads, or global capability lookup."""

    versioning_behavior: ClassVar[Literal["pinned", "auto_upgrade"]]

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
        self._start: PackageRuntimeStartRequest | None = None
        self._binding: PackageExecutionBinding | None = None
        self._routes: dict[str, PackageActivityBinding] = {}
        self._continue_requested = False
        self._continuation_count = 0
        self._authorized_loaded_build: str | None = None
        self._rejected_loaded_build: str | None = None

    def authorize_loaded_release(self, build_id: str) -> None:
        """Trusted host interceptor attests its validated installed handler closure.

        This is an instance hook, never a user payload, query, or signal. A new
        worker must validate the captured revision against its installed manifest
        before replay/execution and then attest its own exact Build ID.
        """
        self._authorized_loaded_build = build_id

    def reject_loaded_release(self, build_id: str) -> None:
        """Host rejection is enforced when recorded routing reaches this build.

        Previously recorded commands must still replay on an incompatible host;
        the first boundary on its actual build fails before the next side effect.
        """
        self._rejected_loaded_build = build_id

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

    @workflow.signal(name="continue_execution")
    def continue_execution(self, payload: dict[str, Any]) -> None:
        """Request inherited continuation at the next completed-step boundary."""
        if not payload:
            self._continue_requested = True

    async def _run(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            start = PackageRuntimeStartRequest.model_validate(payload).model_copy(deep=True)
            definition = _prepare_start(start, self.versioning_behavior)
            self._start = start
            self._binding = start.release_binding
            self._routes = {route.capability: route for route in self._binding.activity_bindings}
            self._check_deployment(require_admitted=start.continuation is None)
            if start.continuation is not None:
                self._results = deepcopy(start.continuation.results)
                self._status["transitions"] = deepcopy(list(start.continuation.transitions))
                self._continuation_count = start.continuation.continuation_count
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
                release_binding=self._binding.model_dump(mode="json"),
                continuation_count=self._continuation_count,
            )

            current: str | None = (
                start.continuation.next_step
                if start.continuation is not None
                else definition.start_at
            )
            while current is not None:
                self._check_deployment()
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
                if current is not None and self._continue_requested:
                    self._continue(current)

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
            if isinstance(exc, ApplicationError) and exc.type == "PackageCompatibilityError":
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

    def _check_deployment(self, *, require_admitted: bool = False) -> None:
        assert self._binding is not None
        info = workflow.info()
        actual = info.get_current_deployment_version()
        if actual is not None and actual.build_id == self._rejected_loaded_build:
            raise ApplicationError(
                "Installed package does not retain the execution's exact revision and handler contracts",
                type="PackageCompatibilityError",
                non_retryable=True,
            )
        if (
            actual is None
            or actual.deployment_name != self._binding.worker_deployment_name
            or (
                actual.build_id not in self._binding.eligible_build_ids
                and (require_admitted or actual.build_id != self._authorized_loaded_build)
            )
            or (self.versioning_behavior == "pinned" and actual.build_id != self._binding.build_id)
            or info.namespace != self._binding.temporal_namespace
        ):
            raise ValueError("Worker deployment is outside the captured eligible routing policy")
        self._status["actual_build_id"] = actual.build_id

    def _continue(self, next_step: str) -> None:
        assert self._start is not None
        self._check_deployment()
        self._record(next_step, "CONTINUED_AS_NEW", "inherited release binding")
        state = PackageContinuationState(
            next_step=next_step,
            results=deepcopy(self._results),
            transitions=tuple(deepcopy(self._status["transitions"])),
            continuation_count=self._continuation_count + 1,
        )
        continued = self._start.model_copy(update={"continuation": state}, deep=True)
        # No initial version override: the server inherits the execution's routing.
        workflow.continue_as_new(continued.model_dump(mode="json"))

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
        route: PackageActivityBinding,
        input_document: JsonObject,
        timeout_seconds: int,
        retry: RetryPolicyModel,
    ) -> JsonObject:
        assert self._context is not None
        assert self._binding is not None
        self._check_deployment()
        request = PackageActivityRequest(
            release_binding=self._binding.model_copy(deep=True),
            workflow_id=self._workflow_id,
            run_id=self._run_id,
            step_id=step_id,
            capability=route.capability,
            contract_version=route.contract_version,
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
        route = self._routes[step.capability]
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
            self._routes["create_approval_task"],
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


@workflow.defn(name="PackageWorkflowV1", versioning_behavior=VersioningBehavior.PINNED)
class PackageWorkflowV1(_PackageInterpreter):
    """Execute the captured revision on its explicitly pinned deployment version."""

    versioning_behavior = "pinned"

    @workflow.run
    async def run(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._run(payload)


@workflow.defn(
    name="PackageAutoUpgradeWorkflowV1", versioning_behavior=VersioningBehavior.AUTO_UPGRADE
)
class PackageAutoUpgradeWorkflowV1(_PackageInterpreter):
    """Allow compatible worker upgrades while retaining the captured definition."""

    versioning_behavior = "auto_upgrade"

    @workflow.run
    async def run(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._run(payload)
