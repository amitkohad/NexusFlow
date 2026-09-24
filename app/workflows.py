from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy


@workflow.defn(name="LightweightProcess")
class LightweightProcess:
    """A small declarative workflow interpreter on top of Temporal.

    The workflow definition is passed as JSON at start time, so the prototype
    can demonstrate a low-code/lightweight workflow model without requiring a
    code change for every process definition.
    """

    def __init__(self) -> None:
        self._approval: dict[str, Any] | None = None
        self._status: dict[str, Any] = {
            "state": "CREATED",
            "current_step": None,
            "transitions": [],
            "results": {},
        }

    @workflow.signal(name="approve")
    def approve(self, decision: dict[str, Any]) -> None:
        """Human-task signal. Expected input: {approved, approver, comment}."""
        self._approval = {
            "approved": bool(decision.get("approved", False)),
            "approver": decision.get("approver", "unknown"),
            "comment": decision.get("comment", ""),
        }

    @workflow.query(name="status")
    def status(self) -> dict[str, Any]:
        """Return the current process state for a portal/dashboard."""
        return self._status

    @workflow.run
    async def run(self, spec: dict[str, Any]) -> dict[str, Any]:
        self._validate_spec(spec)

        process_id = spec.get("process_id", workflow.info().workflow_id)
        self._status.update(
            {
                "process_id": process_id,
                "workflow_name": spec.get("workflow_name", "unnamed-process"),
                "state": "RUNNING",
            }
        )

        context: dict[str, Any] = {
            "request": spec.get("request", {}),
            "variables": spec.get("variables", {}),
            "results": {},
        }

        current_step = spec["start_at"]
        steps: dict[str, dict[str, Any]] = spec["steps"]

        while current_step:
            step = steps[current_step]
            step_type = step["type"]
            self._status["current_step"] = current_step
            self._record_transition(current_step, "STARTED", step_type)

            if step_type == "activity":
                current_step = await self._run_activity(current_step, step, context)

            elif step_type == "decision":
                current_step = self._run_decision(current_step, step, context)

            elif step_type == "approval":
                current_step = await self._run_approval(current_step, step, context)
                if current_step is None and self._status["state"] in {"REJECTED", "TIMED_OUT"}:
                    return self._final_result(context)

            elif step_type == "timer":
                seconds = int(step.get("seconds", 1))
                await workflow.sleep(timedelta(seconds=seconds))
                self._record_transition(current_step, "COMPLETED", f"slept {seconds}s")
                current_step = step.get("next")

            elif step_type == "end":
                outcome = step.get("outcome", "COMPLETED")
                self._status["state"] = outcome
                self._record_transition(current_step, outcome, "end")
                current_step = None

            else:
                raise ValueError(f"Unsupported step type: {step_type}")

        if self._status["state"] == "RUNNING":
            self._status["state"] = "COMPLETED"
        self._status["current_step"] = None
        return self._final_result(context)

    async def _run_activity(
        self,
        step_id: str,
        step: dict[str, Any],
        context: dict[str, Any],
    ) -> str | None:
        retry_cfg = step.get("retry", {})
        retry_policy = RetryPolicy(
            maximum_attempts=int(retry_cfg.get("maximum_attempts", 3)),
            initial_interval=timedelta(seconds=float(retry_cfg.get("initial_interval_seconds", 1))),
            maximum_interval=timedelta(seconds=float(retry_cfg.get("maximum_interval_seconds", 10))),
            backoff_coefficient=float(retry_cfg.get("backoff_coefficient", 2.0)),
        )

        payload = {
            "capability": step["capability"],
            "step_id": step_id,
            "input": step.get("input", {}),
            "context": context,
        }

        result = await workflow.execute_activity(
            "execute_capability",
            payload,
            start_to_close_timeout=timedelta(seconds=int(step.get("timeout_seconds", 30))),
            retry_policy=retry_policy,
        )

        context["results"][step_id] = result
        self._status["results"] = context["results"]
        self._record_transition(step_id, "COMPLETED", step["capability"])
        return step.get("next")

    def _run_decision(
        self,
        step_id: str,
        step: dict[str, Any],
        context: dict[str, Any],
    ) -> str | None:
        left = self._get_path(context, step["field"])
        operator = step.get("operator", "==")
        right = step.get("value")
        decision = self._compare(left, operator, right)
        target = step.get("on_true") if decision else step.get("on_false")
        self._record_transition(
            step_id,
            "ROUTED",
            f"{step['field']} {operator} {right} -> {decision}",
        )
        return target

    async def _run_approval(
        self,
        step_id: str,
        step: dict[str, Any],
        context: dict[str, Any],
    ) -> str | None:
        self._approval = None
        self._status["state"] = "WAITING_FOR_APPROVAL"
        self._status["approval"] = {
            "step_id": step_id,
            "assignee_group": step.get("assignee_group", "approvers"),
        }

        timeout_seconds = int(step.get("timeout_seconds", 300))
        try:
            await workflow.wait_condition(
                lambda: self._approval is not None,
                timeout=timedelta(seconds=timeout_seconds),
            )
        except asyncio.TimeoutError:
            self._status["state"] = "TIMED_OUT"
            self._record_transition(step_id, "TIMED_OUT", f"after {timeout_seconds}s")
            return step.get("on_timeout")

        assert self._approval is not None
        context["results"][step_id] = self._approval
        self._status["results"] = context["results"]

        if self._approval["approved"]:
            self._status["state"] = "RUNNING"
            self._record_transition(step_id, "APPROVED", self._approval.get("approver", ""))
            return step.get("on_approved")

        self._status["state"] = "REJECTED"
        self._record_transition(step_id, "REJECTED", self._approval.get("approver", ""))
        return step.get("on_rejected")

    def _record_transition(self, step: str, state: str, detail: str) -> None:
        self._status["transitions"].append(
            {
                "step": step,
                "state": state,
                "detail": detail,
                "workflow_time": workflow.now().isoformat(),
            }
        )

    def _final_result(self, context: dict[str, Any]) -> dict[str, Any]:
        return {
            "state": self._status["state"],
            "process_id": self._status.get("process_id"),
            "results": context.get("results", {}),
            "transitions": self._status.get("transitions", []),
        }

    @staticmethod
    def _validate_spec(spec: dict[str, Any]) -> None:
        for required in ("start_at", "steps"):
            if required not in spec:
                raise ValueError(f"Workflow spec is missing required field: {required}")
        if spec["start_at"] not in spec["steps"]:
            raise ValueError("start_at must reference a step in steps")

    @staticmethod
    def _get_path(data: dict[str, Any], path: str) -> Any:
        value: Any = data
        for part in path.split("."):
            if not isinstance(value, dict) or part not in value:
                return None
            value = value[part]
        return value

    @staticmethod
    def _compare(left: Any, operator: str, right: Any) -> bool:
        operations = {
            "==": lambda a, b: a == b,
            "!=": lambda a, b: a != b,
            ">": lambda a, b: a > b,
            ">=": lambda a, b: a >= b,
            "<": lambda a, b: a < b,
            "<=": lambda a, b: a <= b,
            "in": lambda a, b: a in b,
        }
        if operator not in operations:
            raise ValueError(f"Unsupported decision operator: {operator}")
        return bool(operations[operator](left, right))
