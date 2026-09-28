"""Explicit environment connection and capacity for generic package executors."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from uuid import uuid4

from contracts import ExecutorPool
from pydantic import ValidationError

from .config import WorkerSettings, load_settings
from .errors import ConfigurationError

_INTEGER_ENV = {
    "NEXUSFLOW_WORKFLOW_TASK_SLOTS": "workflow_task_slots",
    "NEXUSFLOW_ACTIVITY_TASK_SLOTS": "activity_task_slots",
    "NEXUSFLOW_WORKFLOW_TASK_POLLERS": "workflow_task_pollers",
    "NEXUSFLOW_ACTIVITY_TASK_POLLERS": "activity_task_pollers",
    "NEXUSFLOW_SHUTDOWN_GRACE_SECONDS": "shutdown_grace_seconds",
}
_RATE_ENV = {
    "NEXUSFLOW_MAX_ACTIVITIES_PER_SECOND": "max_activities_per_second",
    "NEXUSFLOW_MAX_TASK_QUEUE_ACTIVITIES_PER_SECOND": "max_task_queue_activities_per_second",
}


@dataclass(frozen=True)
class ExecutorSettings:
    """One process instance; replica targets are desired capacity, not a launcher."""

    pool: ExecutorPool
    connection: WorkerSettings
    instance_id: str
    probe_host: str
    probe_port: int

    @property
    def identity(self) -> str:
        return f"{self.pool.package_id}:{self.pool.build_id}:{self.pool.pool_id}:{self.instance_id}"


def load_executor_settings(
    pool: ExecutorPool, environment: Mapping[str, str] | None = None
) -> ExecutorSettings:
    values = dict(os.environ if environment is None else environment)
    document = pool.model_dump(mode="json")
    try:
        for variable, field in _INTEGER_ENV.items():
            if variable in values:
                if not re.fullmatch(r"[0-9]+", values[variable]):
                    raise ValueError()
                document[field] = int(values[variable])
        for variable, field in _RATE_ENV.items():
            if variable in values:
                document[field] = float(values[variable])
        effective_pool = ExecutorPool.model_validate(document)
        host = values.get("NEXUSFLOW_PROBE_HOST", "127.0.0.1")
        raw_port = values.get("NEXUSFLOW_PROBE_PORT", "8090")
        if host not in {"127.0.0.1", "0.0.0.0", "::1", "::"}:
            raise ValueError()
        if not re.fullmatch(r"[0-9]+", raw_port) or not 1 <= int(raw_port) <= 65535:
            raise ValueError()
        instance = values.get("NEXUSFLOW_EXECUTOR_INSTANCE_ID", uuid4().hex)
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", instance):
            raise ValueError()
        allowed_queues = set(effective_pool.queue_bindings.values())
        if values.get("TEMPORAL_TASK_QUEUE", next(iter(allowed_queues))) not in allowed_queues:
            raise ValueError()
        if values.get("TEMPORAL_NAMESPACE", pool.temporal_namespace) != pool.temporal_namespace:
            raise ValueError()
        if values.get("NEXUSFLOW_ENVIRONMENT", pool.environment) != pool.environment:
            raise ValueError()
    except (ValueError, ValidationError):
        raise ConfigurationError("Invalid executor connection or capacity configuration") from None
    values.setdefault("TEMPORAL_TASK_QUEUE", next(iter(sorted(allowed_queues))))
    values["TEMPORAL_NAMESPACE"] = effective_pool.temporal_namespace
    values["NEXUSFLOW_ENVIRONMENT"] = effective_pool.environment
    connection = load_settings(values)
    return ExecutorSettings(effective_pool, connection, instance, host, int(raw_port))
