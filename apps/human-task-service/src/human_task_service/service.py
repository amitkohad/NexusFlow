"""Task operations and independently retryable outbox delivery."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime

from .dispatch import (
    TaskChainMismatch,
    TaskSignalDispatcher,
    TaskWorkflowClosed,
    TaskWorkflowMissing,
)
from .repository import TaskRepository

logger = logging.getLogger("human_task_service.dispatch")


class TaskService:
    def __init__(self, repository: TaskRepository, dispatcher: TaskSignalDispatcher) -> None:
        self.repository = repository
        self.dispatcher = dispatcher

    def process_due_tasks(self, now: datetime | None = None, *, limit: int = 100) -> int:
        return self.repository.process_due_tasks(now, limit=limit)

    async def dispatch_pending(self, *, limit: int = 100) -> int:
        leases = await asyncio.to_thread(self.repository.lease_outbox, limit=limit)
        delivered = 0
        for item in leases:
            try:
                await self.dispatcher.signal(
                    item.workflow_id,
                    item.first_execution_run_id,
                    item.signal_name,
                    item.payload,
                )
            except TaskChainMismatch:
                logger.error(
                    "task_signal_chain_mismatch",
                    extra={"event_id": item.event_id, "task_id": item.task_id},
                )
                await asyncio.to_thread(
                    self.repository.mark_blocked,
                    item.event_id,
                    item.lease_token,
                    "workflow_chain_mismatch",
                )
            except TaskWorkflowClosed:
                logger.error(
                    "task_signal_workflow_closed",
                    extra={"event_id": item.event_id, "task_id": item.task_id},
                )
                await asyncio.to_thread(
                    self.repository.mark_blocked,
                    item.event_id,
                    item.lease_token,
                    "workflow_closed",
                )
            except TaskWorkflowMissing:
                logger.error(
                    "task_signal_workflow_missing",
                    extra={"event_id": item.event_id, "task_id": item.task_id},
                )
                await asyncio.to_thread(
                    self.repository.mark_blocked,
                    item.event_id,
                    item.lease_token,
                    "workflow_missing",
                )
            except Exception:
                logger.warning(
                    "task_signal_delivery_retry",
                    extra={"event_id": item.event_id, "task_id": item.task_id},
                )
                await asyncio.to_thread(
                    self.repository.mark_retry, item.event_id, item.lease_token, item.attempts
                )
            else:
                await asyncio.to_thread(
                    self.repository.mark_delivered, item.event_id, item.lease_token
                )
                delivered += 1
        return delivered
