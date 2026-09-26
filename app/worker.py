from __future__ import annotations

import asyncio
import os

from temporalio.client import Client
from temporalio.worker import Worker

from app.activities import execute_capability
from app.workflows import LightweightProcess

TASK_QUEUE = os.getenv("TEMPORAL_TASK_QUEUE", "lightweight-workflows")
TEMPORAL_ADDRESS = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")


async def main() -> None:
    client = await Client.connect(TEMPORAL_ADDRESS)
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[LightweightProcess],
        activities=[execute_capability],
    )
    print(f"Worker connected to {TEMPORAL_ADDRESS}; polling task queue '{TASK_QUEUE}'")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
