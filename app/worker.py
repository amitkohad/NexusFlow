from __future__ import annotations

import asyncio
from temporalio.worker import Worker

from app.activities import execute_capability
from app.config import settings
from app.temporal_client import connect_temporal
from app.workflows import LightweightProcess


async def main() -> None:
    while True:
        client = await connect_temporal()
        worker = Worker(
            client,
            task_queue=settings.temporal_task_queue,
            workflows=[LightweightProcess],
            activities=[execute_capability],
        )
        print(
            f"Worker connected to {settings.temporal_address}; "
            f"polling task queue '{settings.temporal_task_queue}'"
        )
        run = asyncio.create_task(worker.run())
        try:
            await asyncio.wait_for(asyncio.shield(run), timeout=45 * 60)
        except TimeoutError:
            await worker.shutdown()
            await run


if __name__ == "__main__":
    asyncio.run(main())
