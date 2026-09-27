"""Run the independently deployable sample-business-worker."""

from __future__ import annotations

import asyncio

from nexusflow_common.worker import run_worker

from . import ACTIVITIES, SERVICE, TASK_QUEUE


def main() -> None:
    asyncio.run(run_worker(SERVICE, TASK_QUEUE, activities=list(ACTIVITIES)))


if __name__ == "__main__":
    main()
