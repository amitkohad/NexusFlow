"""Run only orchestration on its owned queue; capability workers are separate."""

import asyncio

from nexusflow_common.worker import run_worker

from workflows.common.catalog import RUNTIME_TASK_QUEUE

from .workflows import GovernedWorkflowV1


def main() -> None:
    asyncio.run(run_worker("workflow-runtime", RUNTIME_TASK_QUEUE, workflows=[GovernedWorkflowV1]))


if __name__ == "__main__":
    main()
