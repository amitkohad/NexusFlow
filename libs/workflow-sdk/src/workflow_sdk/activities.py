"""Shared typed contract publication for the versioned capability catalog."""

from contracts import ActivityContract, ActivityRequest, ActivityResponse

from workflows.common.catalog import resolve_capability


def contract_for(capability: str) -> ActivityContract:
    route = resolve_capability(capability)
    return ActivityContract(
        capability=route.capability,
        action=route.capability,
        contract_version=route.contract_version,
        task_queue=route.task_queue,
        input_schema=ActivityRequest.model_json_schema(),
        output_schema=ActivityResponse.model_json_schema(),
        worker_compatibility=("1.0",),
    )
