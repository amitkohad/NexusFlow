"""Package transport validation shared by explicit named handlers."""

from contracts import PackageActivityRequest
from temporalio.exceptions import ApplicationError


def validate_envelope(payload: PackageActivityRequest, capability: str) -> None:
    binding = payload.release_binding
    activities = {item.capability: item for item in binding.activity_bindings}
    declared = activities.get(capability)
    if (
        payload.capability != capability
        or payload.contract_version != "1.0"
        or declared is None
        or declared.activity_name != f"{capability}.pkg.v1"
        or declared.contract_version != payload.contract_version
        or binding.definition_id != payload.context.definition_id
        or binding.definition_version != payload.context.definition_version
    ):
        raise ApplicationError(
            "Activity envelope does not match the pinned package contract",
            type="ValidationError",
            non_retryable=True,
        )
