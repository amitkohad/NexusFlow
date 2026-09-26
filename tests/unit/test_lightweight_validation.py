from typing import Any

import pytest

from app.workflows import LightweightProcess


@pytest.mark.parametrize(
    ("spec", "message"),
    [
        ({}, "Workflow spec is missing required field: start_at"),
        ({"start_at": "validate"}, "Workflow spec is missing required field: steps"),
        (
            {"start_at": "validate", "steps": {}},
            "start_at must reference a step in steps",
        ),
    ],
)
def test_invalid_spec_is_rejected_by_existing_validator(spec: dict[str, Any], message: str) -> None:
    # Calling the validator directly avoids the prototype's endless workflow-task
    # retry on ordinary ValueError. The application failure policy is unchanged.
    with pytest.raises(ValueError, match=f"^{message}$"):
        LightweightProcess._validate_spec(spec)
