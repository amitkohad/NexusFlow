from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any


_TEMPLATE = json.loads(
    (Path(__file__).resolve().parents[1] / "examples" / "customer_adjustment.json").read_text(encoding="utf-8")
)


def customer_adjustment(request: dict[str, Any], requested_by: str, process_id: str) -> dict[str, Any]:
    spec = deepcopy(_TEMPLATE)
    spec["process_id"] = process_id
    spec["request"] = request
    spec["variables"] = {"requested_by": requested_by}
    return spec
