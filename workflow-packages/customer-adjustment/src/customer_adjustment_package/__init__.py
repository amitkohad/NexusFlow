"""Installed workflow package registration marker; resource manifest is authoritative."""

import json
from importlib.resources import files
from typing import Any


def registration() -> dict[str, Any]:
    return json.loads(files(__package__).joinpath("manifest.json").read_text(encoding="utf-8"))
