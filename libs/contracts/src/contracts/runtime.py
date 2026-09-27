"""Version 1 runtime and Activity envelopes, independent of Temporal SDK types."""

from __future__ import annotations

from typing import Literal

from pydantic import Field

from .base import ContractModel, Identifier, JsonObject, NonEmptyString
from .definitions import DefinitionDocument
from .domain import BusinessContext

RuntimeProfile = Literal["legacy", "governed"]


class RuntimeContext(BusinessContext):
    workflow_type: NonEmptyString
    definition_id: NonEmptyString
    definition_version: NonEmptyString
    business_reference: NonEmptyString
    correlation_id: NonEmptyString
    actor: NonEmptyString


class RuntimeStartRequest(ContractModel):
    runtime_version: Literal["1.0"] = "1.0"
    context: RuntimeContext
    definition_document: DefinitionDocument
    request: JsonObject = Field(default_factory=dict)
    variables: JsonObject = Field(default_factory=dict)


class ActivityRequest(ContractModel):
    workflow_id: NonEmptyString
    run_id: NonEmptyString
    step_id: Identifier
    capability: Identifier
    contract_version: Literal["1.0"] = "1.0"
    context: RuntimeContext
    idempotency_key: NonEmptyString
    input: JsonObject = Field(default_factory=dict)
    request: JsonObject = Field(default_factory=dict)
    variables: JsonObject = Field(default_factory=dict)
    results: JsonObject = Field(default_factory=dict)


class ActivityResponse(ContractModel):
    capability: Identifier
    contract_version: Literal["1.0"] = "1.0"
    output: JsonObject
