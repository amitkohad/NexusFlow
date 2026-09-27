from __future__ import annotations

import asyncio

import pytest
from contracts import ErrorCategory
from nexusflow_common.config import WorkerSettings, load_settings
from nexusflow_common.errors import (
    AuthorizationError,
    BusinessError,
    ConfigurationError,
    TechnicalError,
    ValidationError,
    classify_error,
)
from pydantic import ValidationError as ModelValidationError

from workflows.common import SemanticsError


def test_settings_read_explicit_environment_without_import_side_effects() -> None:
    settings = load_settings(
        {
            "TEMPORAL_ADDRESS": "temporal.internal:7233",
            "TEMPORAL_NAMESPACE": "finance",
            "TEMPORAL_TASK_QUEUE": "validation-tq",
            "NEXUSFLOW_MAX_DEFINITION_STEPS": "100",
            "UNRELATED_SECRET": "must-never-be-in-the-model",
        }
    )
    assert settings.temporal_address == "temporal.internal:7233"
    assert settings.temporal_namespace == "finance"
    assert settings.temporal_task_queue == "validation-tq"
    assert settings.max_definition_steps == 100
    assert "must-never-be-in-the-model" not in settings.model_dump_json()
    assert load_settings({}).temporal_task_queue == "workflow-orchestration-tq"


@pytest.mark.parametrize(
    "environment",
    [
        {"TEMPORAL_ADDRESS": "https://host:7233"},
        {"TEMPORAL_ADDRESS": "user:secret@host:7233"},
        {"TEMPORAL_ADDRESS": "host:99999"},
        {"TEMPORAL_ADDRESS": "bad host:7233"},
        {"TEMPORAL_ADDRESS": "host\tname:7233"},
        {"TEMPORAL_ADDRESS": "host\nname:7233"},
        {"TEMPORAL_ADDRESS": "host\x00name:7233"},
        {"TEMPORAL_TLS": "sometimes"},
        {"NEXUSFLOW_MAX_DEFINITION_STEPS": "1.5"},
        {"NEXUSFLOW_MAX_DEFINITION_STEPS": "0"},
        {"TEMPORAL_NAMESPACE": ""},
        {"NEXUSFLOW_ENVIRONMENT": "production-typo"},
        {"NEXUSFLOW_MAX_DEFINITION_STEPS": "1" * 5000},
    ],
)
def test_invalid_environment_is_actionable_without_echoing_values(
    environment: dict[str, str],
) -> None:
    with pytest.raises(ConfigurationError) as caught:
        load_settings(environment)
    detail = classify_error(caught.value)
    assert detail.category == ErrorCategory.CONFIGURATION
    assert not detail.retryable
    assert detail.issues
    assert "secret@" not in str(caught.value)
    assert "sometimes" not in detail.model_dump_json()


def test_production_connection_requires_tls_and_accepts_ipv6() -> None:
    with pytest.raises(ConfigurationError):
        load_settings({"NEXUSFLOW_ENVIRONMENT": "prod"})
    settings = load_settings(
        {"NEXUSFLOW_ENVIRONMENT": "prod", "TEMPORAL_TLS": "true", "TEMPORAL_ADDRESS": "[::1]:7233"}
    )
    assert settings.temporal_tls is True
    # TLS validation is only configuration; no connection or credential provider runs here.


def test_direct_settings_reject_boolean_numeric_limits() -> None:
    with pytest.raises(ModelValidationError):
        WorkerSettings.model_validate({"max_definition_steps": True})


@pytest.mark.parametrize(
    ("error", "category", "retryable"),
    [
        (ValidationError("Invalid definition"), ErrorCategory.VALIDATION, False),
        (BusinessError("Amount must be positive"), ErrorCategory.BUSINESS, False),
        (AuthorizationError("Operation denied"), ErrorCategory.AUTHORIZATION, False),
        (TechnicalError("Service temporarily unavailable"), ErrorCategory.TECHNICAL, True),
        (
            TechnicalError("Permanent adapter failure", retryable=False),
            ErrorCategory.TECHNICAL,
            False,
        ),
        (TimeoutError("secret upstream address"), ErrorCategory.TIMEOUT, True),
        (ConnectionError("secret connection string"), ErrorCategory.TECHNICAL, True),
        (PermissionError("secret file path"), ErrorCategory.AUTHORIZATION, False),
        (asyncio.CancelledError(), ErrorCategory.CANCELLATION, False),
        (ValueError("password=secret"), ErrorCategory.TECHNICAL, False),
    ],
)
def test_error_classification_requires_explicit_business_and_retry_intent(
    error: BaseException, category: ErrorCategory, retryable: bool
) -> None:
    detail = classify_error(error)
    assert detail.category == category
    assert detail.retryable is retryable
    assert "secret" not in detail.model_dump_json()


def test_pydantic_errors_are_validation_failures_and_do_not_leak_input() -> None:
    try:
        WorkerSettings.model_validate({"max_definition_steps": "password=secret"})
    except ModelValidationError as error:
        detail = classify_error(error)
    else:
        pytest.fail("Expected strict model validation")
    assert detail.category == ErrorCategory.VALIDATION
    assert detail.issues[0].path == "max_definition_steps"
    assert not detail.retryable
    assert "secret" not in detail.model_dump_json()


def test_runtime_expression_errors_share_the_non_retryable_validation_taxonomy() -> None:
    detail = classify_error(
        SemanticsError("Sensitive raw expression", code="missing_path", path="results.risk")
    )
    assert detail.category == ErrorCategory.VALIDATION
    assert detail.issues[0].path == "results.risk"
    assert not detail.retryable
    assert "Sensitive" not in detail.model_dump_json()
