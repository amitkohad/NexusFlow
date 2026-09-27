"""Explicit failure taxonomy; classification never performs I/O or exposes raw exceptions."""

from __future__ import annotations

import asyncio
import builtins

from contracts import ErrorCategory, ErrorDetail, ValidationIssue
from pydantic import ValidationError as ModelValidationError

from workflows.common import SemanticsError


class PlatformError(Exception):
    def __init__(
        self,
        message: str,
        *,
        code: str,
        category: ErrorCategory,
        retryable: bool = False,
        issues: tuple[ValidationIssue, ...] = (),
    ) -> None:
        self.detail = ErrorDetail(
            code=code, category=category, message=message, retryable=retryable, issues=issues
        )
        super().__init__(message)


class ValidationError(PlatformError):
    def __init__(
        self,
        message: str,
        *,
        code: str = "validation_failed",
        issues: tuple[ValidationIssue, ...] = (),
    ) -> None:
        super().__init__(message, code=code, category=ErrorCategory.VALIDATION, issues=issues)


class BusinessError(PlatformError):
    def __init__(self, message: str, *, code: str = "business_rule_failed") -> None:
        super().__init__(message, code=code, category=ErrorCategory.BUSINESS)


class AuthorizationError(PlatformError):
    def __init__(
        self, message: str = "Operation denied", *, code: str = "authorization_denied"
    ) -> None:
        super().__init__(message, code=code, category=ErrorCategory.AUTHORIZATION)


class TechnicalError(PlatformError):
    def __init__(
        self, message: str, *, code: str = "technical_failure", retryable: bool = True
    ) -> None:
        super().__init__(message, code=code, category=ErrorCategory.TECHNICAL, retryable=retryable)


class TimeoutError(PlatformError):
    def __init__(self, message: str = "Operation timed out", *, retryable: bool = True) -> None:
        super().__init__(
            message, code="operation_timeout", category=ErrorCategory.TIMEOUT, retryable=retryable
        )


class CancelledError(PlatformError):
    def __init__(self, message: str = "Operation cancelled") -> None:
        super().__init__(message, code="operation_cancelled", category=ErrorCategory.CANCELLATION)


class ConfigurationError(PlatformError):
    def __init__(self, message: str, *, issues: tuple[ValidationIssue, ...] = ()) -> None:
        super().__init__(
            message,
            code="configuration_invalid",
            category=ErrorCategory.CONFIGURATION,
            issues=issues,
        )


def model_issues(error: ModelValidationError) -> tuple[ValidationIssue, ...]:
    """Keep locations and stable error types, excluding values and validator context."""
    return tuple(
        ValidationIssue(
            code=item["type"],
            path=".".join(str(part) for part in item["loc"]) or "$",
            message="Value does not satisfy the contract",
        )
        for item in error.errors(include_url=False, include_context=False, include_input=False)
    )


def classify_error(error: BaseException) -> ErrorDetail:
    """Only explicitly classified failures and known I/O failures may be retried."""
    if isinstance(error, PlatformError):
        return error.detail
    if isinstance(error, SemanticsError):
        return ErrorDetail(
            code=error.code,
            category=ErrorCategory.VALIDATION,
            message="Workflow expression validation failed",
            issues=(
                ValidationIssue(
                    code=error.code, path=error.path or "$", message="Invalid workflow expression"
                ),
            ),
        )
    if isinstance(error, ModelValidationError):
        return ErrorDetail(
            code="contract_invalid",
            category=ErrorCategory.VALIDATION,
            message="Contract validation failed",
            issues=model_issues(error),
        )
    if isinstance(error, asyncio.CancelledError):
        return CancelledError().detail
    if isinstance(error, builtins.TimeoutError):
        return TimeoutError().detail
    if isinstance(error, PermissionError):
        return AuthorizationError().detail
    if isinstance(error, ConnectionError):
        return TechnicalError(
            "Connection temporarily unavailable", code="connection_unavailable"
        ).detail
    # A raw ValueError is not proof of a business error; adapters must declare intent.
    return TechnicalError(
        "Unexpected technical failure", code="unexpected_failure", retryable=False
    ).detail
