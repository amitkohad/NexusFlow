"""Sanitized API errors shared by service and persistence boundaries."""

from __future__ import annotations

from contracts import ValidationIssue


class ApiError(Exception):
    def __init__(
        self,
        status_code: int,
        code: str,
        message: str,
        issues: tuple[ValidationIssue, ...] = (),
    ) -> None:
        self.status_code = status_code
        self.code = code
        self.message = message
        self.issues = issues
        super().__init__(message)
