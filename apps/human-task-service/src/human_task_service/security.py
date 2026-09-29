"""Verified identity boundary for task actors and service callers."""

from __future__ import annotations

import hmac
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class TaskPrincipal:
    subject: str
    tenant: str
    business_domain: str
    application: str
    permissions: frozenset[str]
    groups: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        if any(
            not item or len(item) > 256
            for item in (self.subject, self.tenant, self.business_domain, self.application)
        ):
            raise ValueError("Identity and scope fields must be bounded and nonempty")


class Authenticator(Protocol):
    async def authenticate(self, token: str) -> TaskPrincipal | None: ...


class DenyAuthenticator:
    async def authenticate(self, token: str) -> TaskPrincipal | None:
        return None


class StaticTokenAuthenticator:
    """Explicit local/test token adapter; deployments inject verified identity."""

    def __init__(self, principals: dict[str, TaskPrincipal]) -> None:
        if any(not token or len(token) < 32 for token in principals):
            raise ValueError("Local task tokens must contain at least 32 characters")
        self._principals = dict(principals)

    async def authenticate(self, token: str) -> TaskPrincipal | None:
        for configured, principal in self._principals.items():
            if hmac.compare_digest(token.encode(), configured.encode()):
                return principal
        return None
