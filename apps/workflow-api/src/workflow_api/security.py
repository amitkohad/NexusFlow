"""Verified identity boundary, with an explicit local/test token adapter.

An enterprise provider implements Authenticator in Phase 8. Caller headers and
request bodies never establish identity, permissions, or tenant scope.
"""

from __future__ import annotations

import hmac
from dataclasses import dataclass
from typing import Protocol

from .repository import Scope


@dataclass(frozen=True)
class Principal:
    subject: str
    tenant: str
    business_domain: str
    application: str
    permissions: frozenset[str]

    @property
    def scope(self) -> Scope:
        return Scope(self.tenant, self.business_domain, self.application)


class Authenticator(Protocol):
    async def authenticate(self, token: str) -> Principal | None: ...


class DenyAuthenticator:
    async def authenticate(self, token: str) -> Principal | None:
        return None


class StaticTokenAuthenticator:
    """Explicit development adapter; never used automatically in production."""

    def __init__(self, principals: dict[str, Principal]) -> None:
        if any(not token or not principal.subject for token, principal in principals.items()):
            raise ValueError("Tokens and authenticated subjects must be nonempty")
        self._principals = dict(principals)

    async def authenticate(self, token: str) -> Principal | None:
        for configured, principal in self._principals.items():
            if hmac.compare_digest(token.encode(), configured.encode()):
                return principal
        return None
