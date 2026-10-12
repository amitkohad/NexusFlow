from __future__ import annotations

from pathlib import Path

import httpx
from temporalio.client import Client, TLSConfig

from app.config import settings


async def _workload_access_token() -> str | None:
    if not settings.azure_federated_token_file:
        return None
    assertion = Path(settings.azure_federated_token_file).read_text(encoding="utf-8").strip()
    async with httpx.AsyncClient(timeout=15) as client:
        response = await client.post(
            f"https://login.microsoftonline.com/{settings.azure_tenant_id}/oauth2/v2.0/token",
            data={
                "client_id": settings.azure_client_id,
                "scope": settings.temporal_api_scope,
                "grant_type": "client_credentials",
                "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
                "client_assertion": assertion,
            },
        )
        response.raise_for_status()
        return response.json()["access_token"]


async def connect_temporal() -> Client:
    tls = False
    if settings.temporal_tls_ca:
        tls = TLSConfig(
            server_root_ca_cert=Path(settings.temporal_tls_ca).read_bytes(),
            client_cert=Path(settings.temporal_tls_cert).read_bytes() if settings.temporal_tls_cert else None,
            client_private_key=Path(settings.temporal_tls_key).read_bytes() if settings.temporal_tls_key else None,
            domain=settings.temporal_tls_server_name,
        )
    token = await _workload_access_token()
    metadata = {"authorization": f"Bearer {token}"} if token else {}
    return await Client.connect(
        settings.temporal_address,
        namespace=settings.temporal_namespace,
        tls=tls,
        rpc_metadata=metadata,
    )
