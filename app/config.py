from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    temporal_address: str = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
    temporal_namespace: str = os.getenv("TEMPORAL_NAMESPACE", "default")
    temporal_task_queue: str = os.getenv("TEMPORAL_TASK_QUEUE", "lightweight-workflows")
    temporal_tls_ca: str | None = os.getenv("TEMPORAL_TLS_CA")
    temporal_tls_cert: str | None = os.getenv("TEMPORAL_TLS_CERT")
    temporal_tls_key: str | None = os.getenv("TEMPORAL_TLS_KEY")
    temporal_tls_server_name: str | None = os.getenv("TEMPORAL_TLS_SERVER_NAME")
    azure_tenant_id: str | None = os.getenv("AZURE_TENANT_ID")
    azure_client_id: str | None = os.getenv("AZURE_CLIENT_ID")
    azure_federated_token_file: str | None = os.getenv("AZURE_FEDERATED_TOKEN_FILE")
    temporal_api_scope: str | None = os.getenv("TEMPORAL_API_SCOPE")
    entra_web_client_id: str | None = os.getenv("ENTRA_WEB_CLIENT_ID")
    entra_web_client_secret: str | None = os.getenv("ENTRA_WEB_CLIENT_SECRET")
    entra_issuer: str | None = os.getenv("ENTRA_ISSUER")
    session_secret: str = os.getenv("SESSION_SECRET", "local-development-only")
    public_base_url: str = os.getenv("PUBLIC_BASE_URL", "http://localhost:8000")


settings = Settings()
