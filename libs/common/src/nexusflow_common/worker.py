"""Owned-queue worker lifecycle, probes, and bounded graceful shutdown."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import signal
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Worker

from .config import WorkerSettings, load_settings
from .errors import ConfigurationError

PROBE_PORTS = {
    "workflow-runtime": 8080,
    "validation-worker": 8081,
    "notification-worker": 8082,
    "integration-worker": 8083,
    "human-task-worker": 8084,
    "sample-business-worker": 8085,
}


@dataclass(frozen=True)
class WorkerServiceSettings:
    service: str
    worker: WorkerSettings
    probe_host: str
    probe_port: int


def load_worker_settings(
    service: str, owned_queue: str, environment: Mapping[str, str] | None = None
) -> WorkerServiceSettings:
    values = dict(os.environ if environment is None else environment)
    values.setdefault("TEMPORAL_TASK_QUEUE", owned_queue)
    worker = load_settings(values)
    if worker.temporal_task_queue != owned_queue:
        raise ConfigurationError("Worker may poll only its owned task queue")
    host = values.get("NEXUSFLOW_PROBE_HOST", "127.0.0.1")
    if host not in {"127.0.0.1", "0.0.0.0", "::1", "::"}:
        raise ConfigurationError("Probe host must be an explicit loopback or bind-all address")
    try:
        raw_port = values.get("NEXUSFLOW_PROBE_PORT", str(PROBE_PORTS[service]))
        if not re.fullmatch(r"[0-9]+", raw_port):
            raise ValueError()
        port = int(raw_port)
        if not 1 <= port <= 65535:
            raise ValueError()
    except (ValueError, KeyError):
        raise ConfigurationError("Probe port must be an integer from 1 to 65535") from None
    return WorkerServiceSettings(service, worker, host, port)


class ProbeServer:
    def __init__(self, host: str, port: int, ready: Callable[[], Any]) -> None:
        self.host, self.port, self.ready = host, port, ready
        self.server: asyncio.Server | None = None

    async def start(self) -> None:
        self.server = await asyncio.start_server(self._request, self.host, self.port, limit=4096)

    async def close(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()

    async def _request(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            header = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=2)
            request = header.split(b"\r\n", 1)[0]
            if request == b"GET /health HTTP/1.1" or request == b"GET /health HTTP/1.0":
                status, state = 200, "ok"
            elif request == b"GET /ready HTTP/1.1" or request == b"GET /ready HTTP/1.0":
                available = await self.ready()
                status, state = (200, "ready") if available else (503, "unavailable")
            else:
                status, state = 404, "not_found"
            payload = json.dumps({"status": state}).encode()
            writer.write(
                f"HTTP/1.1 {status} Response\r\nContent-Type: application/json\r\nContent-Length: {len(payload)}\r\nConnection: close\r\n\r\n".encode()
                + payload
            )
            await writer.drain()
        except (
            asyncio.TimeoutError,
            asyncio.IncompleteReadError,
            asyncio.LimitOverrunError,
            ConnectionError,
        ):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass


async def run_worker(
    service: str,
    owned_queue: str,
    *,
    activities: Sequence[Callable[..., Any]] = (),
    workflows: Sequence[type] = (),
    shutdown_event: asyncio.Event | None = None,
    settings: WorkerServiceSettings | None = None,
) -> None:
    configuration = settings or load_worker_settings(service, owned_queue)
    if configuration.service != service or configuration.worker.temporal_task_queue != owned_queue:
        raise ConfigurationError("Worker configuration does not match its owned service and queue")
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    worker_settings = configuration.worker
    stop = shutdown_event if shutdown_event is not None else asyncio.Event()
    loop = asyncio.get_running_loop()
    previous_signals: dict[signal.Signals, Any] = {}
    if threading.current_thread() is threading.main_thread():
        for name in (signal.SIGINT, signal.SIGTERM):
            previous_signals[name] = signal.signal(
                name, lambda signum, frame: loop.call_soon_threadsafe(stop.set)
            )
    logger = logging.getLogger("nexusflow.workers")
    ready = False
    client: Client | None = None
    worker: Worker | None = None

    async def is_ready() -> bool:
        if not ready or client is None or worker is None or not worker.is_running:
            return False
        try:
            return await client.service_client.check_health(timeout=timedelta(seconds=2))
        except Exception:
            return False

    probes = ProbeServer(configuration.probe_host, configuration.probe_port, is_ready)
    try:
        await probes.start()
        try:
            client = await Client.connect(
                worker_settings.temporal_address,
                namespace=worker_settings.temporal_namespace,
                tls=worker_settings.temporal_tls,
                data_converter=pydantic_data_converter,
                identity=f"{service}:1.0",
            )
        except Exception:
            raise RuntimeError("Worker runtime connection failed") from None
        worker = Worker(
            client,
            task_queue=owned_queue,
            activities=activities,
            workflows=workflows,
            graceful_shutdown_timeout=timedelta(seconds=worker_settings.shutdown_grace_seconds),
            disable_eager_activity_execution=True,
        )
        async with worker:
            ready = True
            logger.info(
                json.dumps(
                    {
                        "event": "worker_ready",
                        "worker": service,
                        "task_queue": owned_queue,
                        "contract_version": "1.0",
                        "probe_port": configuration.probe_port,
                    }
                )
            )
            await stop.wait()
            ready = False
            logger.info(
                json.dumps(
                    {"event": "worker_stopping", "worker": service, "task_queue": owned_queue}
                )
            )
    finally:
        ready = False
        try:
            await probes.close()
        finally:
            for name, previous in previous_signals.items():
                signal.signal(name, previous)
