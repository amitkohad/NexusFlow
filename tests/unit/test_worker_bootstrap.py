"""Worker startup, owned queues, probes, and orderly lifecycle boundaries."""

from __future__ import annotations

import asyncio
import importlib
import json
import signal
from collections.abc import Callable
from datetime import timedelta
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from nexusflow_common import worker as bootstrap
from nexusflow_common.errors import ConfigurationError
from temporalio.contrib.pydantic import pydantic_data_converter


@pytest.mark.parametrize(
    ("service", "queue", "port"),
    [
        ("workflow-runtime", "workflow-orchestration-tq", 8080),
        ("validation-worker", "validation-tq", 8081),
        ("notification-worker", "notification-tq", 8082),
        ("integration-worker", "integration-tq", 8083),
        ("human-task-worker", "human-task-tq", 8084),
        ("sample-business-worker", "sample-business-tq", 8085),
    ],
)
def test_service_defaults_select_owned_queue_and_distinct_local_probe(
    service: str, queue: str, port: int
) -> None:
    settings = bootstrap.load_worker_settings(service, queue, {})
    assert settings.worker.temporal_task_queue == queue
    assert settings.probe_host == "127.0.0.1"
    assert settings.probe_port == port


def test_environment_cannot_redirect_worker_to_another_queue() -> None:
    with pytest.raises(ConfigurationError, match="owned task queue"):
        bootstrap.load_worker_settings(
            "validation-worker", "validation-tq", {"TEMPORAL_TASK_QUEUE": "integration-tq"}
        )


def test_production_requires_tls_and_accepts_explicit_tls() -> None:
    with pytest.raises(ConfigurationError):
        bootstrap.load_worker_settings(
            "validation-worker", "validation-tq", {"NEXUSFLOW_ENVIRONMENT": "prod"}
        )
    settings = bootstrap.load_worker_settings(
        "validation-worker",
        "validation-tq",
        {"NEXUSFLOW_ENVIRONMENT": "prod", "TEMPORAL_TLS": "true"},
    )
    assert settings.worker.temporal_tls


@pytest.mark.parametrize("host", ["127.0.0.1", "0.0.0.0", "::1", "::"])
def test_probe_host_accepts_only_explicit_supported_addresses(host: str) -> None:
    settings = bootstrap.load_worker_settings(
        "validation-worker", "validation-tq", {"NEXUSFLOW_PROBE_HOST": host}
    )
    assert settings.probe_host == host


@pytest.mark.parametrize("host", ["localhost", "example.com", "127.0.0.1:80", "", " 127.0.0.1"])
def test_probe_host_rejects_dns_and_ambiguous_bind_addresses(host: str) -> None:
    with pytest.raises(ConfigurationError):
        bootstrap.load_worker_settings(
            "validation-worker", "validation-tq", {"NEXUSFLOW_PROBE_HOST": host}
        )


@pytest.mark.parametrize(
    "port", ["0", "65536", "bad", "1.5", "+8081", "8_081", " 8081", "8081 ", ""]
)
def test_probe_port_requires_bounded_ascii_decimal_integer(port: str) -> None:
    with pytest.raises(ConfigurationError):
        bootstrap.load_worker_settings(
            "validation-worker", "validation-tq", {"NEXUSFLOW_PROBE_PORT": port}
        )


def test_importing_bootstrap_does_not_connect_or_bind(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("Import must not start network I/O")

    monkeypatch.setattr(bootstrap.Client, "connect", forbidden)
    monkeypatch.setattr(asyncio, "start_server", forbidden)
    importlib.reload(bootstrap)


async def test_real_probe_http_health_readiness_and_unknown_routes() -> None:
    available = False

    async def readiness() -> bool:
        return available

    probes = bootstrap.ProbeServer("127.0.0.1", 0, readiness)
    await probes.start()
    assert probes.server is not None
    port = probes.server.sockets[0].getsockname()[1]
    try:
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{port}", trust_env=False
        ) as client:
            health = await client.get("/health")
            assert health.status_code == 200
            assert health.json() == {"status": "ok"}
            unavailable = await client.get("/ready")
            assert unavailable.status_code == 503
            assert unavailable.json() == {"status": "unavailable"}
            available = True
            ready = await client.get("/ready")
            assert ready.status_code == 200
            assert ready.json() == {"status": "ready"}
            assert (await client.get("/other")).status_code == 404
    finally:
        await probes.close()
    assert not probes.server.is_serving()


async def test_probe_rejects_excessive_headers_and_remains_available() -> None:
    async def readiness() -> bool:
        return True

    probes = bootstrap.ProbeServer("127.0.0.1", 0, readiness)
    await probes.start()
    assert probes.server is not None
    port = probes.server.sockets[0].getsockname()[1]
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET /health HTTP/1.1\r\nX-Large: " + b"x" * 5000 + b"\r\n\r\n")
        await writer.drain()
        assert await asyncio.wait_for(reader.read(), timeout=3) == b""
        writer.close()
        await writer.wait_closed()
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{port}", trust_env=False
        ) as client:
            assert (await client.get("/health")).status_code == 200
    finally:
        await probes.close()


class LifecycleHarness:
    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.connect_calls: list[dict[str, Any]] = []
        self.worker_options: dict[str, Any] = {}
        self.probe: Any = None
        self.entered = asyncio.Event()
        self.closed = False
        self.runtime_healthy = True
        self.connect_error = False
        self.close_error = False
        self.running = False
        self.ready_during_shutdown: bool | None = None
        self.signals: dict[signal.Signals, Any] = {
            signal.SIGINT: object(),
            signal.SIGTERM: object(),
        }
        self.original_signals = dict(self.signals)
        harness = self

        def set_signal(name: signal.Signals, handler: Any) -> Any:
            previous = self.signals[name]
            self.signals[name] = handler
            return previous

        async def check_health(**kwargs: Any) -> bool:
            if not self.runtime_healthy:
                raise RuntimeError("token=private-secret")
            return True

        client = SimpleNamespace(service_client=SimpleNamespace(check_health=check_health))

        async def connect(address: str, **kwargs: Any) -> Any:
            self.connect_calls.append({"address": address, **kwargs})
            if self.connect_error:
                raise RuntimeError("token=private-secret")
            return client

        class StubWorker:
            def __init__(self, actual_client: Any, **kwargs: Any) -> None:
                assert actual_client is client
                harness.worker_options = kwargs

            @property
            def is_running(self) -> bool:
                return harness.running

            async def __aenter__(self) -> StubWorker:
                harness.running = True
                harness.entered.set()
                return self

            async def __aexit__(self, *args: Any) -> None:
                harness.ready_during_shutdown = await harness.probe.ready()
                harness.running = False

        class StubProbe:
            def __init__(self, host: str, port: int, ready: Callable[[], Any]) -> None:
                self.ready = ready
                harness.probe = self

            async def start(self) -> None:
                assert await self.ready() is False

            async def close(self) -> None:
                harness.closed = True
                assert await self.ready() is False
                if harness.close_error:
                    raise RuntimeError("probe cleanup failed")

        monkeypatch.setattr(bootstrap, "Client", SimpleNamespace(connect=connect))
        monkeypatch.setattr(bootstrap, "Worker", StubWorker)
        monkeypatch.setattr(bootstrap, "ProbeServer", StubProbe)
        monkeypatch.setattr(bootstrap.signal, "signal", set_signal)


async def test_run_worker_uses_typed_converter_identity_grace_and_owned_registrations(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    harness = LifecycleHarness(monkeypatch)
    stop = asyncio.Event()
    settings = bootstrap.load_worker_settings(
        "validation-worker",
        "validation-tq",
        {"TEMPORAL_NAMESPACE": "business", "NEXUSFLOW_SHUTDOWN_GRACE_SECONDS": "7"},
    )

    async def registered_activity() -> None:
        pass

    registrations = [registered_activity]
    with caplog.at_level("INFO", logger="nexusflow.workers"):
        running = asyncio.create_task(
            bootstrap.run_worker(
                "validation-worker",
                "validation-tq",
                activities=registrations,
                shutdown_event=stop,
                settings=settings,
            )
        )
        await asyncio.wait_for(harness.entered.wait(), timeout=2)
        assert await harness.probe.ready() is True
        harness.runtime_healthy = False
        assert await harness.probe.ready() is False
        harness.runtime_healthy = True
        stop.set()
        await asyncio.wait_for(running, timeout=2)
    call = harness.connect_calls[0]
    assert call["identity"] == "validation-worker:1.0"
    assert call["namespace"] == "business"
    assert call["data_converter"] is pydantic_data_converter
    assert harness.worker_options["activities"] is registrations
    assert harness.worker_options["workflows"] == ()
    assert harness.worker_options["task_queue"] == "validation-tq"
    assert harness.worker_options["graceful_shutdown_timeout"] == timedelta(seconds=7)
    assert harness.worker_options["disable_eager_activity_execution"] is True
    assert harness.ready_during_shutdown is False
    assert harness.closed
    assert harness.signals == harness.original_signals
    assert "private-secret" not in caplog.text
    assert {json.loads(record.message)["event"] for record in caplog.records} == {
        "worker_ready",
        "worker_stopping",
    }


async def test_workflow_runtime_registration_contains_workflows_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = LifecycleHarness(monkeypatch)
    stop = asyncio.Event()
    stop.set()
    settings = bootstrap.load_worker_settings("workflow-runtime", "workflow-orchestration-tq", {})
    workflow_types = [type("ExampleWorkflow", (), {})]
    await bootstrap.run_worker(
        "workflow-runtime",
        "workflow-orchestration-tq",
        workflows=workflow_types,
        shutdown_event=stop,
        settings=settings,
    )
    assert harness.worker_options["workflows"] is workflow_types
    assert harness.worker_options["activities"] == ()


async def test_configuration_identity_mismatch_fails_before_io(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = LifecycleHarness(monkeypatch)
    settings = bootstrap.load_worker_settings("notification-worker", "notification-tq", {})
    with pytest.raises(ConfigurationError):
        await bootstrap.run_worker("validation-worker", "validation-tq", settings=settings)
    assert harness.connect_calls == []
    assert harness.probe is None


async def test_connection_failure_is_sanitized_and_restores_signals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = LifecycleHarness(monkeypatch)
    harness.connect_error = True
    settings = bootstrap.load_worker_settings("validation-worker", "validation-tq", {})
    with pytest.raises(RuntimeError, match="Worker runtime connection failed") as raised:
        await bootstrap.run_worker("validation-worker", "validation-tq", settings=settings)
    assert "private-secret" not in str(raised.value)
    assert harness.closed
    assert harness.signals == harness.original_signals


async def test_probe_cleanup_failure_still_restores_signal_handlers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = LifecycleHarness(monkeypatch)
    harness.close_error = True
    stop = asyncio.Event()
    stop.set()
    settings = bootstrap.load_worker_settings("validation-worker", "validation-tq", {})
    with pytest.raises(RuntimeError, match="probe cleanup failed"):
        await bootstrap.run_worker(
            "validation-worker", "validation-tq", shutdown_event=stop, settings=settings
        )
    assert harness.signals == harness.original_signals
