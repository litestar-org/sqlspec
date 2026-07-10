"""Regression tests for persistent native event listener hubs."""

# pyright: reportPrivateUsage=false

import asyncio
import threading
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Any
from weakref import WeakKeyDictionary

import pytest

from sqlspec.adapters.asyncpg.events._hub import AsyncpgListenerHub
from sqlspec.adapters.asyncpg.events.backend import AsyncpgEventsBackend, AsyncpgHybridEventsBackend
from sqlspec.adapters.oracledb.events import _hub as oracle_hub
from sqlspec.adapters.psqlpy.events._hub import PsqlpyListenerHub
from sqlspec.adapters.psqlpy.events.backend import PsqlpyEventsBackend, PsqlpyHybridEventsBackend
from sqlspec.adapters.psycopg.events._hub import PsycopgAsyncListenerHub, PsycopgSyncListenerHub
from sqlspec.adapters.psycopg.events.backend import (
    PsycopgAsyncEventsBackend,
    PsycopgAsyncHybridEventsBackend,
    PsycopgSyncEventsBackend,
    PsycopgSyncHybridEventsBackend,
)
from sqlspec.extensions.events import EventMessage


class _AsyncConnectionContext:
    def __init__(self, connection: Any, tracker: Any | None = None) -> None:
        self.connection = connection
        self.tracker = tracker

    async def __aenter__(self) -> Any:
        if self.tracker is not None:
            self.tracker.connection_enters += 1
        return self.connection

    async def __aexit__(self, *_exc_info: object) -> None:
        if self.tracker is not None:
            self.tracker.connection_exits += 1


class _SyncConnectionContext:
    def __init__(self, connection: Any, tracker: Any) -> None:
        self.connection = connection
        self.tracker = tracker

    def __enter__(self) -> Any:
        self.tracker.connection_enters += 1
        return self.connection

    def __exit__(self, *_exc_info: object) -> None:
        self.tracker.connection_exits += 1


class _AsyncpgConnection:
    def __init__(self) -> None:
        self.callbacks: dict[str, Any] = {}
        self.executed: list[str] = []
        self.closed = False

    def is_closed(self) -> bool:
        return self.closed

    async def execute(self, statement: str) -> None:
        self.executed.append(statement)

    async def add_listener(self, channel: str, callback: Any) -> None:
        self.callbacks[channel] = callback

    async def remove_listener(self, channel: str, _callback: Any) -> None:
        self.callbacks.pop(channel, None)


class _StubRuntime:
    def __init__(self) -> None:
        self.metrics: list[str] = []
        self.registered: list[tuple[str, Any]] = []

    def increment_metric(self, metric: str) -> None:
        self.metrics.append(metric)

    def register_lifecycle_hook(self, event: str, callback: Any) -> None:
        self.registered.append((event, callback))


class _AsyncpgConfig:
    def __init__(self, connection: _AsyncpgConnection, *replacement_connections: _AsyncpgConnection) -> None:
        self.connections = [connection, *replacement_connections]
        self.connection_enters = 0
        self.connection_exits = 0
        self._runtime = _StubRuntime()

    def provide_connection(self) -> _AsyncConnectionContext:
        connection = self.connections[min(self.connection_enters, len(self.connections) - 1)]
        return _AsyncConnectionContext(connection, self)

    def get_observability_runtime(self) -> _StubRuntime:
        return self._runtime


class _PsqlpyListener:
    def __init__(self, emit_on_add: str | None = None, *, drop_ready_payloads: bool = False) -> None:
        self.emit_on_add = emit_on_add
        self.drop_ready_payloads = drop_ready_payloads
        self.callbacks: dict[str, Any] = {}
        self.listen_called = False
        self.listen_count = 0
        self.abort_count = 0
        self.emitted: list[tuple[str, str]] = []
        self.shutdown_count = 0
        self.started = False

    @property
    def is_started(self) -> bool:
        return self.started

    async def startup(self) -> None:
        self.started = True

    async def add_callback(self, *, channel: str, callback: Any) -> None:
        self.callbacks[channel] = callback
        if self.emit_on_add is not None:
            await callback(0, channel, self.emit_on_add)

    def listen(self) -> None:
        self.listen_called = True
        self.listen_count += 1

    async def emit(self, channel: str, payload: str) -> None:
        self.emitted.append((channel, payload))
        callback = self.callbacks.get(channel)
        if callback is not None:
            await callback(None, payload, channel, 0)

    async def clear_channel_callbacks(self, *, channel: str) -> None:
        self.callbacks.pop(channel, None)

    def abort_listen(self) -> None:
        self.listen_called = False
        self.abort_count += 1

    async def shutdown(self) -> None:
        self.shutdown_count += 1
        self.started = False


class _PsqlpyPool:
    def __init__(self, listener_handle: _PsqlpyListener, *replacement_listeners: _PsqlpyListener) -> None:
        self.listener_handles = [listener_handle, *replacement_listeners]
        self.listener_calls = 0

    def listener(self) -> _PsqlpyListener:
        listener = self.listener_handles[min(self.listener_calls, len(self.listener_handles) - 1)]
        self.listener_calls += 1
        return listener


class _PsqlpyConfig:
    def __init__(self, listener_handle: _PsqlpyListener | None = None, *replacement_listeners: _PsqlpyListener) -> None:
        self.listener_handle = listener_handle or _PsqlpyListener()
        self.pool = _PsqlpyPool(self.listener_handle, *replacement_listeners)
        self._runtime = _StubRuntime()

    async def provide_pool(self) -> _PsqlpyPool:
        return self.pool

    def provide_session(self) -> Any:
        return _PsqlpySession(self.listener_handle)

    def get_observability_runtime(self) -> _StubRuntime:
        return self._runtime


class _PsqlpyDriver:
    def __init__(self, listener: _PsqlpyListener) -> None:
        self.listener = listener

    async def execute_script(self, statement: Any) -> None:
        channel, payload = statement.parameters
        if self.listener.listen_called and not self.listener.drop_ready_payloads:
            await self.listener.emit(channel, payload)

    async def commit(self) -> None:
        return None


class _PsqlpySession:
    def __init__(self, listener: _PsqlpyListener) -> None:
        self.driver = _PsqlpyDriver(listener)

    async def __aenter__(self) -> _PsqlpyDriver:
        return self.driver

    async def __aexit__(self, *_exc_info: object) -> None:
        return None


class _PsycopgAsyncConnection:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self.closed = False

    async def execute(self, statement: Any) -> None:
        self.executed.append(statement.as_string())

    async def set_autocommit(self, value: bool) -> None:
        _ = value

    def notifies(self, *, timeout: float, stop_after: int) -> AsyncIterator[Any]:
        _ = stop_after

        async def _empty() -> AsyncIterator[Any]:
            await asyncio.sleep(timeout)
            if False:
                yield object()

        return _empty()


class _PsycopgAsyncConfig:
    def __init__(self, connection: _PsycopgAsyncConnection, *replacement_connections: _PsycopgAsyncConnection) -> None:
        self.connections = [connection, *replacement_connections]
        self.connection_enters = 0
        self.connection_exits = 0
        self._runtime = _StubRuntime()

    def provide_connection(self) -> _AsyncConnectionContext:
        connection = self.connections[min(self.connection_enters, len(self.connections) - 1)]
        return _AsyncConnectionContext(connection, self)

    def get_observability_runtime(self) -> _StubRuntime:
        return self._runtime


class _PsycopgSyncConnection:
    def __init__(self) -> None:
        self.autocommit = False
        self.closed = False
        self.executed: list[str] = []

    def execute(self, statement: Any) -> None:
        self.executed.append(statement.as_string())

    def notifies(self, *, timeout: float, stop_after: int) -> list[Any]:
        _ = stop_after
        threading.Event().wait(timeout)
        return []


class _PsycopgSyncConfig:
    def __init__(self, connection: _PsycopgSyncConnection, *replacement_connections: _PsycopgSyncConnection) -> None:
        self.connections = [connection, *replacement_connections]
        self.connection_enters = 0
        self.connection_exits = 0
        self._runtime = _StubRuntime()

    def provide_connection(self) -> _SyncConnectionContext:
        connection = self.connections[min(self.connection_enters, len(self.connections) - 1)]
        return _SyncConnectionContext(connection, self)

    def get_observability_runtime(self) -> _StubRuntime:
        return self._runtime


class _Runtime:
    def __init__(self) -> None:
        self.metrics: list[tuple[str, float]] = []

    def increment_metric(self, metric: str, amount: float = 1.0) -> None:
        self.metrics.append((metric, amount))

    def record_metric(self, metric: str, value: float) -> None:
        self.metrics.append((metric, value))


class _HybridConfig:
    is_async = True

    def __init__(self) -> None:
        self.runtime = _Runtime()

    def get_observability_runtime(self) -> _Runtime:
        return self.runtime


class _PsycopgHybridConfig(_HybridConfig):
    pass


class _PsqlpyHybridConfig(_HybridConfig):
    pass


class _AsyncpgNativeConfig(_HybridConfig):
    pass


class _PsycopgNativeConfig(_HybridConfig):
    pass


class _PsqlpyNativeConfig(_HybridConfig):
    pass


class _PsycopgSyncNativeConfig(_HybridConfig):
    is_async = False


_PsycopgHybridConfig.__module__ = "sqlspec.adapters.psycopg.config"
_PsqlpyHybridConfig.__module__ = "sqlspec.adapters.psqlpy.config"
_AsyncpgNativeConfig.__module__ = "sqlspec.adapters.asyncpg.config"
_PsycopgNativeConfig.__module__ = "sqlspec.adapters.psycopg.config"
_PsqlpyNativeConfig.__module__ = "sqlspec.adapters.psqlpy.config"
_PsycopgSyncNativeConfig.__module__ = "sqlspec.adapters.psycopg.config"


class _TimeoutHub:
    def __init__(self) -> None:
        self.dequeue_calls = 0

    async def dequeue(self, _channel: str, _poll_interval: float) -> None:
        self.dequeue_calls += 1


class _SyncTimeoutHub:
    def dequeue(self, _channel: str, _poll_interval: float) -> None:
        return None


class _MarkerHub:
    def __init__(self, batch_size: int = 1) -> None:
        self.batch_size = batch_size
        self.dequeue_calls = 0

    async def dequeue(self, _channel: str, _poll_interval: float) -> str:
        self.dequeue_calls += 1
        return f'{{"batch_size":{self.batch_size},"marker_id":"marker-1"}}'


class _SyncMarkerHub:
    def __init__(self, batch_size: int = 1) -> None:
        self.batch_size = batch_size
        self.dequeue_calls = 0

    def dequeue(self, _channel: str, _poll_interval: float) -> str:
        self.dequeue_calls += 1
        return f'{{"batch_size":{self.batch_size},"marker_id":"marker-1"}}'


class _SequenceMarkerHub:
    def __init__(self, payloads: list[str]) -> None:
        self.payloads = iter(payloads)
        self.dequeue_calls = 0

    async def dequeue(self, _channel: str, _poll_interval: float) -> str:
        self.dequeue_calls += 1
        await asyncio.sleep(0)
        return next(self.payloads)


class _QueueFallback:
    def __init__(self) -> None:
        now = datetime.now(timezone.utc)
        self.event = EventMessage(
            event_id="event-1",
            channel="alerts",
            payload={"ok": True},
            metadata=None,
            attempts=0,
            available_at=now,
            lease_expires_at=None,
            created_at=now,
        )
        self.dequeue_calls: list[tuple[str, float | None]] = []
        self.reset_calls: list[str] = []

    async def dequeue(self, channel: str, poll_interval: float | None = None) -> EventMessage | None:
        self.dequeue_calls.append((channel, poll_interval))
        return self.event

    async def dequeue_by_event_id(self, _event_id: str) -> None:
        return None

    def _reset_empty_poll_delay(self, channel: str) -> None:
        self.reset_calls.append(channel)


class _EmptyQueueFallback(_QueueFallback):
    async def dequeue(self, channel: str, poll_interval: float | None = None) -> None:
        self.dequeue_calls.append((channel, poll_interval))
        return


class _RecoveringQueueFallback(_QueueFallback):
    def __init__(self) -> None:
        super().__init__()
        self._remaining: list[EventMessage | None] = [self.event, self.event, None]

    async def dequeue(self, channel: str, poll_interval: float | None = None) -> EventMessage | None:
        self.dequeue_calls.append((channel, poll_interval))
        return self._remaining.pop(0)


class _SyncQueueFallback:
    def __init__(self) -> None:
        now = datetime.now(timezone.utc)
        self.event = EventMessage(
            event_id="event-1",
            channel="alerts",
            payload={"ok": True},
            metadata=None,
            attempts=0,
            available_at=now,
            lease_expires_at=None,
            created_at=now,
        )
        self.dequeue_calls: list[tuple[str, float | None]] = []
        self.reset_calls: list[str] = []

    def dequeue(self, channel: str, poll_interval: float | None = None) -> EventMessage:
        self.dequeue_calls.append((channel, poll_interval))
        return self.event

    def dequeue_by_event_id(self, _event_id: str) -> None:
        return None

    def _reset_empty_poll_delay(self, channel: str) -> None:
        self.reset_calls.append(channel)


class _Options:
    def __init__(self) -> None:
        self.wait = -1
        self.visibility: int | None = None


class _Queue:
    def __init__(self) -> None:
        self.deqoptions = _Options()


class _Thread:
    def __init__(self) -> None:
        self.join_timeout: float | None = None

    def join(self, timeout: float | None = None) -> None:
        self.join_timeout = timeout


async def _wait_for_async_consumers(hub: Any, channel: str, expected: int) -> None:
    for _ in range(50):
        queues = hub._queues.get(channel)
        if queues is not None and not isinstance(queues, asyncio.Queue) and len(queues) >= expected:
            return
        await asyncio.sleep(0.01)


def _wait_for_sync_consumers(hub: PsycopgSyncListenerHub, channel: str, expected: int) -> None:
    for _ in range(50):
        queues = hub._queues.get(channel)
        if queues is not None and len(queues) >= expected:
            return
        threading.Event().wait(0.01)


async def test_asyncpg_listener_hub_broadcasts_to_same_channel_consumers() -> None:
    payload = "payload-1"
    connection = _AsyncpgConnection()
    hub = AsyncpgListenerHub(_AsyncpgConfig(connection))  # type: ignore[arg-type]

    async def receive_once() -> str | None:
        return await hub.dequeue("alerts", 0.25)

    task_a = asyncio.create_task(receive_once())
    task_b = asyncio.create_task(receive_once())
    await _wait_for_async_consumers(hub, "alerts", 2)

    hub._dispatch("alerts", payload)

    result_a, result_b = await asyncio.gather(task_a, task_b)
    assert result_a == payload
    assert result_b == payload
    await hub.shutdown()


async def test_async_listener_hubs_acquire_one_listener_resource_for_many_channels() -> None:
    asyncpg_config = _AsyncpgConfig(_AsyncpgConnection())
    asyncpg_hub = AsyncpgListenerHub(asyncpg_config)  # type: ignore[arg-type]
    psycopg_config = _PsycopgAsyncConfig(_PsycopgAsyncConnection())
    psycopg_hub = PsycopgAsyncListenerHub(psycopg_config)  # type: ignore[arg-type]
    psqlpy_config = _PsqlpyConfig()
    psqlpy_hub = PsqlpyListenerHub(psqlpy_config)  # type: ignore[arg-type]

    await asyncpg_hub.subscribe("alerts")
    await asyncpg_hub.subscribe("metrics")
    await psycopg_hub.subscribe("alerts")
    await psycopg_hub.subscribe("metrics")
    await psqlpy_hub.subscribe("alerts")
    await psqlpy_hub.subscribe("metrics")

    assert asyncpg_config.connection_enters == 1
    assert psycopg_config.connection_enters == 1
    assert psqlpy_config.pool.listener_calls == 1
    assert psycopg_config.connections[0].executed[:2] == ['LISTEN "alerts"', 'LISTEN "metrics"']

    await asyncpg_hub.shutdown()
    await psycopg_hub.shutdown()
    await psqlpy_hub.shutdown()


def test_psycopg_sync_listener_hub_acquires_one_listener_resource_for_many_channels() -> None:
    config = _PsycopgSyncConfig(_PsycopgSyncConnection())
    hub = PsycopgSyncListenerHub(config)  # type: ignore[arg-type]

    hub.subscribe("alerts")
    hub.subscribe("metrics")

    assert config.connection_enters == 1

    hub.shutdown()
    assert config.connection_exits == 1


async def test_psqlpy_listener_hub_broadcasts_to_same_channel_consumers() -> None:
    payload = "payload-1"
    hub = PsqlpyListenerHub(_PsqlpyConfig())  # type: ignore[arg-type]

    async def receive_once() -> str | None:
        return await hub.dequeue("alerts", 0.25)

    task_a = asyncio.create_task(receive_once())
    task_b = asyncio.create_task(receive_once())
    await _wait_for_async_consumers(hub, "alerts", 2)

    hub._dispatch("alerts", payload)

    result_a, result_b = await asyncio.gather(task_a, task_b)
    assert result_a == payload
    assert result_b == payload
    await hub.shutdown()


async def test_psqlpy_listener_hub_attaches_consumer_before_callback_registration() -> None:
    payload = "payload-1"
    hub = PsqlpyListenerHub(_PsqlpyConfig(_PsqlpyListener(emit_on_add=payload)))  # type: ignore[arg-type]

    result = await hub.dequeue("alerts", 0.25)

    assert result == payload
    await hub.shutdown()


async def test_psqlpy_listener_hub_filters_ready_probe_payloads() -> None:
    listener = _PsqlpyListener()
    hub = PsqlpyListenerHub(_PsqlpyConfig(listener))  # type: ignore[arg-type]

    result = await hub.dequeue("alerts", 0.01)

    assert result is None
    assert listener.listen_called is True
    assert len(listener.emitted) == 1
    assert listener.emitted[0][0] == "alerts"
    await hub.shutdown()


async def test_psqlpy_listener_hub_treats_ready_probe_timeout_as_empty_poll() -> None:
    listener = _PsqlpyListener(drop_ready_payloads=True)
    hub = PsqlpyListenerHub(_PsqlpyConfig(listener))  # type: ignore[arg-type]

    result = await hub.dequeue("alerts", 0.01)

    assert result is None
    assert listener.listen_called is True
    assert listener.emitted == []
    await hub.shutdown()


async def test_psqlpy_listener_hub_rearms_for_new_channels() -> None:
    listener = _PsqlpyListener()
    hub = PsqlpyListenerHub(_PsqlpyConfig(listener))  # type: ignore[arg-type]

    assert await hub.dequeue("channel_a", 0.01) is None
    assert await hub.dequeue("channel_b", 0.01) is None

    assert listener.listen_count == 2
    assert listener.abort_count == 1
    assert [channel for channel, _ in listener.emitted] == ["channel_a", "channel_b"]
    assert all(payload.startswith("__sqlspec_psqlpy_ready__:") for _, payload in listener.emitted)
    await hub.shutdown()


async def test_psqlpy_listener_hub_reconnects_and_releases_listener_handles() -> None:
    first = _PsqlpyListener()
    replacement = _PsqlpyListener()
    config = _PsqlpyConfig(first, replacement)
    hub = PsqlpyListenerHub(config)  # type: ignore[arg-type]

    await hub.subscribe("alerts")
    first.started = False

    assert await hub.dequeue("alerts", 0.01) is None
    assert config.pool.listener_calls == 2
    assert first.shutdown_count == 1
    assert "alerts" in replacement.callbacks
    assert "events.listener.reconnect" in config.get_observability_runtime().metrics

    await hub.shutdown()
    assert replacement.shutdown_count == 1
    assert config.get_observability_runtime().metrics.count("events.listener.release") == 2


async def test_psycopg_async_listener_hub_broadcasts_to_same_channel_consumers() -> None:
    payload = "payload-1"
    connection = _PsycopgAsyncConnection()
    hub = PsycopgAsyncListenerHub(_PsycopgAsyncConfig(connection))  # type: ignore[arg-type]

    async def receive_once() -> str | None:
        return await hub.dequeue("alerts", 0.25)

    task_a = asyncio.create_task(receive_once())
    task_b = asyncio.create_task(receive_once())
    await _wait_for_async_consumers(hub, "alerts", 2)

    hub._dispatch("alerts", payload)

    result_a, result_b = await asyncio.gather(task_a, task_b)
    assert result_a == payload
    assert result_b == payload
    await hub.shutdown()


async def test_asyncpg_listener_hub_reconnects_and_releases_listener_contexts() -> None:
    first = _AsyncpgConnection()
    replacement = _AsyncpgConnection()
    config = _AsyncpgConfig(first, replacement)
    hub = AsyncpgListenerHub(config)  # type: ignore[arg-type]

    await hub.subscribe("alerts")
    first.closed = True

    assert await hub.dequeue("alerts", 0.01) is None
    assert config.connection_enters == 2
    assert config.connection_exits == 1
    assert "alerts" in replacement.callbacks
    assert "events.listener.reconnect" in config.get_observability_runtime().metrics

    await hub.shutdown()
    assert config.connection_exits == 2
    assert config.get_observability_runtime().metrics.count("events.listener.release") == 2


async def test_psycopg_async_listener_hub_reconnects_and_releases_listener_contexts() -> None:
    first = _PsycopgAsyncConnection()
    replacement = _PsycopgAsyncConnection()
    config = _PsycopgAsyncConfig(first, replacement)
    hub = PsycopgAsyncListenerHub(config)  # type: ignore[arg-type]

    await hub.subscribe("alerts")
    first.closed = True

    assert await hub.dequeue("alerts", 0.01) is None
    assert config.connection_enters == 2
    assert config.connection_exits == 1
    assert 'LISTEN "alerts"' in replacement.executed
    assert "events.listener.reconnect" in config.get_observability_runtime().metrics

    await hub.shutdown()
    assert config.connection_exits == 2
    assert config.get_observability_runtime().metrics.count("events.listener.release") == 2


def test_psycopg_sync_listener_hub_broadcasts_to_same_channel_consumers() -> None:
    payload = "payload-1"
    hub = PsycopgSyncListenerHub(_PsycopgSyncConfig(_PsycopgSyncConnection()))  # type: ignore[arg-type]
    hub.subscribe("alerts")
    results: list[str | None] = []

    def receive_once() -> None:
        results.append(hub.dequeue("alerts", 0.25))

    thread_a = threading.Thread(target=receive_once)
    thread_b = threading.Thread(target=receive_once)
    thread_a.start()
    thread_b.start()
    _wait_for_sync_consumers(hub, "alerts", 2)

    hub._dispatch("alerts", payload)
    thread_a.join(timeout=1.0)
    thread_b.join(timeout=1.0)

    assert results == [payload, payload]
    hub.shutdown()


def test_psycopg_sync_listener_hub_reconnects_and_releases_listener_contexts() -> None:
    first = _PsycopgSyncConnection()
    replacement = _PsycopgSyncConnection()
    config = _PsycopgSyncConfig(first, replacement)
    hub = PsycopgSyncListenerHub(config)  # type: ignore[arg-type]

    hub.subscribe("alerts")
    first.closed = True

    assert hub.dequeue("alerts", 0.01) is None
    assert config.connection_enters == 2
    assert config.connection_exits == 1
    assert 'LISTEN "alerts"' in replacement.executed
    assert "events.listener.reconnect" in config.get_observability_runtime().metrics

    hub.shutdown()
    assert config.connection_exits == 2
    assert config.get_observability_runtime().metrics.count("events.listener.release") == 2


async def test_asyncpg_hybrid_dequeue_polls_durable_queue_after_notify_timeout() -> None:
    config = _HybridConfig()
    await _assert_dropped_marker_recovers(
        AsyncpgHybridEventsBackend(config, _RecoveringQueueFallback()),  # type: ignore[arg-type]
        config,
    )


async def test_psycopg_hybrid_dequeue_polls_durable_queue_after_notify_timeout() -> None:
    pytest.importorskip("psycopg")
    from sqlspec.adapters.psycopg.events.backend import PsycopgAsyncHybridEventsBackend

    config = _PsycopgHybridConfig()
    await _assert_dropped_marker_recovers(
        PsycopgAsyncHybridEventsBackend(config, _RecoveringQueueFallback()),  # type: ignore[arg-type]
        config,
    )


async def test_psqlpy_hybrid_dequeue_polls_durable_queue_after_notify_timeout() -> None:
    pytest.importorskip("psqlpy")
    from sqlspec.adapters.psqlpy.events.backend import PsqlpyHybridEventsBackend

    config = _PsqlpyHybridConfig()
    await _assert_dropped_marker_recovers(
        PsqlpyHybridEventsBackend(config, _RecoveringQueueFallback()),  # type: ignore[arg-type]
        config,
    )


async def test_hybrid_native_marker_resets_empty_poll_backoff() -> None:
    config = _HybridConfig()
    queue = _QueueFallback()
    backend = AsyncpgHybridEventsBackend(config, queue)  # type: ignore[arg-type]
    backend._hub = _MarkerHub()  # type: ignore[assignment]

    assert await backend.dequeue("alerts", 0.01) is queue.event
    assert queue.reset_calls == ["alerts"]


async def test_async_hybrid_batch_marker_drains_all_hinted_rows_without_waiting_again() -> None:
    configs_and_backends = [
        (_HybridConfig(), AsyncpgHybridEventsBackend),
        (_PsycopgHybridConfig(), PsycopgAsyncHybridEventsBackend),
        (_PsqlpyHybridConfig(), PsqlpyHybridEventsBackend),
    ]
    for config, backend_type in configs_and_backends:
        queue = _QueueFallback()
        hub = _MarkerHub(batch_size=3)
        backend = backend_type(config, queue)  # type: ignore[arg-type,call-arg]
        backend._hub = hub  # type: ignore[assignment]

        events = [await backend.dequeue("alerts", 0.5) for _ in range(3)]

        assert events == [queue.event, queue.event, queue.event]
        assert hub.dequeue_calls == 1
        assert queue.dequeue_calls == [("alerts", None)] * 3
        assert not any(metric == "events.marker.miss" for metric, _amount in config.runtime.metrics)


def test_sync_hybrid_batch_marker_drains_all_hinted_rows_without_waiting_again() -> None:
    config = _PsycopgSyncNativeConfig()
    queue = _SyncQueueFallback()
    hub = _SyncMarkerHub(batch_size=3)
    backend = PsycopgSyncHybridEventsBackend(config, queue)  # type: ignore[arg-type]
    backend._hub = hub  # type: ignore[assignment]

    events = [backend.dequeue("alerts", 0.5) for _ in range(3)]

    assert events == [queue.event, queue.event, queue.event]
    assert hub.dequeue_calls == 1
    assert queue.dequeue_calls == [("alerts", None)] * 3
    assert not any(metric == "events.marker.miss" for metric, _amount in config.runtime.metrics)


async def test_hybrid_marker_dedupe_adds_overlapping_flush_counts_once() -> None:
    config = _HybridConfig()
    queue = _QueueFallback()
    marker_a = '{"batch_size":2,"marker_id":"marker-a"}'
    marker_b = '{"batch_size":2,"marker_id":"marker-b"}'
    hub = _SequenceMarkerHub([marker_a, marker_a, marker_b, marker_b])
    backend = AsyncpgHybridEventsBackend(config, queue)  # type: ignore[arg-type]
    backend._hub = hub  # type: ignore[assignment]

    events = await asyncio.gather(*(backend.dequeue("alerts", 0.5) for _ in range(4)))

    assert events == [queue.event] * 4
    assert hub.dequeue_calls == 4
    assert queue.dequeue_calls == [("alerts", None)] * 4
    assert backend._marker_state._pending == {}
    assert list(backend._marker_state._seen) == [("alerts", "marker-a"), ("alerts", "marker-b")]


def test_hybrid_marker_dedupe_state_is_bounded() -> None:
    configs_and_backends = [
        (_HybridConfig(), AsyncpgHybridEventsBackend),
        (_PsycopgHybridConfig(), PsycopgAsyncHybridEventsBackend),
        (_PsqlpyHybridConfig(), PsqlpyHybridEventsBackend),
    ]
    for config, backend_type in configs_and_backends:
        backend = backend_type(config, _QueueFallback())  # type: ignore[arg-type,call-arg]
        for index in range(1_025):
            assert backend._marker_state.register("alerts", f"marker-{index}", 1) == (True, True)

        assert len(backend._marker_state._seen) == 1_024
        assert ("alerts", "marker-0") not in backend._marker_state._seen


async def test_hybrid_backend_does_not_double_count_table_queue_empty_poll() -> None:
    config = _HybridConfig()
    backend = AsyncpgHybridEventsBackend(config, _EmptyQueueFallback())  # type: ignore[arg-type]
    backend._hub = _TimeoutHub()  # type: ignore[assignment]

    assert await backend.dequeue("alerts", 0.01) is None
    assert not any(metric == "events.poll.empty" for metric, _amount in config.runtime.metrics)
    assert not any(metric == "events.marker.miss" for metric, _amount in config.runtime.metrics)
    assert ("events.listener.timeout", 1.0) in config.runtime.metrics


async def test_async_native_notify_timeout_records_empty_wait() -> None:
    configs_and_backends = [
        (_AsyncpgNativeConfig(), AsyncpgEventsBackend),
        (_PsycopgNativeConfig(), PsycopgAsyncEventsBackend),
        (_PsqlpyNativeConfig(), PsqlpyEventsBackend),
    ]
    for config, backend_type in configs_and_backends:
        backend = backend_type(config)  # type: ignore[arg-type,call-arg]
        backend._hub = _TimeoutHub()  # type: ignore[assignment]

        assert await backend.dequeue("alerts", 0.01) is None
        assert ("events.listener.timeout", 1.0) in config.runtime.metrics


def test_sync_native_notify_timeout_records_empty_wait() -> None:
    config = _PsycopgSyncNativeConfig()
    backend = PsycopgSyncEventsBackend(config)  # type: ignore[arg-type]
    backend._hub = _SyncTimeoutHub()  # type: ignore[assignment]

    assert backend.dequeue("alerts", 0.01) is None
    assert ("events.listener.timeout", 1.0) in config.runtime.metrics


def test_oracle_aq_options_round_subsecond_wait_up_to_one_second() -> None:
    queue = _Queue()

    oracle_hub._apply_deq_options(queue, None, None, 0.5)

    assert queue.deqoptions.wait == 1


def test_psycopg_sync_shutdown_submits_stop_before_setting_stop_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    hub = PsycopgSyncListenerHub(object())  # type: ignore[arg-type]
    thread = _Thread()
    stop_states: list[bool] = []

    def _submit(self: PsycopgSyncListenerHub, op: str, _channel: str, *, timeout: float | None = None) -> None:
        if op == "stop":
            stop_states.append(self._stopping.is_set())

    monkeypatch.setattr(PsycopgSyncListenerHub, "_submit", _submit)
    hub._worker_thread = thread  # type: ignore[assignment]
    hub._queues["alerts"] = WeakKeyDictionary()

    hub.shutdown()

    assert stop_states == [False]
    assert thread.join_timeout == 3.0


async def test_asyncpg_hub_registers_pool_destroying_hook_after_subscribe() -> None:
    """Hub registers shutdown for on_pool_destroying once it acquires its connection."""
    connection = _AsyncpgConnection()
    config = _AsyncpgConfig(connection)
    hub = AsyncpgListenerHub(config)  # type: ignore[arg-type]

    assert config.get_observability_runtime().registered == []

    await hub.subscribe("alerts")
    registered = config.get_observability_runtime().registered
    assert len(registered) == 1
    assert registered[0][0] == "on_pool_destroying"

    # Second subscribe must not re-register the same hook.
    await hub.subscribe("metrics")
    assert len(config.get_observability_runtime().registered) == 1
    await hub.shutdown()


async def test_psqlpy_hub_registers_pool_destroying_hook_after_subscribe() -> None:
    config = _PsqlpyConfig()
    hub = PsqlpyListenerHub(config)  # type: ignore[arg-type]

    assert config.get_observability_runtime().registered == []

    await hub.subscribe("alerts")
    registered = config.get_observability_runtime().registered
    assert len(registered) == 1
    assert registered[0][0] == "on_pool_destroying"
    await hub.shutdown()


async def test_psycopg_async_hub_registers_pool_destroying_hook_after_subscribe() -> None:
    connection = _PsycopgAsyncConnection()
    config = _PsycopgAsyncConfig(connection)
    hub = PsycopgAsyncListenerHub(config)  # type: ignore[arg-type]

    assert config.get_observability_runtime().registered == []

    await hub.subscribe("alerts")
    registered = config.get_observability_runtime().registered
    assert len(registered) == 1
    assert registered[0][0] == "on_pool_destroying"
    await hub.shutdown()


async def _assert_dropped_marker_recovers(backend: Any, config: _HybridConfig) -> None:
    queue = backend._queue
    hub = _TimeoutHub()
    backend._hub = hub

    first = await backend.dequeue("alerts", 0.25)
    second = await backend.dequeue("alerts", 0.25)
    exhausted = await backend.dequeue("alerts", 0.25)

    assert first is queue.event
    assert second is queue.event
    assert exhausted is None
    assert hub.dequeue_calls == 1
    assert queue.dequeue_calls == [("alerts", None), ("alerts", None), ("alerts", None)]
    metric_names = [name for name, _ in config.runtime.metrics]
    assert "events.marker.miss" in metric_names
    assert "events.poll.fallback" in metric_names
    assert "events.dequeue.latency_ms" in metric_names
