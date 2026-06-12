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
from sqlspec.adapters.asyncpg.events.backend import AsyncpgHybridEventsBackend
from sqlspec.adapters.oracledb.events import _hub as oracle_hub
from sqlspec.adapters.psqlpy.events._hub import PsqlpyListenerHub
from sqlspec.adapters.psycopg.events._hub import PsycopgAsyncListenerHub, PsycopgSyncListenerHub
from sqlspec.extensions.events import EventMessage


class _AsyncConnectionContext:
    def __init__(self, connection: Any) -> None:
        self.connection = connection

    async def __aenter__(self) -> Any:
        return self.connection

    async def __aexit__(self, *_exc_info: object) -> None:
        return None


class _AsyncpgConnection:
    def __init__(self) -> None:
        self.callbacks: dict[str, Any] = {}
        self.executed: list[str] = []

    async def execute(self, statement: str) -> None:
        self.executed.append(statement)

    async def add_listener(self, channel: str, callback: Any) -> None:
        self.callbacks[channel] = callback

    async def remove_listener(self, channel: str, _callback: Any) -> None:
        self.callbacks.pop(channel, None)


class _StubRuntime:
    def __init__(self) -> None:
        self.registered: list[tuple[str, Any]] = []

    def increment_metric(self, _metric: str) -> None:
        return None

    def register_lifecycle_hook(self, event: str, callback: Any) -> None:
        self.registered.append((event, callback))


class _AsyncpgConfig:
    def __init__(self, connection: _AsyncpgConnection) -> None:
        self.connection = connection
        self._runtime = _StubRuntime()

    def provide_connection(self) -> _AsyncConnectionContext:
        return _AsyncConnectionContext(self.connection)

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

    async def startup(self) -> None:
        pass

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
        pass


class _PsqlpyPool:
    def __init__(self, listener_handle: _PsqlpyListener | None = None) -> None:
        self.listener_handle = listener_handle or _PsqlpyListener()

    def listener(self) -> _PsqlpyListener:
        return self.listener_handle


class _PsqlpyConfig:
    def __init__(self, listener_handle: _PsqlpyListener | None = None) -> None:
        self.listener_handle = listener_handle or _PsqlpyListener()
        self.pool = _PsqlpyPool(self.listener_handle)
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

    async def execute(self, statement: str) -> None:
        self.executed.append(statement)

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
    def __init__(self, connection: _PsycopgAsyncConnection) -> None:
        self.connection = connection
        self._runtime = _StubRuntime()

    def provide_connection(self) -> _AsyncConnectionContext:
        return _AsyncConnectionContext(self.connection)

    def get_observability_runtime(self) -> _StubRuntime:
        return self._runtime


class _Runtime:
    def increment_metric(self, _metric: str) -> None:
        return None


class _HybridConfig:
    is_async = True

    def get_observability_runtime(self) -> _Runtime:
        return _Runtime()


class _TimeoutHub:
    async def dequeue(self, _channel: str, _poll_interval: float) -> None:
        return None


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

    async def dequeue(self, channel: str, poll_interval: float | None = None) -> EventMessage:
        self.dequeue_calls.append((channel, poll_interval))
        return self.event

    async def dequeue_by_event_id(self, _event_id: str) -> None:
        return None


class _Options:
    def __init__(self) -> None:
        self.wait = -1
        self.visibility: int | None = None


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


def test_psycopg_sync_listener_hub_broadcasts_to_same_channel_consumers() -> None:
    payload = "payload-1"
    hub = PsycopgSyncListenerHub(object())  # type: ignore[arg-type]
    hub._queues["alerts"] = WeakKeyDictionary()
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


async def test_asyncpg_hybrid_dequeue_polls_durable_queue_after_notify_timeout() -> None:
    queue = _QueueFallback()
    backend = AsyncpgHybridEventsBackend(_HybridConfig(), queue)  # type: ignore[arg-type]
    backend._hub = _TimeoutHub()  # type: ignore[assignment]

    event = await backend.dequeue("alerts", 0.25)

    assert event is queue.event
    assert queue.dequeue_calls == [("alerts", None)]


def test_oracle_aq_options_round_subsecond_wait_up_to_one_second(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(oracle_hub, "AQDequeueOptions", _Options)

    options = oracle_hub._resolve_options(None, None, 0.5)

    assert options.wait == 1


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
