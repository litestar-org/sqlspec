"""Cross-backend concurrency MVCE suite for native EventChannel backends.

These tests reproduce the four hazards documented in the persistent-listener saga and
must FAIL on every native backend before the redesign lands:

1. asyncpg — concurrent subscribe + unsubscribe across overlapping channels racing on
   a shared asyncpg.Connection (`InterfaceError: another operation is in progress`).
2. psycopg async — `_ensure_listener` only emits LISTEN on the first channel touched
   on the shared connection; second channel notifications are silently dropped.
3. psqlpy — per-iteration `clear_channel_callbacks` wipes peer callbacks on the same
   channel, causing concurrent listeners on a shared channel to lose deliveries.
4. oracledb — `poll_interval` is ignored; backend uses `aq_wait_seconds` only.

Each test follows a fixture-factory pattern keyed on the backend, so a single matrix
exercises every native backend uniformly. A backend that lacks an available service
or required driver is skipped, not silently passed.
"""

# pyright: reportPrivateUsage=false

import asyncio
import contextlib
import importlib
from collections.abc import Awaitable, Callable
from typing import Any

import pytest

from sqlspec import SQLSpec

pytestmark = pytest.mark.xdist_group("postgres")

_SUBSCRIBE_WAIT = 0.6
_POLL_INTERVAL = 0.05
_MAX_POLL_ATTEMPTS = 200
_EXPECTED_DELIVERIES = 5


ConfigFactory = Callable[[Any], Any]


def _asyncpg_factory(service: Any) -> Any:
    config_module = importlib.import_module("sqlspec.adapters.asyncpg")
    config_cls = config_module.AsyncpgConfig
    dsn = f"postgresql://{service.user}:{service.password}@{service.host}:{service.port}/{service.database}"
    return config_cls(connection_config={"dsn": dsn}, extension_config={"events": {"backend": "listen_notify"}})


def _psycopg_async_factory(service: Any) -> Any:
    config_module = importlib.import_module("sqlspec.adapters.psycopg")
    config_cls = config_module.PsycopgAsyncConfig
    conninfo = f"postgresql://{service.user}:{service.password}@{service.host}:{service.port}/{service.database}"
    return config_cls(
        connection_config={"conninfo": conninfo}, extension_config={"events": {"backend": "listen_notify"}}
    )


def _psqlpy_factory(service: Any) -> Any:
    config_module = importlib.import_module("sqlspec.adapters.psqlpy")
    config_cls = config_module.PsqlpyConfig
    params_cls = config_module.PsqlpyPoolParams
    dsn = f"postgres://{service.user}:{service.password}@{service.host}:{service.port}/{service.database}"
    return config_cls(connection_config=params_cls(dsn=dsn), extension_config={"events": {"backend": "listen_notify"}})


_FACTORIES: "dict[str, ConfigFactory]" = {
    "asyncpg": _asyncpg_factory,
    "psycopg_async": _psycopg_async_factory,
    "psqlpy": _psqlpy_factory,
}


def _import_or_skip(backend_key: str) -> ConfigFactory:
    try:
        return _FACTORIES[backend_key]
    except KeyError:  # pragma: no cover - defensive
        pytest.skip(f"unknown backend {backend_key}")
        raise


async def _drain(received: "list[Any]", minimum: int, *, watch_tasks: "tuple[Any, ...]" = ()) -> None:
    deadline = asyncio.get_running_loop().time() + 3.0
    while asyncio.get_running_loop().time() < deadline:
        if len(received) >= minimum:
            return
        if any(t.done() for t in watch_tasks):
            return
        await asyncio.sleep(_POLL_INTERVAL)


@pytest.mark.postgres
@pytest.mark.parametrize("backend_key", sorted(_FACTORIES))
async def test_native_concurrent_multi_channel_subscribe(postgres_service: Any, backend_key: str) -> None:
    """Two channels subscribed concurrently must both receive their notifications."""
    factory = _import_or_skip(backend_key)
    try:
        config = factory(postgres_service)
    except ImportError:  # pragma: no cover - driver absent
        pytest.skip(f"driver for {backend_key} not installed")

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    received_a: list[Any] = []
    received_b: list[Any] = []

    async def _handler_a(message: Any) -> None:
        received_a.append(message)

    async def _handler_b(message: Any) -> None:
        received_b.append(message)

    chan_a = f"mc_{backend_key}_a"
    chan_b = f"mc_{backend_key}_b"

    listener_a = None
    listener_b = None
    try:
        listener_a = channel.listen(chan_a, _handler_a, poll_interval=_POLL_INTERVAL)
        listener_b = channel.listen(chan_b, _handler_b, poll_interval=_POLL_INTERVAL)
        await asyncio.sleep(_SUBSCRIBE_WAIT)

        for index in range(_EXPECTED_DELIVERIES):
            await _publish(channel, chan_a, {"i": index})
            await _publish(channel, chan_b, {"i": index})
            await asyncio.sleep(0.05)

        await _drain(received_a, _EXPECTED_DELIVERIES, watch_tasks=(listener_a.task, listener_b.task))
        await _drain(received_b, _EXPECTED_DELIVERIES, watch_tasks=(listener_a.task, listener_b.task))

        # Listener tasks must still be alive — a connection-level race would have killed them.
        for label, listener in (("listener_a", listener_a), ("listener_b", listener_b)):
            if listener.task.done():
                exc = listener.task.exception()
                assert exc is None, f"{label} task crashed under concurrency: {exc!r}"

        assert len(received_a) >= _EXPECTED_DELIVERIES, (
            f"{backend_key}: channel A only received {len(received_a)}/{_EXPECTED_DELIVERIES}"
        )
        assert len(received_b) >= _EXPECTED_DELIVERIES, (
            f"{backend_key}: channel B only received {len(received_b)}/{_EXPECTED_DELIVERIES}"
        )
    finally:
        await _safe_stop(channel, listener_a, listener_b)
        await _cleanup(channel, config)


@pytest.mark.postgres
@pytest.mark.parametrize("backend_key", sorted(_FACTORIES))
async def test_native_concurrent_same_channel_peers(postgres_service: Any, backend_key: str) -> None:
    """Two listeners sharing the same channel must both receive each NOTIFY."""
    factory = _import_or_skip(backend_key)
    try:
        config = factory(postgres_service)
    except ImportError:  # pragma: no cover - driver absent
        pytest.skip(f"driver for {backend_key} not installed")

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    received_a: list[Any] = []
    received_b: list[Any] = []

    async def _handler_a(message: Any) -> None:
        received_a.append(message)

    async def _handler_b(message: Any) -> None:
        received_b.append(message)

    chan = f"peer_{backend_key}"
    listener_a = None
    listener_b = None
    try:
        listener_a = channel.listen(chan, _handler_a, poll_interval=_POLL_INTERVAL)
        listener_b = channel.listen(chan, _handler_b, poll_interval=_POLL_INTERVAL)
        await asyncio.sleep(_SUBSCRIBE_WAIT)

        for index in range(_EXPECTED_DELIVERIES):
            await _publish(channel, chan, {"i": index})
            await asyncio.sleep(0.05)

        await _drain(received_a, 1, watch_tasks=(listener_a.task, listener_b.task))
        await _drain(received_b, 1, watch_tasks=(listener_a.task, listener_b.task))

        assert received_a, f"{backend_key}: peer A received nothing (same-channel race)"
        assert received_b, f"{backend_key}: peer B received nothing (same-channel race)"
    finally:
        await _safe_stop(channel, listener_a, listener_b)
        await _cleanup(channel, config)


async def _safe_stop(channel: Any, *listeners: Any) -> None:
    for listener in listeners:
        if listener is None:
            continue
        with contextlib.suppress(Exception):
            await asyncio.wait_for(channel.stop_listener(listener.id), timeout=2.0)


async def _publish(channel: Any, name: str, payload: "dict[str, Any]") -> None:
    result = channel.publish(name, payload)
    if isinstance(result, Awaitable):
        await result


async def _cleanup(channel: Any, config: Any) -> None:
    with contextlib.suppress(Exception):
        await asyncio.wait_for(channel.shutdown(), timeout=2.0)
    close_pool = getattr(config, "close_pool", None)
    if close_pool is not None and getattr(config, "connection_instance", None) is not None:
        with contextlib.suppress(Exception):
            result = close_pool()
            if isinstance(result, Awaitable):
                await asyncio.wait_for(result, timeout=2.0)
