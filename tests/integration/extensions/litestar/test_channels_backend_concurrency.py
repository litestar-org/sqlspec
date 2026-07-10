"""Concurrency MVCE for SQLSpecChannelsBackend backed by native PG event backends.

The Litestar ChannelsBackend opens an asyncio.Task per subscribed channel via
`SQLSpecChannelsBackend.subscribe(...)`. Each task drives `iter_events(...)` on the
underlying EventChannel. Under the broken native PG backends, multiple subscribers on
overlapping channels race on the shared LISTEN connection and either crash with
InterfaceError (asyncpg) or silently drop notifications (psycopg/psqlpy).

These tests pin the contract: the Litestar layer must deliver to every subscriber
without exceptions.
"""

# pyright: reportPrivateUsage=false

import asyncio
import contextlib
import importlib
from collections.abc import Awaitable, Callable
from typing import Any, cast

import msgspec.json
import pytest
from litestar.channels.plugin import ChannelsPlugin

from sqlspec.extensions.events import AsyncEventChannel
from sqlspec.extensions.litestar.channels import SQLSpecChannelsBackend

pytestmark = pytest.mark.xdist_group("postgres")


def _timeout_scale() -> float:
    """Return the multiplier applied to delivery-timing budgets in this module."""
    try:
        import coverage

        if coverage.Coverage.current() is not None:
            return 4.0
    except Exception:
        return 1.0
    return 1.0


_SCALE = _timeout_scale()
_EXERCISE_TIMEOUT = 15.0 * _SCALE
_SUBSCRIBE_WAIT = 0.5 * _SCALE
_NEXT_EVENT_TIMEOUT = 3.0 * _SCALE
_CLOSE_TIMEOUT = 2.0 * _SCALE
_POLL_INTERVAL = 0.05
_EXPECTED_DELIVERIES = 3


ConfigFactory = Callable[[Any], Any]


def _asyncpg_factory(service: Any) -> Any:
    config_module = importlib.import_module("sqlspec.adapters.asyncpg")
    config_cls = config_module.AsyncpgConfig
    dsn = f"postgresql://{service.user}:{service.password}@{service.host}:{service.port}/{service.database}"
    return config_cls(connection_config={"dsn": dsn}, extension_config={"events": {"backend": "notify"}})


def _psycopg_async_factory(service: Any) -> Any:
    config_module = importlib.import_module("sqlspec.adapters.psycopg")
    config_cls = config_module.PsycopgAsyncConfig
    conninfo = f"postgresql://{service.user}:{service.password}@{service.host}:{service.port}/{service.database}"
    return config_cls(connection_config={"conninfo": conninfo}, extension_config={"events": {"backend": "notify"}})


def _psqlpy_factory(service: Any) -> Any:
    config_module = importlib.import_module("sqlspec.adapters.psqlpy")
    config_cls = config_module.PsqlpyConfig
    params_cls = config_module.PsqlpyPoolParams
    dsn = f"postgres://{service.user}:{service.password}@{service.host}:{service.port}/{service.database}"
    return config_cls(connection_config=params_cls(dsn=dsn), extension_config={"events": {"backend": "notify"}})


_FACTORIES: "dict[str, ConfigFactory]" = {
    "asyncpg": _asyncpg_factory,
    "psycopg_async": _psycopg_async_factory,
    "psqlpy": _psqlpy_factory,
}


async def _next_event(subscriber: Any) -> bytes:
    async for event in subscriber.iter_events():
        return cast("bytes", event)
    msg = "Subscriber stopped before yielding"
    raise RuntimeError(msg)


@pytest.mark.postgres
@pytest.mark.parametrize("backend_key", sorted(_FACTORIES))
async def test_sqlspec_channels_backend_multi_channel(postgres_service: Any, backend_key: str) -> None:
    """ChannelsPlugin with two channels and one publisher: both subscribers must hear it."""
    factory = _FACTORIES[backend_key]
    try:
        config = factory(postgres_service)
    except ImportError:  # pragma: no cover - driver absent
        pytest.skip(f"driver for {backend_key} not installed")

    event_channel = AsyncEventChannel(config)
    backend = SQLSpecChannelsBackend(event_channel, channel_prefix="litestar", poll_interval=_POLL_INTERVAL)
    plugin = ChannelsPlugin(backend=backend, channels=[f"alpha_{backend_key}", f"beta_{backend_key}"])

    failure: AssertionError | None = None
    try:
        try:
            await asyncio.wait_for(_exercise(plugin, backend_key), timeout=_EXERCISE_TIMEOUT)
        except asyncio.TimeoutError:
            failure = AssertionError(
                f"{backend_key}: test hung — concurrent multi-channel deadlock under broken backend"
            )
        except AssertionError as exc:
            failure = exc
    finally:
        close_pool = getattr(config, "close_pool", None)
        if close_pool is not None and getattr(config, "connection_instance", None) is not None:
            with contextlib.suppress(Exception):
                result = close_pool()
                if isinstance(result, Awaitable):
                    await asyncio.wait_for(result, timeout=_CLOSE_TIMEOUT)

    if failure is not None:
        raise failure


async def _exercise(plugin: ChannelsPlugin, backend_key: str) -> None:
    async with plugin:
        sub_alpha = await plugin.subscribe(f"alpha_{backend_key}")
        sub_beta = await plugin.subscribe(f"beta_{backend_key}")

        await asyncio.sleep(_SUBSCRIBE_WAIT)

        for _ in range(_EXPECTED_DELIVERIES):
            await plugin.wait_published({"action": "alpha"}, f"alpha_{backend_key}")
            await plugin.wait_published({"action": "beta"}, f"beta_{backend_key}")

        try:
            alpha_payload = await asyncio.wait_for(_next_event(sub_alpha), timeout=_NEXT_EVENT_TIMEOUT)
            decoded_alpha = msgspec.json.decode(alpha_payload)
            assert decoded_alpha["action"] == "alpha"
        except asyncio.TimeoutError as exc:
            raise AssertionError(f"{backend_key}: alpha subscriber timed out") from exc

        try:
            beta_payload = await asyncio.wait_for(_next_event(sub_beta), timeout=_NEXT_EVENT_TIMEOUT)
            decoded_beta = msgspec.json.decode(beta_payload)
            assert decoded_beta["action"] == "beta", (
                f"{backend_key}: beta subscriber received no message (multi-channel race)"
            )
        except asyncio.TimeoutError as exc:
            raise AssertionError(f"{backend_key}: beta subscriber timed out (multi-channel race)") from exc
