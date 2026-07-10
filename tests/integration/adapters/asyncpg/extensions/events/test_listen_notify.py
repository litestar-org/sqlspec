# pyright: reportPrivateUsage=false
"""PostgreSQL LISTEN/NOTIFY event channel tests for asyncpg adapter."""

import asyncio
import contextlib
from typing import Any

import pytest

from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.migrations.commands import AsyncMigrationCommands

pytestmark = pytest.mark.xdist_group("postgres")

_SUBSCRIBE_WAIT = 2.0  # Seconds to wait for listener to subscribe before first publish
_POLL_INTERVAL = 0.05  # Seconds between receive checks
_MAX_POLL_ATTEMPTS = 200  # Total receive-check attempts


def _dsn(service: "Any") -> str:
    return f"postgresql://{service.user}:{service.password}@{service.host}:{service.port}/{service.database}"


async def _wait_for_message(received: "list[Any]") -> None:
    """Poll until a message is received or max attempts exhausted."""
    for _ in range(_MAX_POLL_ATTEMPTS):
        if received:
            break
        await asyncio.sleep(_POLL_INTERVAL)


@pytest.mark.postgres
async def test_asyncpg_hybrid_listen_notify_durable(postgres_service: "Any", tmp_path: Any) -> None:
    """Hybrid backend stores event durably then notifies listeners."""

    migrations = tmp_path / "migrations"
    migrations.mkdir()

    config = AsyncpgConfig(
        connection_config={"dsn": _dsn(postgres_service)},
        migration_config={"script_location": str(migrations), "include_extensions": ["events"]},
        extension_config={"events": {"backend": "notify_queue"}},
    )

    await AsyncMigrationCommands(config).upgrade()

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    assert channel._backend_name == "notify_queue"

    received: list[Any] = []

    async def _handler(message: Any) -> None:
        received.append(message)

    listener = channel.listen("alerts", _handler, poll_interval=0.2)
    await asyncio.sleep(_SUBSCRIBE_WAIT)

    event_id = await channel.publish("alerts", {"action": "hybrid_async"})
    await _wait_for_message(received)

    if not received:
        event_id = await channel.publish("alerts", {"action": "hybrid_async"})
        await _wait_for_message(received)

    batch_ids = await channel.publish_many([
        ("alerts", {"index": 1}, None),
        ("alerts", {"index": 2}, {"source": "batch"}),
    ])
    for _ in range(_MAX_POLL_ATTEMPTS):
        if len(received) >= 3:
            break
        await asyncio.sleep(_POLL_INTERVAL)

    await channel.stop_listener(listener.id)

    assert len(received) >= 3, "listener did not receive the single event and complete batch"
    message = received[0]
    assert message.event_id == event_id
    assert message.payload["action"] == "hybrid_async"
    assert [message.event_id for message in received[1:3]] == batch_ids
    assert [message.payload["index"] for message in received[1:3]] == [1, 2]
    assert received[2].metadata == {"source": "batch"}

    await channel.shutdown()
    if config.connection_instance:
        await config.close_pool()


@pytest.mark.postgres
async def test_asyncpg_concurrent_multi_channel_subscribe(postgres_service: "Any") -> None:
    """Concurrent subscribe to multiple channels must not race on the shared LISTEN connection.

    Reproduces the asyncpg.InterfaceError ("another operation is in progress") observed when
    overlapping add_listener / remove_listener calls churn on the shared Connection.
    """

    config = AsyncpgConfig(
        connection_config={"dsn": _dsn(postgres_service)}, extension_config={"events": {"backend": "notify"}}
    )

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    received_a: list[Any] = []
    received_b: list[Any] = []

    async def _handler_a(message: Any) -> None:
        received_a.append(message)

    async def _handler_b(message: Any) -> None:
        received_b.append(message)

    listener_a = None
    listener_b = None
    try:
        listener_a = channel.listen("conc_chan_a", _handler_a, poll_interval=0.05)
        listener_b = channel.listen("conc_chan_b", _handler_b, poll_interval=0.05)
        await asyncio.sleep(0.5)

        for index in range(5):
            await channel.publish("conc_chan_a", {"i": index})
            await channel.publish("conc_chan_b", {"i": index})
            await asyncio.sleep(0.05)

        deadline = asyncio.get_running_loop().time() + 3.0
        while asyncio.get_running_loop().time() < deadline:
            if len(received_a) >= 5 and len(received_b) >= 5:
                break
            if listener_a.task.done() or listener_b.task.done():
                break
            await asyncio.sleep(0.05)

        # Listener tasks must still be alive — InterfaceError races would have killed them.
        if listener_a.task.done():
            exc_a = listener_a.task.exception()
            assert exc_a is None, f"listener_a crashed under concurrency: {exc_a!r}"
        if listener_b.task.done():
            exc_b = listener_b.task.exception()
            assert exc_b is None, f"listener_b crashed under concurrency: {exc_b!r}"
        assert len(received_a) >= 5, f"chan_a delivery degraded under concurrency: {len(received_a)}/5"
        assert len(received_b) >= 5, f"chan_b delivery degraded under concurrency: {len(received_b)}/5"
    finally:
        for listener in (listener_a, listener_b):
            if listener is not None:
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(channel.stop_listener(listener.id), timeout=2.0)
        with contextlib.suppress(Exception):
            await asyncio.wait_for(channel.shutdown(), timeout=2.0)
        if config.connection_instance:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(config.close_pool(), timeout=2.0)
