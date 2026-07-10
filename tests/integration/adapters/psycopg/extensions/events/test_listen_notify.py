# pyright: reportPrivateUsage=false
"""PostgreSQL LISTEN/NOTIFY event channel tests for psycopg adapters."""

import asyncio
import time
from typing import Any

import pytest

from sqlspec import SQLSpec
from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands

pytestmark = pytest.mark.xdist_group("postgres")


def _conninfo(service: "Any") -> str:
    return f"postgresql://{service.user}:{service.password}@{service.host}:{service.port}/{service.database}"


def test_psycopg_sync_hybrid_listen_notify_durable(postgres_service: "Any", tmp_path) -> None:
    """Hybrid backend stores event durably then notifies listeners (sync)."""

    migrations = tmp_path / "migrations"
    migrations.mkdir()

    config = PsycopgSyncConfig(
        connection_config={"conninfo": _conninfo(postgres_service)},
        migration_config={"script_location": str(migrations), "include_extensions": ["events"]},
        extension_config={"events": {"backend": "notify_queue"}},
    )

    SyncMigrationCommands(config).upgrade()

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    received: list[Any] = []
    listener = channel.listen("alerts", lambda message: received.append(message), poll_interval=0.2)
    time.sleep(0.3)  # Allow listener to subscribe before publishing
    event_id = channel.publish("alerts", {"action": "hybrid"})
    for _ in range(200):
        if received:
            break
        time.sleep(0.05)
    channel.stop_listener(listener.id)

    assert received, "listener did not receive message"
    message = received[0]

    assert message.event_id == event_id
    assert message.payload["action"] == "hybrid"


async def test_psycopg_async_multi_channel_listen_delivery(postgres_service: "Any") -> None:
    """Subscribing to two channels on the same backend must emit LISTEN for both.

    Reproduces the latent bug: PsycopgAsyncEventsBackend._ensure_listener emits LISTEN only
    when self._listen_connection is None — the second channel never gets LISTEN, so its
    notifications are silently dropped.
    """

    config = PsycopgAsyncConfig(
        connection_config={"conninfo": _conninfo(postgres_service)}, extension_config={"events": {"backend": "notify"}}
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

    try:
        listener_a = channel.listen("psyc_chan_a", _handler_a, poll_interval=0.2)
        listener_b = channel.listen("psyc_chan_b", _handler_b, poll_interval=0.2)
        await asyncio.sleep(0.5)

        await channel.publish("psyc_chan_a", {"action": "to_a"})
        await channel.publish("psyc_chan_b", {"action": "to_b"})

        for _ in range(200):
            if received_a and received_b:
                break
            await asyncio.sleep(0.05)

        await channel.stop_listener(listener_a.id)
        await channel.stop_listener(listener_b.id)

        assert received_a, "channel A delivery failed"
        assert received_b, "channel B delivery failed (multi-channel LISTEN drop bug)"
    finally:
        await channel.shutdown()


def test_psycopg_sync_multi_channel_listen_delivery(postgres_service: "Any") -> None:
    """Sync psycopg: same multi-channel LISTEN drop bug as the async variant."""

    config = PsycopgSyncConfig(
        connection_config={"conninfo": _conninfo(postgres_service)}, extension_config={"events": {"backend": "notify"}}
    )

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    received_a: list[Any] = []
    received_b: list[Any] = []

    def _handler_a(message: Any) -> None:
        received_a.append(message)

    def _handler_b(message: Any) -> None:
        received_b.append(message)

    try:
        listener_a = channel.listen("psyc_sync_a", _handler_a, poll_interval=0.2)
        listener_b = channel.listen("psyc_sync_b", _handler_b, poll_interval=0.2)
        time.sleep(0.5)

        channel.publish("psyc_sync_a", {"action": "to_a"})
        channel.publish("psyc_sync_b", {"action": "to_b"})

        for _ in range(200):
            if received_a and received_b:
                break
            time.sleep(0.05)

        channel.stop_listener(listener_a.id)
        channel.stop_listener(listener_b.id)

        assert received_a, "channel A delivery failed"
        assert received_b, "channel B delivery failed (multi-channel LISTEN drop bug)"
    finally:
        channel.shutdown()


async def test_psycopg_async_hybrid_listen_notify_durable(postgres_service: "Any", tmp_path) -> None:
    """Hybrid backend stores event durably then notifies listeners (async)."""

    migrations = tmp_path / "migrations"
    migrations.mkdir()

    config = PsycopgAsyncConfig(
        connection_config={"conninfo": _conninfo(postgres_service)},
        migration_config={"script_location": str(migrations), "include_extensions": ["events"]},
        extension_config={"events": {"backend": "notify_queue"}},
    )

    await AsyncMigrationCommands(config).upgrade()

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    received: list[Any] = []

    async def _handler(message: Any) -> None:
        received.append(message)

    listener = channel.listen("alerts", _handler, poll_interval=0.2)
    await asyncio.sleep(0.3)  # Allow listener to subscribe before publishing
    event_id = await channel.publish("alerts", {"action": "hybrid-async"})
    for _ in range(200):
        if received:
            break
        await asyncio.sleep(0.05)
    await channel.stop_listener(listener.id)

    assert received, "listener did not receive message"
    message = received[0]

    assert message.event_id == event_id
    assert message.payload["action"] == "hybrid-async"
