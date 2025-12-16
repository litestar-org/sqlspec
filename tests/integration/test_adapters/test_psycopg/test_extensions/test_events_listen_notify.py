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
    return (
        f"postgresql://{service.user}:{service.password}"
        f"@{service.host}:{service.port}/{service.database}"
    )


def test_psycopg_sync_listen_notify(postgres_service: "Any") -> None:
    """Sync psycopg adapter delivers NOTIFY payloads via EventChannel."""

    config = PsycopgSyncConfig(
        connection_config={"conninfo": _conninfo(postgres_service)},
        driver_features={"events_backend": "listen_notify", "enable_events": True},
    )

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)
    assert hasattr(channel._backend, "_ensure_sync_listener")

    received: list[Any] = []
    listener = channel.listen("alerts", lambda msg: received.append(msg), poll_interval=0.2)
    event_id = channel.publish("alerts", {"action": "ping"})
    for _ in range(200):
        if received:
            break
        time.sleep(0.05)
    channel.stop_listener(listener.id)

    assert received, "listener did not receive message"
    message = received[0]

    assert message.event_id == event_id
    assert message.payload["action"] == "ping"


@pytest.mark.asyncio
async def test_psycopg_async_listen_notify(postgres_service: "Any") -> None:
    """Async psycopg adapter delivers NOTIFY payloads via EventChannel."""

    config = PsycopgAsyncConfig(
        connection_config={"conninfo": _conninfo(postgres_service)},
        driver_features={"events_backend": "listen_notify", "enable_events": True},
    )

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    received: list[Any] = []

    async def _handler(msg):
        received.append(msg)

    listener = channel.listen_async("alerts", _handler, poll_interval=0.2)
    event_id = await channel.publish_async("alerts", {"action": "async"})
    for _ in range(200):
        if received:
            break
        await asyncio.sleep(0.05)
    await channel.stop_listener_async(listener.id)

    assert received, "listener did not receive message"
    message = received[0]

    assert message.event_id == event_id
    assert message.payload["action"] == "async"


def test_psycopg_sync_hybrid_listen_notify_durable(postgres_service: "Any", tmp_path) -> None:
    """Hybrid backend stores event durably then notifies listeners (sync)."""

    migrations = tmp_path / "migrations"
    migrations.mkdir()

    config = PsycopgSyncConfig(
        connection_config={"conninfo": _conninfo(postgres_service)},
        migration_config={"script_location": str(migrations), "include_extensions": ["events"]},
        driver_features={"events_backend": "listen_notify_durable", "enable_events": True},
        extension_config={"events": {}},
    )

    SyncMigrationCommands(config).upgrade()

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    received: list[Any] = []
    listener = channel.listen("alerts", lambda msg: received.append(msg), poll_interval=0.2)
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


@pytest.mark.asyncio
async def test_psycopg_async_hybrid_listen_notify_durable(postgres_service: "Any", tmp_path) -> None:
    """Hybrid backend stores event durably then notifies listeners (async)."""

    migrations = tmp_path / "migrations"
    migrations.mkdir()

    config = PsycopgAsyncConfig(
        connection_config={"conninfo": _conninfo(postgres_service)},
        migration_config={"script_location": str(migrations), "include_extensions": ["events"]},
        driver_features={"events_backend": "listen_notify_durable", "enable_events": True},
        extension_config={"events": {}},
    )

    await AsyncMigrationCommands(config).upgrade()

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    received: list[Any] = []

    async def _handler(msg):
        received.append(msg)

    listener = channel.listen_async("alerts", _handler, poll_interval=0.2)
    event_id = await channel.publish_async("alerts", {"action": "hybrid-async"})
    for _ in range(200):
        if received:
            break
        await asyncio.sleep(0.05)
    await channel.stop_listener_async(listener.id)

    assert received, "listener did not receive message"
    message = received[0]

    assert message.event_id == event_id
    assert message.payload["action"] == "hybrid-async"
