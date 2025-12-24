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


def test_psycopg_sync_listen_notify(postgres_service: "Any") -> None:
    """Sync psycopg adapter delivers NOTIFY payloads via EventChannel."""

    config = PsycopgSyncConfig(
        connection_config={"conninfo": _conninfo(postgres_service)},
        extension_config={"events": {"backend": "listen_notify"}},
    )

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)
    backend = channel._backend
    assert "_ensure_listener" in dir(backend)

    received: list[Any] = []
    listener = channel.listen("alerts", lambda message: received.append(message), poll_interval=0.2)
    time.sleep(0.3)  # Allow listener to subscribe before publishing
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
        extension_config={"events": {"backend": "listen_notify"}},
    )

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    received: list[Any] = []

    async def _handler(message: Any) -> None:
        received.append(message)

    listener = channel.listen("alerts", _handler, poll_interval=0.2)
    await asyncio.sleep(0.3)  # Allow listener to subscribe before publishing
    event_id = await channel.publish("alerts", {"action": "async"})
    for _ in range(200):
        if received:
            break
        await asyncio.sleep(0.05)
    await channel.stop_listener(listener.id)

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
        extension_config={"events": {"backend": "listen_notify_durable"}},
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


@pytest.mark.asyncio
async def test_psycopg_async_hybrid_listen_notify_durable(postgres_service: "Any", tmp_path) -> None:
    """Hybrid backend stores event durably then notifies listeners (async)."""

    migrations = tmp_path / "migrations"
    migrations.mkdir()

    config = PsycopgAsyncConfig(
        connection_config={"conninfo": _conninfo(postgres_service)},
        migration_config={"script_location": str(migrations), "include_extensions": ["events"]},
        extension_config={"events": {"backend": "listen_notify_durable"}},
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
