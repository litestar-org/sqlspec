"""PostgreSQL LISTEN/NOTIFY event channel tests for psqlpy adapter."""

import asyncio
from typing import Any

import pytest

from sqlspec import SQLSpec
from sqlspec.adapters.psqlpy import PsqlpyConfig
from sqlspec.migrations.commands import AsyncMigrationCommands

pytestmark = pytest.mark.xdist_group("postgres")


def _dsn(service: "Any") -> str:
    return f"postgres://{service.user}:{service.password}@{service.host}:{service.port}/{service.database}"


@pytest.mark.asyncio
async def test_psqlpy_listen_notify_native(postgres_service: "Any") -> None:
    """Native LISTEN/NOTIFY path delivers payloads."""

    config = PsqlpyConfig(
        connection_config={"dsn": _dsn(postgres_service)},
        driver_features={"events_backend": "listen_notify", "enable_events": True},
    )

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    received: list[Any] = []

    async def _handler(msg):
        received.append(msg)

    try:
        listener = channel.listen_async("alerts", _handler, poll_interval=0.2)
        await asyncio.sleep(0.1)
        event_id = await channel.publish_async("alerts", {"action": "native"})
        for _ in range(200):
            if received:
                break
            await asyncio.sleep(0.05)
        await channel.stop_listener_async(listener.id)

        assert received, "listener did not receive message"
        message = received[0]
        assert message.event_id == event_id
        assert message.payload["action"] == "native"
    finally:
        backend = getattr(channel, "_backend", None)
        if backend and hasattr(backend, "shutdown_async"):
            await backend.shutdown_async()
        if config.connection_instance:
            await config.close_pool()


@pytest.mark.asyncio
async def test_psqlpy_listen_notify_hybrid(postgres_service: "Any", tmp_path) -> None:
    """Hybrid backend persists then signals via NOTIFY."""

    migrations = tmp_path / "migrations"
    migrations.mkdir()

    config = PsqlpyConfig(
        connection_config={"dsn": _dsn(postgres_service)},
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

    try:
        listener = channel.listen_async("alerts", _handler, poll_interval=0.2)
        await asyncio.sleep(0.1)
        event_id = await channel.publish_async("alerts", {"action": "hybrid"})
        for _ in range(200):
            if received:
                break
            await asyncio.sleep(0.05)
        await channel.stop_listener_async(listener.id)

        assert received, "listener did not receive message"
        message = received[0]
        assert message.event_id == event_id
        assert message.payload["action"] == "hybrid"
    finally:
        backend = getattr(channel, "_backend", None)
        if backend and hasattr(backend, "shutdown_async"):
            await backend.shutdown_async()
        if config.connection_instance:
            await config.close_pool()
