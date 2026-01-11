# pyright: reportArgumentType=false
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
        connection_config={"dsn": _dsn(postgres_service)}, extension_config={"events": {"backend": "listen_notify"}}
    )

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    received: list[Any] = []

    async def _handler(message: Any) -> None:
        received.append(message)

    try:
        listener = channel.listen("alerts", _handler, poll_interval=0.2)
        await asyncio.sleep(0.3)  # Allow listener to subscribe before publishing
        event_id = await channel.publish("alerts", {"action": "native"})
        for _ in range(200):
            if received:
                break
            await asyncio.sleep(0.05)
        await channel.stop_listener(listener.id)

        assert received, "listener did not receive message"
        message = received[0]
        assert message.event_id == event_id
        assert message.payload["action"] == "native"
    finally:
        backend = getattr(channel, "_backend", None)
        if backend and hasattr(backend, "shutdown"):
            await backend.shutdown()
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
        extension_config={"events": {"backend": "listen_notify_durable"}},
    )

    await AsyncMigrationCommands(config).upgrade()

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    received: list[Any] = []

    async def _handler(message: Any) -> None:
        received.append(message)

    try:
        listener = channel.listen("alerts", _handler, poll_interval=0.2)
        await asyncio.sleep(0.3)  # Allow listener to subscribe before publishing
        event_id = await channel.publish("alerts", {"action": "hybrid"})
        for _ in range(200):
            if received:
                break
            await asyncio.sleep(0.05)
        await channel.stop_listener(listener.id)

        assert received, "listener did not receive message"
        message = received[0]
        assert message.event_id == event_id
        assert message.payload["action"] == "hybrid"
    finally:
        backend = getattr(channel, "_backend", None)
        if backend and hasattr(backend, "shutdown"):
            await backend.shutdown()
        if config.connection_instance:
            await config.close_pool()
