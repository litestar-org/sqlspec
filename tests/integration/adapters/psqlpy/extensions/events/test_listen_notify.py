"""PostgreSQL LISTEN/NOTIFY event channel tests for psqlpy adapter."""

import asyncio
from typing import Any

import pytest

from sqlspec import SQLSpec
from sqlspec.adapters.psqlpy import PsqlpyConfig, PsqlpyPoolParams
from sqlspec.migrations.commands import AsyncMigrationCommands

pytestmark = pytest.mark.xdist_group("postgres")


def _dsn(service: "Any") -> str:
    return f"postgres://{service.user}:{service.password}@{service.host}:{service.port}/{service.database}"


async def test_psqlpy_concurrent_same_channel_subscribers(postgres_service: "Any") -> None:
    """Two listeners on the same channel must both receive each NOTIFY.

    Reproduces the latent clear_channel_callbacks race: each dequeue() iteration wipes ALL
    callbacks for the channel, so a concurrent peer's callback gets erased while it is still
    waiting on its asyncio.Event.
    """

    config = PsqlpyConfig(
        connection_config=PsqlpyPoolParams(dsn=_dsn(postgres_service)),
        extension_config={"events": {"backend": "notify"}},
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
        listener_a = channel.listen("psqlpy_shared", _handler_a, poll_interval=0.05)
        listener_b = channel.listen("psqlpy_shared", _handler_b, poll_interval=0.05)
        await asyncio.sleep(0.5)

        for index in range(3):
            await channel.publish("psqlpy_shared", {"i": index})
            await asyncio.sleep(0.1)

        for _ in range(200):
            if received_a and received_b:
                break
            await asyncio.sleep(0.05)

        await channel.stop_listener(listener_a.id)
        await channel.stop_listener(listener_b.id)

        assert received_a, "listener_a never received (clear_channel_callbacks race)"
        assert received_b, "listener_b never received (clear_channel_callbacks race)"
    finally:
        backend = getattr(channel, "_backend", None)
        if backend and hasattr(backend, "shutdown"):
            await backend.shutdown()
        if config.connection_instance:
            await config.close_pool()


async def test_psqlpy_listen_notify_hybrid(postgres_service: "Any", tmp_path) -> None:
    """Hybrid backend persists then signals via NOTIFY."""

    migrations = tmp_path / "migrations"
    migrations.mkdir()

    config = PsqlpyConfig(
        connection_config=PsqlpyPoolParams(dsn=_dsn(postgres_service)),
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
