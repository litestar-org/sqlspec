# pyright: reportPrivateUsage=false
"""PostgreSQL LISTEN/NOTIFY event channel tests for asyncpg adapter."""

import asyncio
from typing import Any

import pytest

from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.migrations.commands import AsyncMigrationCommands

pytestmark = pytest.mark.xdist_group("postgres")


def _dsn(service: "Any") -> str:
    return f"postgresql://{service.user}:{service.password}@{service.host}:{service.port}/{service.database}"


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_asyncpg_listen_notify_publish_and_ack(postgres_service: "Any") -> None:
    """AsyncPG adapter publishes and acknowledges LISTEN/NOTIFY events."""

    config = AsyncpgConfig(
        connection_config={"dsn": _dsn(postgres_service)}, extension_config={"events": {"backend": "listen_notify"}}
    )

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    assert channel._backend_name == "listen_notify"

    event_id = await channel.publish("alerts", {"action": "test"})
    assert event_id is not None
    assert len(event_id) == 32

    await channel.ack(event_id)

    if config.connection_instance:
        await config.close_pool()


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_asyncpg_listen_notify_message_delivery(postgres_service: "Any") -> None:
    """AsyncPG adapter delivers NOTIFY payloads via EventChannel listener."""

    config = AsyncpgConfig(
        connection_config={"dsn": _dsn(postgres_service)}, extension_config={"events": {"backend": "listen_notify"}}
    )

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    received: list[Any] = []

    async def _handler(message: Any) -> None:
        received.append(message)

    listener = channel.listen("notifications", _handler, poll_interval=0.2)
    await asyncio.sleep(0.3)  # Allow listener to subscribe before publishing
    event_id = await channel.publish("notifications", {"action": "async_delivery"})

    for _ in range(200):
        if received:
            break
        await asyncio.sleep(0.05)

    await channel.stop_listener(listener.id)

    assert received, "listener did not receive message"
    message = received[0]
    assert message.event_id == event_id
    assert message.payload["action"] == "async_delivery"

    await channel.shutdown()
    if config.connection_instance:
        await config.close_pool()


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_asyncpg_hybrid_listen_notify_durable(postgres_service: "Any", tmp_path: Any) -> None:
    """Hybrid backend stores event durably then notifies listeners."""

    migrations = tmp_path / "migrations"
    migrations.mkdir()

    config = AsyncpgConfig(
        connection_config={"dsn": _dsn(postgres_service)},
        migration_config={"script_location": str(migrations), "include_extensions": ["events"]},
        extension_config={"events": {"backend": "listen_notify_durable"}},
    )

    await AsyncMigrationCommands(config).upgrade()

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    assert channel._backend_name == "listen_notify_durable"

    received: list[Any] = []

    async def _handler(message: Any) -> None:
        received.append(message)

    listener = channel.listen("alerts", _handler, poll_interval=0.2)
    await asyncio.sleep(0.3)  # Allow listener to subscribe before publishing
    event_id = await channel.publish("alerts", {"action": "hybrid_async"})

    for _ in range(200):
        if received:
            break
        await asyncio.sleep(0.05)

    await channel.stop_listener(listener.id)

    assert received, "listener did not receive message"
    message = received[0]
    assert message.event_id == event_id
    assert message.payload["action"] == "hybrid_async"

    await channel.shutdown()
    if config.connection_instance:
        await config.close_pool()


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_asyncpg_listen_notify_metadata(postgres_service: "Any") -> None:
    """AsyncPG adapter preserves metadata in LISTEN/NOTIFY events."""

    config = AsyncpgConfig(
        connection_config={"dsn": _dsn(postgres_service)}, extension_config={"events": {"backend": "listen_notify"}}
    )

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    received: list[Any] = []

    async def _handler(message: Any) -> None:
        received.append(message)

    listener = channel.listen("meta_channel", _handler, poll_interval=0.2)
    await asyncio.sleep(0.3)  # Allow listener to subscribe before publishing
    event_id = await channel.publish(
        "meta_channel", {"action": "with_metadata"}, metadata={"source": "test", "priority": 1}
    )

    for _ in range(200):
        if received:
            break
        await asyncio.sleep(0.05)

    await channel.stop_listener(listener.id)

    assert received, "listener did not receive message"
    message = received[0]
    assert message.event_id == event_id
    assert message.metadata is not None
    assert message.metadata["source"] == "test"
    assert message.metadata["priority"] == 1

    await channel.shutdown()
    if config.connection_instance:
        await config.close_pool()
