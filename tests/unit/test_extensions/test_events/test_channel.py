"""Tests for the EventChannel queue fallback."""

import pytest

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands


def test_event_channel_publish_and_ack_sync(tmp_path) -> None:
    """EventChannel publishes, yields, and acks rows via the queue table."""

    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    db_path = tmp_path / "events.db"

    config = SqliteConfig(
        pool_config={"database": str(db_path)},
        migration_config={
            "script_location": str(migrations_dir),
            "include_extensions": ["events"],
        },
        extension_config={"events": {"queue_table": "app_events"}},
    )

    commands = SyncMigrationCommands(config)
    commands.upgrade()

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    event_id = channel.publish("notifications", {"action": "refresh"})
    iterator = channel.iter_events("notifications", poll_interval=0.01)
    message = next(iterator)

    assert message.event_id == event_id
    assert message.payload["action"] == "refresh"

    channel.ack(message.event_id)

    with config.provide_session() as driver:
        row = driver.select_one(
            "SELECT status FROM app_events WHERE event_id = :event_id",
            {"event_id": event_id},
        )

    assert row["status"] == "acked"


@pytest.mark.asyncio
async def test_event_channel_async_iteration(tmp_path) -> None:
    """Async adapters can publish and drain events via the iterator helper."""

    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    db_path = tmp_path / "events_async.db"

    config = AiosqliteConfig(
        pool_config={"database": str(db_path)},
        migration_config={
            "script_location": str(migrations_dir),
            "include_extensions": ["events"],
        },
    )

    commands = AsyncMigrationCommands(config)
    await commands.upgrade()

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    event_id = await channel.publish_async("notifications", {"action": "async"})

    generator = channel.iter_events_async("notifications", poll_interval=0.01)
    message = await generator.__anext__()
    await generator.aclose()

    assert message.event_id == event_id
    assert message.payload["action"] == "async"

    await channel.ack_async(message.event_id)

    async with config.provide_session() as driver:
        row = await driver.select_one(
            "SELECT status FROM sqlspec_event_queue WHERE event_id = :event_id",
            {"event_id": event_id},
        )

    assert row["status"] == "acked"


def test_event_channel_backend_fallback(tmp_path) -> None:
    """Unsupported backends fall back to the queue implementation transparently."""

    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    db_path = tmp_path / "events_backend.db"

    config = SqliteConfig(
        pool_config={"database": str(db_path)},
        migration_config={
            "script_location": str(migrations_dir),
            "include_extensions": ["events"],
        },
        driver_features={"events_backend": "oracle_aq"},
    )

    commands = SyncMigrationCommands(config)
    commands.upgrade()

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    event_id = channel.publish("notifications", {"payload": "fallback"})
    iterator = channel.iter_events("notifications", poll_interval=0.01)
    message = next(iterator)
    channel.ack(message.event_id)

    assert message.event_id == event_id

    with config.provide_session() as driver:
        row = driver.select_one(
            "SELECT status FROM sqlspec_event_queue WHERE event_id = :event_id",
            {"event_id": event_id},
        )

    assert row["status"] == "acked"


@pytest.mark.asyncio
async def test_event_channel_portal_bridge_sync_api(tmp_path) -> None:
    """Sync APIs can bridge to async adapters via the portal toggle."""

    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    db_path = tmp_path / "events_portal.db"

    config = AiosqliteConfig(
        pool_config={"database": str(db_path)},
        migration_config={
            "script_location": str(migrations_dir),
            "include_extensions": ["events"],
        },
    )

    commands = AsyncMigrationCommands(config)
    await commands.upgrade()

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    event_id = channel.publish("notifications", {"action": "portal"})

    iterator = channel.iter_events("notifications", poll_interval=0.01)
    message = next(iterator)

    assert message.event_id == event_id
    channel.ack(message.event_id)

    async with config.provide_session() as driver:
        row = await driver.select_one(
            "SELECT status FROM sqlspec_event_queue WHERE event_id = :event_id",
            {"event_id": event_id},
        )

    assert row["status"] == "acked"
