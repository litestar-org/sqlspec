"""AioSQLite integration tests for EventChannel with async table queue backend."""

import pytest

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.migrations.commands import AsyncMigrationCommands


@pytest.mark.integration
@pytest.mark.asyncio
async def test_aiosqlite_event_channel_publish_async(tmp_path) -> None:
    """Aiosqlite event channel publishes events asynchronously."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    db_path = tmp_path / "async_events.db"

    config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
    )

    commands = AsyncMigrationCommands(config)
    await commands.upgrade()

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    event_id = await channel.publish_async("notifications", {"action": "async_test"})

    async with config.provide_session() as driver:
        row = await driver.select_one(
            "SELECT event_id, channel FROM sqlspec_event_queue WHERE event_id = :event_id", {"event_id": event_id}
        )

    assert row["event_id"] == event_id
    assert row["channel"] == "notifications"

    await config.close_pool()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_aiosqlite_event_channel_consume_async(tmp_path) -> None:
    """Aiosqlite event channel consumes events asynchronously."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    db_path = tmp_path / "async_consume.db"

    config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
    )

    commands = AsyncMigrationCommands(config)
    await commands.upgrade()

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    event_id = await channel.publish_async("events", {"data": "async_value"})

    generator = channel.iter_events_async("events", poll_interval=0.01)
    message = await generator.__anext__()
    await generator.aclose()

    assert message.event_id == event_id
    assert message.payload["data"] == "async_value"

    await config.close_pool()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_aiosqlite_event_channel_ack_async(tmp_path) -> None:
    """Aiosqlite event channel acknowledges events asynchronously."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    db_path = tmp_path / "async_ack.db"

    config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
    )

    commands = AsyncMigrationCommands(config)
    await commands.upgrade()

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    event_id = await channel.publish_async("alerts", {"priority": "high"})

    generator = channel.iter_events_async("alerts", poll_interval=0.01)
    message = await generator.__anext__()
    await generator.aclose()

    await channel.ack_async(message.event_id)

    async with config.provide_session() as driver:
        row = await driver.select_one(
            "SELECT status FROM sqlspec_event_queue WHERE event_id = :event_id", {"event_id": event_id}
        )

    assert row["status"] == "acked"

    await config.close_pool()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_aiosqlite_event_channel_portal_bridge_publish(tmp_path) -> None:
    """Aiosqlite portal bridge allows sync publish from async config."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    db_path = tmp_path / "portal_publish.db"

    config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
    )

    commands = AsyncMigrationCommands(config)
    await commands.upgrade()

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    event_id = channel.publish_sync("notifications", {"via": "portal"})

    async with config.provide_session() as driver:
        row = await driver.select_one(
            "SELECT event_id FROM sqlspec_event_queue WHERE event_id = :event_id", {"event_id": event_id}
        )

    assert row["event_id"] == event_id

    await config.close_pool()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_aiosqlite_event_channel_portal_bridge_consume(tmp_path) -> None:
    """Aiosqlite portal bridge allows sync consume from async config."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    db_path = tmp_path / "portal_consume.db"

    config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
    )

    commands = AsyncMigrationCommands(config)
    await commands.upgrade()

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    event_id = channel.publish_sync("events", {"bridge": "consume"})
    iterator = channel.iter_events_sync("events", poll_interval=0.01)
    message = next(iterator)

    assert message.event_id == event_id
    assert message.payload["bridge"] == "consume"

    await config.close_pool()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_aiosqlite_event_channel_portal_bridge_ack(tmp_path) -> None:
    """Aiosqlite portal bridge allows sync ack from async config."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    db_path = tmp_path / "portal_ack.db"

    config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
    )

    commands = AsyncMigrationCommands(config)
    await commands.upgrade()

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    event_id = channel.publish_sync("events", {"bridge": "ack"})
    iterator = channel.iter_events_sync("events", poll_interval=0.01)
    message = next(iterator)
    channel.ack_sync(message.event_id)

    async with config.provide_session() as driver:
        row = await driver.select_one(
            "SELECT status FROM sqlspec_event_queue WHERE event_id = :event_id", {"event_id": event_id}
        )

    assert row["status"] == "acked"

    await config.close_pool()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_aiosqlite_event_channel_metadata_async(tmp_path) -> None:
    """Aiosqlite event channel preserves metadata in async operations."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    db_path = tmp_path / "async_metadata.db"

    config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
    )

    commands = AsyncMigrationCommands(config)
    await commands.upgrade()

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    event_id = await channel.publish_async(
        "events", {"action": "async_meta"}, metadata={"request_id": "req_abc", "timestamp": "2024-01-15T10:00:00Z"}
    )

    generator = channel.iter_events_async("events", poll_interval=0.01)
    message = await generator.__anext__()
    await generator.aclose()

    assert message.event_id == event_id
    assert message.metadata is not None
    assert message.metadata["request_id"] == "req_abc"

    await config.close_pool()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_aiosqlite_event_channel_telemetry_async(tmp_path) -> None:
    """Aiosqlite event operations are tracked in telemetry."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    db_path = tmp_path / "async_telemetry.db"

    config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
    )

    commands = AsyncMigrationCommands(config)
    await commands.upgrade()

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    await channel.publish_async("events", {"track": "async"})
    generator = channel.iter_events_async("events", poll_interval=0.01)
    message = await generator.__anext__()
    await generator.aclose()
    await channel.ack_async(message.event_id)

    snapshot = spec.telemetry_snapshot()

    assert snapshot.get("AiosqliteConfig.events.publish") == pytest.approx(1.0)
    assert snapshot.get("AiosqliteConfig.events.ack") == pytest.approx(1.0)

    await config.close_pool()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_aiosqlite_event_channel_custom_table_name(tmp_path) -> None:
    """Custom queue table name is used for events."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    db_path = tmp_path / "custom_events.db"

    config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
        extension_config={"events": {"queue_table": "app_events"}},
    )

    commands = AsyncMigrationCommands(config)
    await commands.upgrade()

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    event_id = await channel.publish_async("events", {"custom": True})

    async with config.provide_session() as driver:
        row = await driver.select_one(
            "SELECT event_id FROM app_events WHERE event_id = :event_id", {"event_id": event_id}
        )

    assert row["event_id"] == event_id

    await config.close_pool()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_aiosqlite_event_channel_multiple_channels(tmp_path) -> None:
    """Events are correctly filtered by channel."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    db_path = tmp_path / "multi_channel.db"

    config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
    )

    commands = AsyncMigrationCommands(config)
    await commands.upgrade()

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    id_alerts = await channel.publish_async("alerts", {"type": "alert"})
    await channel.publish_async("notifications", {"type": "notification"})

    generator = channel.iter_events_async("alerts", poll_interval=0.01)
    alert_msg = await generator.__anext__()
    await generator.aclose()

    assert alert_msg.event_id == id_alerts
    assert alert_msg.payload["type"] == "alert"
    assert alert_msg.channel == "alerts"

    await config.close_pool()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_aiosqlite_event_channel_attempts_tracked(tmp_path) -> None:
    """Event attempts counter is incremented on dequeue."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    db_path = tmp_path / "attempts.db"

    config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
    )

    commands = AsyncMigrationCommands(config)
    await commands.upgrade()

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    event_id = await channel.publish_async("events", {"action": "test"})

    generator = channel.iter_events_async("events", poll_interval=0.01)
    await generator.__anext__()
    await generator.aclose()

    async with config.provide_session() as driver:
        row = await driver.select_one(
            "SELECT attempts FROM sqlspec_event_queue WHERE event_id = :event_id", {"event_id": event_id}
        )

    assert row["attempts"] >= 1

    await config.close_pool()
