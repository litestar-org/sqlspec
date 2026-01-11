# pyright: reportAttributeAccessIssue=false, reportArgumentType=false
"""AioSQLite integration tests for EventChannel with async table queue backend."""

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from tests.integration.adapters._events_helpers import prepare_events_migrations, setup_async_event_channel


@pytest.mark.integration
@pytest.mark.asyncio
async def test_aiosqlite_event_channel_publish(tmp_path) -> None:
    """Aiosqlite event channel publishes events asynchronously."""
    migrations_dir = prepare_events_migrations(tmp_path)
    db_path = tmp_path / "async_events.db"

    config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
    )

    _spec, channel = await setup_async_event_channel(config)

    event_id = await channel.publish("notifications", {"action": "async_test"})

    async with config.provide_session() as driver:
        row = await driver.select_one(
            "SELECT event_id, channel FROM sqlspec_event_queue WHERE event_id = :event_id", {"event_id": event_id}
        )

    assert row["event_id"] == event_id
    assert row["channel"] == "notifications"

    await config.close_pool()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_aiosqlite_event_channel_consume(tmp_path) -> None:
    """Aiosqlite event channel consumes events asynchronously."""
    migrations_dir = prepare_events_migrations(tmp_path)
    db_path = tmp_path / "async_consume.db"

    config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
    )

    _spec, channel = await setup_async_event_channel(config)

    event_id = await channel.publish("events", {"data": "async_value"})

    generator = channel.iter_events("events", poll_interval=0.01)
    message = await generator.__anext__()
    await generator.aclose()

    assert message.event_id == event_id
    assert message.payload["data"] == "async_value"

    await config.close_pool()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_aiosqlite_event_channel_ack(tmp_path) -> None:
    """Aiosqlite event channel acknowledges events asynchronously."""
    migrations_dir = prepare_events_migrations(tmp_path)
    db_path = tmp_path / "async_ack.db"

    config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
    )

    _spec, channel = await setup_async_event_channel(config)

    event_id = await channel.publish("alerts", {"priority": "high"})

    generator = channel.iter_events("alerts", poll_interval=0.01)
    message = await generator.__anext__()
    await generator.aclose()

    await channel.ack(message.event_id)

    async with config.provide_session() as driver:
        row = await driver.select_one(
            "SELECT status FROM sqlspec_event_queue WHERE event_id = :event_id", {"event_id": event_id}
        )

    assert row["status"] == "acked"

    await config.close_pool()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_aiosqlite_event_channel_metadata(tmp_path) -> None:
    """Aiosqlite event channel preserves metadata in async operations."""
    migrations_dir = prepare_events_migrations(tmp_path)
    db_path = tmp_path / "async_metadata.db"

    config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
    )

    _spec, channel = await setup_async_event_channel(config)

    event_id = await channel.publish(
        "events", {"action": "async_meta"}, metadata={"request_id": "req_abc", "timestamp": "2024-01-15T10:00:00Z"}
    )

    generator = channel.iter_events("events", poll_interval=0.01)
    message = await generator.__anext__()
    await generator.aclose()

    assert message.event_id == event_id
    assert message.metadata is not None
    assert message.metadata["request_id"] == "req_abc"

    await config.close_pool()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_aiosqlite_event_channel_telemetry(tmp_path) -> None:
    """Aiosqlite event operations are tracked in telemetry."""
    migrations_dir = prepare_events_migrations(tmp_path)
    db_path = tmp_path / "async_telemetry.db"

    config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
    )

    spec, channel = await setup_async_event_channel(config)

    await channel.publish("events", {"track": "async"})
    generator = channel.iter_events("events", poll_interval=0.01)
    message = await generator.__anext__()
    await generator.aclose()
    await channel.ack(message.event_id)

    snapshot = spec.telemetry_snapshot()

    assert snapshot.get("AiosqliteConfig.events.publish") == pytest.approx(1.0)
    assert snapshot.get("AiosqliteConfig.events.ack") == pytest.approx(1.0)

    await config.close_pool()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_aiosqlite_event_channel_custom_table_name(tmp_path) -> None:
    """Custom queue table name is used for events."""
    migrations_dir = prepare_events_migrations(tmp_path)
    db_path = tmp_path / "custom_events.db"

    config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
        extension_config={"events": {"queue_table": "app_events"}},
    )

    _spec, channel = await setup_async_event_channel(config)

    event_id = await channel.publish("events", {"custom": True})

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
    migrations_dir = prepare_events_migrations(tmp_path)
    db_path = tmp_path / "multi_channel.db"

    config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
    )

    _spec, channel = await setup_async_event_channel(config)

    id_alerts = await channel.publish("alerts", {"type": "alert"})
    await channel.publish("notifications", {"type": "notification"})

    generator = channel.iter_events("alerts", poll_interval=0.01)
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
    migrations_dir = prepare_events_migrations(tmp_path)
    db_path = tmp_path / "attempts.db"

    config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
    )

    _spec, channel = await setup_async_event_channel(config)

    event_id = await channel.publish("events", {"action": "test"})

    generator = channel.iter_events("events", poll_interval=0.01)
    await generator.__anext__()
    await generator.aclose()

    async with config.provide_session() as driver:
        row = await driver.select_one(
            "SELECT attempts FROM sqlspec_event_queue WHERE event_id = :event_id", {"event_id": event_id}
        )

    assert row["attempts"] >= 1

    await config.close_pool()
