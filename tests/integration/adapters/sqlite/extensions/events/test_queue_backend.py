# pyright: reportArgumentType=false
"""SQLite integration tests for EventChannel with table queue backend."""

import pytest

from sqlspec.adapters.sqlite import SqliteConfig
from tests.integration.adapters._events_helpers import prepare_events_migrations, setup_sync_event_channel


@pytest.mark.integration
@pytest.mark.sqlite
def test_sqlite_event_channel_publish_and_consume(tmp_path) -> None:
    """SQLite event channel publishes and consumes events via queue."""
    migrations_dir = prepare_events_migrations(tmp_path)
    db_path = tmp_path / "events.db"

    config = SqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
    )

    _spec, channel = setup_sync_event_channel(config)

    event_id = channel.publish("notifications", {"action": "test"})
    iterator = channel.iter_events("notifications", poll_interval=0.01)
    message = next(iterator)

    assert message.event_id == event_id
    assert message.payload["action"] == "test"
    assert message.channel == "notifications"


@pytest.mark.integration
@pytest.mark.sqlite
def test_sqlite_event_channel_ack_updates_status(tmp_path) -> None:
    """Acknowledging an event updates its status to acked."""
    migrations_dir = prepare_events_migrations(tmp_path)
    db_path = tmp_path / "events_ack.db"

    config = SqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
    )

    _spec, channel = setup_sync_event_channel(config)

    event_id = channel.publish("alerts", {"level": "info"})
    iterator = channel.iter_events("alerts", poll_interval=0.01)
    message = next(iterator)
    channel.ack(message.event_id)

    with config.provide_session() as driver:
        row = driver.select_one(
            "SELECT status FROM sqlspec_event_queue WHERE event_id = :event_id", {"event_id": event_id}
        )

    assert row["status"] == "acked"


@pytest.mark.integration
@pytest.mark.sqlite
def test_sqlite_event_channel_custom_table_name(tmp_path) -> None:
    """Custom queue table name is used for events."""
    migrations_dir = prepare_events_migrations(tmp_path)
    db_path = tmp_path / "custom_events.db"

    config = SqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
        extension_config={"events": {"queue_table": "app_events"}},
    )

    _spec, channel = setup_sync_event_channel(config)

    event_id = channel.publish("events", {"custom": True})

    with config.provide_session() as driver:
        row = driver.select_one("SELECT event_id FROM app_events WHERE event_id = :event_id", {"event_id": event_id})

    assert row["event_id"] == event_id


@pytest.mark.integration
@pytest.mark.sqlite
def test_sqlite_event_channel_multiple_channels(tmp_path) -> None:
    """Events are correctly filtered by channel."""
    migrations_dir = prepare_events_migrations(tmp_path)
    db_path = tmp_path / "multi_channel.db"

    config = SqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
    )

    _spec, channel = setup_sync_event_channel(config)

    id_alerts = channel.publish("alerts", {"type": "alert"})
    channel.publish("notifications", {"type": "notification"})

    alerts_iter = channel.iter_events("alerts", poll_interval=0.01)
    alert_msg = next(alerts_iter)

    assert alert_msg.event_id == id_alerts
    assert alert_msg.payload["type"] == "alert"
    assert alert_msg.channel == "alerts"


@pytest.mark.integration
@pytest.mark.sqlite
def test_sqlite_event_channel_metadata_preserved(tmp_path) -> None:
    """Event metadata is preserved through publish/consume cycle."""
    migrations_dir = prepare_events_migrations(tmp_path)
    db_path = tmp_path / "metadata.db"

    config = SqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
    )

    _spec, channel = setup_sync_event_channel(config)

    event_id = channel.publish("events", {"action": "create"}, metadata={"user_id": "user_123", "source": "api"})

    iterator = channel.iter_events("events", poll_interval=0.01)
    message = next(iterator)

    assert message.event_id == event_id
    assert message.metadata is not None
    assert message.metadata["user_id"] == "user_123"
    assert message.metadata["source"] == "api"


@pytest.mark.integration
@pytest.mark.sqlite
def test_sqlite_event_channel_attempts_tracked(tmp_path) -> None:
    """Event attempts counter is incremented on dequeue."""
    migrations_dir = prepare_events_migrations(tmp_path)
    db_path = tmp_path / "attempts.db"

    config = SqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
    )

    _spec, channel = setup_sync_event_channel(config)

    event_id = channel.publish("events", {"action": "test"})

    iterator = channel.iter_events("events", poll_interval=0.01)
    next(iterator)

    with config.provide_session() as driver:
        row = driver.select_one(
            "SELECT attempts FROM sqlspec_event_queue WHERE event_id = :event_id", {"event_id": event_id}
        )

    assert row["attempts"] >= 1


@pytest.mark.integration
@pytest.mark.sqlite
def test_sqlite_event_channel_telemetry(tmp_path) -> None:
    """Event operations are tracked in telemetry."""
    migrations_dir = prepare_events_migrations(tmp_path)
    db_path = tmp_path / "telemetry.db"

    config = SqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
    )

    spec, channel = setup_sync_event_channel(config)

    channel.publish("events", {"action": "telemetry_test"})
    iterator = channel.iter_events("events", poll_interval=0.01)
    message = next(iterator)
    channel.ack(message.event_id)

    snapshot = spec.telemetry_snapshot()

    assert snapshot.get("SqliteConfig.events.publish") == pytest.approx(1.0)
    assert snapshot.get("SqliteConfig.events.ack") == pytest.approx(1.0)
