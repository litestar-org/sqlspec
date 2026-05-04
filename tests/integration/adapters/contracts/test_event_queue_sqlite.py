# pyright: reportAttributeAccessIssue=false, reportArgumentType=false
"""SQLite-family event queue contract tests."""

from pathlib import Path
from typing import Any

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig, SqliteConnectionParams
from tests.integration.adapters._events_helpers import setup_async_event_channel, setup_sync_event_channel

pytestmark = [pytest.mark.integration, pytest.mark.sqlite, pytest.mark.xdist_group("sqlite")]


def _sqlite_config(tmp_path: Path, database_name: str, *, queue_table: str = "sqlspec_event_queue") -> SqliteConfig:
    migrations_dir = tmp_path / f"{database_name}_migrations"
    migrations_dir.mkdir()
    return SqliteConfig(
        connection_config=SqliteConnectionParams(database=str(tmp_path / database_name)),
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
        extension_config={"events": {"queue_table": queue_table}},
    )


def _aiosqlite_config(
    tmp_path: Path, database_name: str, *, queue_table: str = "sqlspec_event_queue"
) -> AiosqliteConfig:
    migrations_dir = tmp_path / f"{database_name}_migrations"
    migrations_dir.mkdir()
    return AiosqliteConfig(
        connection_config={"database": str(tmp_path / database_name)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
        extension_config={"events": {"queue_table": queue_table}},
    )


def test_sqlite_event_channel_publish_and_consume(tmp_path: Path) -> None:
    """SQLite event channel publishes and consumes events via queue."""
    config = _sqlite_config(tmp_path, "events.db")
    _spec, channel = setup_sync_event_channel(config)

    event_id = channel.publish("notifications", {"action": "test"})
    iterator = channel.iter_events("notifications", poll_interval=0.01)
    message = next(iterator)

    assert message.event_id == event_id
    assert message.payload["action"] == "test"
    assert message.channel == "notifications"


async def test_aiosqlite_event_channel_publish(tmp_path: Path) -> None:
    """Aiosqlite event channel publishes events asynchronously."""
    config = _aiosqlite_config(tmp_path, "async_events.db")
    _spec, channel = await setup_async_event_channel(config)

    event_id = await channel.publish("notifications", {"action": "async_test"})

    async with config.provide_session() as driver:
        row = await driver.select_one(
            "SELECT event_id, channel FROM sqlspec_event_queue WHERE event_id = :event_id", {"event_id": event_id}
        )

    assert row["event_id"] == event_id
    assert row["channel"] == "notifications"

    await config.close_pool()


async def test_aiosqlite_event_channel_consume(tmp_path: Path) -> None:
    """Aiosqlite event channel consumes events asynchronously."""
    config = _aiosqlite_config(tmp_path, "async_consume.db")
    _spec, channel = await setup_async_event_channel(config)

    event_id = await channel.publish("events", {"data": "async_value"})

    generator = channel.iter_events("events", poll_interval=0.01)
    message = await generator.__anext__()
    await generator.aclose()

    assert message.event_id == event_id
    assert message.payload["data"] == "async_value"

    await config.close_pool()


@pytest.mark.parametrize("adapter", [pytest.param("sqlite", id="sqlite"), pytest.param("aiosqlite", id="aiosqlite")])
async def test_sqlite_family_event_channel_ack_updates_status(adapter: str, tmp_path: Path) -> None:
    """Acknowledging an event updates its status to acked."""
    if adapter == "sqlite":
        config = _sqlite_config(tmp_path, "events_ack.db")
        _spec, channel = setup_sync_event_channel(config)
        event_id = channel.publish("alerts", {"level": "info"})
        message = next(channel.iter_events("alerts", poll_interval=0.01))
        channel.ack(message.event_id)
        with config.provide_session() as driver:
            row = driver.select_one(
                "SELECT status FROM sqlspec_event_queue WHERE event_id = :event_id", {"event_id": event_id}
            )
    else:
        config = _aiosqlite_config(tmp_path, "async_ack.db")
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
        await config.close_pool()  # type: ignore[func-returns-value,misc]

    assert row["status"] == "acked"


@pytest.mark.parametrize("adapter", [pytest.param("sqlite", id="sqlite"), pytest.param("aiosqlite", id="aiosqlite")])
async def test_sqlite_family_event_channel_custom_table_name(adapter: str, tmp_path: Path) -> None:
    """Custom queue table name is used for events."""
    if adapter == "sqlite":
        config = _sqlite_config(tmp_path, "custom_events.db", queue_table="app_events")
        _spec, channel = setup_sync_event_channel(config)
        event_id = channel.publish("events", {"custom": True})
        with config.provide_session() as driver:
            row = driver.select_one(
                "SELECT event_id FROM app_events WHERE event_id = :event_id", {"event_id": event_id}
            )
    else:
        config = _aiosqlite_config(tmp_path, "custom_events.db", queue_table="app_events")
        _spec, channel = await setup_async_event_channel(config)
        event_id = await channel.publish("events", {"custom": True})
        async with config.provide_session() as driver:
            row = await driver.select_one(
                "SELECT event_id FROM app_events WHERE event_id = :event_id", {"event_id": event_id}
            )
        await config.close_pool()  # type: ignore[func-returns-value,misc]

    assert row["event_id"] == event_id


@pytest.mark.parametrize("adapter", [pytest.param("sqlite", id="sqlite"), pytest.param("aiosqlite", id="aiosqlite")])
async def test_sqlite_family_event_channel_multiple_channels(adapter: str, tmp_path: Path) -> None:
    """Events are correctly filtered by channel."""
    if adapter == "sqlite":
        config = _sqlite_config(tmp_path, "multi_channel.db")
        _spec, channel = setup_sync_event_channel(config)
        event_id = channel.publish("alerts", {"type": "alert"})
        channel.publish("notifications", {"type": "notification"})
        message = next(channel.iter_events("alerts", poll_interval=0.01))
    else:
        config = _aiosqlite_config(tmp_path, "multi_channel.db")
        _spec, channel = await setup_async_event_channel(config)
        event_id = await channel.publish("alerts", {"type": "alert"})
        await channel.publish("notifications", {"type": "notification"})
        generator = channel.iter_events("alerts", poll_interval=0.01)
        message = await generator.__anext__()
        await generator.aclose()
        await config.close_pool()  # type: ignore[func-returns-value,misc]

    assert message.event_id == event_id
    assert message.payload["type"] == "alert"
    assert message.channel == "alerts"


@pytest.mark.parametrize("adapter", [pytest.param("sqlite", id="sqlite"), pytest.param("aiosqlite", id="aiosqlite")])
async def test_sqlite_family_event_channel_metadata_preserved(adapter: str, tmp_path: Path) -> None:
    """Event metadata is preserved through publish/consume cycle."""
    metadata: dict[str, Any] = {"request_id": "req_abc", "source": "api"}
    if adapter == "sqlite":
        config = _sqlite_config(tmp_path, "metadata.db")
        _spec, channel = setup_sync_event_channel(config)
        event_id = channel.publish("events", {"action": "create"}, metadata=metadata)
        message = next(channel.iter_events("events", poll_interval=0.01))
    else:
        config = _aiosqlite_config(tmp_path, "async_metadata.db")
        _spec, channel = await setup_async_event_channel(config)
        event_id = await channel.publish("events", {"action": "async_meta"}, metadata=metadata)
        generator = channel.iter_events("events", poll_interval=0.01)
        message = await generator.__anext__()
        await generator.aclose()
        await config.close_pool()  # type: ignore[func-returns-value,misc]

    assert message.event_id == event_id
    assert message.metadata is not None
    assert message.metadata["request_id"] == "req_abc"


@pytest.mark.parametrize("adapter", [pytest.param("sqlite", id="sqlite"), pytest.param("aiosqlite", id="aiosqlite")])
async def test_sqlite_family_event_channel_attempts_tracked(adapter: str, tmp_path: Path) -> None:
    """Event attempts counter is incremented on dequeue."""
    if adapter == "sqlite":
        config = _sqlite_config(tmp_path, "attempts.db")
        _spec, channel = setup_sync_event_channel(config)
        event_id = channel.publish("events", {"action": "test"})
        next(channel.iter_events("events", poll_interval=0.01))
        with config.provide_session() as driver:
            row = driver.select_one(
                "SELECT attempts FROM sqlspec_event_queue WHERE event_id = :event_id", {"event_id": event_id}
            )
    else:
        config = _aiosqlite_config(tmp_path, "attempts.db")
        _spec, channel = await setup_async_event_channel(config)
        event_id = await channel.publish("events", {"action": "test"})
        generator = channel.iter_events("events", poll_interval=0.01)
        await generator.__anext__()
        await generator.aclose()
        async with config.provide_session() as driver:
            row = await driver.select_one(
                "SELECT attempts FROM sqlspec_event_queue WHERE event_id = :event_id", {"event_id": event_id}
            )
        await config.close_pool()  # type: ignore[func-returns-value,misc]

    assert row["attempts"] >= 1


@pytest.mark.parametrize("adapter", [pytest.param("sqlite", id="sqlite"), pytest.param("aiosqlite", id="aiosqlite")])
async def test_sqlite_family_event_channel_telemetry(adapter: str, tmp_path: Path) -> None:
    """Event operations are tracked in telemetry."""
    if adapter == "sqlite":
        config = _sqlite_config(tmp_path, "telemetry.db")
        spec, channel = setup_sync_event_channel(config)
        channel.publish("events", {"action": "telemetry_test"})
        message = next(channel.iter_events("events", poll_interval=0.01))
        channel.ack(message.event_id)
        key_prefix = "SqliteConfig"
    else:
        config = _aiosqlite_config(tmp_path, "async_telemetry.db")
        spec, channel = await setup_async_event_channel(config)
        await channel.publish("events", {"track": "async"})
        generator = channel.iter_events("events", poll_interval=0.01)
        message = await generator.__anext__()
        await generator.aclose()
        await channel.ack(message.event_id)
        await config.close_pool()  # type: ignore[func-returns-value,misc]
        key_prefix = "AiosqliteConfig"

    snapshot = spec.telemetry_snapshot()

    assert snapshot.get(f"{key_prefix}.events.publish") == pytest.approx(1.0)
    assert snapshot.get(f"{key_prefix}.events.ack") == pytest.approx(1.0)
