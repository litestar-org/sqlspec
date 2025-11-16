"""Tests for the EventChannel queue fallback."""

import pytest

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.extensions.events import EventChannel, TableEventQueue
from sqlspec.extensions.events._hints import EventRuntimeHints
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands


class _FakeAsyncmyConfig(AiosqliteConfig):
    """Aiosqlite-based stub that overrides event runtime hints."""

    __slots__ = ()

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        return EventRuntimeHints(poll_interval=0.25, lease_seconds=5, select_for_update=True, skip_locked=True)


_FakeAsyncmyConfig.__module__ = "sqlspec.adapters.asyncmy.config"


class _FakeDuckDBConfig(SqliteConfig):
    """Sqlite-based stub that overrides duckdb event runtime hints."""

    __slots__ = ()

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        return EventRuntimeHints(poll_interval=0.15, lease_seconds=15)


_FakeDuckDBConfig.__module__ = "sqlspec.adapters.duckdb.config"


def test_event_channel_publish_and_ack_sync(tmp_path) -> None:
    """EventChannel publishes, yields, and acks rows via the queue table."""

    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    db_path = tmp_path / "events.db"

    config = SqliteConfig(
        pool_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
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
        row = driver.select_one("SELECT status FROM app_events WHERE event_id = :event_id", {"event_id": event_id})

    assert row["status"] == "acked"

    snapshot = spec.telemetry_snapshot()
    assert snapshot.get("SqliteConfig.events.publish") == pytest.approx(1.0)
    assert snapshot.get("SqliteConfig.events.ack") == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_event_channel_async_iteration(tmp_path) -> None:
    """Async adapters can publish and drain events via the iterator helper."""

    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    db_path = tmp_path / "events_async.db"

    config = AiosqliteConfig(
        pool_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
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
            "SELECT status FROM sqlspec_event_queue WHERE event_id = :event_id", {"event_id": event_id}
        )

    assert row["status"] == "acked"


def test_event_channel_backend_fallback(tmp_path) -> None:
    """Unsupported backends fall back to the queue implementation transparently."""

    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    db_path = tmp_path / "events_backend.db"

    config = SqliteConfig(
        pool_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
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
            "SELECT status FROM sqlspec_event_queue WHERE event_id = :event_id", {"event_id": event_id}
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
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
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
            "SELECT status FROM sqlspec_event_queue WHERE event_id = :event_id", {"event_id": event_id}
        )

    assert row["status"] == "acked"


def test_event_channel_runtime_hints_for_asyncmy(tmp_path) -> None:
    """Asyncmy adapters inherit poll/lease hints and locking flags."""

    db_path = tmp_path / "fake_asyncmy.db"
    config = _FakeAsyncmyConfig(pool_config={"database": str(db_path)})

    channel = EventChannel(config)

    assert channel._poll_interval_default == pytest.approx(0.25)
    assert channel._adapter_name == "asyncmy"

    queue = channel._backend._queue
    assert queue._lease_seconds == 5
    assert queue._select_for_update is True
    assert queue._skip_locked is True


def test_event_channel_runtime_hints_for_duckdb(tmp_path) -> None:
    """DuckDB adapters receive shorter poll intervals by default."""

    config = _FakeDuckDBConfig(pool_config={"database": str(tmp_path / "duck.db")})
    channel = EventChannel(config)

    assert channel._adapter_name == "duckdb"
    assert channel._poll_interval_default == pytest.approx(0.15)


def test_event_channel_extension_config_overrides_hints(tmp_path) -> None:
    """Explicit extension settings take precedence over hint defaults."""

    config = _FakeDuckDBConfig(
        pool_config={"database": str(tmp_path / "duck_override.db")},
        extension_config={"events": {"poll_interval": 3.5, "lease_seconds": 42, "retention_seconds": 99}},
    )

    channel = EventChannel(config)
    assert channel._poll_interval_default == pytest.approx(3.5)

    queue = channel._backend._queue
    assert queue._lease_seconds == 42
    assert queue._retention_seconds == 99


def test_table_event_queue_locking_clause(tmp_path) -> None:
    """Locking hints are embedded when select_for_update/skip_locked are enabled."""

    config = SqliteConfig(pool_config={"database": str(tmp_path / "locks.db")})
    queue = TableEventQueue(config, select_for_update=True, skip_locked=True)

    assert "FOR UPDATE SKIP LOCKED" in queue._select_sql.upper()
