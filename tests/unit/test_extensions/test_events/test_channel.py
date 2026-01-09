# pyright: reportPrivateUsage=false, reportAttributeAccessIssue=false, reportArgumentType=false
"""Tests for the EventChannel queue fallback."""

import pytest

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.extensions.events import AsyncEventChannel, EventRuntimeHints, SyncEventChannel, SyncTableEventQueue
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
        connection_config={"database": str(db_path)},
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
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
    )

    commands = AsyncMigrationCommands(config)
    await commands.upgrade()

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    event_id = await channel.publish("notifications", {"action": "async"})

    generator = channel.iter_events("notifications", poll_interval=0.01)
    message = await generator.__anext__()
    await generator.aclose()

    assert message.event_id == event_id
    assert message.payload["action"] == "async"

    await channel.ack(message.event_id)

    async with config.provide_session() as driver:
        row = await driver.select_one(
            "SELECT status FROM sqlspec_event_queue WHERE event_id = :event_id", {"event_id": event_id}
        )

    assert row["status"] == "acked"
    await config.close_pool()


def test_event_channel_backend_fallback(tmp_path) -> None:
    """Unsupported backends fall back to the queue implementation transparently."""

    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    db_path = tmp_path / "events_backend.db"

    config = SqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
        extension_config={"events": {"backend": "advanced_queue"}},
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
    """Async adapters publish and consume events via the event_channel helper."""

    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    db_path = tmp_path / "events_portal.db"

    config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
    )

    commands = AsyncMigrationCommands(config)
    await commands.upgrade()

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    event_id = await channel.publish("notifications", {"action": "portal"})

    iterator = channel.iter_events("notifications", poll_interval=0.01)
    message = await iterator.__anext__()
    await iterator.aclose()

    assert message.event_id == event_id
    await channel.ack(message.event_id)

    async with config.provide_session() as driver:
        row = await driver.select_one(
            "SELECT status FROM sqlspec_event_queue WHERE event_id = :event_id", {"event_id": event_id}
        )

    assert row["status"] == "acked"
    await config.close_pool()


def test_event_channel_runtime_hints_for_asyncmy(tmp_path) -> None:
    """Asyncmy adapters inherit poll/lease hints and locking flags."""

    db_path = tmp_path / "fake_asyncmy.db"
    config = _FakeAsyncmyConfig(connection_config={"database": str(db_path)})

    channel = AsyncEventChannel(config)

    assert channel._poll_interval_default == pytest.approx(0.25)
    assert channel._adapter_name == "asyncmy"

    backend = channel._backend
    assert backend._lease_seconds == 5
    assert "FOR UPDATE SKIP LOCKED" in backend._select_sql.upper()


def test_event_channel_runtime_hints_for_duckdb(tmp_path) -> None:
    """DuckDB adapters receive shorter poll intervals by default."""

    config = _FakeDuckDBConfig(connection_config={"database": str(tmp_path / "duck.db")})
    channel = SyncEventChannel(config)

    assert channel._adapter_name == "duckdb"
    assert channel._poll_interval_default == pytest.approx(0.15)


def test_event_channel_extension_config_overrides_hints(tmp_path) -> None:
    """Explicit extension settings take precedence over hint defaults."""

    config = _FakeDuckDBConfig(
        connection_config={"database": str(tmp_path / "duck_override.db")},
        extension_config={"events": {"poll_interval": 3.5, "lease_seconds": 42, "retention_seconds": 99}},
    )

    channel = SyncEventChannel(config)
    assert channel._poll_interval_default == pytest.approx(3.5)

    backend = channel._backend
    assert backend._lease_seconds == 42
    assert backend._retention_seconds == 99


def test_table_event_queue_locking_clause(tmp_path) -> None:
    """Locking hints are embedded when select_for_update/skip_locked are enabled."""

    config = SqliteConfig(connection_config={"database": str(tmp_path / "locks.db")})
    queue = SyncTableEventQueue(config, select_for_update=True, skip_locked=True)

    assert "FOR UPDATE SKIP LOCKED" in queue._select_sql.upper()
