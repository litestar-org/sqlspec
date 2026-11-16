# pyright: reportAttributeAccessIssue=false
"""Tests for the EventChannel queue fallback."""

from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.asyncpg.events.backend import AsyncpgHybridEventsBackend
from sqlspec.adapters.oracledb.events import backend as oracle_backend
from sqlspec.adapters.psqlpy.events.backend import PsqlpyHybridEventsBackend
from sqlspec.adapters.psycopg.events.backend import PsycopgAsyncHybridEventsBackend, PsycopgSyncHybridEventsBackend
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.events import (
    AsyncEventChannel,
    EventMessage,
    EventRuntimeHints,
    SyncEventChannel,
    SyncTableEventQueue,
)
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
        row = driver.select_one("SELECT status FROM app_events WHERE event_id = :event_id", event_id=event_id)
    assert row["status"] == "acked"
    snapshot = spec.telemetry_snapshot()
    assert snapshot.get("SqliteConfig.events.publish") == pytest.approx(1.0)
    assert snapshot.get("SqliteConfig.events.ack") == pytest.approx(1.0)


def test_event_channel_publish_many_sync_roundtrip(tmp_path) -> None:
    """A grouped table-queue publish preserves every independent event envelope."""
    migrations_dir = tmp_path / "migrations_batch_sync"
    migrations_dir.mkdir()
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "events_batch_sync.db")},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
    )
    SyncMigrationCommands(config).upgrade()
    channel = SyncEventChannel(config)
    events = [("notifications", {"index": index}, {"source": "batch"}) for index in range(20)]

    event_ids = channel.publish_many(events)
    iterator = channel.iter_events("notifications", poll_interval=0.01)
    messages = [next(iterator) for _ in events]
    for message in messages:
        channel.ack(message.event_id)

    assert {message.event_id for message in messages} == set(event_ids)
    assert {message.payload["index"] for message in messages} == set(range(20))
    assert all(message.metadata == {"source": "batch"} for message in messages)
    with config.provide_session() as driver:
        acknowledged = driver.select_value(
            "SELECT COUNT(*) FROM sqlspec_event_queue WHERE status = :status", {"status": "acked"}
        )
    assert acknowledged == 20
    config.close_pool()


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


async def test_event_channel_publish_many_async_roundtrip(tmp_path) -> None:
    """The async table queue consumes and acknowledges every grouped event."""
    migrations_dir = tmp_path / "migrations_batch_async"
    migrations_dir.mkdir()
    config = AiosqliteConfig(
        connection_config={"database": str(tmp_path / "events_batch_async.db")},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
    )
    await AsyncMigrationCommands(config).upgrade()
    channel = AsyncEventChannel(config)
    events = [("notifications", {"index": index}, None) for index in range(20)]

    event_ids = await channel.publish_many(events)
    iterator = channel.iter_events("notifications", poll_interval=0.01)
    messages = [await iterator.__anext__() for _ in events]
    await iterator.aclose()
    for message in messages:
        await channel.ack(message.event_id)

    assert {message.event_id for message in messages} == set(event_ids)
    assert {message.payload["index"] for message in messages} == set(range(20))
    async with config.provide_session() as driver:
        acknowledged = await driver.select_value(
            "SELECT COUNT(*) FROM sqlspec_event_queue WHERE status = :status", {"status": "acked"}
        )
    assert acknowledged == 20
    await config.close_pool()


def test_event_channel_backend_fallback(tmp_path) -> None:
    """Unsupported backends fall back to the queue implementation transparently."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    db_path = tmp_path / "events_backend.db"
    config = SqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
        extension_config={"events": {"backend": "aq"}},
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
    assert "FOR UPDATE SKIP LOCKED" in backend._select_statement.upper()


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
    assert "FOR UPDATE SKIP LOCKED" in queue._select_statement.upper()


class _SyncBackend:
    backend_name = "sync-test"

    def __init__(self, *, supports_sync: bool) -> None:
        self.supports_sync = supports_sync
        self.nacked: list[str] = []

    def nack(self, event_id: str) -> None:
        self.nacked.append(event_id)


class _AsyncBackend:
    backend_name = "async-test"

    def __init__(self, *, supports_async: bool) -> None:
        self.supports_async = supports_async
        self.nacked: list[str] = []

    async def nack(self, event_id: str) -> None:
        self.nacked.append(event_id)


def test_channel_nack_guard_sync_event_channel_nack_rejects_backend_without_sync_support(tmp_path) -> None:
    channel = SyncEventChannel(SqliteConfig(connection_config={"database": str(tmp_path / "events.db")}))
    channel._backend = _SyncBackend(supports_sync=False)
    with pytest.raises(ImproperConfigurationError, match="does not support sync nack"):
        channel.nack("event-1")


def test_channel_nack_guard_sync_event_channel_nack_delegates_when_supported(tmp_path) -> None:
    backend = _SyncBackend(supports_sync=True)
    channel = SyncEventChannel(SqliteConfig(connection_config={"database": str(tmp_path / "events.db")}))
    channel._backend = backend
    channel.nack("event-1")
    assert backend.nacked == ["event-1"]


async def test_channel_nack_guard_async_event_channel_nack_rejects_backend_without_async_support(tmp_path) -> None:
    channel = AsyncEventChannel(AiosqliteConfig(connection_config={"database": str(tmp_path / "events.db")}))
    channel._backend = _AsyncBackend(supports_async=False)
    with pytest.raises(ImproperConfigurationError, match="does not support async nack"):
        await channel.nack("event-1")


async def test_channel_nack_guard_async_event_channel_nack_delegates_when_supported(tmp_path) -> None:
    backend = _AsyncBackend(supports_async=True)
    channel = AsyncEventChannel(AiosqliteConfig(connection_config={"database": str(tmp_path / "events.db")}))
    channel._backend = backend
    await channel.nack("event-1")
    assert backend.nacked == ["event-1"]


def test_metrics_snapshot_public(tmp_path) -> None:
    """The sync channel exposes recorded event metrics without private access."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "events_metrics.db")},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
    )
    SyncMigrationCommands(config).upgrade()
    channel = SyncEventChannel(config)

    channel.publish("notifications", {"action": "refresh"})

    snapshot = channel.metrics_snapshot()
    assert snapshot["SqliteConfig.events.publish"] == pytest.approx(1.0)
    assert all(isinstance(value, float) for value in snapshot.values())
    config.close_pool()


async def test_metrics_snapshot_public_async(tmp_path) -> None:
    """The async channel exposes recorded event metrics without private access."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    config = AiosqliteConfig(
        connection_config={"database": str(tmp_path / "events_metrics_async.db")},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
    )
    await AsyncMigrationCommands(config).upgrade()
    channel = AsyncEventChannel(config)

    await channel.publish("notifications", {"action": "refresh"})

    snapshot = channel.metrics_snapshot()
    assert snapshot["AiosqliteConfig.events.publish"] == pytest.approx(1.0)
    assert all(isinstance(value, float) for value in snapshot.values())
    await config.close_pool()


def test_backend_name_public(tmp_path) -> None:
    """Both channel classes report the resolved backend kind."""
    sync_channel = SyncEventChannel(SqliteConfig(connection_config={"database": str(tmp_path / "sync.db")}))
    async_channel = AsyncEventChannel(AiosqliteConfig(connection_config={"database": str(tmp_path / "async.db")}))

    assert sync_channel.backend_name == "poll_queue"
    assert async_channel.backend_name == "poll_queue"


def test_event_channel_forwards_max_attempts_and_retry_backoff_settings(tmp_path) -> None:
    """extension_config['events'] passes max_attempts and retry_backoff_seconds through validation to the queue backend."""
    sync_cfg = SqliteConfig(
        connection_config={"database": str(tmp_path / "retry_sync.db")},
        extension_config={"events": {"max_attempts": 9, "retry_backoff_seconds": 2.5}},
    )
    async_cfg = AiosqliteConfig(
        connection_config={"database": str(tmp_path / "retry_async.db")},
        extension_config={"events": {"max_attempts": 7, "retry_backoff_seconds": 0.25}},
    )
    sync_channel = SyncEventChannel(sync_cfg)
    async_channel = AsyncEventChannel(async_cfg)

    assert sync_channel._backend._max_attempts == 9
    assert sync_channel._backend._retry_backoff_seconds == pytest.approx(2.5)
    assert async_channel._backend._max_attempts == 7
    assert async_channel._backend._retry_backoff_seconds == pytest.approx(0.25)


@pytest.mark.anyio
@pytest.mark.parametrize("backend_kind", ["asyncpg", "psycopg_sync", "psycopg_async", "psqlpy"])
async def test_postgres_hybrid_backends_pass_channel_to_dequeue_by_event_id(backend_kind: str) -> None:
    """All PostgreSQL hybrid notify backends pass (event_id, channel) to dequeue_by_event_id."""
    now = datetime.now(UTC)
    delivered = EventMessage(
        event_id="evt-99",
        channel="billing",
        payload={"ok": True},
        metadata=None,
        attempts=1,
        available_at=now,
        lease_expires_at=now,
        created_at=now,
    )
    recorded_calls: list[tuple[str, str]] = []
    runtime = SimpleNamespace(increment_metric=lambda *_a, **_k: None, record_metric=lambda *_a, **_k: None)

    if backend_kind == "psycopg_sync":

        class _SyncCfg:
            is_async = False

            @staticmethod
            def get_observability_runtime() -> Any:
                return runtime

        _SyncCfg.__module__ = "sqlspec.adapters.psycopg.config"

        class _SyncQueue:
            @staticmethod
            def _reset_empty_poll_delay(_ch: str) -> None:
                return None

            @staticmethod
            def dequeue_by_event_id(event_id: str, channel: str) -> EventMessage:
                recorded_calls.append((event_id, channel))
                return delivered

        sync_backend = PsycopgSyncHybridEventsBackend(_SyncCfg(), _SyncQueue())  # type: ignore[arg-type]
        sync_backend._hub = SimpleNamespace(dequeue=lambda _ch, _pi: '{"event_id":"evt-99"}')
        result = sync_backend.dequeue("billing", 0.1)
    else:

        class _AsyncCfg:
            is_async = True

            @staticmethod
            def get_observability_runtime() -> Any:
                return runtime

        if backend_kind == "asyncpg":
            _AsyncCfg.__module__ = "sqlspec.adapters.asyncpg.config"
            backend_cls: Any = AsyncpgHybridEventsBackend
        elif backend_kind == "psycopg_async":
            _AsyncCfg.__module__ = "sqlspec.adapters.psycopg.config"
            backend_cls = PsycopgAsyncHybridEventsBackend
        else:
            _AsyncCfg.__module__ = "sqlspec.adapters.psqlpy.config"
            backend_cls = PsqlpyHybridEventsBackend

        class _AsyncQueue:
            @staticmethod
            def _reset_empty_poll_delay(_ch: str) -> None:
                return None

            @staticmethod
            async def dequeue_by_event_id(event_id: str, channel: str) -> EventMessage:
                recorded_calls.append((event_id, channel))
                return delivered

        async def _hub_dequeue(_ch: str, _pi: float) -> str:
            return '{"event_id":"evt-99"}'

        async_backend = backend_cls(_AsyncCfg(), _AsyncQueue())
        async_backend._hub = SimpleNamespace(dequeue=_hub_dequeue)
        result = await async_backend.dequeue("billing", 0.1)

    assert result is delivered
    assert recorded_calls == [("evt-99", "billing")]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("backend_cls_name", "is_async"),
    [
        ("OracleSyncAQEventBackend", False),
        ("OracleAsyncAQEventBackend", True),
        ("OracleSyncTxEventQEventBackend", False),
        ("OracleAsyncTxEventQEventBackend", True),
    ],
)
async def test_oracle_aq_backends_publish_normalized_channel_correlation(backend_cls_name: str, is_async: bool) -> None:
    """Oracle sync and async AQ and TxEventQ backends set correlation to the normalized channel on publish."""
    captured_props: list[dict[str, Any]] = []
    enqueued: list[Any] = []

    class _FakeQueue:
        def enqone(self, props: Any) -> Any:
            enqueued.append(props)
            if is_async:

                async def _done() -> None:
                    return None

                return _done()
            return None

    class _FakeConn:
        @staticmethod
        def queue(_name: str, payload_type: str = "JSON") -> _FakeQueue:
            _ = payload_type
            return _FakeQueue()

        @staticmethod
        def msgproperties(**kwargs: Any) -> dict[str, Any]:
            captured_props.append(kwargs)
            return kwargs

    class _OracleCfg:
        def __init__(self, async_flag: bool) -> None:
            self.is_async = async_flag

        @staticmethod
        def get_observability_runtime() -> Any:
            return SimpleNamespace(increment_metric=lambda *_a, **_k: None)

        def provide_session(self) -> Any:
            if self.is_async:

                @asynccontextmanager
                async def _async_sess():
                    async def _commit() -> None:
                        return None

                    yield SimpleNamespace(connection=_FakeConn(), commit=_commit)

                return _async_sess()

            @contextmanager
            def _sync_sess():
                yield SimpleNamespace(connection=_FakeConn(), commit=lambda: None)

            return _sync_sess()

    _OracleCfg.__module__ = "sqlspec.adapters.oracledb.config"
    backend_cls = getattr(oracle_backend, backend_cls_name)
    instance = backend_cls(_OracleCfg(is_async))

    if is_async:
        await instance.publish("OrderEvents", {"id": 1})
    else:
        instance.publish("OrderEvents", {"id": 1})

    assert len(captured_props) == 1
    assert captured_props[0]["correlation"] == "OrderEvents"
    assert len(enqueued) == 1
