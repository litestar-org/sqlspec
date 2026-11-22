# pyright: reportPrivateUsage=false, reportAttributeAccessIssue=false, reportArgumentType=false
"""Extended unit tests for EventChannel configuration and backend selection."""

import asyncio
import importlib
import logging
import threading
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

import pytest

from sqlspec import ObservabilityRuntime
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.spanner import SpannerSyncConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import EventChannelError, ImproperConfigurationError, MigrationError
from sqlspec.extensions.events import (
    AsyncEventChannel,
    AsyncEventListener,
    BaseEventQueueStore,
    EventMessage,
    SyncEventChannel,
    SyncEventListener,
)
from sqlspec.migrations.context import MigrationContext

if TYPE_CHECKING:
    from sqlspec.config import AsyncDatabaseConfig, SyncDatabaseConfig


def _run_async(coro: "Any") -> "Any":
    """Run a coroutine in a dedicated event loop."""

    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def test_event_channel_adapter_name_resolution(tmp_path) -> None:
    """EventChannel resolves adapter name from config module path."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    channel = SyncEventChannel(config)

    assert channel._adapter_name == "sqlite"


async def test_async_event_listener_stop_cancels_blocked_wait() -> None:
    stop_event = asyncio.Event()
    task = asyncio.create_task(asyncio.Event().wait())
    listener = AsyncEventListener("listener", "events", task, stop_event, 60.0)

    await asyncio.wait_for(listener.stop(), timeout=0.1)

    assert stop_event.is_set()
    assert task.cancelled()


def test_sync_event_listener_stop_uses_bounded_join(monkeypatch: pytest.MonkeyPatch) -> None:
    blocker = threading.Event()
    thread = threading.Thread(target=blocker.wait, daemon=True)
    thread.start()
    stop_event = threading.Event()
    listener = SyncEventListener("listener", "events", thread, stop_event, 60.0)
    monkeypatch.setattr("sqlspec.extensions.events._channel._LISTENER_SHUTDOWN_TIMEOUT", 0.01)

    listener.stop()

    assert stop_event.is_set()
    assert thread.is_alive()
    blocker.set()
    thread.join(timeout=0.1)


def test_event_channel_default_poll_interval(tmp_path) -> None:
    """EventChannel uses hint default poll interval."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    channel = SyncEventChannel(config)

    assert channel._poll_interval_default == 1.0


def test_event_channel_custom_poll_interval(tmp_path) -> None:
    """Extension settings override default poll interval."""
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "test.db")}, extension_config={"events": {"poll_interval": 0.5}}
    )
    channel = SyncEventChannel(config)

    assert channel._poll_interval_default == 0.5


def test_event_channel_event_poll_interval_takes_precedence(tmp_path) -> None:
    """The explicit event interval wins over the legacy poll_interval input."""
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "event_poll.db")},
        extension_config={"events": {"event_poll_interval": 2.5, "poll_interval": 0.1}},
    )

    channel = SyncEventChannel(config)

    assert channel._event_poll_interval == 2.5
    assert channel._poll_interval_default == 2.5
    snapshot = channel._runtime.metrics_snapshot()
    assert snapshot["SqliteConfig.events.poll.interval"] == pytest.approx(2.5)
    assert snapshot["SqliteConfig.events.backend.poll_queue.resolved"] == pytest.approx(1.0)


def test_event_channel_backend_name_poll_queue(tmp_path) -> None:
    """EventChannel defaults to the durable polling queue backend."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    channel = SyncEventChannel(config)

    assert channel.backend_name == "poll_queue"


def test_event_channel_rejects_unknown_backend(tmp_path) -> None:
    """Unknown backend names must not silently change delivery semantics."""
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "test.db")},
        extension_config={"events": {"backend": "nonexistent_backend"}},
    )
    with pytest.raises(ImproperConfigurationError, match="Unknown event backend"):
        SyncEventChannel(config)


@pytest.mark.parametrize(
    ("retired", "replacement"),
    (("listen_notify", "notify"), ("listen_notify_durable", "notify_queue"), ("table_queue", "poll_queue")),
)
def test_event_channel_rejects_retired_backend_with_migration_guidance(
    tmp_path, retired: str, replacement: str
) -> None:
    """Retired backend names fail explicitly with their canonical replacement."""
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "test.db")}, extension_config={"events": {"backend": retired}}
    )

    with pytest.raises(ImproperConfigurationError, match=rf"use '{replacement}'"):
        SyncEventChannel(config)


def test_event_channel_honors_driver_feature_backend(caplog: pytest.LogCaptureFixture, tmp_path) -> None:
    """An unavailable driver-feature backend reports the requested transport."""
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "test.db")}, driver_features={"events_backend": "notify"}
    )
    channel = SyncEventChannel(config)

    assert channel._backend_name == "poll_queue"
    warnings = [record.__dict__["extra_fields"] for record in caplog.records if record.message == "event.listen"]
    assert warnings == [
        {
            "adapter_name": "sqlite",
            "backend_name": "notify",
            "fallback_backend": "poll_queue",
            "status": "backend_unavailable",
        }
    ]


def test_event_extension_backend_takes_precedence_over_driver_feature(tmp_path) -> None:
    """The extension setting is the primary event transport selector."""
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "test.db")},
        driver_features={"events_backend": "listen_notify"},
        extension_config={"events": {"backend": "poll_queue"}},
    )

    channel = SyncEventChannel(config)

    assert channel._backend_name == "poll_queue"


def test_sync_event_channel_is_sync_class(tmp_path) -> None:
    """SyncEventChannel is the correct type for sync configurations."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    channel = SyncEventChannel(config)

    assert isinstance(channel, SyncEventChannel)


def test_async_event_channel_is_async_class(tmp_path) -> None:
    """AsyncEventChannel is the correct type for async configurations."""
    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    config = AiosqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    channel = AsyncEventChannel(config)

    assert isinstance(channel, AsyncEventChannel)


def test_sync_event_channel_rejects_async_config(tmp_path) -> None:
    """SyncEventChannel raises error for async configurations."""
    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    config = AiosqliteConfig(connection_config={"database": str(tmp_path / "test.db")})

    with pytest.raises(ImproperConfigurationError, match="sync configuration"):
        SyncEventChannel(cast("SyncDatabaseConfig[Any, Any, Any]", config))


def test_async_event_channel_rejects_sync_config(tmp_path) -> None:
    """AsyncEventChannel raises error for sync configurations."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})

    with pytest.raises(ImproperConfigurationError, match="async configuration"):
        AsyncEventChannel(cast("AsyncDatabaseConfig[Any, Any, Any]", config))


def test_event_channel_normalize_channel_name_valid(tmp_path) -> None:
    """Valid channel names are accepted."""
    from sqlspec.extensions.events import normalize_event_channel_name

    result = normalize_event_channel_name("notifications")
    assert result == "notifications"


def test_event_channel_normalize_channel_name_invalid(tmp_path) -> None:
    """Invalid channel names raise EventChannelError."""
    from sqlspec.extensions.events import normalize_event_channel_name

    with pytest.raises(EventChannelError, match="Invalid events channel name"):
        normalize_event_channel_name("invalid-channel")


def test_event_channel_resolve_poll_interval_default(tmp_path) -> None:
    """None poll_interval uses configured default."""
    from sqlspec.extensions.events import resolve_poll_interval

    result = resolve_poll_interval(None, 2.5)
    assert result == 2.5


def test_event_channel_resolve_poll_interval_explicit(tmp_path) -> None:
    """Explicit poll_interval overrides default."""
    from sqlspec.extensions.events import resolve_poll_interval

    result = resolve_poll_interval(0.1, 2.5)
    assert result == 0.1


def test_event_channel_resolve_poll_interval_zero_raises(tmp_path) -> None:
    """Zero poll_interval raises ImproperConfigurationError."""
    from sqlspec.extensions.events import resolve_poll_interval

    with pytest.raises(ImproperConfigurationError, match="poll_interval must be greater than zero"):
        resolve_poll_interval(0, 1.0)


def test_event_channel_resolve_poll_interval_negative_raises(tmp_path) -> None:
    """Negative poll_interval raises ImproperConfigurationError."""
    from sqlspec.extensions.events import resolve_poll_interval

    with pytest.raises(ImproperConfigurationError, match="poll_interval must be greater than zero"):
        resolve_poll_interval(-1.0, 1.0)


def test_event_channel_resolve_event_poll_interval_precedence() -> None:
    """New setting, compatibility input, and adapter hint have deterministic precedence."""
    from sqlspec.extensions.events import resolve_event_poll_interval

    assert resolve_event_poll_interval(2.0, 0.1, 1.0) == 2.0
    assert resolve_event_poll_interval(None, 0.1, 1.0) == 0.1
    assert resolve_event_poll_interval(None, None, 1.0) == 1.0


def test_event_channel_resolve_event_poll_interval_rejects_non_positive() -> None:
    from sqlspec.extensions.events import resolve_event_poll_interval

    with pytest.raises(ImproperConfigurationError, match="event_poll_interval must be greater than zero"):
        resolve_event_poll_interval(0, None, 1.0)


def test_event_channel_backend_supports_sync(tmp_path) -> None:
    """Table queue backend supports sync operations."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    channel = SyncEventChannel(config)

    assert getattr(channel._backend, "supports_sync", False) is True


def test_sync_event_channel_backend_no_async(tmp_path) -> None:
    """Sync channel uses sync backend which does not support async."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    channel = SyncEventChannel(config)

    assert getattr(channel._backend, "supports_async", False) is False


def test_event_channel_listeners_initialized_empty(tmp_path) -> None:
    """Listener dictionaries are initialized empty."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    channel = SyncEventChannel(config)

    assert len(channel._listeners) == 0


def test_event_channel_resolve_adapter_name_non_sqlspec_module(tmp_path) -> None:
    """_resolve_adapter_name returns None for non-sqlspec configs."""
    from sqlspec.extensions.events import resolve_adapter_name as _resolve_adapter_name

    class CustomConfig:
        is_async = False
        extension_config: dict[str, Any] = {}
        driver_features: dict[str, Any] = {}
        statement_config: None = None

        def get_observability_runtime(self) -> Any:

            return ObservabilityRuntime()

    CustomConfig.__module__ = "myapp.database.config"
    result = _resolve_adapter_name(CustomConfig())

    assert result is None


def test_event_channel_load_native_backend_poll_queue_returns_none(tmp_path) -> None:
    """load_native_backend returns None for poll_queue backend name."""
    from sqlspec.extensions.events import load_native_backend

    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    result = load_native_backend(config, "poll_queue", {})

    assert result is None


def test_event_channel_load_native_backend_none_returns_none(tmp_path) -> None:
    """load_native_backend returns None when backend_name is None."""
    from sqlspec.extensions.events import load_native_backend

    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    result = load_native_backend(config, None, {})

    assert result is None


def test_load_native_backend_uses_pre_resolved_adapter_name(monkeypatch: pytest.MonkeyPatch) -> None:
    """A caller-provided adapter name avoids module-path parsing."""
    from sqlspec.extensions.events import load_native_backend

    imported_modules: list[str] = []

    def fake_import_module(module_name: str) -> Any:
        imported_modules.append(module_name)
        raise ModuleNotFoundError(module_name)

    class CustomConfig:
        pass

    CustomConfig.__module__ = "myapp.database.config"
    monkeypatch.setattr("sqlspec.extensions.events._channel.importlib.import_module", fake_import_module)

    result = load_native_backend(CustomConfig(), "notify", {}, adapter_name="sqlite")

    assert result is None
    assert imported_modules == ["sqlspec.adapters.sqlite.events.backend"]


def test_event_channel_custom_queue_table_via_extension(tmp_path) -> None:
    """Custom queue table name is passed to backend."""
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "test.db")},
        extension_config={"events": {"queue_table": "custom_events"}},
    )
    channel = SyncEventChannel(config)

    backend = channel._backend
    assert backend._table_name == "custom_events"


def test_event_channel_custom_lease_seconds_via_extension(tmp_path) -> None:
    """Custom lease_seconds is passed to backend."""
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "test.db")}, extension_config={"events": {"lease_seconds": 120}}
    )
    channel = SyncEventChannel(config)

    backend = channel._backend
    assert backend._lease_seconds == 120


def test_event_channel_custom_retention_seconds_via_extension(tmp_path) -> None:
    """Custom retention_seconds is passed to backend."""
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "test.db")},
        extension_config={"events": {"retention_seconds": 7200}},
    )
    channel = SyncEventChannel(config)

    backend = channel._backend
    assert backend._retention_seconds == 7200


class _ControllableSyncBackend:
    """Mock sync backend for testing listener dequeue interruption and lifecycle."""

    supports_sync = True
    supports_async = False
    backend_name = "controllable-sync-test"

    def __init__(self) -> None:
        self.entered = threading.Event()
        self.observed_poll_intervals: list[float] = []
        self.queued_events: list[EventMessage] = []
        self.acked_ids: list[str] = []

    def dequeue(self, channel: str, poll_interval: float) -> EventMessage | None:
        self.observed_poll_intervals.append(poll_interval)
        self.entered.set()
        if self.queued_events:
            return self.queued_events.pop(0)
        time.sleep(poll_interval)
        return None

    def ack(self, event_id: str) -> None:
        self.acked_ids.append(event_id)

    def nack(self, event_id: str) -> None:
        pass

    def shutdown(self) -> None:
        pass


def test_sync_listener_delivers_and_acknowledges(tmp_path) -> None:
    """Sync listener processes delivered events and acknowledges them before joining."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    channel = SyncEventChannel(config)
    backend = _ControllableSyncBackend()
    now = datetime.now(UTC)
    event = EventMessage(
        event_id="evt-101",
        channel="test_channel",
        payload={"key": "value"},
        metadata=None,
        attempts=0,
        available_at=now,
        lease_expires_at=None,
        created_at=now,
    )
    backend.queued_events.append(event)
    channel._backend = backend

    received: list[EventMessage] = []
    delivered = threading.Event()

    def handle_event(message: EventMessage) -> None:
        received.append(message)
        delivered.set()

    listener = channel.listen("test_channel", handle_event, poll_interval=0.02)
    assert delivered.wait(timeout=1.0)
    channel.stop_listener(listener.id)

    assert len(received) == 1
    assert received[0].event_id == "evt-101"
    assert backend.acked_ids == ["evt-101"]
    assert backend.observed_poll_intervals[0] == 0.02
    assert not listener.thread.is_alive()
    assert listener.id not in channel._listeners


@pytest.mark.parametrize("select_for_update", [False, True])
def test_sync_table_listener_preserves_idle_poll_interval(
    monkeypatch: pytest.MonkeyPatch, tmp_path, select_for_update: bool
) -> None:
    """Stopping a long idle table poll wakes the listener without extra queries."""
    from sqlspec.migrations.commands import SyncMigrationCommands

    polled = threading.Event()
    polls: list[str] = []

    def trace(statement: str) -> None:
        if statement.startswith("SELECT") and "sqlspec_event_queue" in statement:
            polls.append(statement)
            polled.set()

    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "test.db")},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["events"]},
        driver_features={"on_connection_create": lambda connection: connection.set_trace_callback(trace)},
    )
    SyncMigrationCommands(config).upgrade()
    polls.clear()
    polled.clear()
    channel = SyncEventChannel(config)
    monkeypatch.setattr(channel._backend, "_select_for_update", select_for_update)
    listener = channel.listen("test_channel", lambda _: None, poll_interval=10.0)
    try:
        assert polled.wait(timeout=1.0)
        time.sleep(0.3)
        assert len(polls) == 1
    finally:
        channel.stop_listener(listener.id)
        config.close_pool()
    assert not listener.thread.is_alive()


@pytest.mark.anyio
async def test_events_migration_resolves_external_config_with_spanner_adapter_name() -> None:
    """Events migration resolves Spanner store for external config setting adapter_name = 'spanner'."""
    events_migration = importlib.import_module("sqlspec.extensions.events.migrations.0001_create_event_queue")

    class ExternalSpannerConfig(SpannerSyncConfig):
        adapter_name = "spanner"

    ExternalSpannerConfig.__module__ = "myapp.database.config"
    config = ExternalSpannerConfig(
        connection_config={"project": "test-p", "instance_id": "test-i", "database_id": "test-d"}
    )
    context = MigrationContext(config=config, dialect="spanner")

    up_sql = await events_migration.up(context)
    down_sql = await events_migration.down(context)

    assert any("STRING(" in stmt and "INT64" in stmt for stmt in up_sql)
    assert any("DROP TABLE" in stmt and "sqlspec_event_queue" in stmt for stmt in down_sql)


@pytest.mark.anyio
async def test_events_migration_resolves_subclass_via_mro_and_custom_store_classvar() -> None:
    """Events migration resolves built-in subclasses via MRO and honors events_store_class."""
    events_migration = importlib.import_module("sqlspec.extensions.events.migrations.0001_create_event_queue")

    class SubclassedSqliteConfig(SqliteConfig):
        pass

    SubclassedSqliteConfig.__module__ = "myapp.database.config"
    subclass_config = SubclassedSqliteConfig(connection_config={"database": ":memory:"})
    subclass_up = await events_migration.up(MigrationContext(config=subclass_config, dialect="sqlite"))
    assert any("CREATE TABLE IF NOT EXISTS sqlspec_event_queue" in stmt for stmt in subclass_up)

    class CustomEventStore(BaseEventQueueStore[Any]):
        def _column_types(self) -> tuple[str, str, str]:
            return "CUSTOM_JSON", "CUSTOM_META", "CUSTOM_TS"

    class ThirdPartyConfig:
        is_async = False
        extension_config: dict[str, Any] = {}
        events_store_class = CustomEventStore

    ThirdPartyConfig.__module__ = "third_party_pkg.config"
    custom_up = await events_migration.up(MigrationContext(config=ThirdPartyConfig(), dialect="sqlite"))
    assert any("CUSTOM_JSON" in stmt and "CUSTOM_META" in stmt for stmt in custom_up)


def test_sync_iter_events_drops_and_logs_malformed_payloads(caplog: pytest.LogCaptureFixture, tmp_path) -> None:
    """SyncEventChannel.iter_events drops malformed notification payloads without stopping iteration."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "sync_iter_bad.db")})
    channel = SyncEventChannel(config)
    good_event = EventMessage(
        event_id="evt_valid",
        channel="notifications",
        payload={"ok": True},
        metadata=None,
        attempts=0,
        available_at=datetime.now(UTC),
        lease_expires_at=None,
        created_at=datetime.now(UTC),
    )

    class _SyncBadThenGoodBackend:
        supports_sync = True
        supports_async = False
        backend_name = "notify"

        def __init__(self) -> None:
            self._calls = 0

        def dequeue(self, _channel: str, _poll_interval: float) -> EventMessage | None:
            self._calls += 1
            if self._calls == 1:
                raise EventChannelError("Malformed event notification payload")
            if self._calls == 2:
                raise ValueError("Invalid JSON")
            return good_event

    channel._backend = _SyncBadThenGoodBackend()  # type: ignore[assignment]
    iterator = channel.iter_events("notifications", poll_interval=0.01)

    with caplog.at_level(logging.WARNING, logger="sqlspec.events.channel"):
        received = next(iterator)

    assert received is good_event
    malformed_statuses = [
        getattr(record, "extra_fields", {}).get("status")
        for record in caplog.records
        if record.getMessage() == "event.receive"
    ]
    assert malformed_statuses[:2] == ["malformed_payload", "malformed_payload"]
    assert channel.metrics_snapshot().get("SqliteConfig.events.dequeue.error") == pytest.approx(2.0)


@pytest.mark.anyio
async def test_async_iter_events_drops_and_logs_malformed_payloads(caplog: pytest.LogCaptureFixture, tmp_path) -> None:
    """AsyncEventChannel.iter_events drops malformed notification payloads without stopping iteration."""
    config = AiosqliteConfig(connection_config={"database": str(tmp_path / "async_iter_bad.db")})
    try:
        channel = AsyncEventChannel(config)
        good_event = EventMessage(
            event_id="evt_async_valid",
            channel="notifications",
            payload={"ok": True},
            metadata=None,
            attempts=0,
            available_at=datetime.now(UTC),
            lease_expires_at=None,
            created_at=datetime.now(UTC),
        )

        class _AsyncBadThenGoodBackend:
            supports_sync = False
            supports_async = True
            backend_name = "notify"

            def __init__(self) -> None:
                self._calls = 0

            async def dequeue(self, _channel: str, _poll_interval: float) -> EventMessage | None:
                self._calls += 1
                if self._calls == 1:
                    raise EventChannelError("Malformed event notification payload")
                if self._calls == 2:
                    raise ValueError("Invalid JSON")
                return good_event

        channel._backend = _AsyncBadThenGoodBackend()  # type: ignore[assignment]
        iterator = channel.iter_events("notifications", poll_interval=0.01)

        with caplog.at_level(logging.WARNING, logger="sqlspec.events.channel"):
            received = await anext(iterator)

        assert received is good_event
        malformed_statuses = [
            getattr(record, "extra_fields", {}).get("status")
            for record in caplog.records
            if record.getMessage() == "event.receive"
        ]
        assert malformed_statuses[:2] == ["malformed_payload", "malformed_payload"]
        assert channel.metrics_snapshot().get("AiosqliteConfig.events.dequeue.error") == pytest.approx(2.0)
    finally:
        await config.close_pool()


def test_resolve_backend_name_public_export() -> None:
    """resolve_backend_name is publicly exported from sqlspec.extensions.events and _channel."""
    import sqlspec.extensions.events as events_pkg
    from sqlspec.extensions.events import _channel, resolve_backend_name

    assert "resolve_backend_name" in events_pkg.__all__
    assert "resolve_backend_name" in dir(events_pkg)
    assert "resolve_backend_name" in _channel.__all__
    assert resolve_backend_name is _channel.resolve_backend_name
    assert not hasattr(_channel, "_resolve_backend_name")
    assert not hasattr(_channel, "_get_default_backend")


@pytest.mark.parametrize("backend", ("notify", "notify_queue", "poll_queue", "aq", "txeventq"))
def test_resolve_backend_name_explicit_setting_override(backend: str) -> None:
    """Explicit extension_settings['backend'] wins over driver_features and adapter defaults."""
    from sqlspec.extensions.events import resolve_backend_name

    fallback_feature = "poll_queue" if backend == "notify" else "notify"

    class _StubConfig:
        driver_features = {"events_backend": fallback_feature}

    assert resolve_backend_name(_StubConfig(), {"backend": backend}, "sqlite") == backend


@pytest.mark.parametrize("backend", ("notify", "notify_queue", "poll_queue", "aq", "txeventq"))
def test_resolve_backend_name_driver_features_fallback(backend: str) -> None:
    """driver_features['events_backend'] is used when extension_settings omits 'backend'."""
    from sqlspec.extensions.events import resolve_backend_name

    class _StubConfig:
        driver_features = {"events_backend": backend}

    assert resolve_backend_name(_StubConfig(), {}, "sqlite") == backend


def test_resolve_backend_name_extension_setting_beats_driver_features() -> None:
    """extension_settings['backend'] takes precedence over driver_features['events_backend']."""
    from sqlspec.extensions.events import resolve_backend_name

    class _StubConfig:
        driver_features = {"events_backend": "notify"}

    assert resolve_backend_name(_StubConfig(), {"backend": "poll_queue"}, "asyncpg") == "poll_queue"


@pytest.mark.parametrize("driver_features", (None, "notify", ["notify"], 42, "MISSING"))
def test_resolve_backend_name_ignores_non_dict_driver_features(driver_features: Any) -> None:
    """Non-dict or missing driver_features attribute falls back cleanly to adapter default."""
    from sqlspec.extensions.events import resolve_backend_name

    class _ConfigWithoutFeatures:
        pass

    config: Any = _ConfigWithoutFeatures()
    if driver_features != "MISSING":
        config.driver_features = driver_features

    assert resolve_backend_name(config, {}, "sqlite") == "poll_queue"
    assert resolve_backend_name(config, {}, "asyncpg") == "notify"


@pytest.mark.parametrize(
    ("adapter_name", "expected"),
    (
        ("asyncpg", "notify"),
        ("psycopg", "notify"),
        ("psqlpy", "notify"),
        ("sqlite", "poll_queue"),
        ("oracledb", "poll_queue"),
        ("cockroach_asyncpg", "poll_queue"),
        ("", "poll_queue"),
    ),
)
def test_resolve_backend_name_explicit_adapter_defaults_skip_mro(
    monkeypatch: pytest.MonkeyPatch, adapter_name: str, expected: str
) -> None:
    """Explicit adapter_name selects default without running MRO adapter lookup."""
    from sqlspec.extensions.events import resolve_backend_name

    def _fail_resolve(_config: Any) -> str:
        msg = "resolve_adapter_name must not be called when adapter_name is provided"
        raise AssertionError(msg)

    monkeypatch.setattr("sqlspec.extensions.events._channel.resolve_adapter_name", _fail_resolve)

    class _StubConfig:
        driver_features: dict[str, Any] = {}

    assert resolve_backend_name(_StubConfig(), {}, adapter_name) == expected


@pytest.mark.parametrize("pass_explicit_none", (False, True))
@pytest.mark.parametrize(
    ("config_kind", "expected"),
    (("asyncpg", "notify"), ("psycopg", "notify"), ("sqlite", "poll_queue"), ("custom", "poll_queue")),
)
def test_resolve_backend_name_auto_resolves_adapter_when_none_or_omitted(
    config_kind: str, expected: str, pass_explicit_none: bool
) -> None:
    """Omitted or None adapter_name auto-resolves PostgreSQL vs default adapters via MRO."""
    from sqlspec.adapters.asyncpg import AsyncpgConfig
    from sqlspec.adapters.psycopg import PsycopgSyncConfig
    from sqlspec.extensions.events import resolve_backend_name

    config: Any
    if config_kind == "asyncpg":
        config = AsyncpgConfig(driver_features={"events_backend": None})
    elif config_kind == "psycopg":
        config = PsycopgSyncConfig()
    elif config_kind == "sqlite":
        config = SqliteConfig()
    else:

        class _UnknownConfig:
            driver_features: dict[str, Any] = {}

        _UnknownConfig.__module__ = "myapp.database.config"
        config = _UnknownConfig()

    if pass_explicit_none:
        assert resolve_backend_name(config, {}, adapter_name=None) == expected
    else:
        assert resolve_backend_name(config, {}) == expected


@pytest.mark.parametrize(("adapter_name", "expected"), (("sqlite", "poll_queue"), ("", "poll_queue")))
def test_resolve_backend_name_explicit_adapter_overrides_mro(adapter_name: str, expected: str) -> None:
    """An explicit adapter_name overrides MRO resolution on PostgreSQL configs."""
    from sqlspec.adapters.asyncpg import AsyncpgConfig
    from sqlspec.adapters.psycopg import PsycopgSyncConfig
    from sqlspec.extensions.events import resolve_backend_name

    asyncpg_config = AsyncpgConfig(driver_features={"events_backend": None})
    psycopg_config = PsycopgSyncConfig()

    assert resolve_backend_name(asyncpg_config, {}, adapter_name=adapter_name) == expected
    assert resolve_backend_name(psycopg_config, {}, adapter_name=adapter_name) == expected


@pytest.mark.parametrize("source", ("extension_settings", "driver_features"))
@pytest.mark.parametrize(
    ("retired", "replacement"),
    (("listen_notify", "notify"), ("listen_notify_durable", "notify_queue"), ("table_queue", "poll_queue")),
)
def test_resolve_backend_name_rejects_retired_backends(retired: str, replacement: str, source: str) -> None:
    """Retired backend names raise ImproperConfigurationError pointing to replacement."""
    from sqlspec.extensions.events import resolve_backend_name

    class _StubConfig:
        driver_features: dict[str, Any] = {}

    config = _StubConfig()
    settings: dict[str, Any] = {}
    if source == "extension_settings":
        settings = {"backend": retired}
    else:
        config.driver_features = {"events_backend": retired}

    with pytest.raises(ImproperConfigurationError, match=rf"use '{replacement}'"):
        resolve_backend_name(config, settings, "sqlite")


@pytest.mark.parametrize("source", ("extension_settings", "driver_features"))
@pytest.mark.parametrize("bad_backend", ("unknown", "", 123, True, ["notify"], {"backend": "notify"}))
def test_resolve_backend_name_rejects_unknown_or_non_string_backend(bad_backend: Any, source: str) -> None:
    """Unknown strings and non-string backend values raise ImproperConfigurationError."""
    from sqlspec.extensions.events import resolve_backend_name

    class _StubConfig:
        driver_features: dict[str, Any] = {}

    config = _StubConfig()
    settings: dict[str, Any] = {}
    if source == "extension_settings":
        settings = {"backend": bad_backend}
    else:
        config.driver_features = {"events_backend": bad_backend}

    with pytest.raises(ImproperConfigurationError, match="Unknown event backend"):
        resolve_backend_name(config, settings, "sqlite")


@pytest.mark.parametrize(
    ("bad_backend", "expected_pattern"),
    (("unknown", "Unknown event backend"), ("listen_notify", "use 'notify'"), (["notify"], "Unknown event backend")),
)
def test_events_reconcile_wraps_resolve_backend_name_error_in_migration_error(
    tmp_path, bad_backend: Any, expected_pattern: str
) -> None:
    """events.migrations.reconcile wraps invalid backend ImproperConfigurationError in MigrationError."""
    from sqlspec.extensions.events.migrations.reconcile import reconcile

    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "bad_backend.db")},
        extension_config={"events": {"backend": bad_backend}},
    )
    with config.provide_session() as driver:
        context = MigrationContext(config=config, driver=driver)
        with pytest.raises(MigrationError, match=expected_pattern):
            reconcile(context)
