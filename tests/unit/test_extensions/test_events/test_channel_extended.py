"""Extended unit tests for EventChannel configuration and backend selection."""

import pytest

from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.events import EventChannel


def test_event_channel_adapter_name_resolution(tmp_path) -> None:
    """EventChannel resolves adapter name from config module path."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    channel = EventChannel(config)

    assert channel._adapter_name == "sqlite"


def test_event_channel_default_poll_interval(tmp_path) -> None:
    """EventChannel uses hint default poll interval."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    channel = EventChannel(config)

    assert channel._poll_interval_default == 1.0


def test_event_channel_custom_poll_interval(tmp_path) -> None:
    """Extension settings override default poll interval."""
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "test.db")}, extension_config={"events": {"poll_interval": 0.5}}
    )
    channel = EventChannel(config)

    assert channel._poll_interval_default == 0.5


def test_event_channel_backend_name_table_queue(tmp_path) -> None:
    """EventChannel defaults to table_queue backend."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    channel = EventChannel(config)

    assert channel._backend_name == "table_queue"


def test_event_channel_backend_fallback_warning(tmp_path) -> None:
    """EventChannel falls back to table_queue for unavailable backends."""
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "test.db")},
        driver_features={"events_backend": "nonexistent_backend"},
    )
    channel = EventChannel(config)

    assert channel._backend_name == "table_queue"


def test_event_channel_is_async_flag_sync(tmp_path) -> None:
    """EventChannel correctly identifies sync configurations."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    channel = EventChannel(config)

    assert channel._is_async is False


def test_event_channel_is_async_flag_async(tmp_path) -> None:
    """EventChannel correctly identifies async configurations."""
    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    config = AiosqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    channel = EventChannel(config)

    assert channel._is_async is True


def test_event_channel_portal_bridge_disabled_for_sync(tmp_path) -> None:
    """Portal bridge is disabled for sync configurations."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    channel = EventChannel(config)

    assert channel._portal_bridge is False


def test_event_channel_portal_bridge_enabled_for_async(tmp_path) -> None:
    """Portal bridge is enabled by default for async configurations."""
    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    config = AiosqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    channel = EventChannel(config)

    assert channel._portal_bridge is True


def test_event_channel_portal_bridge_override(tmp_path) -> None:
    """Portal bridge can be explicitly disabled."""
    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    config = AiosqliteConfig(
        connection_config={"database": str(tmp_path / "test.db")}, extension_config={"events": {"portal_bridge": False}}
    )
    channel = EventChannel(config)

    assert channel._portal_bridge is False


def test_event_channel_normalize_channel_name_valid(tmp_path) -> None:
    """Valid channel names are accepted."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    channel = EventChannel(config)

    result = channel._normalize_channel_name("notifications")
    assert result == "notifications"


def test_event_channel_normalize_channel_name_invalid(tmp_path) -> None:
    """Invalid channel names raise ImproperConfigurationError."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    channel = EventChannel(config)

    with pytest.raises(ImproperConfigurationError, match="Invalid events channel name"):
        channel._normalize_channel_name("invalid-channel")


def test_event_channel_resolve_poll_interval_default(tmp_path) -> None:
    """None poll_interval uses configured default."""
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "test.db")}, extension_config={"events": {"poll_interval": 2.5}}
    )
    channel = EventChannel(config)

    result = channel._resolve_poll_interval(None)
    assert result == 2.5


def test_event_channel_resolve_poll_interval_explicit(tmp_path) -> None:
    """Explicit poll_interval overrides default."""
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "test.db")}, extension_config={"events": {"poll_interval": 2.5}}
    )
    channel = EventChannel(config)

    result = channel._resolve_poll_interval(0.1)
    assert result == 0.1


def test_event_channel_resolve_poll_interval_zero_raises(tmp_path) -> None:
    """Zero poll_interval raises ImproperConfigurationError."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    channel = EventChannel(config)

    with pytest.raises(ImproperConfigurationError, match="poll_interval must be greater than zero"):
        channel._resolve_poll_interval(0)


def test_event_channel_resolve_poll_interval_negative_raises(tmp_path) -> None:
    """Negative poll_interval raises ImproperConfigurationError."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    channel = EventChannel(config)

    with pytest.raises(ImproperConfigurationError, match="poll_interval must be greater than zero"):
        channel._resolve_poll_interval(-1.0)


def test_event_channel_publish_async_on_sync_raises(tmp_path) -> None:
    """publish_async on sync config raises ImproperConfigurationError."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    channel = EventChannel(config)

    with pytest.raises(ImproperConfigurationError, match="async configuration"):
        import asyncio

        asyncio.get_event_loop().run_until_complete(channel.publish_async("test", {"action": "test"}))


def test_event_channel_publish_sync_on_async_without_bridge_raises(tmp_path) -> None:
    """publish_sync on async config without portal_bridge raises."""
    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    config = AiosqliteConfig(
        connection_config={"database": str(tmp_path / "test.db")}, extension_config={"events": {"portal_bridge": False}}
    )
    channel = EventChannel(config)

    with pytest.raises(ImproperConfigurationError, match="sync configuration"):
        channel.publish_sync("test", {"action": "test"})


def test_event_channel_ack_sync_on_async_without_bridge_raises(tmp_path) -> None:
    """ack_sync on async config without portal_bridge raises."""
    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    config = AiosqliteConfig(
        connection_config={"database": str(tmp_path / "test.db")}, extension_config={"events": {"portal_bridge": False}}
    )
    channel = EventChannel(config)

    with pytest.raises(ImproperConfigurationError, match="sync configuration"):
        channel.ack_sync("test-event-id")


def test_event_channel_iter_events_sync_on_async_without_bridge_raises(tmp_path) -> None:
    """iter_events_sync on async config without portal_bridge raises."""
    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    config = AiosqliteConfig(
        connection_config={"database": str(tmp_path / "test.db")}, extension_config={"events": {"portal_bridge": False}}
    )
    channel = EventChannel(config)

    with pytest.raises(ImproperConfigurationError, match="sync configuration"):
        list(channel.iter_events_sync("test"))


def test_event_channel_listen_sync_on_async_without_bridge_raises(tmp_path) -> None:
    """listen on async config without portal_bridge raises."""
    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    config = AiosqliteConfig(
        connection_config={"database": str(tmp_path / "test.db")}, extension_config={"events": {"portal_bridge": False}}
    )
    channel = EventChannel(config)

    def handler(msg):
        pass

    with pytest.raises(ImproperConfigurationError, match="sync configuration"):
        channel.listen("test", handler)


def test_event_channel_iter_events_async_on_sync_raises(tmp_path) -> None:
    """iter_events_async on sync config raises ImproperConfigurationError."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    channel = EventChannel(config)

    with pytest.raises(ImproperConfigurationError, match="async configuration"):
        import asyncio

        async def iterate():
            async for _ in channel.iter_events_async("test"):
                break

        asyncio.get_event_loop().run_until_complete(iterate())


def test_event_channel_listen_async_on_sync_raises(tmp_path) -> None:
    """listen_async on sync config raises ImproperConfigurationError."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    channel = EventChannel(config)

    async def handler(msg):
        pass

    with pytest.raises(ImproperConfigurationError, match="async configuration"):
        channel.listen_async("test", handler)


def test_event_channel_ack_async_on_sync_raises(tmp_path) -> None:
    """ack_async on sync config raises ImproperConfigurationError."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    channel = EventChannel(config)

    with pytest.raises(ImproperConfigurationError, match="async configuration"):
        import asyncio

        asyncio.get_event_loop().run_until_complete(channel.ack_async("test-event-id"))


def test_event_channel_backend_supports_sync(tmp_path) -> None:
    """Table queue backend supports sync operations."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    channel = EventChannel(config)

    assert getattr(channel._backend, "supports_sync", False) is True


def test_event_channel_backend_supports_async(tmp_path) -> None:
    """Table queue backend supports async operations."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    channel = EventChannel(config)

    assert getattr(channel._backend, "supports_async", False) is True


def test_event_channel_listeners_initialized_empty(tmp_path) -> None:
    """Listener dictionaries are initialized empty."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    channel = EventChannel(config)

    assert len(channel._listeners_sync) == 0
    assert len(channel._listeners_async) == 0


def test_event_channel_resolve_adapter_name_non_sqlspec_module(tmp_path) -> None:
    """_resolve_adapter_name returns None for non-sqlspec configs."""

    class CustomConfig:
        is_async = False
        extension_config = {}
        driver_features = {}
        statement_config = None

        def get_observability_runtime(self):
            from sqlspec.observability import NullObservabilityRuntime

            return NullObservabilityRuntime()

    CustomConfig.__module__ = "myapp.database.config"
    result = EventChannel._resolve_adapter_name(CustomConfig())

    assert result is None


def test_event_channel_load_native_backend_table_queue_returns_none(tmp_path) -> None:
    """_load_native_backend returns None for table_queue backend name."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    result = EventChannel._load_native_backend(config, "table_queue", {})

    assert result is None


def test_event_channel_load_native_backend_none_returns_none(tmp_path) -> None:
    """_load_native_backend returns None when backend_name is None."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    result = EventChannel._load_native_backend(config, None, {})

    assert result is None


def test_event_channel_custom_queue_table_via_extension(tmp_path) -> None:
    """Custom queue table name is passed to backend."""
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "test.db")},
        extension_config={"events": {"queue_table": "custom_events"}},
    )
    channel = EventChannel(config)

    queue = channel._backend._queue
    assert queue._table_name == "custom_events"


def test_event_channel_custom_lease_seconds_via_extension(tmp_path) -> None:
    """Custom lease_seconds is passed to backend."""
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "test.db")}, extension_config={"events": {"lease_seconds": 120}}
    )
    channel = EventChannel(config)

    queue = channel._backend._queue
    assert queue._lease_seconds == 120


def test_event_channel_custom_retention_seconds_via_extension(tmp_path) -> None:
    """Custom retention_seconds is passed to backend."""
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "test.db")},
        extension_config={"events": {"retention_seconds": 7200}},
    )
    channel = EventChannel(config)

    queue = channel._backend._queue
    assert queue._retention_seconds == 7200
