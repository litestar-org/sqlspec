# pyright: reportPrivateUsage=false, reportAttributeAccessIssue=false, reportArgumentType=false
"""Extended unit tests for EventChannel configuration and backend selection."""

import asyncio
from typing import TYPE_CHECKING, Any, cast

import pytest

from sqlspec import ObservabilityRuntime
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import EventChannelError, ImproperConfigurationError
from sqlspec.extensions.events import AsyncEventChannel, SyncEventChannel

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


def test_event_channel_backend_name_table_queue(tmp_path) -> None:
    """EventChannel defaults to table_queue backend."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    channel = SyncEventChannel(config)

    assert channel._backend_name == "table_queue"


def test_event_channel_backend_fallback_warning(tmp_path) -> None:
    """EventChannel falls back to table_queue for unavailable backends."""
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "test.db")},
        extension_config={"events": {"backend": "nonexistent_backend"}},
    )
    channel = SyncEventChannel(config)

    assert channel._backend_name == "table_queue"


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
        statement_config = None

        def get_observability_runtime(self) -> Any:

            return ObservabilityRuntime()

    CustomConfig.__module__ = "myapp.database.config"
    result = _resolve_adapter_name(CustomConfig())

    assert result is None


def test_event_channel_load_native_backend_table_queue_returns_none(tmp_path) -> None:
    """load_native_backend returns None for table_queue backend name."""
    from sqlspec.extensions.events import load_native_backend

    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    result = load_native_backend(config, "table_queue", {})

    assert result is None


def test_event_channel_load_native_backend_none_returns_none(tmp_path) -> None:
    """load_native_backend returns None when backend_name is None."""
    from sqlspec.extensions.events import load_native_backend

    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    result = load_native_backend(config, None, {})

    assert result is None


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
