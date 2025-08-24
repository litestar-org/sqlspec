"""Test SQLSpec Litestar configuration extensions."""

from typing import TYPE_CHECKING, Any, cast
from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.sqlite.config import SqliteConfig

if TYPE_CHECKING:
    from litestar.types import Scope
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.litestar._utils import set_sqlspec_scope_state
from sqlspec.extensions.litestar.config import AsyncDatabaseConfig, DatabaseConfig, SyncDatabaseConfig
from sqlspec.extensions.litestar.plugin import SQLSpec


def test_get_request_session_with_existing_session() -> None:
    """Test get_request_session returns existing session from scope."""
    # Arrange
    sqlite_config = SqliteConfig(pool_config={"database": ":memory:"})
    db_config = DatabaseConfig(config=sqlite_config)

    state = MagicMock()
    scope = cast("Scope", {})

    # Mock existing session in scope
    session_scope_key = f"{db_config.session_key}_instance"
    expected_session = MagicMock()
    set_sqlspec_scope_state(scope, session_scope_key, expected_session)

    # Act
    result = db_config.get_request_session(state, scope)

    # Assert
    assert result is expected_session


def test_get_request_session_creates_new_session() -> None:
    """Test get_request_session creates new session when none exists."""
    # Arrange
    sqlite_config = SqliteConfig(pool_config={"database": ":memory:"})
    db_config = DatabaseConfig(config=sqlite_config)

    state = MagicMock()
    scope = cast("Scope", {})

    # Mock connection in scope
    mock_connection = MagicMock()
    set_sqlspec_scope_state(scope, db_config.connection_key, mock_connection)

    # Act
    result = db_config.get_request_session(state, scope)

    # Assert
    assert result is not None
    # Verify the session was created with the connection
    assert hasattr(result, "connection")


def test_get_request_session_raises_when_no_connection() -> None:
    """Test get_request_session raises error when no connection in scope."""
    # Arrange
    sqlite_config = SqliteConfig(pool_config={"database": ":memory:"})
    db_config = DatabaseConfig(config=sqlite_config)

    state = MagicMock()
    scope = cast("Scope", {})  # No connection in scope

    # Act & Assert
    with pytest.raises(ImproperConfigurationError) as exc_info:
        db_config.get_request_session(state, scope)

    assert "No database connection found in scope" in str(exc_info.value)
    assert db_config.connection_key in str(exc_info.value)


def test_get_request_connection_returns_connection() -> None:
    """Test get_request_connection returns connection from scope."""
    # Arrange
    sqlite_config = SqliteConfig(pool_config={"database": ":memory:"})
    db_config = DatabaseConfig(config=sqlite_config)

    state = MagicMock()
    scope = cast("Scope", {})

    # Mock connection in scope
    expected_connection = MagicMock()
    set_sqlspec_scope_state(scope, db_config.connection_key, expected_connection)

    # Act
    result = db_config.get_request_connection(state, scope)

    # Assert
    assert result is expected_connection


def test_get_request_connection_raises_when_no_connection() -> None:
    """Test get_request_connection raises error when no connection in scope."""
    # Arrange
    sqlite_config = SqliteConfig(pool_config={"database": ":memory:"})
    db_config = DatabaseConfig(config=sqlite_config)

    state = MagicMock()
    scope = cast("Scope", {})  # No connection in scope

    # Act & Assert
    with pytest.raises(ImproperConfigurationError) as exc_info:
        db_config.get_request_connection(state, scope)

    assert "No database connection found in scope" in str(exc_info.value)
    assert db_config.connection_key in str(exc_info.value)


def test_get_request_session_caches_session_in_scope() -> None:
    """Test get_request_session stores created session in scope for reuse."""
    # Arrange
    sqlite_config = SqliteConfig(pool_config={"database": ":memory:"})
    db_config = DatabaseConfig(config=sqlite_config)

    state = MagicMock()
    scope = cast("Scope", {})

    # Mock connection in scope
    mock_connection = MagicMock()
    set_sqlspec_scope_state(scope, db_config.connection_key, mock_connection)

    # Act - call twice
    result1 = db_config.get_request_session(state, scope)
    result2 = db_config.get_request_session(state, scope)

    # Assert - should return the same cached session
    assert result1 is result2


def test_database_config_provides_both_methods() -> None:
    """Test DatabaseConfig exposes both get_request_session and get_request_connection methods."""
    # Arrange
    sqlite_config = SqliteConfig(pool_config={"database": ":memory:"})
    db_config = DatabaseConfig(config=sqlite_config)

    # Assert
    assert hasattr(db_config, "get_request_session")
    assert callable(db_config.get_request_session)
    assert hasattr(db_config, "get_request_connection")
    assert callable(db_config.get_request_connection)


def test_sqlspec_plugin_get_config_by_connection_key() -> None:
    """Test SQLSpec plugin get_config method with connection key."""
    # Arrange
    sqlite_config = SqliteConfig(pool_config={"database": ":memory:"})
    auth_db_config = DatabaseConfig(config=sqlite_config, connection_key="auth_db_connection")

    plugin = SQLSpec(config=auth_db_config)

    # Act
    result = plugin.get_config("auth_db_connection")

    # Assert
    assert result is auth_db_config


def test_sqlspec_plugin_get_config_by_pool_key() -> None:
    """Test SQLSpec plugin get_config method with pool key."""
    # Arrange
    sqlite_config = SqliteConfig(pool_config={"database": ":memory:"})
    analytics_db_config = DatabaseConfig(config=sqlite_config, pool_key="analytics_db_pool")

    plugin = SQLSpec(config=analytics_db_config)

    # Act
    result = plugin.get_config("analytics_db_pool")

    # Assert
    assert result is analytics_db_config


def test_sqlspec_plugin_get_config_by_session_key() -> None:
    """Test SQLSpec plugin get_config method with session key."""
    # Arrange
    sqlite_config = SqliteConfig(pool_config={"database": ":memory:"})
    reporting_db_config = DatabaseConfig(config=sqlite_config, session_key="reporting_db_session")

    plugin = SQLSpec(config=reporting_db_config)

    # Act
    result = plugin.get_config("reporting_db_session")

    # Assert
    assert result is reporting_db_config


def test_sqlspec_plugin_get_config_with_multiple_configs() -> None:
    """Test SQLSpec plugin get_config method with multiple database configurations."""
    # Arrange
    auth_sqlite_config = SqliteConfig(pool_config={"database": ":memory:"})
    auth_db_config = DatabaseConfig(config=auth_sqlite_config, connection_key="auth_db_connection")

    analytics_sqlite_config = SqliteConfig(pool_config={"database": ":memory:"})
    analytics_db_config = DatabaseConfig(config=analytics_sqlite_config, connection_key="analytics_db_connection")

    plugin = SQLSpec(config=[auth_db_config, analytics_db_config])

    # Act & Assert
    auth_result = plugin.get_config("auth_db_connection")
    analytics_result = plugin.get_config("analytics_db_connection")

    assert auth_result is auth_db_config
    assert analytics_result is analytics_db_config


def test_sqlspec_plugin_get_config_raises_keyerror_for_unknown_key() -> None:
    """Test SQLSpec plugin get_config raises KeyError for unknown key."""
    # Arrange
    sqlite_config = SqliteConfig(pool_config={"database": ":memory:"})
    db_config = DatabaseConfig(config=sqlite_config)

    plugin = SQLSpec(config=db_config)

    # Act & Assert
    with pytest.raises(KeyError) as exc_info:
        plugin.get_config("nonexistent_key")

    assert "No database configuration found for name 'nonexistent_key'" in str(exc_info.value)
    assert "Available keys:" in str(exc_info.value)


def test_sqlspec_plugin_provide_request_session() -> None:
    """Test SQLSpec plugin provide_request_session method."""
    # Arrange
    sqlite_config = SqliteConfig(pool_config={"database": ":memory:"})
    db_config = DatabaseConfig(config=sqlite_config, connection_key="test_db_connection")

    plugin = SQLSpec(config=db_config)

    state = MagicMock()
    scope = cast("Scope", {})

    # Mock connection in scope
    mock_connection = MagicMock()
    set_sqlspec_scope_state(scope, db_config.connection_key, mock_connection)

    # Act
    result: Any = plugin.provide_request_session("test_db_connection", state, scope)

    # Assert
    assert result is not None
    assert hasattr(result, "connection")


def test_sqlspec_plugin_provide_request_connection() -> None:
    """Test SQLSpec plugin provide_request_connection method."""
    # Arrange
    sqlite_config = SqliteConfig(pool_config={"database": ":memory:"})
    db_config = DatabaseConfig(config=sqlite_config, connection_key="test_db_connection")

    plugin = SQLSpec(config=db_config)

    state = MagicMock()
    scope = cast("Scope", {})

    # Mock connection in scope
    expected_connection = MagicMock()
    set_sqlspec_scope_state(scope, db_config.connection_key, expected_connection)

    # Act
    result: Any = plugin.provide_request_connection("test_db_connection", state, scope)

    # Assert
    assert result is expected_connection


def test_sqlspec_plugin_provide_request_session_raises_keyerror_for_unknown_key() -> None:
    """Test SQLSpec plugin provide_request_session raises KeyError for unknown key."""
    # Arrange
    sqlite_config = SqliteConfig(pool_config={"database": ":memory:"})
    db_config = DatabaseConfig(config=sqlite_config)

    plugin = SQLSpec(config=db_config)

    state = MagicMock()
    scope = cast("Scope", {})

    # Act & Assert
    with pytest.raises(KeyError):
        plugin.provide_request_session("nonexistent_key", state, scope)


def test_sqlspec_plugin_provide_request_connection_raises_keyerror_for_unknown_key() -> None:
    """Test SQLSpec plugin provide_request_connection raises KeyError for unknown key."""
    # Arrange
    sqlite_config = SqliteConfig(pool_config={"database": ":memory:"})
    db_config = DatabaseConfig(config=sqlite_config)

    plugin = SQLSpec(config=db_config)

    state = MagicMock()
    scope = cast("Scope", {})

    # Act & Assert
    with pytest.raises(KeyError):
        plugin.provide_request_connection("nonexistent_key", state, scope)


def test_sync_database_config_returns_sync_driver_type() -> None:
    """Test SyncDatabaseConfig.get_request_session returns SyncDriverAdapterBase type."""
    # Arrange

    sqlite_config = SqliteConfig(pool_config={"database": ":memory:"})
    sync_db_config = SyncDatabaseConfig(config=sqlite_config)

    state = MagicMock()
    scope = cast("Scope", {})

    # Mock connection in scope
    mock_connection = MagicMock()
    set_sqlspec_scope_state(scope, sync_db_config.connection_key, mock_connection)

    # Act
    result = sync_db_config.get_request_session(state, scope)

    # Assert
    assert result is not None
    # The type checker should now know this is SyncDriverAdapterBase
    assert hasattr(result, "execute")  # Basic driver interface check


def test_async_database_config_returns_async_driver_type() -> None:
    """Test AsyncDatabaseConfig.get_request_session returns AsyncDriverAdapterBase type."""
    # Arrange - using aiosqlite for async example
    from sqlspec.adapters.aiosqlite.config import AiosqliteConfig

    aiosqlite_config = AiosqliteConfig(pool_config={"database": ":memory:"})
    async_db_config = AsyncDatabaseConfig(config=aiosqlite_config)

    state = MagicMock()
    scope = cast("Scope", {})

    # Mock connection in scope
    mock_connection = MagicMock()
    set_sqlspec_scope_state(scope, async_db_config.connection_key, mock_connection)

    # Act
    result = async_db_config.get_request_session(state, scope)

    # Assert
    assert result is not None
    # The type checker should now know this is AsyncDriverAdapterBase
    assert hasattr(result, "execute")  # Basic driver interface check


def test_specialized_configs_inherit_from_base_config() -> None:
    """Test that specialized configs inherit all functionality from DatabaseConfig."""
    # Arrange
    sqlite_config = SqliteConfig(pool_config={"database": ":memory:"})
    sync_config = SyncDatabaseConfig(config=sqlite_config)

    from sqlspec.adapters.aiosqlite.config import AiosqliteConfig

    aiosqlite_config = AiosqliteConfig(pool_config={"database": ":memory:"})
    async_config = AsyncDatabaseConfig(config=aiosqlite_config)

    # Assert - Should have all the same attributes as base DatabaseConfig
    base_attrs = [
        "connection_key",
        "pool_key",
        "session_key",
        "commit_mode",
        "get_request_session",
        "get_request_connection",
    ]

    for attr in base_attrs:
        assert hasattr(sync_config, attr), f"SyncDatabaseConfig missing {attr}"
        assert hasattr(async_config, attr), f"AsyncDatabaseConfig missing {attr}"


def test_specialized_configs_work_with_plugin() -> None:
    """Test that SQLSpec plugin works with specialized database configs."""
    # Arrange
    sqlite_config = SqliteConfig(pool_config={"database": ":memory:"})
    sync_db_config = SyncDatabaseConfig(config=sqlite_config, connection_key="sync_db")

    from sqlspec.adapters.aiosqlite.config import AiosqliteConfig

    aiosqlite_config = AiosqliteConfig(pool_config={"database": ":memory:"})
    async_db_config = AsyncDatabaseConfig(config=aiosqlite_config, connection_key="async_db")

    plugin = SQLSpec(config=[sync_db_config, async_db_config])

    # Act & Assert
    sync_config = plugin.get_config("sync_db")
    async_config = plugin.get_config("async_db")

    assert isinstance(sync_config, SyncDatabaseConfig)
    assert isinstance(async_config, AsyncDatabaseConfig)
    assert sync_config is sync_db_config
    assert async_config is async_db_config


def test_sqlspec_plugin_provide_sync_request_session() -> None:
    """Test SQLSpec plugin provide_sync_request_session method returns properly typed session."""
    # Arrange
    sqlite_config = SqliteConfig(pool_config={"database": ":memory:"})
    db_config = DatabaseConfig(config=sqlite_config, connection_key="sync_db_connection")

    plugin = SQLSpec(config=db_config)

    state = MagicMock()
    scope = cast("Scope", {})

    # Mock connection in scope
    mock_connection = MagicMock()
    set_sqlspec_scope_state(scope, db_config.connection_key, mock_connection)

    # Act
    result: Any = plugin.provide_sync_request_session("sync_db_connection", state, scope)

    # Assert
    assert result is not None
    assert hasattr(result, "connection")
    # The returned type should be SyncDriverAdapterBase according to type hints


def test_sqlspec_plugin_provide_async_request_session() -> None:
    """Test SQLSpec plugin provide_async_request_session method returns properly typed session."""
    # Arrange
    from sqlspec.adapters.aiosqlite.config import AiosqliteConfig

    aiosqlite_config = AiosqliteConfig(pool_config={"database": ":memory:"})
    db_config = DatabaseConfig(config=aiosqlite_config, connection_key="async_db_connection")

    plugin = SQLSpec(config=db_config)

    state = MagicMock()
    scope = cast("Scope", {})

    # Mock connection in scope
    mock_connection = MagicMock()
    set_sqlspec_scope_state(scope, db_config.connection_key, mock_connection)

    # Act
    result: Any = plugin.provide_async_request_session("async_db_connection", state, scope)

    # Assert
    assert result is not None
    assert hasattr(result, "connection")
    # The returned type should be AsyncDriverAdapterBase according to type hints


def test_sqlspec_plugin_provide_typed_session_methods_with_multiple_configs() -> None:
    """Test typed session methods work with multiple database configurations."""
    # Arrange
    sync_sqlite_config = SqliteConfig(pool_config={"database": ":memory:"})
    sync_db_config = DatabaseConfig(config=sync_sqlite_config, connection_key="sync_db")

    from sqlspec.adapters.aiosqlite.config import AiosqliteConfig

    async_sqlite_config = AiosqliteConfig(pool_config={"database": ":memory:"})
    async_db_config = DatabaseConfig(config=async_sqlite_config, connection_key="async_db")

    plugin = SQLSpec(config=[sync_db_config, async_db_config])

    state = MagicMock()
    scope = cast("Scope", {})

    # Mock connections in scope
    mock_sync_connection = MagicMock()
    mock_async_connection = MagicMock()
    set_sqlspec_scope_state(scope, sync_db_config.connection_key, mock_sync_connection)
    set_sqlspec_scope_state(scope, async_db_config.connection_key, mock_async_connection)

    # Act & Assert - sync session
    sync_result: Any = plugin.provide_sync_request_session("sync_db", state, scope)
    assert sync_result is not None
    assert hasattr(sync_result, "connection")

    # Act & Assert - async session
    async_result: Any = plugin.provide_async_request_session("async_db", state, scope)
    assert async_result is not None
    assert hasattr(async_result, "connection")
