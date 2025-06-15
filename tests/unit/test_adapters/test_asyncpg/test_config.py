"""Unit tests for asyncpg configuration."""

from unittest.mock import AsyncMock, Mock, patch

import pytest

from sqlspec.adapters.asyncpg import CONNECTION_FIELDS, POOL_FIELDS, AsyncpgConfig, AsyncpgDriver
from sqlspec.statement.sql import SQLConfig


def test_asyncpg_field_constants() -> None:
    """Test AsyncPG CONNECTION_FIELDS and POOL_FIELDS constants."""
    expected_connection_fields = {
        "dsn",
        "host",
        "port",
        "user",
        "password",
        "database",
        "ssl",
        "passfile",
        "direct_tls",
        "connect_timeout",
        "command_timeout",
        "statement_cache_size",
        "max_cached_statement_lifetime",
        "max_cacheable_statement_size",
        "server_settings",
    }
    assert CONNECTION_FIELDS == expected_connection_fields

    # POOL_FIELDS should be a superset of CONNECTION_FIELDS
    assert CONNECTION_FIELDS.issubset(POOL_FIELDS)

    # Check pool-specific fields
    pool_specific = POOL_FIELDS - CONNECTION_FIELDS
    expected_pool_specific = {
        "min_size",
        "max_size",
        "max_queries",
        "max_inactive_connection_lifetime",
        "setup",
        "init",
        "loop",
        "connection_class",
        "record_class",
    }
    assert pool_specific == expected_pool_specific


def test_asyncpg_config_basic_creation() -> None:
    """Test AsyncPG config creation with basic parameters."""
    # Test minimal config creation
    config = AsyncpgConfig(host="localhost", port=5432, user="test_user", password="test_password", database="test_db")
    assert config.host == "localhost"
    assert config.port == 5432
    assert config.user == "test_user"
    assert config.password == "test_password"
    assert config.database == "test_db"
    assert config.extras == {}

    # Test with all parameters
    config_full = AsyncpgConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        database="test_db",
        connect_timeout=30.0,
        command_timeout=60.0,
        server_settings={"application_name": "test_app"},
        ssl=None,
        min_size=1,
        max_size=10,
        max_queries=50000,
    )
    assert config_full.host == "localhost"
    assert config_full.connect_timeout == 30.0
    assert config_full.command_timeout == 60.0
    assert config_full.server_settings == {"application_name": "test_app"}
    assert config_full.ssl is None
    assert config_full.min_size == 1
    assert config_full.max_size == 10
    assert config_full.max_queries == 50000


def test_asyncpg_config_extras_handling() -> None:
    """Test AsyncPG config extras parameter handling."""
    # Test with explicit extras
    config = AsyncpgConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        database="test_db",
        extras={"custom_param": "value", "debug": True},
    )
    assert config.extras["custom_param"] == "value"
    assert config.extras["debug"] is True

    # Test with kwargs going to extras
    config2 = AsyncpgConfig(host="localhost", port=5432, user="test_user", password="test_password", database="test_db")
    assert config2.extras["unknown_param"] == "test"
    assert config2.extras["another_param"] == 42


def test_asyncpg_config_initialization() -> None:
    """Test asyncpg config initialization."""
    # Test with default parameters
    config = AsyncpgConfig(host="localhost", port=5432, user="test_user", password="test_password", database="test_db")
    assert config.host == "localhost"
    assert isinstance(config.statement_config, SQLConfig)
    # Test with custom parameters
    custom_statement_config = SQLConfig()
    config = AsyncpgConfig(
        host="custom_host",
        port=5433,
        user="custom_user",
        password="custom_password",
        database="custom_db",
        connect_timeout=60.0,
        min_size=2,
        max_size=20,
        statement_config=custom_statement_config,
    )
    assert config.host == "custom_host"
    assert config.port == 5433
    assert config.connect_timeout == 60.0
    assert config.statement_config is custom_statement_config


@pytest.mark.asyncio
async def test_asyncpg_config_connection_creation() -> None:
    """Test asyncpg config connection creation (mocked)."""
    mock_connection = AsyncMock()

    # Patch the asyncpg module in the create_connection method's local scope
    with patch("asyncpg.connect") as mock_connect:
        mock_connect.return_value = mock_connection

        config = AsyncpgConfig(
            host="localhost",
            port=5432,
            user="test_user",
            password="test_password",
            database="test_db",
            connect_timeout=30.0,
        )

        connection = await config.create_connection()

        # Verify connection creation was called with correct parameters
        mock_connect.assert_called_once_with(
            host="localhost",
            port=5432,
            user="test_user",
            password="test_password",
            database="test_db",
            connect_timeout=30.0,
        )
        assert connection is mock_connection


@patch("asyncpg.connect")
@pytest.mark.asyncio
async def test_asyncpg_config_provide_connection(mock_connect: Mock) -> None:
    """Test asyncpg config provide_connection context manager."""
    mock_connection = AsyncMock()
    mock_connect.return_value = mock_connection

    config = AsyncpgConfig(host="localhost", port=5432, user="test_user", password="test_password", database="test_db")

    # Test context manager behavior (without pool)
    async with config.provide_connection() as conn:
        assert conn is mock_connection
        # Verify connection is not closed yet
        mock_connection.close.assert_not_called()

    # Verify connection was closed after context exit
    mock_connection.close.assert_called_once()


@patch("asyncpg.connect")
@pytest.mark.asyncio
async def test_asyncpg_config_provide_connection_error_handling(mock_connect: Mock) -> None:
    """Test asyncpg config provide_connection error handling."""
    mock_connection = AsyncMock()
    mock_connect.return_value = mock_connection

    config = AsyncpgConfig(host="localhost", port=5432, user="test_user", password="test_password", database="test_db")

    # Test error handling and cleanup
    with pytest.raises(ValueError):
        async with config.provide_connection() as conn:
            assert conn is mock_connection
            raise ValueError("Test error")

    # Verify connection was still closed despite error
    mock_connection.close.assert_called_once()


@patch("asyncpg.connect")
@pytest.mark.asyncio
async def test_asyncpg_config_provide_session(mock_connect: Mock) -> None:
    """Test asyncpg config provide_session context manager."""
    mock_connection = AsyncMock()
    mock_connect.return_value = mock_connection

    config = AsyncpgConfig(host="localhost", port=5432, user="test_user", password="test_password", database="test_db")

    # Test session context manager behavior
    async with config.provide_session() as session:
        assert isinstance(session, AsyncpgDriver)
        assert session.connection is mock_connection
        # The config might be modified to include parameter styles
        assert session.config.strict_mode == config.statement_config.strict_mode
        assert session.config.enable_parsing == config.statement_config.enable_parsing
        assert session.config.enable_validation == config.statement_config.enable_validation
        # Check that parameter styles were set
        assert session.config.allowed_parameter_styles == ("numeric",)
        assert session.config.target_parameter_style == "numeric"  # Verify connection is not closed yet
        mock_connection.close.assert_not_called()

    # Verify connection was closed after context exit
    mock_connection.close.assert_called_once()


def test_asyncpg_config_connection_config_dict() -> None:
    """Test asyncpg config connection_config_dict and pool_config_dict properties."""
    config = AsyncpgConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        database="test_db",
        connect_timeout=30.0,
        command_timeout=60.0,
        min_size=1,
        max_size=10,
    )

    # Test connection_config_dict returns only connection parameters
    conn_dict = config.connection_config_dict
    expected_conn = {
        "host": "localhost",
        "port": 5432,
        "user": "test_user",
        "password": "test_password",
        "database": "test_db",
        "connect_timeout": 30.0,
        "command_timeout": 60.0,
    }
    for key, value in expected_conn.items():
        assert conn_dict[key] == value

    # Connection config should not include pool-specific parameters
    assert "min_size" not in conn_dict
    assert "max_size" not in conn_dict

    # Test pool_config_dict returns all parameters
    pool_dict = config.pool_config_dict
    expected_pool = {
        "host": "localhost",
        "port": 5432,
        "user": "test_user",
        "password": "test_password",
        "database": "test_db",
        "connect_timeout": 30.0,
        "command_timeout": 60.0,
        "min_size": 1,
        "max_size": 10,
    }
    for key, value in expected_pool.items():
        assert pool_dict[key] == value


def test_asyncpg_config_driver_type() -> None:
    """Test asyncpg config driver_type property."""
    config = AsyncpgConfig(host="localhost", port=5432, user="test_user", password="test_password", database="test_db")
    assert config.driver_type is AsyncpgDriver


def test_asyncpg_config_connection_type() -> None:
    """Test asyncpg config connection_type property."""
    config = AsyncpgConfig(host="localhost", port=5432, user="test_user", password="test_password", database="test_db")
    # asyncpg uses PoolConnectionProxy when using pools
    from asyncpg.pool import PoolConnectionProxy

    assert config.connection_type is PoolConnectionProxy


def test_asyncpg_config_ssl_configuration() -> None:
    """Test asyncpg config with SSL configuration."""
    import ssl

    ssl_context = ssl.create_default_context()

    config = AsyncpgConfig(
        host="localhost", port=5432, user="test_user", password="test_password", database="test_db", ssl=ssl_context
    )
    assert config.ssl is ssl_context


def test_asyncpg_config_server_settings() -> None:
    """Test asyncpg config with server settings."""
    server_settings = {"application_name": "test_app", "timezone": "UTC", "search_path": "public,test_schema"}

    config = AsyncpgConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        database="test_db",
        server_settings=server_settings,
    )
    assert config.server_settings == server_settings


def test_asyncpg_config_timeouts() -> None:
    """Test asyncpg config with different timeout settings."""
    config = AsyncpgConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        database="test_db",
        connect_timeout=30.0,
        command_timeout=120.0,
    )
    assert config.connect_timeout == 30.0
    assert config.command_timeout == 120.0


def test_asyncpg_config_is_async() -> None:
    """Test asyncpg config is_async attribute."""
    config = AsyncpgConfig(host="localhost", port=5432, user="test_user", password="test_password", database="test_db")
    assert config.is_async is True
    assert AsyncpgConfig.is_async is True


def test_asyncpg_config_supports_connection_pooling() -> None:
    """Test asyncpg config supports_connection_pooling attribute."""
    config = AsyncpgConfig(host="localhost", port=5432, user="test_user", password="test_password", database="test_db")
    assert config.supports_connection_pooling is True
    assert AsyncpgConfig.supports_connection_pooling is True


def test_asyncpg_config_dsn_configuration() -> None:
    """Test asyncpg config with DSN."""
    dsn = "postgresql://test_user:test_password@localhost:5432/test_db"
    config = AsyncpgConfig(dsn=dsn)

    # The config should be able to construct connection parameters
    config_dict = config.connection_config_dict
    assert "dsn" in config_dict
    assert config_dict["dsn"] == dsn


def test_asyncpg_config_minimal_connection() -> None:
    """Test asyncpg config with minimal connection parameters."""
    config = AsyncpgConfig(host="localhost", port=5432, user="test_user", password="test_password", database="test_db")

    # Should work with just the required parameters
    assert config.host == "localhost"
    assert config.port == 5432
    assert config.user == "test_user"
    assert config.password == "test_password"
    assert config.database == "test_db"


def test_asyncpg_config_pool_settings() -> None:
    """Test asyncpg config with pool-specific settings."""
    config = AsyncpgConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        database="test_db",
        min_size=5,
        max_size=20,
        max_queries=50000,
        max_inactive_connection_lifetime=300.0,
    )
    assert config.min_size == 5
    assert config.max_size == 20
    assert config.max_queries == 50000
    assert config.max_inactive_connection_lifetime == 300.0
