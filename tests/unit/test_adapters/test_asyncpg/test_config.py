"""Unit tests for asyncpg configuration."""

from unittest.mock import AsyncMock, Mock, patch

import pytest

from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgConnectionConfig, AsyncpgDriver, AsyncpgPoolConfig
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.sql import SQLConfig


def test_asyncpg_connection_config_creation() -> None:
    """Test asyncpg connection config creation with valid parameters."""
    # Test basic config creation
    config = AsyncpgConnectionConfig(
        host="localhost", port=5432, user="test_user", password="test_password", database="test_db"
    )
    assert config.get("host") == "localhost"
    assert config.get("port") == 5432
    assert config.get("user") == "test_user"
    assert config.get("password") == "test_password"
    assert config.get("database") == "test_db"

    # Test with all parameters
    config_full = AsyncpgConnectionConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        database="test_db",
        connect_timeout=30.0,
        command_timeout=60.0,
        server_settings={"application_name": "test_app"},
        ssl=None,
    )
    assert config_full.get("host") == "localhost"
    assert config_full.get("connect_timeout") == 30.0
    assert config_full.get("command_timeout") == 60.0
    assert config_full.get("server_settings") == {"application_name": "test_app"}
    assert config_full.get("ssl") is None


def test_asyncpg_pool_config_creation() -> None:
    """Test asyncpg pool config creation with valid parameters."""
    # Test basic pool config creation
    config = AsyncpgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        database="test_db",
        min_size=1,
        max_size=10,
    )
    assert config.get("host") == "localhost"
    assert config.get("port") == 5432
    assert config.get("min_size") == 1
    assert config.get("max_size") == 10


def test_asyncpg_config_initialization() -> None:
    """Test asyncpg config initialization."""
    # Test with pool config
    pool_config = AsyncpgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        database="test_db",
        min_size=1,
        max_size=10,
    )
    config = AsyncpgConfig(pool_config=pool_config)
    assert config.pool_config.get("host") == "localhost"
    assert config.pool_config.get("port") == 5432
    assert isinstance(config.statement_config, SQLConfig)
    assert isinstance(config.instrumentation, InstrumentationConfig)

    # Test with custom parameters
    custom_pool_config = AsyncpgPoolConfig(
        host="custom_host",
        port=5433,
        user="custom_user",
        password="custom_password",
        database="custom_db",
        connect_timeout=60.0,
        min_size=2,
        max_size=20,
    )
    custom_statement_config = SQLConfig()
    custom_instrumentation = InstrumentationConfig(log_queries=True)

    config = AsyncpgConfig(
        pool_config=custom_pool_config,
        statement_config=custom_statement_config,
        instrumentation=custom_instrumentation,
    )
    assert config.pool_config.get("host") == "custom_host"
    assert config.pool_config.get("port") == 5433
    assert config.pool_config.get("connect_timeout") == 60.0
    assert config.statement_config is custom_statement_config
    assert config.instrumentation.log_queries is True


@pytest.mark.asyncio
async def test_asyncpg_config_connection_creation() -> None:
    """Test asyncpg config connection creation (mocked)."""
    mock_connection = AsyncMock()

    # Patch the asyncpg module in the create_connection method's local scope
    with patch("asyncpg.connect") as mock_connect:
        mock_connect.return_value = mock_connection

        pool_config = AsyncpgPoolConfig(
            host="localhost",
            port=5432,
            user="test_user",
            password="test_password",
            database="test_db",
            connect_timeout=30.0,
        )
        # For create_connection (not from pool), we need connection_config
        connection_config = AsyncpgConnectionConfig(
            host="localhost",
            port=5432,
            user="test_user",
            password="test_password",
            database="test_db",
            connect_timeout=30.0,
        )
        config = AsyncpgConfig(pool_config=pool_config, connection_config=connection_config)

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

    pool_config = AsyncpgPoolConfig(
        host="localhost", port=5432, user="test_user", password="test_password", database="test_db"
    )
    config = AsyncpgConfig(pool_config=pool_config)

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

    pool_config = AsyncpgPoolConfig(
        host="localhost", port=5432, user="test_user", password="test_password", database="test_db"
    )
    config = AsyncpgConfig(pool_config=pool_config)

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

    pool_config = AsyncpgPoolConfig(
        host="localhost", port=5432, user="test_user", password="test_password", database="test_db"
    )
    config = AsyncpgConfig(pool_config=pool_config)

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
        assert session.config.target_parameter_style == "numeric"
        assert session.instrumentation_config is config.instrumentation
        # Verify connection is not closed yet
        mock_connection.close.assert_not_called()

    # Verify connection was closed after context exit
    mock_connection.close.assert_called_once()


def test_asyncpg_config_connection_config_dict() -> None:
    """Test asyncpg config connection_config_dict and pool_config_dict properties."""
    # Test with connection_config
    connection_config = AsyncpgConnectionConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        database="test_db",
        connect_timeout=30.0,
    )
    pool_config = AsyncpgPoolConfig(
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
    config = AsyncpgConfig(pool_config=pool_config, connection_config=connection_config)

    # Test connection_config_dict returns connection config
    conn_dict = config.connection_config_dict
    expected_conn = {
        "host": "localhost",
        "port": 5432,
        "user": "test_user",
        "password": "test_password",
        "database": "test_db",
        "connect_timeout": 30.0,
    }
    for key, value in expected_conn.items():
        assert conn_dict[key] == value

    # Test pool_config_dict returns pool config
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
    pool_config = AsyncpgPoolConfig(
        host="localhost", port=5432, user="test_user", password="test_password", database="test_db"
    )
    config = AsyncpgConfig(pool_config=pool_config)
    assert config.driver_type is AsyncpgDriver


def test_asyncpg_config_connection_type() -> None:
    """Test asyncpg config connection_type property."""
    pool_config = AsyncpgPoolConfig(
        host="localhost", port=5432, user="test_user", password="test_password", database="test_db"
    )
    config = AsyncpgConfig(pool_config=pool_config)
    # asyncpg uses PoolConnectionProxy when using pools
    from asyncpg.pool import PoolConnectionProxy

    assert config.connection_type is PoolConnectionProxy


def test_asyncpg_config_ssl_configuration() -> None:
    """Test asyncpg config with SSL configuration."""
    import ssl

    ssl_context = ssl.create_default_context()

    pool_config = AsyncpgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        database="test_db",
        ssl=ssl_context,
    )
    config = AsyncpgConfig(pool_config=pool_config)
    assert config.pool_config.get("ssl") is ssl_context


def test_asyncpg_config_server_settings() -> None:
    """Test asyncpg config with server settings."""
    server_settings = {
        "application_name": "test_app",
        "timezone": "UTC",
        "search_path": "public,test_schema",
    }

    pool_config = AsyncpgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        database="test_db",
        server_settings=server_settings,
    )
    config = AsyncpgConfig(pool_config=pool_config)
    assert config.pool_config.get("server_settings") == server_settings


def test_asyncpg_config_timeouts() -> None:
    """Test asyncpg config with different timeout settings."""
    pool_config = AsyncpgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        database="test_db",
        connect_timeout=30.0,
        command_timeout=120.0,
    )
    config = AsyncpgConfig(pool_config=pool_config)
    assert config.pool_config.get("connect_timeout") == 30.0
    assert config.pool_config.get("command_timeout") == 120.0


def test_asyncpg_config_is_async() -> None:
    """Test asyncpg config __is_async__ attribute."""
    pool_config = AsyncpgPoolConfig(
        host="localhost", port=5432, user="test_user", password="test_password", database="test_db"
    )
    config = AsyncpgConfig(pool_config=pool_config)
    assert config.__is_async__ is True
    assert AsyncpgConfig.__is_async__ is True


def test_asyncpg_config_supports_connection_pooling() -> None:
    """Test asyncpg config __supports_connection_pooling__ attribute."""
    pool_config = AsyncpgPoolConfig(
        host="localhost", port=5432, user="test_user", password="test_password", database="test_db"
    )
    config = AsyncpgConfig(pool_config=pool_config)
    assert config.__supports_connection_pooling__ is True
    assert AsyncpgConfig.__supports_connection_pooling__ is True


def test_asyncpg_config_dsn_configuration() -> None:
    """Test asyncpg config with DSN."""
    dsn = "postgresql://test_user:test_password@localhost:5432/test_db"
    pool_config = AsyncpgPoolConfig(dsn=dsn)
    config = AsyncpgConfig(pool_config=pool_config)

    # The config should be able to construct connection parameters
    config_dict = config.connection_config_dict
    assert "dsn" in config_dict
    assert config_dict["dsn"] == dsn


def test_asyncpg_config_minimal_connection() -> None:
    """Test asyncpg config with minimal connection parameters."""
    pool_config = AsyncpgPoolConfig(
        host="localhost", port=5432, user="test_user", password="test_password", database="test_db"
    )
    config = AsyncpgConfig(pool_config=pool_config)

    # Should work with just the required parameters
    assert config.pool_config.get("host") == "localhost"
    assert config.pool_config.get("port") == 5432
    assert config.pool_config.get("user") == "test_user"
    assert config.pool_config.get("password") == "test_password"
    assert config.pool_config.get("database") == "test_db"


def test_asyncpg_config_pool_settings() -> None:
    """Test asyncpg config with pool-specific settings."""
    pool_config = AsyncpgPoolConfig(
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
    config = AsyncpgConfig(pool_config=pool_config)
    assert config.pool_config.get("min_size") == 5
    assert config.pool_config.get("max_size") == 20
    assert config.pool_config.get("max_queries") == 50000
    assert config.pool_config.get("max_inactive_connection_lifetime") == 300.0


def test_asyncpg_config_with_connection_config() -> None:
    """Test asyncpg config with separate connection config."""
    connection_config = AsyncpgConnectionConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        database="test_db",
        connect_timeout=30.0,
    )
    pool_config = AsyncpgPoolConfig(
        min_size=1,
        max_size=10,
    )
    config = AsyncpgConfig(
        pool_config=pool_config,
        connection_config=connection_config,
    )

    # Connection config should be merged with pool config
    config_dict = config.connection_config_dict
    assert config_dict["host"] == "localhost"
    assert config_dict["port"] == 5432
    assert config_dict["connect_timeout"] == 30.0
    assert config_dict["min_size"] == 1
    assert config_dict["max_size"] == 10


def test_asyncpg_config_improper_configuration() -> None:
    """Test asyncpg config with minimal configuration."""
    # Pool config without host or dsn (validation removed, so this should work)
    pool_config = AsyncpgPoolConfig(
        user="test_user",
        password="test_password",
        database="test_db",
    )
    config = AsyncpgConfig(pool_config=pool_config)

    # Should return config dict without validation
    config_dict = config.connection_config_dict
    assert config_dict["user"] == "test_user"
    assert config_dict["password"] == "test_password"
    assert config_dict["database"] == "test_db"
