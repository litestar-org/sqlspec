"""Unit tests for Asyncmy configuration."""

from unittest.mock import AsyncMock, Mock, patch

import pytest

from sqlspec.adapters.asyncmy import AsyncmyConfig, AsyncmyDriver, AsyncmyPoolConfig
from sqlspec.adapters.asyncmy.config import AsyncmyConnectionConfig
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.sql import SQLConfig


def test_asyncmy_connection_config_creation() -> None:
    """Test Asyncmy connection config creation with valid parameters."""
    # Test basic config creation
    config = AsyncmyConnectionConfig(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_password",
        database="test_db",
    )
    assert config.get("host") == "localhost"
    assert config.get("port") == 3306
    assert config.get("user") == "test_user"
    assert config.get("password") == "test_password"
    assert config.get("database") == "test_db"

    # Test with all core parameters
    config_full = AsyncmyConnectionConfig(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_password",
        database="test_db",
        unix_socket="/var/run/mysqld/mysqld.sock",
        charset="utf8mb4",
        connect_timeout=30.0,
        read_default_file="/etc/mysql/my.cnf",
        read_default_group="client",
        autocommit=True,
        local_infile=False,
        ssl={"ssl_disabled": True},
        sql_mode="STRICT_TRANS_TABLES",
        init_command="SET time_zone = '+00:00'",
    )
    assert config_full.get("host") == "localhost"
    assert config_full.get("port") == 3306
    assert config_full.get("unix_socket") == "/var/run/mysqld/mysqld.sock"
    assert config_full.get("charset") == "utf8mb4"
    assert config_full.get("connect_timeout") == 30.0
    assert config_full.get("read_default_file") == "/etc/mysql/my.cnf"
    assert config_full.get("read_default_group") == "client"
    assert config_full.get("autocommit") is True
    assert config_full.get("local_infile") is False
    assert config_full.get("ssl") == {"ssl_disabled": True}
    assert config_full.get("sql_mode") == "STRICT_TRANS_TABLES"
    assert config_full.get("init_command") == "SET time_zone = '+00:00'"


def test_asyncmy_pool_config_creation() -> None:
    """Test Asyncmy pool config creation with valid parameters."""
    # Test basic pool config creation
    config = AsyncmyPoolConfig(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_password",
        database="test_db",
        minsize=1,
        maxsize=10,
    )
    assert config.get("host") == "localhost"
    assert config.get("port") == 3306
    assert config.get("user") == "test_user"
    assert config.get("password") == "test_password"
    assert config.get("database") == "test_db"
    assert config.get("minsize") == 1
    assert config.get("maxsize") == 10

    # Test with all pool-specific parameters
    config_full = AsyncmyPoolConfig(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_password",
        database="test_db",
        minsize=2,
        maxsize=20,
        echo=True,
        pool_recycle=3600,
        charset="utf8mb4",
        connect_timeout=60.0,
        autocommit=False,
    )
    assert config_full.get("minsize") == 2
    assert config_full.get("maxsize") == 20
    assert config_full.get("echo") is True
    assert config_full.get("pool_recycle") == 3600
    assert config_full.get("charset") == "utf8mb4"
    assert config_full.get("connect_timeout") == 60.0
    assert config_full.get("autocommit") is False


def test_asyncmy_config_initialization() -> None:
    """Test Asyncmy config initialization."""
    # Test with pool config only
    pool_config = AsyncmyPoolConfig(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_password",
        database="test_db",
        minsize=1,
        maxsize=10,
    )
    config = AsyncmyConfig(pool_config=pool_config)
    assert config.pool_config.get("host") == "localhost"
    assert config.pool_config.get("port") == 3306
    assert config.pool_config.get("minsize") == 1
    assert config.pool_config.get("maxsize") == 10
    assert isinstance(config.statement_config, SQLConfig)
    assert isinstance(config.instrumentation, InstrumentationConfig)
    assert config.connection_config == {}

    # Test with custom parameters
    custom_connection_config = AsyncmyConnectionConfig(
        charset="latin1",
        connect_timeout=45.0,
        autocommit=True,
    )
    custom_pool_config = AsyncmyPoolConfig(
        host="custom_host",
        port=3307,
        user="custom_user",
        password="custom_password",
        database="custom_db",
        minsize=2,
        maxsize=15,
    )
    custom_statement_config = SQLConfig()
    custom_instrumentation = InstrumentationConfig(log_queries=True)

    config = AsyncmyConfig(
        pool_config=custom_pool_config,
        connection_config=custom_connection_config,
        statement_config=custom_statement_config,
        instrumentation=custom_instrumentation,
    )
    assert config.pool_config.get("host") == "custom_host"
    assert config.pool_config.get("port") == 3307
    assert config.pool_config.get("minsize") == 2
    assert config.pool_config.get("maxsize") == 15
    assert config.connection_config.get("charset") == "latin1"
    assert config.connection_config.get("connect_timeout") == 45.0
    assert config.connection_config.get("autocommit") is True
    assert config.statement_config is custom_statement_config
    assert config.instrumentation.log_queries is True


def test_asyncmy_config_connection_config_dict() -> None:
    """Test Asyncmy config connection_config_dict property."""
    connection_config = AsyncmyConnectionConfig(
        charset="utf8mb4",
        connect_timeout=30.0,
        autocommit=True,
    )
    pool_config = AsyncmyPoolConfig(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_password",
        database="test_db",
        minsize=1,
        maxsize=10,
        echo=True,
        # Overlapping parameter to test precedence
        charset="latin1",  # Should override connection_config charset
    )
    config = AsyncmyConfig(
        pool_config=pool_config,
        connection_config=connection_config,
    )

    config_dict = config.connection_config_dict
    expected = {
        "host": "localhost",
        "port": 3306,
        "user": "test_user",
        "password": "test_password",
        "database": "test_db",
        "minsize": 1,
        "maxsize": 10,
        "echo": True,
        "charset": "latin1",  # Pool config takes precedence
        "connect_timeout": 30.0,  # From connection config
        "autocommit": True,  # From connection config
    }

    # Check that all expected keys are present
    for key, value in expected.items():
        assert config_dict[key] == value


def test_asyncmy_config_connection_config_dict_validation() -> None:
    """Test Asyncmy config connection_config_dict validation."""
    from sqlspec.exceptions import ImproperConfigurationError

    # Test with neither host nor unix_socket
    pool_config = AsyncmyPoolConfig(
        user="test_user",
        password="test_password",
        database="test_db",
    )
    config = AsyncmyConfig(pool_config=pool_config)

    with pytest.raises(ImproperConfigurationError, match="requires either 'host' or 'unix_socket'"):
        config.connection_config_dict

    # Test with host (should work)
    pool_config_with_host = AsyncmyPoolConfig(
        host="localhost",
        user="test_user",
        password="test_password",
        database="test_db",
    )
    config_with_host = AsyncmyConfig(pool_config=pool_config_with_host)
    config_dict = config_with_host.connection_config_dict
    assert config_dict["host"] == "localhost"

    # Test with unix_socket (should work)
    pool_config_with_socket = AsyncmyPoolConfig(
        unix_socket="/var/run/mysqld/mysqld.sock",
        user="test_user",
        password="test_password",
        database="test_db",
    )
    config_with_socket = AsyncmyConfig(pool_config=pool_config_with_socket)
    config_dict = config_with_socket.connection_config_dict
    assert config_dict["unix_socket"] == "/var/run/mysqld/mysqld.sock"


@patch("asyncmy.create_pool")
@pytest.mark.asyncio
async def test_asyncmy_config_create_pool_impl(mock_create_pool: Mock) -> None:
    """Test Asyncmy config _create_pool_impl method (mocked)."""
    mock_pool = AsyncMock()
    mock_create_pool.return_value = mock_pool

    pool_config = AsyncmyPoolConfig(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_password",
        database="test_db",
        minsize=1,
        maxsize=10,
    )
    config = AsyncmyConfig(pool_config=pool_config)

    pool = await config._create_pool_impl()

    # Verify create_pool was called with correct parameters
    mock_create_pool.assert_called_once_with(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_password",
        database="test_db",
        minsize=1,
        maxsize=10,
    )
    assert pool is mock_pool


@patch("asyncmy.connect")
@pytest.mark.asyncio
async def test_asyncmy_config_create_connection(mock_connect: Mock) -> None:
    """Test Asyncmy config create_connection method (mocked)."""
    mock_connection = AsyncMock()
    mock_connect.return_value = mock_connection

    connection_config = AsyncmyConnectionConfig(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_password",
        database="test_db",
        charset="utf8mb4",
        connect_timeout=30.0,
    )
    pool_config = AsyncmyPoolConfig(host="localhost")  # Basic pool config
    config = AsyncmyConfig(
        pool_config=pool_config,
        connection_config=connection_config,
    )

    connection = await config.create_connection()

    # Verify connect was called with connection config parameters only
    mock_connect.assert_called_once_with(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_password",
        database="test_db",
        charset="utf8mb4",
        connect_timeout=30.0,
    )
    assert connection is mock_connection


@patch("asyncmy.connect")
@pytest.mark.asyncio
async def test_asyncmy_config_provide_connection_without_pool(mock_connect: Mock) -> None:
    """Test Asyncmy config provide_connection context manager without pool."""
    mock_connection = AsyncMock()
    mock_connect.return_value = mock_connection

    pool_config = AsyncmyPoolConfig(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_password",
        database="test_db",
    )
    config = AsyncmyConfig(pool_config=pool_config)

    # Test context manager behavior (without pool)
    async with config.provide_connection() as conn:
        assert conn is mock_connection
        # Verify connection is not closed yet
        mock_connection.close.assert_not_called()

    # Verify connection was closed after context exit
    mock_connection.close.assert_called_once()


@pytest.mark.asyncio
async def test_asyncmy_config_provide_connection_with_pool() -> None:
    """Test Asyncmy config provide_connection context manager with pool."""
    mock_pool = AsyncMock()
    mock_connection = AsyncMock()

    # Setup mock pool acquire context manager
    mock_pool.acquire.return_value.__aenter__.return_value = mock_connection
    mock_pool.acquire.return_value.__aexit__.return_value = None

    pool_config = AsyncmyPoolConfig(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_password",
        database="test_db",
    )
    config = AsyncmyConfig(pool_config=pool_config)
    config.pool_instance = mock_pool

    # Test context manager behavior (with pool)
    async with config.provide_connection() as conn:
        assert conn is mock_connection

    # Verify pool acquire was used
    mock_pool.acquire.assert_called_once()


@patch("asyncmy.connect")
@pytest.mark.asyncio
async def test_asyncmy_config_provide_connection_error_handling(mock_connect: Mock) -> None:
    """Test Asyncmy config provide_connection error handling."""
    mock_connection = AsyncMock()
    mock_connect.return_value = mock_connection

    pool_config = AsyncmyPoolConfig(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_password",
        database="test_db",
    )
    config = AsyncmyConfig(pool_config=pool_config)

    # Test error handling and cleanup
    with pytest.raises(ValueError):
        async with config.provide_connection() as conn:
            assert conn is mock_connection
            raise ValueError("Test error")

    # Verify connection was still closed despite error
    mock_connection.close.assert_called_once()


@patch("asyncmy.connect")
@pytest.mark.asyncio
async def test_asyncmy_config_provide_session(mock_connect: Mock) -> None:
    """Test Asyncmy config provide_session context manager."""
    mock_connection = AsyncMock()
    mock_connect.return_value = mock_connection

    pool_config = AsyncmyPoolConfig(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_password",
        database="test_db",
    )
    config = AsyncmyConfig(pool_config=pool_config)

    # Test session context manager behavior
    async with config.provide_session() as session:
        assert isinstance(session, AsyncmyDriver)
        assert session.connection is mock_connection
        assert session.config is config.statement_config
        assert session.instrumentation_config is config.instrumentation
        # Verify connection is not closed yet
        mock_connection.close.assert_not_called()

    # Verify connection was closed after context exit
    mock_connection.close.assert_called_once()


def test_asyncmy_config_driver_type() -> None:
    """Test Asyncmy config driver_type property."""
    pool_config = AsyncmyPoolConfig(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_password",
        database="test_db",
    )
    config = AsyncmyConfig(pool_config=pool_config)
    assert config.driver_type is AsyncmyDriver


def test_asyncmy_config_connection_type() -> None:
    """Test Asyncmy config connection_type property."""
    from asyncmy.connection import Connection

    pool_config = AsyncmyPoolConfig(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_password",
        database="test_db",
    )
    config = AsyncmyConfig(pool_config=pool_config)
    assert config.connection_type is Connection


def test_asyncmy_config_is_async() -> None:
    """Test Asyncmy config __is_async__ attribute."""
    pool_config = AsyncmyPoolConfig(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_password",
        database="test_db",
    )
    config = AsyncmyConfig(pool_config=pool_config)
    assert config.__is_async__ is True
    assert AsyncmyConfig.__is_async__ is True


def test_asyncmy_config_supports_connection_pooling() -> None:
    """Test Asyncmy config __supports_connection_pooling__ attribute."""
    pool_config = AsyncmyPoolConfig(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_password",
        database="test_db",
    )
    config = AsyncmyConfig(pool_config=pool_config)
    assert config.__supports_connection_pooling__ is True
    assert AsyncmyConfig.__supports_connection_pooling__ is True


def test_asyncmy_config_ssl_configuration() -> None:
    """Test Asyncmy config with SSL configuration."""
    ssl_config = {
        "ssl_disabled": False,
        "ssl_ca": "/path/to/ca.pem",
        "ssl_cert": "/path/to/cert.pem",
        "ssl_key": "/path/to/key.pem",
    }

    pool_config = AsyncmyPoolConfig(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_password",
        database="test_db",
        ssl=ssl_config,
    )
    config = AsyncmyConfig(pool_config=pool_config)
    assert config.pool_config.get("ssl") == ssl_config


def test_asyncmy_config_charset_and_sql_mode() -> None:
    """Test Asyncmy config with charset and SQL mode settings."""
    pool_config = AsyncmyPoolConfig(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_password",
        database="test_db",
        charset="utf8mb4",
        sql_mode="STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO",
        init_command="SET time_zone = '+00:00'",
    )
    config = AsyncmyConfig(pool_config=pool_config)
    assert config.pool_config.get("charset") == "utf8mb4"
    assert (
        config.pool_config.get("sql_mode")
        == "STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO"
    )
    assert config.pool_config.get("init_command") == "SET time_zone = '+00:00'"


def test_asyncmy_config_timeouts() -> None:
    """Test Asyncmy config with timeout settings."""
    pool_config = AsyncmyPoolConfig(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_password",
        database="test_db",
        connect_timeout=45.0,
        pool_recycle=7200,
    )
    config = AsyncmyConfig(pool_config=pool_config)
    assert config.pool_config.get("connect_timeout") == 45.0
    assert config.pool_config.get("pool_recycle") == 7200


def test_asyncmy_config_autocommit_and_local_infile() -> None:
    """Test Asyncmy config with autocommit and local_infile settings."""
    pool_config = AsyncmyPoolConfig(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_password",
        database="test_db",
        autocommit=False,
        local_infile=True,
    )
    config = AsyncmyConfig(pool_config=pool_config)
    assert config.pool_config.get("autocommit") is False
    assert config.pool_config.get("local_infile") is True


def test_asyncmy_config_unix_socket() -> None:
    """Test Asyncmy config with Unix socket connection."""
    pool_config = AsyncmyPoolConfig(
        unix_socket="/var/run/mysqld/mysqld.sock",
        user="test_user",
        password="test_password",
        database="test_db",
    )
    config = AsyncmyConfig(pool_config=pool_config)
    assert config.pool_config.get("unix_socket") == "/var/run/mysqld/mysqld.sock"


def test_asyncmy_config_read_default_file() -> None:
    """Test Asyncmy config with MySQL configuration file settings."""
    pool_config = AsyncmyPoolConfig(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_password",
        database="test_db",
        read_default_file="/etc/mysql/my.cnf",
        read_default_group="client",
    )
    config = AsyncmyConfig(pool_config=pool_config)
    assert config.pool_config.get("read_default_file") == "/etc/mysql/my.cnf"
    assert config.pool_config.get("read_default_group") == "client"


@pytest.mark.asyncio
async def test_asyncmy_config_close_pool_impl() -> None:
    """Test Asyncmy config _close_pool_impl method."""
    mock_pool = AsyncMock()

    pool_config = AsyncmyPoolConfig(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_password",
        database="test_db",
    )
    config = AsyncmyConfig(pool_config=pool_config)
    config.pool_instance = mock_pool

    await config._close_pool_impl()

    # Verify pool close was called
    mock_pool.close.assert_called_once()


@pytest.mark.asyncio
async def test_asyncmy_config_provide_pool() -> None:
    """Test Asyncmy config provide_pool method."""
    mock_pool = AsyncMock()

    pool_config = AsyncmyPoolConfig(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_password",
        database="test_db",
    )
    config = AsyncmyConfig(pool_config=pool_config)

    # Mock the create_pool method
    config.create_pool = AsyncMock(return_value=mock_pool)

    # First call should create pool
    pool = await config.provide_pool()
    assert pool is mock_pool
    assert config.pool_instance is mock_pool
    config.create_pool.assert_called_once()

    # Second call should return existing pool
    config.create_pool.reset_mock()
    pool2 = await config.provide_pool()
    assert pool2 is mock_pool
    config.create_pool.assert_not_called()
