"""Unit tests for PSQLPy configuration."""

from types import TracebackType
from typing import Any, Optional
from unittest.mock import AsyncMock, Mock, patch

import pytest

from sqlspec.adapters.psqlpy import PsqlpyConfig, PsqlpyConnectionConfig, PsqlpyDriver, PsqlpyPoolConfig
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.sql import SQLConfig


def test_psqlpy_connection_config_creation() -> None:
    """Test PSQLPy connection config creation with valid parameters."""
    # Test basic config creation
    config = PsqlpyConnectionConfig(
        host="localhost",
        port=5432,
        username="test_user",
        password="test_password",
        db_name="test_db",
        options="-c search_path=public",
        application_name="test_app",
        target_session_attrs="read-write",
        ssl_mode="prefer",
        sslcert="/path/to/cert.pem",
        sslkey="/path/to/key.pem",
        sslrootcert="/path/to/ca.pem",
        connect_timeout_sec=30,
        tcp_user_timeout_sec=60,
    )
    assert config.get("host") == "localhost"
    assert config.get("port") == 5432
    assert config.get("username") == "test_user"
    assert config.get("password") == "test_password"
    assert config.get("db_name") == "test_db"
    assert config.get("options") == "-c search_path=public"
    assert config.get("application_name") == "test_app"
    assert config.get("target_session_attrs") == "read-write"
    assert config.get("ssl_mode") == "prefer"
    assert config.get("sslcert") == "/path/to/cert.pem"
    assert config.get("sslkey") == "/path/to/key.pem"
    assert config.get("sslrootcert") == "/path/to/ca.pem"
    assert config.get("connect_timeout_sec") == 30
    assert config.get("tcp_user_timeout_sec") == 60

    # Test with minimal parameters
    config_minimal = PsqlpyConnectionConfig(
        host="localhost",
        username="user",
        password="pass",
        db_name="db",
    )
    assert config_minimal.get("host") == "localhost"
    assert config_minimal.get("username") == "user"
    assert config_minimal.get("password") == "pass"
    assert config_minimal.get("db_name") == "db"


def test_psqlpy_pool_config_creation() -> None:
    """Test PSQLPy pool config creation with valid parameters."""
    # Test basic pool config creation
    config = PsqlpyPoolConfig(
        host="localhost",
        port=5432,
        username="test_user",
        password="test_password",
        db_name="test_db",
        max_db_pool_size=20,
        conn_recycling_method="fast",
    )
    assert config.get("host") == "localhost"
    assert config.get("port") == 5432
    assert config.get("username") == "test_user"
    assert config.get("password") == "test_password"
    assert config.get("db_name") == "test_db"
    assert config.get("max_db_pool_size") == 20
    assert config.get("conn_recycling_method") == "fast"

    # Test with all pool-specific parameters
    config_full = PsqlpyPoolConfig(
        host="localhost",
        port=5432,
        username="test_user",
        password="test_password",
        db_name="test_db",
        max_db_pool_size=50,
        conn_recycling_method="verified",
        ssl_mode="require",
        application_name="pool_app",
        connect_timeout_sec=45,
    )
    assert config_full.get("max_db_pool_size") == 50
    assert config_full.get("conn_recycling_method") == "verified"
    assert config_full.get("ssl_mode") == "require"
    assert config_full.get("application_name") == "pool_app"
    assert config_full.get("connect_timeout_sec") == 45


def test_psqlpy_config_initialization() -> None:
    """Test PSQLPy config initialization."""
    # Test with pool config only
    pool_config = PsqlpyPoolConfig(
        host="localhost",
        port=5432,
        username="test_user",
        password="test_password",
        db_name="test_db",
        max_db_pool_size=10,
    )
    config = PsqlpyConfig(pool_config=pool_config)
    assert config.pool_config.get("host") == "localhost"
    assert config.pool_config.get("port") == 5432
    assert config.pool_config.get("max_db_pool_size") == 10
    assert isinstance(config.statement_config, SQLConfig)
    assert isinstance(config.instrumentation, InstrumentationConfig)
    assert config.connection_config == {}

    # Test with custom parameters
    custom_connection_config = PsqlpyConnectionConfig(
        ssl_mode="require",
        connect_timeout_sec=60,
        application_name="custom_app",
    )
    custom_pool_config = PsqlpyPoolConfig(
        host="custom_host",
        port=5433,
        username="custom_user",
        password="custom_password",
        db_name="custom_db",
        max_db_pool_size=25,
    )
    custom_statement_config = SQLConfig(strict_mode=False)
    custom_instrumentation = InstrumentationConfig(log_queries=True)

    config = PsqlpyConfig(
        pool_config=custom_pool_config,
        connection_config=custom_connection_config,
        statement_config=custom_statement_config,
        instrumentation=custom_instrumentation,
    )
    assert config.pool_config.get("host") == "custom_host"
    assert config.pool_config.get("port") == 5433
    assert config.pool_config.get("max_db_pool_size") == 25
    assert config.connection_config.get("ssl_mode") == "require"
    assert config.connection_config.get("connect_timeout_sec") == 60
    assert config.connection_config.get("application_name") == "custom_app"
    assert config.statement_config is custom_statement_config
    assert config.instrumentation.log_queries is True


def test_psqlpy_config_connection_config_dict() -> None:
    """Test PSQLPy config connection_config_dict property."""
    connection_config = PsqlpyConnectionConfig(
        ssl_mode="require",
        connect_timeout_sec=30,
        application_name="test_app",
    )
    pool_config = PsqlpyPoolConfig(
        host="localhost",
        port=5432,
        username="test_user",
        password="test_password",
        db_name="test_db",
        max_db_pool_size=10,
        # Overlapping parameter to test precedence
        ssl_mode="prefer",  # Should override connection_config ssl_mode
    )
    config = PsqlpyConfig(
        pool_config=pool_config,
        connection_config=connection_config,
    )

    config_dict = config.connection_config_dict
    expected = {
        "host": "localhost",
        "port": 5432,
        "username": "test_user",
        "password": "test_password",
        "db_name": "test_db",
        "max_db_pool_size": 10,
        "ssl_mode": "prefer",  # Pool config takes precedence
        "connect_timeout_sec": 30,  # From connection config
        "application_name": "test_app",  # From connection config
    }

    # Check that all expected keys are present
    for key, value in expected.items():
        assert config_dict[key] == value


def test_psqlpy_config_connection_config_dict_validation() -> None:
    """Test PSQLPy config connection_config_dict validation."""
    from sqlspec.exceptions import ImproperConfigurationError

    # Test with missing required parameters
    pool_config = PsqlpyPoolConfig(
        username="test_user",
        password="test_password",
        db_name="test_db",
    )
    config = PsqlpyConfig(pool_config=pool_config)

    with pytest.raises(ImproperConfigurationError, match="requires 'host'"):
        config.connection_config_dict

    # Test with host but missing username
    pool_config_with_host = PsqlpyPoolConfig(
        host="localhost",
        password="test_password",
        db_name="test_db",
    )
    config_with_host = PsqlpyConfig(pool_config=pool_config_with_host)

    with pytest.raises(ImproperConfigurationError, match="requires 'username'"):
        config_with_host.connection_config_dict

    # Test with valid config (should work)
    pool_config_valid = PsqlpyPoolConfig(
        host="localhost",
        username="test_user",
        password="test_password",
        db_name="test_db",
    )
    config_valid = PsqlpyConfig(pool_config=pool_config_valid)
    config_dict = config_valid.connection_config_dict
    assert config_dict["host"] == "localhost"
    assert config_dict["username"] == "test_user"


@patch("sqlspec.adapters.psqlpy.config.ConnectionPool")
@pytest.mark.asyncio
async def test_psqlpy_config_create_pool_impl(mock_pool_class: Mock) -> None:
    """Test PSQLPy config _create_pool_impl method (mocked)."""
    mock_pool = AsyncMock()
    mock_pool_class.return_value = mock_pool

    pool_config = PsqlpyPoolConfig(
        host="localhost",
        port=5432,
        username="test_user",
        password="test_password",
        db_name="test_db",
        max_db_pool_size=10,
    )
    config = PsqlpyConfig(pool_config=pool_config)

    pool = await config._create_pool_impl()

    # Verify ConnectionPool was called with correct parameters
    mock_pool_class.assert_called_once_with(
        host="localhost",
        port=5432,
        username="test_user",
        password="test_password",
        db_name="test_db",
        max_db_pool_size=10,
    )
    assert pool is mock_pool


@patch("sqlspec.adapters.psqlpy.config.Connection")
@pytest.mark.asyncio
async def test_psqlpy_config_create_connection(mock_connection_class: Mock) -> None:
    """Test PSQLPy config create_connection method (mocked)."""
    mock_connection = AsyncMock()
    mock_connection_class.return_value = mock_connection

    connection_config = PsqlpyConnectionConfig(
        host="localhost",
        port=5432,
        username="test_user",
        password="test_password",
        db_name="test_db",
        ssl_mode="require",
        connect_timeout_sec=30,
    )
    pool_config = PsqlpyPoolConfig(host="localhost")  # Basic pool config
    config = PsqlpyConfig(
        pool_config=pool_config,
        connection_config=connection_config,
    )

    connection = await config.create_connection()

    # Verify connect was called with connection config parameters only
    mock_connection_class.assert_called_once_with(
        host="localhost",
        port=5432,
        username="test_user",
        password="test_password",
        db_name="test_db",
        ssl_mode="require",
        connect_timeout_sec=30,
    )
    assert connection is mock_connection


@patch("sqlspec.adapters.psqlpy.config.Connection")
@pytest.mark.asyncio
async def test_psqlpy_config_provide_connection_without_pool(mock_connection_class: Mock) -> None:
    """Test PSQLPy config provide_connection context manager without pool."""
    mock_connection = AsyncMock()
    mock_connection_class.return_value = mock_connection

    pool_config = PsqlpyPoolConfig(
        host="localhost",
        port=5432,
        username="test_user",
        password="test_password",
        db_name="test_db",
    )
    config = PsqlpyConfig(pool_config=pool_config)

    # Test context manager behavior (without pool)
    async with config.provide_connection() as conn:
        assert conn is mock_connection
        # Verify connection is not closed yet
        mock_connection.close.assert_not_called()

    # Verify connection was closed after context exit
    mock_connection.close.assert_called_once()


@patch("sqlspec.adapters.psqlpy.config.Connection")
@pytest.mark.asyncio
async def test_psqlpy_config_provide_connection_with_pool(mock_connection_class: Mock) -> None:
    """Test PSQLPy config provide_connection context manager with pool."""
    mock_pool = AsyncMock()
    mock_connection = AsyncMock()
    mock_connection_class.return_value = mock_connection

    class MockAcquireCM:
        async def __aenter__(self) -> "Any":
            return mock_connection

        async def __aexit__(
            self,
            exc_type: "Optional[type[BaseException]]",
            exc: "Optional[BaseException]",
            tb: "Optional[TracebackType]",
        ) -> None:
            return None

    mock_pool.acquire = lambda *a, **kw: MockAcquireCM()  # type: ignore
    pool_config = PsqlpyPoolConfig(
        host="localhost",
        port=5432,
        username="test_user",
        password="test_password",
        db_name="test_db",
    )
    config = PsqlpyConfig(pool_config=pool_config)
    config.pool_instance = mock_pool
    # Test context manager behavior (with pool)
    async with config.provide_connection() as conn:
        assert conn is mock_connection
    # Verify pool acquire was used
    # mock_pool.acquire.assert_called_once()


@patch("sqlspec.adapters.psqlpy.config.Connection")
@pytest.mark.asyncio
async def test_psqlpy_config_provide_connection_error_handling(mock_connection_class: Mock) -> None:
    """Test PSQLPy config provide_connection error handling."""
    mock_connection = AsyncMock()
    mock_connection_class.return_value = mock_connection

    pool_config = PsqlpyPoolConfig(
        host="localhost",
        port=5432,
        username="test_user",
        password="test_password",
        db_name="test_db",
    )
    config = PsqlpyConfig(pool_config=pool_config)

    # Test error handling and cleanup
    with pytest.raises(ValueError):
        async with config.provide_connection() as conn:
            assert conn is mock_connection
            raise ValueError("Test error")

    # Verify connection was still closed despite error
    mock_connection.close.assert_called_once()


@patch("sqlspec.adapters.psqlpy.config.Connection")
@pytest.mark.asyncio
async def test_psqlpy_config_provide_session(mock_connection_class: Mock) -> None:
    """Test PSQLPy config provide_session context manager."""
    mock_connection = AsyncMock()
    mock_connection_class.return_value = mock_connection

    pool_config = PsqlpyPoolConfig(
        host="localhost",
        port=5432,
        username="test_user",
        password="test_password",
        db_name="test_db",
    )
    config = PsqlpyConfig(pool_config=pool_config)

    # Test session context manager behavior
    async with config.provide_session() as session:
        assert isinstance(session, PsqlpyDriver)
        assert session.connection is mock_connection
        assert session.config is config.statement_config
        assert session.instrumentation_config is config.instrumentation
        # Verify connection is not closed yet
        mock_connection.close.assert_not_called()

    # Verify connection was closed after context exit
    mock_connection.close.assert_called_once()


def test_psqlpy_config_driver_type() -> None:
    """Test PSQLPy config driver_type property."""
    pool_config = PsqlpyPoolConfig(
        host="localhost",
        port=5432,
        username="test_user",
        password="test_password",
        db_name="test_db",
    )
    config = PsqlpyConfig(pool_config=pool_config)
    assert config.driver_type is PsqlpyDriver


def test_psqlpy_config_connection_type() -> None:
    """Test PSQLPy config connection_type property."""
    from psqlpy import Connection

    pool_config = PsqlpyPoolConfig(
        host="localhost",
        port=5432,
        username="test_user",
        password="test_password",
        db_name="test_db",
    )
    config = PsqlpyConfig(pool_config=pool_config)
    assert config.connection_type is Connection


def test_psqlpy_config_is_async() -> None:
    """Test PSQLPy config __is_async__ attribute."""
    pool_config = PsqlpyPoolConfig(
        host="localhost",
        port=5432,
        username="test_user",
        password="test_password",
        db_name="test_db",
    )
    config = PsqlpyConfig(pool_config=pool_config)
    assert config.__is_async__ is True
    assert PsqlpyConfig.__is_async__ is True


def test_psqlpy_config_supports_connection_pooling() -> None:
    """Test PSQLPy config __supports_connection_pooling__ attribute."""
    pool_config = PsqlpyPoolConfig(
        host="localhost",
        port=5432,
        username="test_user",
        password="test_password",
        db_name="test_db",
    )
    config = PsqlpyConfig(pool_config=pool_config)
    assert config.__supports_connection_pooling__ is True
    assert PsqlpyConfig.__supports_connection_pooling__ is True


def test_psqlpy_config_ssl_configuration() -> None:
    """Test PSQLPy config with SSL configuration."""
    pool_config = PsqlpyPoolConfig(
        host="localhost",
        port=5432,
        username="test_user",
        password="test_password",
        db_name="test_db",
        ssl_mode="require",
        sslcert="/path/to/cert.pem",
        sslkey="/path/to/key.pem",
        sslrootcert="/path/to/ca.pem",
    )
    config = PsqlpyConfig(pool_config=pool_config)
    assert config.pool_config.get("ssl_mode") == "require"
    assert config.pool_config.get("sslcert") == "/path/to/cert.pem"
    assert config.pool_config.get("sslkey") == "/path/to/key.pem"
    assert config.pool_config.get("sslrootcert") == "/path/to/ca.pem"


def test_psqlpy_config_application_name() -> None:
    """Test PSQLPy config with application name settings."""
    pool_config = PsqlpyPoolConfig(
        host="localhost",
        port=5432,
        username="test_user",
        password="test_password",
        db_name="test_db",
        application_name="my_app",
    )
    config = PsqlpyConfig(pool_config=pool_config)
    assert config.pool_config.get("application_name") == "my_app"


def test_psqlpy_config_timeouts() -> None:
    """Test PSQLPy config with timeout settings."""
    pool_config = PsqlpyPoolConfig(
        host="localhost",
        port=5432,
        username="test_user",
        password="test_password",
        db_name="test_db",
        connect_timeout_sec=45,
        tcp_user_timeout_sec=90,
    )
    config = PsqlpyConfig(pool_config=pool_config)
    assert config.pool_config.get("connect_timeout_sec") == 45
    assert config.pool_config.get("tcp_user_timeout_sec") == 90


def test_psqlpy_config_pool_settings() -> None:
    """Test PSQLPy config with pool-specific settings."""
    pool_config = PsqlpyPoolConfig(
        host="localhost",
        port=5432,
        username="test_user",
        password="test_password",
        db_name="test_db",
        max_db_pool_size=50,
        conn_recycling_method="verified",
    )
    config = PsqlpyConfig(pool_config=pool_config)
    assert config.pool_config.get("max_db_pool_size") == 50
    assert config.pool_config.get("conn_recycling_method") == "verified"


def test_psqlpy_config_target_session_attrs() -> None:
    """Test PSQLPy config with target session attributes."""
    pool_config = PsqlpyPoolConfig(
        host="localhost",
        port=5432,
        username="test_user",
        password="test_password",
        db_name="test_db",
        target_session_attrs="read-write",
    )
    config = PsqlpyConfig(pool_config=pool_config)
    assert config.pool_config.get("target_session_attrs") == "read-write"


def test_psqlpy_config_options() -> None:
    """Test PSQLPy config with PostgreSQL options."""
    pool_config = PsqlpyPoolConfig(
        host="localhost",
        port=5432,
        username="test_user",
        password="test_password",
        db_name="test_db",
        options="-c search_path=public,private -c statement_timeout=30s",
    )
    config = PsqlpyConfig(pool_config=pool_config)
    assert config.pool_config.get("options") == "-c search_path=public,private -c statement_timeout=30s"


@pytest.mark.asyncio
async def test_psqlpy_config_close_pool_impl() -> None:
    """Test PSQLPy config _close_pool_impl method."""
    mock_pool = AsyncMock()
    pool_config = PsqlpyPoolConfig(
        host="localhost",
        port=5432,
        username="test_user",
        password="test_password",
        db_name="test_db",
    )
    config = PsqlpyConfig(pool_config=pool_config)
    config.pool_instance = mock_pool
    await config._close_pool_impl()
    # Verify pool close was called
    mock_pool.close.assert_called_once()


@pytest.mark.asyncio
async def test_psqlpy_config_provide_pool() -> None:
    """Test PSQLPy config provide_pool method."""
    mock_pool = AsyncMock()
    pool_config = PsqlpyPoolConfig(
        host="localhost",
        port=5432,
        username="test_user",
        password="test_password",
        db_name="test_db",
    )
    config = PsqlpyConfig(pool_config=pool_config)
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
