"""Unit tests for OracleDB configuration."""

from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest

from sqlspec.adapters.oracledb import (
    OracleAsyncConfig,
    OracleAsyncDriver,
    OracleConnectionConfig,
    OraclePoolConfig,
    OracleSyncConfig,
    OracleSyncDriver,
)
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.sql import SQLConfig


def test_oracle_connection_config_creation() -> None:
    """Test Oracle connection config creation with valid parameters."""
    # Test basic config creation
    config = OracleConnectionConfig(
        dsn="localhost:1521/XEPDB1",
        user="test_user",
        password="test_password",
        host="localhost",
        port=1521,
        service_name="XEPDB1",
        sid="XE",
        wallet_location="/path/to/wallet",
        wallet_password="wallet_pass",
        config_dir="/path/to/config",
        tcp_connect_timeout=30.0,
        retry_count=3,
        retry_delay=5,
        events=True,
        edition="test_edition",
    )
    assert config.get("dsn") == "localhost:1521/XEPDB1"
    assert config.get("user") == "test_user"
    assert config.get("password") == "test_password"
    assert config.get("host") == "localhost"
    assert config.get("port") == 1521
    assert config.get("service_name") == "XEPDB1"
    assert config.get("sid") == "XE"
    assert config.get("wallet_location") == "/path/to/wallet"
    assert config.get("wallet_password") == "wallet_pass"
    assert config.get("config_dir") == "/path/to/config"
    assert config.get("tcp_connect_timeout") == 30.0
    assert config.get("retry_count") == 3
    assert config.get("retry_delay") == 5
    assert config.get("events") is True
    assert config.get("edition") == "test_edition"

    # Test with minimal parameters
    config_minimal = OracleConnectionConfig(
        user="user",
        password="pass",
        service_name="service",
    )
    assert config_minimal.get("user") == "user"
    assert config_minimal.get("password") == "pass"
    assert config_minimal.get("service_name") == "service"

    # Test with DSN only
    config_dsn = OracleConnectionConfig(dsn="oracle://user:pass@localhost:1521/XEPDB1")
    assert config_dsn.get("dsn") == "oracle://user:pass@localhost:1521/XEPDB1"


def test_oracle_pool_config_creation() -> None:
    """Test Oracle pool config creation with valid parameters."""
    # Test basic pool config creation
    config = OraclePoolConfig(
        dsn="localhost:1521/XEPDB1",
        user="test_user",
        password="test_password",
        host="localhost",
        port=1521,
        service_name="XEPDB1",
        min=1,
        max=10,
        increment=2,
        threaded=True,
        getmode=1,
        homogeneous=True,
        timeout=300,
        wait_timeout=30,
        max_lifetime_session=3600,
        max_sessions_per_shard=100,
        soda_metadata_cache=True,
        ping_interval=60,
    )
    assert config.get("dsn") == "localhost:1521/XEPDB1"
    assert config.get("user") == "test_user"
    assert config.get("password") == "test_password"
    assert config.get("host") == "localhost"
    assert config.get("port") == 1521
    assert config.get("service_name") == "XEPDB1"
    assert config.get("min") == 1
    assert config.get("max") == 10
    assert config.get("increment") == 2
    assert config.get("threaded") is True
    assert config.get("getmode") == 1
    assert config.get("homogeneous") is True
    assert config.get("timeout") == 300
    assert config.get("wait_timeout") == 30
    assert config.get("max_lifetime_session") == 3600
    assert config.get("max_sessions_per_shard") == 100
    assert config.get("soda_metadata_cache") is True
    assert config.get("ping_interval") == 60

    # Test with all pool-specific parameters
    config_full = OraclePoolConfig(
        user="test_user",
        password="test_password",
        service_name="XEPDB1",
        min=2,
        max=20,
        increment=5,
        threaded=False,
        getmode=2,
        homogeneous=False,
        timeout=600,
        wait_timeout=60,
        max_lifetime_session=7200,
        wallet_location="/path/to/wallet",
        config_dir="/path/to/config",
        tcp_connect_timeout=45.0,
        retry_count=5,
        retry_delay=10,
        events=False,
        edition="prod_edition",
    )
    assert config_full.get("min") == 2
    assert config_full.get("max") == 20
    assert config_full.get("increment") == 5
    assert config_full.get("threaded") is False
    assert config_full.get("getmode") == 2
    assert config_full.get("homogeneous") is False
    assert config_full.get("timeout") == 600
    assert config_full.get("wait_timeout") == 60
    assert config_full.get("max_lifetime_session") == 7200
    assert config_full.get("wallet_location") == "/path/to/wallet"
    assert config_full.get("config_dir") == "/path/to/config"
    assert config_full.get("tcp_connect_timeout") == 45.0
    assert config_full.get("retry_count") == 5
    assert config_full.get("retry_delay") == 10
    assert config_full.get("events") is False
    assert config_full.get("edition") == "prod_edition"


def test_oracle_sync_config_initialization() -> None:
    """Test Oracle sync config initialization."""
    # Test with pool config only
    pool_config = OraclePoolConfig(
        user="test_user",
        password="test_password",
        service_name="XEPDB1",
        min=1,
        max=10,
    )
    config = OracleSyncConfig(pool_config=pool_config)
    assert config.pool_config.get("user") == "test_user"
    assert config.pool_config.get("password") == "test_password"
    assert config.pool_config.get("service_name") == "XEPDB1"
    assert config.pool_config.get("min") == 1
    assert config.pool_config.get("max") == 10
    assert isinstance(config.statement_config, SQLConfig)
    assert isinstance(config.instrumentation, InstrumentationConfig)
    assert config.connection_config == {}

    # Test with custom parameters
    custom_connection_config = OracleConnectionConfig(
        wallet_location="/path/to/wallet",
        tcp_connect_timeout=60.0,
        edition="custom_edition",
    )
    custom_pool_config = OraclePoolConfig(
        user="custom_user",
        password="custom_password",
        service_name="CUSTOM_SERVICE",
        host="custom_host",
        port=1522,
        min=2,
        max=15,
    )
    custom_statement_config = SQLConfig(strict_mode=False)
    custom_instrumentation = InstrumentationConfig(log_queries=True)

    config = OracleSyncConfig(
        pool_config=custom_pool_config,
        connection_config=custom_connection_config,
        statement_config=custom_statement_config,
        instrumentation=custom_instrumentation,
    )
    assert config.pool_config.get("user") == "custom_user"
    assert config.pool_config.get("password") == "custom_password"
    assert config.pool_config.get("service_name") == "CUSTOM_SERVICE"
    assert config.pool_config.get("host") == "custom_host"
    assert config.pool_config.get("port") == 1522
    assert config.pool_config.get("min") == 2
    assert config.pool_config.get("max") == 15
    assert config.connection_config.get("wallet_location") == "/path/to/wallet"
    assert config.connection_config.get("tcp_connect_timeout") == 60.0
    assert config.connection_config.get("edition") == "custom_edition"
    assert config.statement_config is custom_statement_config
    assert config.instrumentation.log_queries is True


def test_oracle_async_config_initialization() -> None:
    """Test Oracle async config initialization."""
    # Test with pool config only
    pool_config = OraclePoolConfig(
        user="test_user",
        password="test_password",
        service_name="XEPDB1",
        min=1,
        max=10,
    )
    config = OracleAsyncConfig(pool_config=pool_config)
    assert config.pool_config.get("user") == "test_user"
    assert config.pool_config.get("password") == "test_password"
    assert config.pool_config.get("service_name") == "XEPDB1"
    assert config.pool_config.get("min") == 1
    assert config.pool_config.get("max") == 10
    assert isinstance(config.statement_config, SQLConfig)
    assert isinstance(config.instrumentation, InstrumentationConfig)
    assert config.connection_config == {}

    # Test with custom parameters
    custom_connection_config = OracleConnectionConfig(
        wallet_location="/path/to/wallet",
        tcp_connect_timeout=60.0,
        edition="custom_edition",
    )
    custom_pool_config = OraclePoolConfig(
        user="custom_user",
        password="custom_password",
        service_name="CUSTOM_SERVICE",
        host="custom_host",
        port=1522,
        min=2,
        max=15,
    )
    custom_statement_config = SQLConfig(strict_mode=False)
    custom_instrumentation = InstrumentationConfig(log_queries=True)

    config = OracleAsyncConfig(
        pool_config=custom_pool_config,
        connection_config=custom_connection_config,
        statement_config=custom_statement_config,
        instrumentation=custom_instrumentation,
    )
    assert config.pool_config.get("user") == "custom_user"
    assert config.pool_config.get("password") == "custom_password"
    assert config.pool_config.get("service_name") == "CUSTOM_SERVICE"
    assert config.pool_config.get("host") == "custom_host"
    assert config.pool_config.get("port") == 1522
    assert config.pool_config.get("min") == 2
    assert config.pool_config.get("max") == 15
    assert config.connection_config.get("wallet_location") == "/path/to/wallet"
    assert config.connection_config.get("tcp_connect_timeout") == 60.0
    assert config.connection_config.get("edition") == "custom_edition"
    assert config.statement_config is custom_statement_config
    assert config.instrumentation.log_queries is True


def test_oracle_sync_config_connection_config_dict() -> None:
    """Test Oracle sync config connection_config_dict property."""
    connection_config = OracleConnectionConfig(
        wallet_location="/path/to/wallet",
        tcp_connect_timeout=30.0,
        edition="test_edition",
    )
    pool_config = OraclePoolConfig(
        user="test_user",
        password="test_password",
        service_name="XEPDB1",
        host="localhost",
        port=1521,
        min=1,
        max=10,
        # Overlapping parameter to test precedence
        tcp_connect_timeout=45.0,  # Should override connection_config tcp_connect_timeout
    )
    config = OracleSyncConfig(
        pool_config=pool_config,
        connection_config=connection_config,
    )

    config_dict = config.connection_config_dict
    expected_keys = {
        "user": "test_user",
        "password": "test_password",
        "service_name": "XEPDB1",
        "host": "localhost",
        "port": 1521,
        "min": 1,
        "max": 10,
        "tcp_connect_timeout": 45.0,  # Pool config takes precedence
        "wallet_location": "/path/to/wallet",  # From connection config
        "edition": "test_edition",  # From connection config
    }

    # Check that all expected keys are present
    for key, value in expected_keys.items():
        assert config_dict[key] == value


def test_oracle_async_config_connection_config_dict() -> None:
    """Test Oracle async config connection_config_dict property."""
    connection_config = OracleConnectionConfig(
        wallet_location="/path/to/wallet",
        tcp_connect_timeout=30.0,
        edition="test_edition",
    )
    pool_config = OraclePoolConfig(
        user="test_user",
        password="test_password",
        service_name="XEPDB1",
        host="localhost",
        port=1521,
        min=1,
        max=10,
        # Overlapping parameter to test precedence
        tcp_connect_timeout=45.0,  # Should override connection_config tcp_connect_timeout
    )
    config = OracleAsyncConfig(
        pool_config=pool_config,
        connection_config=connection_config,
    )

    config_dict = config.connection_config_dict
    expected_keys = {
        "user": "test_user",
        "password": "test_password",
        "service_name": "XEPDB1",
        "host": "localhost",
        "port": 1521,
        "min": 1,
        "max": 10,
        "tcp_connect_timeout": 45.0,  # Pool config takes precedence
        "wallet_location": "/path/to/wallet",  # From connection config
        "edition": "test_edition",  # From connection config
    }

    # Check that all expected keys are present
    for key, value in expected_keys.items():
        assert config_dict[key] == value


@patch("oracledb.create_pool")
def test_oracle_sync_config_create_pool_impl(mock_pool_func: Mock) -> None:
    """Test Oracle sync config _create_pool_impl method (mocked)."""
    mock_pool = Mock()
    mock_pool_func.return_value = mock_pool

    pool_config = OraclePoolConfig(
        user="test_user",
        password="test_password",
        service_name="XEPDB1",
        host="localhost",
        port=1521,
        min=1,
        max=10,
    )
    config = OracleSyncConfig(pool_config=pool_config)

    pool = config._create_pool_impl()

    # Verify create_pool was called with correct parameters
    mock_pool_func.assert_called_once()
    call_kwargs = mock_pool_func.call_args[1]
    assert call_kwargs["user"] == "test_user"
    assert call_kwargs["password"] == "test_password"
    assert call_kwargs["service_name"] == "XEPDB1"
    assert call_kwargs["host"] == "localhost"
    assert call_kwargs["port"] == 1521
    assert call_kwargs["min"] == 1
    assert call_kwargs["max"] == 10
    assert pool is mock_pool


@patch("oracledb.create_pool_async")
@pytest.mark.asyncio
async def test_oracle_async_config_create_pool_impl(mock_pool_func: Mock) -> None:
    """Test Oracle async config _create_pool_impl method (mocked)."""
    mock_pool = AsyncMock()
    mock_pool_func.return_value = mock_pool

    pool_config = OraclePoolConfig(
        user="test_user",
        password="test_password",
        service_name="XEPDB1",
        host="localhost",
        port=1521,
        min=1,
        max=10,
    )
    config = OracleAsyncConfig(pool_config=pool_config)

    pool = await config._create_pool_impl()

    # Verify create_pool_async was called with correct parameters
    mock_pool_func.assert_called_once()
    call_kwargs = mock_pool_func.call_args[1]
    assert call_kwargs["user"] == "test_user"
    assert call_kwargs["password"] == "test_password"
    assert call_kwargs["service_name"] == "XEPDB1"
    assert call_kwargs["host"] == "localhost"
    assert call_kwargs["port"] == 1521
    assert call_kwargs["min"] == 1
    assert call_kwargs["max"] == 10
    assert pool is mock_pool


@patch("oracledb.connect")
def test_oracle_sync_config_create_connection(mock_connect: Mock) -> None:
    """Test Oracle sync config create_connection method (mocked)."""
    mock_connection = Mock()
    mock_connect.return_value = mock_connection

    connection_config = OracleConnectionConfig(
        user="test_user",
        password="test_password",
        service_name="XEPDB1",
        host="localhost",
        port=1521,
        wallet_location="/path/to/wallet",
        tcp_connect_timeout=30.0,
    )
    pool_config = OraclePoolConfig(user="test_user")  # Basic pool config
    config = OracleSyncConfig(
        pool_config=pool_config,
        connection_config=connection_config,
    )

    connection = config.create_connection()

    # Verify connect was called with connection config parameters only
    mock_connect.assert_called_once()
    call_kwargs = mock_connect.call_args[1]
    assert call_kwargs["user"] == "test_user"
    assert call_kwargs["password"] == "test_password"
    assert call_kwargs["service_name"] == "XEPDB1"
    assert call_kwargs["host"] == "localhost"
    assert call_kwargs["port"] == 1521
    assert call_kwargs["wallet_location"] == "/path/to/wallet"
    assert call_kwargs["tcp_connect_timeout"] == 30.0
    assert connection is mock_connection


@patch("oracledb.connect_async")
@pytest.mark.asyncio
async def test_oracle_async_config_create_connection(mock_connect: Mock) -> None:
    """Test Oracle async config create_connection method (mocked)."""
    mock_connection = AsyncMock()
    # Make connect_async async

    async def mock_connect_async(**kwargs: Any) -> AsyncMock:
        return mock_connection

    mock_connect.side_effect = mock_connect_async

    connection_config = OracleConnectionConfig(
        user="test_user",
        password="test_password",
        service_name="XEPDB1",
        host="localhost",
        port=1521,
        wallet_location="/path/to/wallet",
        tcp_connect_timeout=30.0,
    )
    pool_config = OraclePoolConfig(user="test_user")  # Basic pool config
    config = OracleAsyncConfig(
        pool_config=pool_config,
        connection_config=connection_config,
    )

    connection = await config.create_connection()

    # Verify connect_async was called with connection config parameters only
    mock_connect.assert_called_once()
    call_kwargs = mock_connect.call_args[1]
    assert call_kwargs["user"] == "test_user"
    assert call_kwargs["password"] == "test_password"
    assert call_kwargs["service_name"] == "XEPDB1"
    assert call_kwargs["host"] == "localhost"
    assert call_kwargs["port"] == 1521
    assert call_kwargs["wallet_location"] == "/path/to/wallet"
    assert call_kwargs["tcp_connect_timeout"] == 30.0
    assert connection is mock_connection


@patch("oracledb.connect")
def test_oracle_sync_config_provide_connection_without_pool(mock_connect: Mock) -> None:
    """Test Oracle sync config provide_connection context manager without pool."""
    mock_connection = Mock()
    mock_connect.return_value = mock_connection

    pool_config = OraclePoolConfig(
        user="test_user",
        password="test_password",
        service_name="XEPDB1",
    )
    config = OracleSyncConfig(pool_config=pool_config)

    # Test context manager behavior (without pool)
    with config.provide_connection() as conn:
        assert conn is mock_connection
        # Verify connection is not closed yet
        mock_connection.close.assert_not_called()

    # Verify connection was closed after context exit
    mock_connection.close.assert_called_once()


@patch("oracledb.connect_async")
@pytest.mark.asyncio
async def test_oracle_async_config_provide_connection_without_pool(mock_connect: Mock) -> None:
    """Test Oracle async config provide_connection context manager without pool."""
    mock_connection = AsyncMock()
    # Make connect_async async

    async def mock_connect_async(**kwargs: Any) -> AsyncMock:
        return mock_connection

    mock_connect.side_effect = mock_connect_async

    pool_config = OraclePoolConfig(
        user="test_user",
        password="test_password",
        service_name="XEPDB1",
    )
    config = OracleAsyncConfig(pool_config=pool_config)

    # Test context manager behavior (without pool)
    async with config.provide_connection() as conn:
        assert conn is mock_connection
        # Verify connection is not closed yet
        mock_connection.close.assert_not_called()

    # Verify connection was closed after context exit
    mock_connection.close.assert_called_once()


def test_oracle_sync_config_provide_connection_with_pool() -> None:
    """Test Oracle sync config provide_connection context manager with pool."""
    mock_pool = Mock()
    mock_connection = Mock()

    # Setup mock pool acquire/release methods
    mock_pool.acquire.return_value = mock_connection
    mock_pool.release.return_value = None

    pool_config = OraclePoolConfig(
        user="test_user",
        password="test_password",
        service_name="XEPDB1",
    )
    config = OracleSyncConfig(pool_config=pool_config)
    config.pool_instance = mock_pool

    # Test context manager behavior (with pool)
    with config.provide_connection() as conn:
        assert conn is mock_connection

    # Verify pool acquire/release was used
    mock_pool.acquire.assert_called_once()
    mock_pool.release.assert_called_once_with(mock_connection)


@pytest.mark.asyncio
async def test_oracle_async_config_provide_connection_with_pool() -> None:
    """Test Oracle async config provide_connection context manager with pool."""
    mock_pool = AsyncMock()
    mock_connection = AsyncMock()

    # Setup mock pool acquire/release methods
    mock_pool.acquire.return_value = mock_connection
    mock_pool.release.return_value = None

    pool_config = OraclePoolConfig(
        user="test_user",
        password="test_password",
        service_name="XEPDB1",
    )
    config = OracleAsyncConfig(pool_config=pool_config)
    config.pool_instance = mock_pool

    # Test context manager behavior (with pool)
    async with config.provide_connection() as conn:
        assert conn is mock_connection

    # Verify pool acquire/release was used
    mock_pool.acquire.assert_called_once()
    mock_pool.release.assert_called_once_with(mock_connection)


@patch("oracledb.connect")
def test_oracle_sync_config_provide_connection_error_handling(mock_connect: Mock) -> None:
    """Test Oracle sync config provide_connection error handling."""
    mock_connection = Mock()
    mock_connect.return_value = mock_connection

    pool_config = OraclePoolConfig(
        user="test_user",
        password="test_password",
        service_name="XEPDB1",
    )
    config = OracleSyncConfig(pool_config=pool_config)

    # Test error handling and cleanup
    with pytest.raises(ValueError, match="Test error"):
        with config.provide_connection() as conn:
            assert conn is mock_connection
            raise ValueError("Test error")

    # Verify connection was still closed despite error
    mock_connection.close.assert_called_once()


@patch("oracledb.connect_async")
@pytest.mark.asyncio
async def test_oracle_async_config_provide_connection_error_handling(mock_connect: Mock) -> None:
    """Test Oracle async config provide_connection error handling."""
    mock_connection = AsyncMock()
    # Make connect_async async

    async def mock_connect_async(**kwargs: Any) -> AsyncMock:
        return mock_connection

    mock_connect.side_effect = mock_connect_async

    pool_config = OraclePoolConfig(
        user="test_user",
        password="test_password",
        service_name="XEPDB1",
    )
    config = OracleAsyncConfig(pool_config=pool_config)

    # Test error handling and cleanup
    with pytest.raises(ValueError, match="Test error"):
        async with config.provide_connection() as conn:
            assert conn is mock_connection
            raise ValueError("Test error")

    # Verify connection was still closed despite error
    mock_connection.close.assert_called_once()


@patch("oracledb.connect")
def test_oracle_sync_config_provide_session(mock_connect: Mock) -> None:
    """Test Oracle sync config provide_session context manager."""
    mock_connection = Mock()
    mock_connect.return_value = mock_connection

    pool_config = OraclePoolConfig(
        user="test_user",
        password="test_password",
        service_name="XEPDB1",
    )
    config = OracleSyncConfig(pool_config=pool_config)

    # Test session context manager behavior
    with config.provide_session() as session:
        assert isinstance(session, OracleSyncDriver)
        assert session.connection is mock_connection
        assert session.config is config.statement_config
        assert session.instrumentation_config is config.instrumentation
        # Verify connection is not closed yet
        mock_connection.close.assert_not_called()

    # Verify connection was closed after context exit
    mock_connection.close.assert_called_once()


@patch("oracledb.connect_async")
@pytest.mark.asyncio
async def test_oracle_async_config_provide_session(mock_connect: Mock) -> None:
    """Test Oracle async config provide_session context manager."""
    mock_connection = AsyncMock()
    # Make connect_async async

    async def mock_connect_async(**kwargs: Any) -> AsyncMock:
        return mock_connection

    mock_connect.side_effect = mock_connect_async

    pool_config = OraclePoolConfig(
        user="test_user",
        password="test_password",
        service_name="XEPDB1",
    )
    config = OracleAsyncConfig(pool_config=pool_config)

    # Test session context manager behavior
    async with config.provide_session() as session:
        assert isinstance(session, OracleAsyncDriver)
        assert session.connection is mock_connection
        assert session.config is config.statement_config
        assert session.instrumentation_config is config.instrumentation
        # Verify connection is not closed yet
        mock_connection.close.assert_not_called()

    # Verify connection was closed after context exit
    mock_connection.close.assert_called_once()


def test_oracle_sync_config_driver_type() -> None:
    """Test Oracle sync config driver_type property."""
    pool_config = OraclePoolConfig(
        user="test_user",
        password="test_password",
        service_name="XEPDB1",
    )
    config = OracleSyncConfig(pool_config=pool_config)
    assert config.driver_type is OracleSyncDriver


def test_oracle_async_config_driver_type() -> None:
    """Test Oracle async config driver_type property."""
    pool_config = OraclePoolConfig(
        user="test_user",
        password="test_password",
        service_name="XEPDB1",
    )
    config = OracleAsyncConfig(pool_config=pool_config)
    assert config.driver_type is OracleAsyncDriver


def test_oracle_sync_config_connection_type() -> None:
    """Test Oracle sync config connection_type property."""
    import oracledb

    pool_config = OraclePoolConfig(
        user="test_user",
        password="test_password",
        service_name="XEPDB1",
    )
    config = OracleSyncConfig(pool_config=pool_config)
    assert config.connection_type is oracledb.Connection


def test_oracle_async_config_connection_type() -> None:
    """Test Oracle async config connection_type property."""
    import oracledb

    pool_config = OraclePoolConfig(
        user="test_user",
        password="test_password",
        service_name="XEPDB1",
    )
    config = OracleAsyncConfig(pool_config=pool_config)
    assert config.connection_type is oracledb.AsyncConnection


def test_oracle_sync_config_is_async() -> None:
    """Test Oracle sync config __is_async__ attribute."""
    pool_config = OraclePoolConfig(
        user="test_user",
        password="test_password",
        service_name="XEPDB1",
    )
    config = OracleSyncConfig(pool_config=pool_config)
    assert config.__is_async__ is False
    assert OracleSyncConfig.__is_async__ is False


def test_oracle_async_config_is_async() -> None:
    """Test Oracle async config __is_async__ attribute."""
    pool_config = OraclePoolConfig(
        user="test_user",
        password="test_password",
        service_name="XEPDB1",
    )
    config = OracleAsyncConfig(pool_config=pool_config)
    assert config.__is_async__ is True
    assert OracleAsyncConfig.__is_async__ is True


def test_oracle_sync_config_supports_connection_pooling() -> None:
    """Test Oracle sync config __supports_connection_pooling__ attribute."""
    pool_config = OraclePoolConfig(
        user="test_user",
        password="test_password",
        service_name="XEPDB1",
    )
    config = OracleSyncConfig(pool_config=pool_config)
    assert config.__supports_connection_pooling__ is True
    assert OracleSyncConfig.__supports_connection_pooling__ is True


def test_oracle_async_config_supports_connection_pooling() -> None:
    """Test Oracle async config __supports_connection_pooling__ attribute."""
    pool_config = OraclePoolConfig(
        user="test_user",
        password="test_password",
        service_name="XEPDB1",
    )
    config = OracleAsyncConfig(pool_config=pool_config)
    assert config.__supports_connection_pooling__ is True
    assert OracleAsyncConfig.__supports_connection_pooling__ is True


def test_oracle_config_wallet_configuration() -> None:
    """Test Oracle config with wallet configuration."""
    pool_config = OraclePoolConfig(
        user="test_user",
        password="test_password",
        service_name="XEPDB1",
        wallet_location="/path/to/wallet",
        wallet_password="wallet_pass",
        config_dir="/path/to/config",
    )
    config = OracleSyncConfig(pool_config=pool_config)
    assert config.pool_config.get("wallet_location") == "/path/to/wallet"
    assert config.pool_config.get("wallet_password") == "wallet_pass"
    assert config.pool_config.get("config_dir") == "/path/to/config"


def test_oracle_config_connection_timeouts() -> None:
    """Test Oracle config with timeout settings."""
    pool_config = OraclePoolConfig(
        user="test_user",
        password="test_password",
        service_name="XEPDB1",
        tcp_connect_timeout=45.0,
        timeout=300,
        wait_timeout=60,
        max_lifetime_session=3600,
        retry_count=5,
        retry_delay=10,
    )
    config = OracleSyncConfig(pool_config=pool_config)
    assert config.pool_config.get("tcp_connect_timeout") == 45.0
    assert config.pool_config.get("timeout") == 300
    assert config.pool_config.get("wait_timeout") == 60
    assert config.pool_config.get("max_lifetime_session") == 3600
    assert config.pool_config.get("retry_count") == 5
    assert config.pool_config.get("retry_delay") == 10


def test_oracle_config_pool_settings() -> None:
    """Test Oracle config with pool-specific settings."""
    pool_config = OraclePoolConfig(
        user="test_user",
        password="test_password",
        service_name="XEPDB1",
        min=5,
        max=50,
        increment=10,
        threaded=True,
        getmode=2,
        homogeneous=False,
        max_sessions_per_shard=200,
        soda_metadata_cache=True,
        ping_interval=120,
    )
    config = OracleSyncConfig(pool_config=pool_config)
    assert config.pool_config.get("min") == 5
    assert config.pool_config.get("max") == 50
    assert config.pool_config.get("increment") == 10
    assert config.pool_config.get("threaded") is True
    assert config.pool_config.get("getmode") == 2
    assert config.pool_config.get("homogeneous") is False
    assert config.pool_config.get("max_sessions_per_shard") == 200
    assert config.pool_config.get("soda_metadata_cache") is True
    assert config.pool_config.get("ping_interval") == 120


def test_oracle_config_edition_support() -> None:
    """Test Oracle config with edition-based redefinition."""
    pool_config = OraclePoolConfig(
        user="test_user",
        password="test_password",
        service_name="XEPDB1",
        edition="test_edition",
    )
    config = OracleSyncConfig(pool_config=pool_config)
    assert config.pool_config.get("edition") == "test_edition"


def test_oracle_config_events_and_mode() -> None:
    """Test Oracle config with events and authentication mode."""
    pool_config = OraclePoolConfig(
        user="test_user",
        password="test_password",
        service_name="XEPDB1",
        events=True,
    )
    config = OracleSyncConfig(pool_config=pool_config)
    assert config.pool_config.get("events") is True


def test_oracle_sync_config_close_pool_impl() -> None:
    """Test Oracle sync config _close_pool_impl method."""
    mock_pool = Mock()

    pool_config = OraclePoolConfig(
        user="test_user",
        password="test_password",
        service_name="XEPDB1",
    )
    config = OracleSyncConfig(pool_config=pool_config)
    config.pool_instance = mock_pool

    config._close_pool_impl()

    # Verify pool close was called
    mock_pool.close.assert_called_once()


@pytest.mark.asyncio
async def test_oracle_async_config_close_pool_impl() -> None:
    """Test Oracle async config _close_pool_impl method."""
    mock_pool = AsyncMock()

    pool_config = OraclePoolConfig(
        user="test_user",
        password="test_password",
        service_name="XEPDB1",
    )
    config = OracleAsyncConfig(pool_config=pool_config)
    config.pool_instance = mock_pool

    await config._close_pool_impl()

    # Verify pool close was called
    mock_pool.close.assert_called_once()


def test_oracle_sync_config_provide_pool() -> None:
    """Test Oracle sync config provide_pool method."""
    mock_pool = Mock()

    pool_config = OraclePoolConfig(
        user="test_user",
        password="test_password",
        service_name="XEPDB1",
    )
    config = OracleSyncConfig(pool_config=pool_config)

    # Mock the create_pool method
    config.create_pool = Mock(return_value=mock_pool)

    # First call should create pool
    pool = config.provide_pool()
    assert pool is mock_pool
    assert config.pool_instance is mock_pool
    config.create_pool.assert_called_once()

    # Second call should return existing pool
    config.create_pool.reset_mock()
    pool2 = config.provide_pool()
    assert pool2 is mock_pool
    config.create_pool.assert_not_called()


@pytest.mark.asyncio
async def test_oracle_async_config_provide_pool() -> None:
    """Test Oracle async config provide_pool method."""
    mock_pool = AsyncMock()

    pool_config = OraclePoolConfig(
        user="test_user",
        password="test_password",
        service_name="XEPDB1",
    )
    config = OracleAsyncConfig(pool_config=pool_config)

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


def test_oracle_config_dsn_connection() -> None:
    """Test Oracle config with DSN connection string."""
    connection_config = OracleConnectionConfig(dsn="oracle://user:pass@localhost:1521/XEPDB1")
    pool_config = OraclePoolConfig(dsn="oracle://user:pass@localhost:1521/XEPDB1", min=1, max=5)
    config = OracleSyncConfig(
        pool_config=pool_config,
        connection_config=connection_config,
    )

    config_dict = config.connection_config_dict
    assert config_dict["dsn"] == "oracle://user:pass@localhost:1521/XEPDB1"


def test_oracle_config_sid_vs_service_name() -> None:
    """Test Oracle config with SID vs service_name."""
    # Test with SID
    pool_config_sid = OraclePoolConfig(
        user="test_user",
        password="test_password",
        host="localhost",
        port=1521,
        sid="XE",
    )
    config_sid = OracleSyncConfig(pool_config=pool_config_sid)
    assert config_sid.pool_config.get("sid") == "XE"
    assert config_sid.pool_config.get("service_name") is None

    # Test with service_name
    pool_config_service = OraclePoolConfig(
        user="test_user",
        password="test_password",
        host="localhost",
        port=1521,
        service_name="XEPDB1",
    )
    config_service = OracleSyncConfig(pool_config=pool_config_service)
    assert config_service.pool_config.get("service_name") == "XEPDB1"
    assert config_service.pool_config.get("sid") is None
