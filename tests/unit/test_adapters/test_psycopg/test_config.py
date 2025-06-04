"""Unit tests for Psycopg configuration."""

from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from sqlspec.adapters.psycopg import (
    PsycopgAsyncConfig,
    PsycopgAsyncDriver,
    PsycopgSyncConfig,
    PsycopgSyncDriver,
)
from sqlspec.adapters.psycopg.config import PsycopgConnectionConfig, PsycopgPoolConfig
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.sql import SQLConfig


def test_psycopg_connection_config_creation() -> None:
    """Test Psycopg connection config creation with valid parameters."""
    # Test basic config creation
    config = PsycopgConnectionConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
        connect_timeout=30.0,
        options="-c search_path=public",
        application_name="test_app",
        sslmode="prefer",
        sslcert="/path/to/cert.pem",
        sslkey="/path/to/key.pem",
        sslrootcert="/path/to/ca.pem",
        autocommit=True,
    )
    assert config.get("host") == "localhost"
    assert config.get("port") == 5432
    assert config.get("user") == "test_user"
    assert config.get("password") == "test_password"
    assert config.get("dbname") == "test_db"
    assert config.get("connect_timeout") == 30.0
    assert config.get("options") == "-c search_path=public"
    assert config.get("application_name") == "test_app"
    assert config.get("sslmode") == "prefer"
    assert config.get("sslcert") == "/path/to/cert.pem"
    assert config.get("sslkey") == "/path/to/key.pem"
    assert config.get("sslrootcert") == "/path/to/ca.pem"
    assert config.get("autocommit") is True

    # Test with minimal parameters
    config_minimal = PsycopgConnectionConfig(
        host="localhost",
        user="user",
        password="pass",
        dbname="db",
    )
    assert config_minimal.get("host") == "localhost"
    assert config_minimal.get("user") == "user"
    assert config_minimal.get("password") == "pass"
    assert config_minimal.get("dbname") == "db"

    # Test with conninfo string
    config_conninfo = PsycopgConnectionConfig(conninfo="postgresql://user:pass@localhost:5432/testdb")
    assert config_conninfo.get("conninfo") == "postgresql://user:pass@localhost:5432/testdb"


def test_psycopg_pool_config_creation() -> None:
    """Test Psycopg pool config creation with valid parameters."""
    # Test basic pool config creation
    config = PsycopgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
        min_size=1,
        max_size=10,
        name="test_pool",
        timeout=5.0,
        max_waiting=20,
        max_lifetime=3600.0,
        max_idle=600.0,
        reconnect_timeout=30.0,
        num_workers=2,
    )
    assert config.get("host") == "localhost"
    assert config.get("port") == 5432
    assert config.get("user") == "test_user"
    assert config.get("password") == "test_password"
    assert config.get("dbname") == "test_db"
    assert config.get("min_size") == 1
    assert config.get("max_size") == 10
    assert config.get("name") == "test_pool"
    assert config.get("timeout") == 5.0
    assert config.get("max_waiting") == 20
    assert config.get("max_lifetime") == 3600.0
    assert config.get("max_idle") == 600.0
    assert config.get("reconnect_timeout") == 30.0
    assert config.get("num_workers") == 2

    # Test with all pool-specific parameters
    config_full = PsycopgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
        min_size=2,
        max_size=20,
        name="full_pool",
        timeout=10.0,
        max_waiting=50,
        max_lifetime=7200.0,
        max_idle=1200.0,
        reconnect_timeout=60.0,
        num_workers=4,
        sslmode="require",
        application_name="pool_app",
        connect_timeout=45.0,
    )
    assert config_full.get("min_size") == 2
    assert config_full.get("max_size") == 20
    assert config_full.get("name") == "full_pool"
    assert config_full.get("timeout") == 10.0
    assert config_full.get("max_waiting") == 50
    assert config_full.get("max_lifetime") == 7200.0
    assert config_full.get("max_idle") == 1200.0
    assert config_full.get("reconnect_timeout") == 60.0
    assert config_full.get("num_workers") == 4
    assert config_full.get("sslmode") == "require"
    assert config_full.get("application_name") == "pool_app"
    assert config_full.get("connect_timeout") == 45.0


def test_psycopg_sync_config_initialization() -> None:
    """Test Psycopg sync config initialization."""
    # Test with pool config only
    pool_config = PsycopgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
        min_size=1,
        max_size=10,
    )
    config = PsycopgSyncConfig(pool_config=pool_config)
    assert config.pool_config.get("host") == "localhost"
    assert config.pool_config.get("port") == 5432
    assert config.pool_config.get("min_size") == 1
    assert config.pool_config.get("max_size") == 10
    assert isinstance(config.statement_config, SQLConfig)
    assert isinstance(config.instrumentation, InstrumentationConfig)
    assert config.connection_config == {}

    # Test with custom parameters
    custom_connection_config = PsycopgConnectionConfig(
        sslmode="require",
        connect_timeout=60.0,
        application_name="custom_app",
    )
    custom_pool_config = PsycopgPoolConfig(
        host="custom_host",
        port=5433,
        user="custom_user",
        password="custom_password",
        dbname="custom_db",
        min_size=2,
        max_size=15,
    )
    custom_statement_config = SQLConfig(strict_mode=False)
    custom_instrumentation = InstrumentationConfig(log_queries=True)

    config = PsycopgSyncConfig(
        pool_config=custom_pool_config,
        connection_config=custom_connection_config,
        statement_config=custom_statement_config,
        instrumentation=custom_instrumentation,
    )
    assert config.pool_config.get("host") == "custom_host"
    assert config.pool_config.get("port") == 5433
    assert config.pool_config.get("min_size") == 2
    assert config.pool_config.get("max_size") == 15
    assert config.connection_config.get("sslmode") == "require"
    assert config.connection_config.get("connect_timeout") == 60.0
    assert config.connection_config.get("application_name") == "custom_app"
    assert config.statement_config is custom_statement_config
    assert config.instrumentation.log_queries is True


def test_psycopg_async_config_initialization() -> None:
    """Test Psycopg async config initialization."""
    # Test with pool config only
    pool_config = PsycopgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
        min_size=1,
        max_size=10,
    )
    config = PsycopgAsyncConfig(pool_config=pool_config)
    assert config.pool_config.get("host") == "localhost"
    assert config.pool_config.get("port") == 5432
    assert config.pool_config.get("min_size") == 1
    assert config.pool_config.get("max_size") == 10
    assert isinstance(config.statement_config, SQLConfig)
    assert isinstance(config.instrumentation, InstrumentationConfig)
    assert config.connection_config == {}

    # Test with custom parameters
    custom_connection_config = PsycopgConnectionConfig(
        sslmode="require",
        connect_timeout=60.0,
        application_name="custom_app",
    )
    custom_pool_config = PsycopgPoolConfig(
        host="custom_host",
        port=5433,
        user="custom_user",
        password="custom_password",
        dbname="custom_db",
        min_size=2,
        max_size=15,
    )
    custom_statement_config = SQLConfig(strict_mode=False)
    custom_instrumentation = InstrumentationConfig(log_queries=True)

    config = PsycopgAsyncConfig(
        pool_config=custom_pool_config,
        connection_config=custom_connection_config,
        statement_config=custom_statement_config,
        instrumentation=custom_instrumentation,
    )
    assert config.pool_config.get("host") == "custom_host"
    assert config.pool_config.get("port") == 5433
    assert config.pool_config.get("min_size") == 2
    assert config.pool_config.get("max_size") == 15
    assert config.connection_config.get("sslmode") == "require"
    assert config.connection_config.get("connect_timeout") == 60.0
    assert config.connection_config.get("application_name") == "custom_app"
    assert config.statement_config is custom_statement_config
    assert config.instrumentation.log_queries is True


def test_psycopg_sync_config_connection_config_dict() -> None:
    """Test Psycopg sync config connection_config_dict property."""
    connection_config = PsycopgConnectionConfig(
        sslmode="require",
        connect_timeout=30.0,
        application_name="test_app",
    )
    pool_config = PsycopgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
        min_size=1,
        max_size=10,
        # Overlapping parameter to test precedence
        sslmode="prefer",  # Should override connection_config sslmode
    )
    config = PsycopgSyncConfig(
        pool_config=pool_config,
        connection_config=connection_config,
    )

    config_dict = config.connection_config_dict
    expected_keys = {
        "host": "localhost",
        "port": 5432,
        "user": "test_user",
        "password": "test_password",
        "dbname": "test_db",
        "min_size": 1,
        "max_size": 10,
        "sslmode": "prefer",  # Pool config takes precedence
        "connect_timeout": 30.0,  # From connection config
        "application_name": "test_app",  # From connection config
    }

    # Check that all expected keys are present
    for key, value in expected_keys.items():
        assert config_dict[key] == value

    # Verify DictRow is set
    from psycopg.rows import DictRow

    assert config_dict["row_factory"] is DictRow


def test_psycopg_async_config_connection_config_dict() -> None:
    """Test Psycopg async config connection_config_dict property."""
    connection_config = PsycopgConnectionConfig(
        sslmode="require",
        connect_timeout=30.0,
        application_name="test_app",
    )
    pool_config = PsycopgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
        min_size=1,
        max_size=10,
        # Overlapping parameter to test precedence
        sslmode="prefer",  # Should override connection_config sslmode
    )
    config = PsycopgAsyncConfig(
        pool_config=pool_config,
        connection_config=connection_config,
    )

    config_dict = config.connection_config_dict
    expected_keys = {
        "host": "localhost",
        "port": 5432,
        "user": "test_user",
        "password": "test_password",
        "dbname": "test_db",
        "min_size": 1,
        "max_size": 10,
        "sslmode": "prefer",  # Pool config takes precedence
        "connect_timeout": 30.0,  # From connection config
        "application_name": "test_app",  # From connection config
    }

    # Check that all expected keys are present
    for key, value in expected_keys.items():
        assert config_dict[key] == value

    # Verify DictRow is set
    from psycopg.rows import DictRow

    assert config_dict["row_factory"] is DictRow


@patch("psycopg_pool.ConnectionPool")
def test_psycopg_sync_config_create_pool_impl(mock_pool_class: Mock) -> None:
    """Test Psycopg sync config _create_pool_impl method (mocked)."""
    mock_pool = Mock()
    mock_pool_class.side_effect = lambda *args: mock_pool  # type: ignore
    pool_config = PsycopgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
        min_size=1,
        max_size=10,
    )
    config = PsycopgSyncConfig(pool_config=pool_config)
    pool = config._create_pool_impl()
    assert pool is mock_pool


@patch("psycopg_pool.AsyncConnectionPool")
@pytest.mark.asyncio
async def test_psycopg_async_config_create_pool_impl(mock_pool_class: Mock) -> None:
    """Test Psycopg async config _create_pool_impl method (mocked)."""
    mock_pool = AsyncMock()
    mock_pool_class.side_effect = lambda *args: mock_pool  # type: ignore
    pool_config = PsycopgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
        min_size=1,
        max_size=10,
    )
    config = PsycopgAsyncConfig(pool_config=pool_config)
    pool = await config._create_pool_impl()
    assert pool is mock_pool


@patch("psycopg.connect")
def test_psycopg_sync_config_create_connection(mock_connect: Mock) -> None:
    """Test Psycopg sync config create_connection method (mocked)."""
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection
    connection_config = PsycopgConnectionConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
        sslmode="require",
        connect_timeout=30.0,
    )
    pool_config = PsycopgPoolConfig(host="localhost")
    config = PsycopgSyncConfig(
        pool_config=pool_config,
        connection_config=connection_config,
    )
    connection = config.create_connection()
    assert connection is mock_connection


@patch("psycopg.AsyncConnection.connect")
@pytest.mark.asyncio
async def test_psycopg_async_config_create_connection(mock_connect: Mock) -> None:
    """Test Psycopg async config create_connection method (mocked)."""
    mock_connection = AsyncMock()
    mock_connect.return_value = mock_connection

    connection_config = PsycopgConnectionConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
        sslmode="require",
        connect_timeout=30.0,
    )
    pool_config = PsycopgPoolConfig(host="localhost")  # Basic pool config
    config = PsycopgAsyncConfig(
        pool_config=pool_config,
        connection_config=connection_config,
    )

    connection = await config.create_connection()

    # Verify connect was called with connection config parameters only
    mock_connect.assert_called_once()
    call_args = mock_connect.call_args[1]
    assert call_args["host"] == "localhost"
    assert call_args["port"] == 5432
    assert call_args["user"] == "test_user"
    assert call_args["password"] == "test_password"
    assert call_args["dbname"] == "test_db"
    assert call_args["sslmode"] == "require"
    assert call_args["connect_timeout"] == 30.0
    assert connection is mock_connection


@patch("psycopg.connect")
def test_psycopg_sync_config_provide_connection_error_handling(mock_connect: Mock) -> None:
    """Test Psycopg sync config provide_connection error handling."""
    MagicMock()
    mock_connect.side_effect = ValueError("mock error")
    pool_config = PsycopgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )
    config = PsycopgSyncConfig(pool_config=pool_config)
    with pytest.raises(ValueError, match="mock error"):
        with config.provide_connection():
            pass


@patch("psycopg.AsyncConnection.connect")
@pytest.mark.asyncio
async def test_psycopg_async_config_provide_connection_error_handling(mock_connect: Mock) -> None:
    """Test Psycopg async config provide_connection error handling."""
    mock_connection = AsyncMock()
    mock_connect.return_value = mock_connection

    pool_config = PsycopgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )
    config = PsycopgAsyncConfig(pool_config=pool_config)

    # Test error handling and cleanup
    with pytest.raises(ValueError):
        async with config.provide_connection() as conn:
            assert conn is mock_connection
            raise ValueError("Test error")

    # Verify connection was still closed despite error
    mock_connection.close.assert_called_once()


@patch("psycopg.connect")
def test_psycopg_sync_config_provide_session(mock_connect: Mock) -> None:
    """Test Psycopg sync config provide_session context manager."""
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection
    pool_config = PsycopgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )
    config = PsycopgSyncConfig(pool_config=pool_config)
    with config.provide_session() as session:
        assert session is mock_connection


@patch("psycopg.AsyncConnection.connect")
@pytest.mark.asyncio
async def test_psycopg_async_config_provide_session(mock_connect: Mock) -> None:
    """Test Psycopg async config provide_session context manager."""
    mock_connection = AsyncMock()
    mock_connect.return_value = mock_connection

    pool_config = PsycopgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )
    config = PsycopgAsyncConfig(pool_config=pool_config)

    # Test session context manager behavior
    async with config.provide_session() as session:
        assert isinstance(session, PsycopgAsyncDriver)
        assert session.connection is mock_connection
        assert session.config is config.statement_config
        assert session.instrumentation_config is config.instrumentation
        # Verify connection is not closed yet
        mock_connection.close.assert_not_called()

    # Verify connection was closed after context exit
    mock_connection.close.assert_called_once()


def test_psycopg_sync_config_driver_type() -> None:
    """Test Psycopg sync config driver_type property."""
    pool_config = PsycopgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )
    config = PsycopgSyncConfig(pool_config=pool_config)
    assert config.driver_type is PsycopgSyncDriver


def test_psycopg_async_config_driver_type() -> None:
    """Test Psycopg async config driver_type property."""
    pool_config = PsycopgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )
    config = PsycopgAsyncConfig(pool_config=pool_config)
    assert config.driver_type is PsycopgAsyncDriver


def test_psycopg_sync_config_connection_type() -> None:
    """Test Psycopg sync config connection_type property."""
    from psycopg import Connection

    pool_config = PsycopgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )
    config = PsycopgSyncConfig(pool_config=pool_config)
    assert config.connection_type is Connection


def test_psycopg_async_config_connection_type() -> None:
    """Test Psycopg async config connection_type property."""
    from psycopg import AsyncConnection

    pool_config = PsycopgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )
    config = PsycopgAsyncConfig(pool_config=pool_config)
    assert config.connection_type is AsyncConnection


def test_psycopg_sync_config_is_async() -> None:
    """Test Psycopg sync config __is_async__ attribute."""
    pool_config = PsycopgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )
    config = PsycopgSyncConfig(pool_config=pool_config)
    assert config.__is_async__ is False
    assert PsycopgSyncConfig.__is_async__ is False


def test_psycopg_async_config_is_async() -> None:
    """Test Psycopg async config __is_async__ attribute."""
    pool_config = PsycopgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )
    config = PsycopgAsyncConfig(pool_config=pool_config)
    assert config.__is_async__ is True
    assert PsycopgAsyncConfig.__is_async__ is True


def test_psycopg_sync_config_supports_connection_pooling() -> None:
    """Test Psycopg sync config __supports_connection_pooling__ attribute."""
    pool_config = PsycopgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )
    config = PsycopgSyncConfig(pool_config=pool_config)
    assert config.__supports_connection_pooling__ is True
    assert PsycopgSyncConfig.__supports_connection_pooling__ is True


def test_psycopg_async_config_supports_connection_pooling() -> None:
    """Test Psycopg async config __supports_connection_pooling__ attribute."""
    pool_config = PsycopgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )
    config = PsycopgAsyncConfig(pool_config=pool_config)
    assert config.__supports_connection_pooling__ is True
    assert PsycopgAsyncConfig.__supports_connection_pooling__ is True


def test_psycopg_config_ssl_configuration() -> None:
    """Test Psycopg config with SSL configuration."""
    pool_config = PsycopgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
        sslmode="require",
        sslcert="/path/to/cert.pem",
        sslkey="/path/to/key.pem",
        sslrootcert="/path/to/ca.pem",
    )
    config = PsycopgSyncConfig(pool_config=pool_config)
    assert config.pool_config.get("sslmode") == "require"
    assert config.pool_config.get("sslcert") == "/path/to/cert.pem"
    assert config.pool_config.get("sslkey") == "/path/to/key.pem"
    assert config.pool_config.get("sslrootcert") == "/path/to/ca.pem"


def test_psycopg_config_application_name() -> None:
    """Test Psycopg config with application name settings."""
    pool_config = PsycopgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
        application_name="my_app",
    )
    config = PsycopgSyncConfig(pool_config=pool_config)
    assert config.pool_config.get("application_name") == "my_app"


def test_psycopg_config_timeouts() -> None:
    """Test Psycopg config with timeout settings."""
    pool_config = PsycopgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
        connect_timeout=45.0,
        timeout=10.0,
        max_lifetime=3600.0,
        max_idle=600.0,
        reconnect_timeout=30.0,
    )
    config = PsycopgSyncConfig(pool_config=pool_config)
    assert config.pool_config.get("connect_timeout") == 45.0
    assert config.pool_config.get("timeout") == 10.0
    assert config.pool_config.get("max_lifetime") == 3600.0
    assert config.pool_config.get("max_idle") == 600.0
    assert config.pool_config.get("reconnect_timeout") == 30.0


def test_psycopg_config_pool_settings() -> None:
    """Test Psycopg config with pool-specific settings."""
    pool_config = PsycopgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
        min_size=5,
        max_size=50,
        name="my_pool",
        max_waiting=100,
        num_workers=8,
    )
    config = PsycopgSyncConfig(pool_config=pool_config)
    assert config.pool_config.get("min_size") == 5
    assert config.pool_config.get("max_size") == 50
    assert config.pool_config.get("name") == "my_pool"
    assert config.pool_config.get("max_waiting") == 100
    assert config.pool_config.get("num_workers") == 8


def test_psycopg_config_autocommit() -> None:
    """Test Psycopg config with autocommit settings."""
    pool_config = PsycopgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
        autocommit=True,
    )
    config = PsycopgSyncConfig(pool_config=pool_config)
    assert config.pool_config.get("autocommit") is True


def test_psycopg_config_options() -> None:
    """Test Psycopg config with PostgreSQL options."""
    pool_config = PsycopgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
        options="-c search_path=public,private -c statement_timeout=30s",
    )
    config = PsycopgSyncConfig(pool_config=pool_config)
    assert config.pool_config.get("options") == "-c search_path=public,private -c statement_timeout=30s"


def test_psycopg_sync_config_close_pool_impl() -> None:
    """Test Psycopg sync config _close_pool_impl method."""
    mock_pool = Mock()

    pool_config = PsycopgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )
    config = PsycopgSyncConfig(pool_config=pool_config)
    config.pool_instance = mock_pool

    config._close_pool_impl()

    # Verify pool close was called
    mock_pool.close.assert_called_once()


@pytest.mark.asyncio
async def test_psycopg_async_config_close_pool_impl() -> None:
    """Test Psycopg async config _close_pool_impl method."""
    mock_pool = AsyncMock()

    pool_config = PsycopgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )
    config = PsycopgAsyncConfig(pool_config=pool_config)
    config.pool_instance = mock_pool

    await config._close_pool_impl()

    # Verify pool close was called
    mock_pool.close.assert_called_once()


def test_psycopg_sync_config_provide_pool() -> None:
    """Test Psycopg sync config provide_pool method."""
    mock_pool = Mock()

    pool_config = PsycopgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )
    config = PsycopgSyncConfig(pool_config=pool_config)

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
async def test_psycopg_async_config_provide_pool() -> None:
    """Test Psycopg async config provide_pool method."""
    mock_pool = AsyncMock()

    pool_config = PsycopgPoolConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )
    config = PsycopgAsyncConfig(pool_config=pool_config)

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
