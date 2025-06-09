"""Unit tests for Psycopg configuration."""

from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from sqlspec.adapters.psycopg import (
    CONNECTION_FIELDS,
    POOL_FIELDS,
    PsycopgAsyncConfig,
    PsycopgAsyncDriver,
    PsycopgSyncConfig,
    PsycopgSyncDriver,
)
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.sql import SQLConfig


def test_psycopg_field_constants() -> None:
    """Test Psycopg CONNECTION_FIELDS and POOL_FIELDS constants."""
    expected_connection_fields = frozenset(
        {
            "conninfo",
            "host",
            "port",
            "user",
            "password",
            "dbname",
            "connect_timeout",
            "options",
            "application_name",
            "sslmode",
            "sslcert",
            "sslkey",
            "sslrootcert",
            "autocommit",
        }
    )
    assert CONNECTION_FIELDS == expected_connection_fields

    # POOL_FIELDS should be a superset of CONNECTION_FIELDS
    assert CONNECTION_FIELDS.issubset(POOL_FIELDS)

    # Check pool-specific fields
    pool_specific = POOL_FIELDS - CONNECTION_FIELDS
    expected_pool_specific = {
        "min_size",
        "max_size",
        "name",
        "timeout",
        "max_waiting",
        "max_lifetime",
        "max_idle",
        "reconnect_timeout",
        "num_workers",
        "configure",
        "kwargs",
    }
    assert pool_specific == expected_pool_specific


def test_psycopg_sync_config_basic_creation() -> None:
    """Test Psycopg sync config creation with basic parameters."""
    # Test minimal config creation
    config = PsycopgSyncConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )
    assert config.host == "localhost"
    assert config.port == 5432
    assert config.user == "test_user"
    assert config.password == "test_password"
    assert config.dbname == "test_db"
    assert config.extras == {}

    # Test with all parameters
    config_full = PsycopgSyncConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
        connect_timeout=30.0,
        options="-c search_path=public",
        application_name="test_app",
        sslmode="require",
        sslcert="/path/to/cert.pem",
        sslkey="/path/to/key.pem",
        sslrootcert="/path/to/ca.pem",
        autocommit=True,
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
    assert config_full.host == "localhost"
    assert config_full.connect_timeout == 30.0
    assert config_full.options == "-c search_path=public"
    assert config_full.application_name == "test_app"
    assert config_full.sslmode == "require"
    assert config_full.min_size == 1
    assert config_full.max_size == 10
    assert config_full.name == "test_pool"


def test_psycopg_async_config_basic_creation() -> None:
    """Test Psycopg async config creation with basic parameters."""
    # Test minimal config creation
    config = PsycopgAsyncConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )
    assert config.host == "localhost"
    assert config.port == 5432
    assert config.user == "test_user"
    assert config.password == "test_password"
    assert config.dbname == "test_db"
    assert config.extras == {}


def test_psycopg_config_extras_handling() -> None:
    """Test Psycopg config extras parameter handling."""
    # Test with explicit extras
    config = PsycopgSyncConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
        extras={"custom_param": "value", "debug": True},
    )
    assert config.extras["custom_param"] == "value"
    assert config.extras["debug"] is True

    # Test with kwargs going to extras
    config2 = PsycopgSyncConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
        unknown_param="test",
        another_param=42,
    )
    assert config2.extras["unknown_param"] == "test"
    assert config2.extras["another_param"] == 42


def test_psycopg_sync_config_initialization() -> None:
    """Test Psycopg sync config initialization."""
    # Test with default parameters
    config = PsycopgSyncConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )
    assert config.host == "localhost"
    assert isinstance(config.statement_config, SQLConfig)
    assert isinstance(config.instrumentation, InstrumentationConfig)

    # Test with custom parameters
    custom_statement_config = SQLConfig(strict_mode=False)
    custom_instrumentation = InstrumentationConfig(log_queries=True)

    config = PsycopgSyncConfig(
        host="custom_host",
        port=5433,
        user="custom_user",
        password="custom_password",
        dbname="custom_db",
        min_size=2,
        max_size=15,
        statement_config=custom_statement_config,
        instrumentation=custom_instrumentation,
    )
    assert config.host == "custom_host"
    assert config.port == 5433
    assert config.min_size == 2
    assert config.max_size == 15
    assert config.statement_config is custom_statement_config
    assert config.instrumentation.log_queries is True


def test_psycopg_async_config_initialization() -> None:
    """Test Psycopg async config initialization."""
    # Test with default parameters
    config = PsycopgAsyncConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )
    assert config.host == "localhost"
    assert isinstance(config.statement_config, SQLConfig)
    assert isinstance(config.instrumentation, InstrumentationConfig)


def test_psycopg_config_connection_config_dict() -> None:
    """Test Psycopg config connection_config_dict and pool_config_dict properties."""
    config = PsycopgSyncConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
        connect_timeout=30.0,
        application_name="test_app",
        min_size=1,
        max_size=10,
        timeout=5.0,
    )

    # Test connection_config_dict returns only connection parameters
    conn_dict = config.connection_config_dict
    expected_conn = {
        "host": "localhost",
        "port": 5432,
        "user": "test_user",
        "password": "test_password",
        "dbname": "test_db",
        "connect_timeout": 30.0,
        "application_name": "test_app",
    }
    for key, value in expected_conn.items():
        assert conn_dict[key] == value

    # Connection config should not include pool-specific parameters
    assert "min_size" not in conn_dict
    assert "max_size" not in conn_dict
    assert "timeout" not in conn_dict

    # Test pool_config_dict returns all parameters
    pool_dict = config.pool_config_dict
    expected_pool = {
        "host": "localhost",
        "port": 5432,
        "user": "test_user",
        "password": "test_password",
        "dbname": "test_db",
        "connect_timeout": 30.0,
        "application_name": "test_app",
        "min_size": 1,
        "max_size": 10,
        "timeout": 5.0,
    }
    for key, value in expected_pool.items():
        assert pool_dict[key] == value

    # Verify DictRow is set
    from psycopg.rows import DictRow

    assert conn_dict["row_factory"] is DictRow
    assert pool_dict["row_factory"] is DictRow


@patch("sqlspec.adapters.psycopg.config.ConnectionPool")
def test_psycopg_sync_config_create_pool(mock_pool_class: Mock) -> None:
    """Test Psycopg sync config _create_pool method (mocked)."""
    mock_pool = Mock()
    mock_pool_class.return_value = mock_pool  # type: ignore
    config = PsycopgSyncConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
        min_size=1,
        max_size=10,
    )
    pool = config._create_pool()
    assert pool is mock_pool
    # Verify ConnectionPool was called with conninfo string
    mock_pool_class.assert_called_once()


@patch("sqlspec.adapters.psycopg.config.AsyncConnectionPool")
@pytest.mark.asyncio
async def test_psycopg_async_config_create_pool(mock_pool_class: Mock) -> None:
    """Test Psycopg async config _create_pool method (mocked)."""
    mock_pool = AsyncMock()
    mock_pool_class.return_value = mock_pool  # type: ignore
    config = PsycopgAsyncConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
        min_size=1,
        max_size=10,
    )
    pool = await config._create_pool()
    assert pool is mock_pool
    # Verify AsyncConnectionPool was called with conninfo string
    mock_pool_class.assert_called_once()


@patch("sqlspec.adapters.psycopg.config.connect")
def test_psycopg_sync_config_create_connection(mock_connect: Mock) -> None:
    """Test Psycopg sync config create_connection method (mocked)."""
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection
    config = PsycopgSyncConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
        sslmode="require",
        connect_timeout=30.0,
    )
    connection = config.create_connection()
    assert connection is mock_connection


@patch("sqlspec.adapters.psycopg.config.AsyncConnection.connect")
@pytest.mark.asyncio
async def test_psycopg_async_config_create_connection(mock_connect: Mock) -> None:
    """Test Psycopg async config create_connection method (mocked)."""
    mock_connection = AsyncMock()
    mock_connect.return_value = mock_connection

    config = PsycopgAsyncConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
        sslmode="require",
        connect_timeout=30.0,
    )

    connection = await config.create_connection()

    # Verify connect was called with correct parameters
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


@patch("sqlspec.adapters.psycopg.config.connect")
def test_psycopg_sync_config_provide_connection(mock_connect: Mock) -> None:
    """Test Psycopg sync config provide_connection context manager."""
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection
    config = PsycopgSyncConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )

    # Test context manager behavior (without pool)
    with config.provide_connection() as conn:
        assert conn is mock_connection
        # Verify connection is not closed yet
        mock_connection.close.assert_not_called()

    # Verify connection was closed after context exit
    mock_connection.close.assert_called_once()


@patch("sqlspec.adapters.psycopg.config.AsyncConnection.connect")
@pytest.mark.asyncio
async def test_psycopg_async_config_provide_connection(mock_connect: Mock) -> None:
    """Test Psycopg async config provide_connection context manager."""
    mock_connection = AsyncMock()
    mock_connect.return_value = mock_connection

    config = PsycopgAsyncConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )

    # Test context manager behavior (without pool)
    async with config.provide_connection() as conn:
        assert conn is mock_connection
        # Verify connection is not closed yet
        mock_connection.close.assert_not_called()

    # Verify connection was closed after context exit
    mock_connection.close.assert_called_once()


@patch("sqlspec.adapters.psycopg.config.connect")
def test_psycopg_sync_config_provide_connection_error_handling(mock_connect: Mock) -> None:
    """Test Psycopg sync config provide_connection error handling."""
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection
    config = PsycopgSyncConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )

    # Test error handling and cleanup
    with pytest.raises(ValueError):
        with config.provide_connection() as conn:
            assert conn is mock_connection
            raise ValueError("Test error")

    # Verify connection was still closed despite error
    mock_connection.close.assert_called_once()


@patch("sqlspec.adapters.psycopg.config.AsyncConnection.connect")
@pytest.mark.asyncio
async def test_psycopg_async_config_provide_connection_error_handling(mock_connect: Mock) -> None:
    """Test Psycopg async config provide_connection error handling."""
    mock_connection = AsyncMock()
    mock_connect.return_value = mock_connection

    config = PsycopgAsyncConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )

    # Test error handling and cleanup
    with pytest.raises(ValueError):
        async with config.provide_connection() as conn:
            assert conn is mock_connection
            raise ValueError("Test error")

    # Verify connection was still closed despite error
    mock_connection.close.assert_called_once()


@patch("sqlspec.adapters.psycopg.config.connect")
def test_psycopg_sync_config_provide_session(mock_connect: Mock) -> None:
    """Test Psycopg sync config provide_session context manager."""
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection
    config = PsycopgSyncConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )

    # Test session context manager behavior
    with config.provide_session() as session:
        assert isinstance(session, PsycopgSyncDriver)
        assert session.connection is mock_connection
        # Check that parameter styles were set
        assert session.config.allowed_parameter_styles == ("pyformat_positional", "pyformat_named")
        assert session.config.target_parameter_style == "pyformat_positional"
        assert session.instrumentation_config is config.instrumentation
        # Verify connection is not closed yet
        mock_connection.close.assert_not_called()

    # Verify connection was closed after context exit
    mock_connection.close.assert_called_once()


@patch("sqlspec.adapters.psycopg.config.AsyncConnection.connect")
@pytest.mark.asyncio
async def test_psycopg_async_config_provide_session(mock_connect: Mock) -> None:
    """Test Psycopg async config provide_session context manager."""
    mock_connection = AsyncMock()
    mock_connect.return_value = mock_connection

    config = PsycopgAsyncConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )

    # Test session context manager behavior
    async with config.provide_session() as session:
        assert isinstance(session, PsycopgAsyncDriver)
        assert session.connection is mock_connection
        # Check that parameter styles were set
        assert session.config.allowed_parameter_styles == ("pyformat_positional", "pyformat_named")
        assert session.config.target_parameter_style == "pyformat_positional"
        assert session.instrumentation_config is config.instrumentation
        # Verify connection is not closed yet
        mock_connection.close.assert_not_called()

    # Verify connection was closed after context exit
    mock_connection.close.assert_called_once()


def test_psycopg_sync_config_driver_type() -> None:
    """Test Psycopg sync config driver_type property."""
    config = PsycopgSyncConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )
    assert config.driver_type is PsycopgSyncDriver


def test_psycopg_async_config_driver_type() -> None:
    """Test Psycopg async config driver_type property."""
    config = PsycopgAsyncConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )
    assert config.driver_type is PsycopgAsyncDriver


def test_psycopg_sync_config_connection_type() -> None:
    """Test Psycopg sync config connection_type property."""
    from psycopg import Connection

    config = PsycopgSyncConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )
    assert config.connection_type is Connection


def test_psycopg_async_config_connection_type() -> None:
    """Test Psycopg async config connection_type property."""
    from psycopg import AsyncConnection

    config = PsycopgAsyncConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )
    assert config.connection_type is AsyncConnection


def test_psycopg_sync_config_is_async() -> None:
    """Test Psycopg sync config __is_async__ attribute."""
    config = PsycopgSyncConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )
    assert config.__is_async__ is False
    assert PsycopgSyncConfig.__is_async__ is False


def test_psycopg_async_config_is_async() -> None:
    """Test Psycopg async config __is_async__ attribute."""
    config = PsycopgAsyncConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )
    assert config.__is_async__ is True
    assert PsycopgAsyncConfig.__is_async__ is True


def test_psycopg_sync_config_supports_connection_pooling() -> None:
    """Test Psycopg sync config __supports_connection_pooling__ attribute."""
    config = PsycopgSyncConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )
    assert config.__supports_connection_pooling__ is True
    assert PsycopgSyncConfig.__supports_connection_pooling__ is True


def test_psycopg_async_config_supports_connection_pooling() -> None:
    """Test Psycopg async config __supports_connection_pooling__ attribute."""
    config = PsycopgAsyncConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
    )
    assert config.__supports_connection_pooling__ is True
    assert PsycopgAsyncConfig.__supports_connection_pooling__ is True


def test_psycopg_config_ssl_configuration() -> None:
    """Test Psycopg config with SSL configuration."""
    config = PsycopgSyncConfig(
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
    assert config.sslmode == "require"
    assert config.sslcert == "/path/to/cert.pem"
    assert config.sslkey == "/path/to/key.pem"
    assert config.sslrootcert == "/path/to/ca.pem"


def test_psycopg_config_from_pool_config() -> None:
    """Test Psycopg config from_pool_config backward compatibility."""
    # Test basic backward compatibility
    pool_config = {
        "host": "localhost",
        "port": 5432,
        "user": "test_user",
        "password": "test_password",
        "dbname": "test_db",
        "min_size": 1,
        "max_size": 10,
    }
    config = PsycopgSyncConfig.from_pool_config(pool_config)
    assert config.host == "localhost"
    assert config.port == 5432
    assert config.user == "test_user"
    assert config.min_size == 1
    assert config.max_size == 10
    assert config.extras == {}

    # Test with connection config
    connection_config = {
        "sslmode": "require",
        "connect_timeout": 30.0,
    }
    pool_config_override = {
        "host": "pool_host",
        "port": 5432,
        "user": "test_user",
        "password": "test_password",
        "dbname": "test_db",
        "min_size": 2,
        "max_size": 20,
        "sslmode": "prefer",  # Should override connection_config
    }
    config2 = PsycopgSyncConfig.from_pool_config(pool_config_override, connection_config=connection_config)
    assert config2.host == "pool_host"
    assert config2.port == 5432
    assert config2.connect_timeout == 30.0  # From connection_config
    assert config2.sslmode == "prefer"  # Pool config takes precedence
    assert config2.min_size == 2
    assert config2.max_size == 20

    # Test with extra parameters
    pool_config_with_extras = {
        "host": "localhost",
        "port": 5432,
        "user": "test_user",
        "password": "test_password",
        "dbname": "test_db",
        "unknown_param": "test_value",
        "another_param": 42,
    }
    config_extras = PsycopgSyncConfig.from_pool_config(pool_config_with_extras)
    assert config_extras.host == "localhost"
    assert config_extras.extras["unknown_param"] == "test_value"
    assert config_extras.extras["another_param"] == 42


def test_psycopg_config_conninfo() -> None:
    """Test Psycopg config with conninfo string."""
    config = PsycopgSyncConfig(conninfo="postgresql://user:pass@localhost:5432/testdb")
    assert config.conninfo == "postgresql://user:pass@localhost:5432/testdb"


def test_psycopg_config_application_name() -> None:
    """Test Psycopg config with application name settings."""
    config = PsycopgSyncConfig(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        dbname="test_db",
        application_name="my_app",
    )
    assert config.application_name == "my_app"


def test_psycopg_config_timeouts() -> None:
    """Test Psycopg config with timeout settings."""
    config = PsycopgSyncConfig(
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
    assert config.connect_timeout == 45.0
    assert config.timeout == 10.0
    assert config.max_lifetime == 3600.0
    assert config.max_idle == 600.0
    assert config.reconnect_timeout == 30.0
