"""Unit tests for Psqlpy configuration."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sqlspec.adapters.psqlpy import CONNECTION_FIELDS, POOL_FIELDS, PsqlpyConfig, PsqlpyDriver
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.sql import SQLConfig


def test_psqlpy_field_constants() -> None:
    """Test Psqlpy CONNECTION_FIELDS and POOL_FIELDS constants."""
    expected_connection_fields = {
        "dsn",
        "username",
        "password",
        "db_name",
        "host",
        "port",
        "connect_timeout_sec",
        "connect_timeout_nanosec",
        "tcp_user_timeout_sec",
        "tcp_user_timeout_nanosec",
        "keepalives",
        "keepalives_idle_sec",
        "keepalives_idle_nanosec",
        "keepalives_interval_sec",
        "keepalives_interval_nanosec",
        "keepalives_retries",
        "ssl_mode",
        "ca_file",
        "target_session_attrs",
        "options",
        "application_name",
        "client_encoding",
        "gssencmode",
        "sslnegotiation",
        "sslcompression",
        "sslcert",
        "sslkey",
        "sslpassword",
        "sslrootcert",
        "sslcrl",
        "require_auth",
        "channel_binding",
        "krbsrvname",
        "gsslib",
        "gssdelegation",
        "service",
        "load_balance_hosts",
    }
    assert CONNECTION_FIELDS == expected_connection_fields

    # POOL_FIELDS should be a superset of CONNECTION_FIELDS
    assert CONNECTION_FIELDS.issubset(POOL_FIELDS)

    # Check pool-specific fields
    pool_specific = POOL_FIELDS - CONNECTION_FIELDS
    expected_pool_specific = {
        "hosts",
        "ports",
        "conn_recycling_method",
        "max_db_pool_size",
        "configure",
    }
    assert pool_specific == expected_pool_specific


def test_psqlpy_config_basic_creation() -> None:
    """Test Psqlpy config creation with basic parameters."""
    # Test minimal config creation
    config = PsqlpyConfig(dsn="postgresql://test_user:test_password@localhost:5432/test_db")
    assert config.dsn == "postgresql://test_user:test_password@localhost:5432/test_db"

    # Test with all parameters
    config_full = PsqlpyConfig(
        dsn="postgresql://test_user:test_password@localhost:5432/test_db", extras={"custom": "value"}
    )
    assert config.dsn == "postgresql://test_user:test_password@localhost:5432/test_db"
    assert config_full.extras["custom"] == "value"


def test_psqlpy_config_extras_handling() -> None:
    """Test Psqlpy config extras parameter handling."""
    # Test with explicit extras
    config = PsqlpyConfig(
        dsn="postgresql://test_user:test_password@localhost:5432/test_db",
        extras={"custom_param": "value", "debug": True},
    )
    assert config.extras["custom_param"] == "value"
    assert config.extras["debug"] is True

    # Test with kwargs going to extras
    config2 = PsqlpyConfig(
        dsn="postgresql://test_user:test_password@localhost:5432/test_db", unknown_param="test", another_param=42
    )
    assert config2.extras["unknown_param"] == "test"
    assert config2.extras["another_param"] == 42


def test_psqlpy_config_initialization() -> None:
    """Test Psqlpy config initialization."""
    # Test with default parameters
    config = PsqlpyConfig(dsn="postgresql://test_user:test_password@localhost:5432/test_db")
    assert isinstance(config.statement_config, SQLConfig)
    assert isinstance(config.instrumentation, InstrumentationConfig)

    # Test with custom parameters
    custom_statement_config = SQLConfig()
    custom_instrumentation = InstrumentationConfig(log_queries=True)

    config = PsqlpyConfig(
        dsn="postgresql://test_user:test_password@localhost:5432/test_db",
        statement_config=custom_statement_config,
        instrumentation=custom_instrumentation,
    )
    assert config.statement_config is custom_statement_config
    assert config.instrumentation.log_queries is True


@pytest.mark.asyncio
async def test_psqlpy_config_provide_session() -> None:
    """Test Psqlpy config provide_session context manager."""
    config = PsqlpyConfig(dsn="postgresql://test_user:test_password@localhost:5432/test_db")

    # Mock the pool creation to avoid real database connection
    with patch.object(config, "_create_pool") as mock_create_pool:
        # Create a mock pool with acquire context manager
        mock_pool = MagicMock()
        mock_connection = AsyncMock()
        mock_connection.close = AsyncMock()

        # Set up the acquire method to return an async context manager
        mock_pool.acquire = MagicMock()
        mock_acquire_cm = AsyncMock()
        mock_acquire_cm.__aenter__ = AsyncMock(return_value=mock_connection)
        mock_acquire_cm.__aexit__ = AsyncMock(return_value=None)
        mock_pool.acquire.return_value = mock_acquire_cm

        mock_create_pool.return_value = mock_pool

        # Test session context manager behavior
        async with config.provide_session() as session:
            assert isinstance(session, PsqlpyDriver)
            # Check that parameter styles were set
            assert session.config.allowed_parameter_styles == ("numeric",)
            assert session.config.target_parameter_style == "numeric"
            assert session.instrumentation_config is config.instrumentation


def test_psqlpy_config_driver_type() -> None:
    """Test Psqlpy config driver_type property."""
    config = PsqlpyConfig(dsn="postgresql://test_user:test_password@localhost:5432/test_db")
    assert config.driver_type is PsqlpyDriver


def test_psqlpy_config_is_async() -> None:
    """Test Psqlpy config __is_async__ attribute."""
    config = PsqlpyConfig(dsn="postgresql://test_user:test_password@localhost:5432/test_db")
    assert config.__is_async__ is True
    assert PsqlpyConfig.__is_async__ is True


def test_psqlpy_config_supports_connection_pooling() -> None:
    """Test Psqlpy config __supports_connection_pooling__ attribute."""
    config = PsqlpyConfig(dsn="postgresql://test_user:test_password@localhost:5432/test_db")
    assert config.__supports_connection_pooling__ is True
    assert PsqlpyConfig.__supports_connection_pooling__ is True


def test_psqlpy_config_from_pool_config() -> None:
    """Test Psqlpy config from_pool_config backward compatibility."""
    # Test basic backward compatibility
    pool_config = {
        "user": "test_user",
        "ports": "test_ports",
        "port": "test_port",
        "max_db_pool_size": 10,
        "conn_recycling_method": 10,
    }
    config = PsqlpyConfig.from_pool_config(pool_config)
    # Add specific assertions based on fields
    # 'user' is not a recognized PSQLPy parameter (should be 'username'), so it goes to extras
    # 'ports' is a valid pool field
    assert config.extras == {"user": "test_user"}

    # Test with extra parameters
    pool_config_with_extras = {
        "user": "test_user",
        "ports": "test_ports",
        "unknown_param": "test_value",
        "another_param": 42,
    }
    config_extras = PsqlpyConfig.from_pool_config(pool_config_with_extras)
    assert config_extras.extras["unknown_param"] == "test_value"
    assert config_extras.extras["another_param"] == 42
