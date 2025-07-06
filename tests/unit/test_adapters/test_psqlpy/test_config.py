"""Unit tests for Psqlpy configuration."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sqlspec.adapters.psqlpy import CONNECTION_FIELDS, POOL_FIELDS, PsqlpyConfig, PsqlpyDriver
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
    expected_pool_specific = {"hosts", "ports", "conn_recycling_method", "max_db_pool_size", "configure"}
    assert pool_specific == expected_pool_specific


def test_psqlpy_config_basic_creation() -> None:
    """Test Psqlpy config creation with basic parameters."""
    # Test minimal config creation
    config = PsqlpyConfig(dsn="postgresql://test_user:test_password@localhost:5432/test_db")
    assert config.dsn == "postgresql://test_user:test_password@localhost:5432/test_db"

    # Test with all parameters
    config_full = PsqlpyConfig(dsn="postgresql://test_user:test_password@localhost:5432/test_db", custom="value")
    assert config_full.dsn == "postgresql://test_user:test_password@localhost:5432/test_db"
    assert config_full.extras["custom"] == "value"


def test_psqlpy_config_extras_handling() -> None:
    """Test Psqlpy config extras parameter handling."""
    # Test with kwargs going to extras
    config = PsqlpyConfig(
        dsn="postgresql://test_user:test_password@localhost:5432/test_db", custom_param="value", debug=True
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
    # Test with custom parameters
    custom_statement_config = SQLConfig()
    config = PsqlpyConfig(
        dsn="postgresql://test_user:test_password@localhost:5432/test_db", statement_config=custom_statement_config
    )
    assert config.statement_config is custom_statement_config


@pytest.mark.asyncio
async def test_psqlpy_config_provide_session() -> None:
    """Test Psqlpy config provide_session context manager."""
    config = PsqlpyConfig(dsn="postgresql://test_user:test_password@localhost:5432/test_db")

    # Mock the pool creation to avoid real database connection
    with patch.object(PsqlpyConfig, "_create_pool") as mock_create_pool:
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


def test_psqlpy_config_driver_type() -> None:
    """Test Psqlpy config driver_type property."""
    config = PsqlpyConfig(dsn="postgresql://test_user:test_password@localhost:5432/test_db")
    assert config.driver_type is PsqlpyDriver


def test_psqlpy_config_is_async() -> None:
    """Test Psqlpy config is_async attribute."""
    config = PsqlpyConfig(dsn="postgresql://test_user:test_password@localhost:5432/test_db")
    assert config.is_async is True
    assert PsqlpyConfig.is_async is True


def test_psqlpy_config_supports_connection_pooling() -> None:
    """Test Psqlpy config supports_connection_pooling attribute."""
    config = PsqlpyConfig(dsn="postgresql://test_user:test_password@localhost:5432/test_db")
    assert config.supports_connection_pooling is True
    assert PsqlpyConfig.supports_connection_pooling is True
