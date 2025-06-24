"""Unit tests for OracleDB configuration."""

from unittest.mock import MagicMock, patch

from sqlspec.adapters.oracledb import CONNECTION_FIELDS, POOL_FIELDS, OracleSyncConfig, OracleSyncDriver
from sqlspec.statement.sql import SQLConfig


def test_oracledb_field_constants() -> None:
    """Test OracleDB CONNECTION_FIELDS and POOL_FIELDS constants."""
    expected_connection_fields = {
        "dsn",
        "user",
        "password",
        "host",
        "port",
        "service_name",
        "sid",
        "wallet_location",
        "wallet_password",
        "config_dir",
        "tcp_connect_timeout",
        "retry_count",
        "retry_delay",
        "mode",
        "events",
        "edition",
    }
    assert CONNECTION_FIELDS == expected_connection_fields

    # POOL_FIELDS should be a superset of CONNECTION_FIELDS
    assert CONNECTION_FIELDS.issubset(POOL_FIELDS)

    # Check pool-specific fields
    pool_specific = POOL_FIELDS - CONNECTION_FIELDS
    expected_pool_specific = {
        "min",
        "max",
        "increment",
        "threaded",
        "getmode",
        "homogeneous",
        "timeout",
        "wait_timeout",
        "max_lifetime_session",
        "session_callback",
        "max_sessions_per_shard",
        "soda_metadata_cache",
        "ping_interval",
    }
    assert pool_specific == expected_pool_specific


def test_oracledb_config_basic_creation() -> None:
    """Test OracleDB config creation with basic parameters."""
    # Test minimal config creation
    config = OracleSyncConfig(dsn="localhost:1521/freepdb1", user="test_user", password="test_password")
    assert config.dsn == "localhost:1521/freepdb1"
    assert config.user == "test_user"
    assert config.password == "test_password"

    # Test with all parameters
    config_full = OracleSyncConfig(
        dsn="localhost:1521/freepdb1", user="test_user", password="test_password", custom="value"
    )
    assert config_full.dsn == "localhost:1521/freepdb1"
    assert config_full.user == "test_user"
    assert config_full.password == "test_password"
    assert config_full.extras["custom"] == "value"


def test_oracledb_config_extras_handling() -> None:
    """Test OracleDB config extras parameter handling."""
    # Test with kwargs going to extras
    config = OracleSyncConfig(
        dsn="localhost:1521/freepdb1", user="test_user", password="test_password", custom_param="value", debug=True
    )
    assert config.extras["custom_param"] == "value"
    assert config.extras["debug"] is True

    # Test with kwargs going to extras
    config2 = OracleSyncConfig(
        dsn="localhost:1521/freepdb1",
        user="test_user",
        password="test_password",
        unknown_param="test",
        another_param=42,
    )
    assert config2.extras["unknown_param"] == "test"
    assert config2.extras["another_param"] == 42


def test_oracledb_config_initialization() -> None:
    """Test OracleDB config initialization."""
    # Test with default parameters
    config = OracleSyncConfig(dsn="localhost:1521/freepdb1", user="test_user", password="test_password")
    assert isinstance(config.statement_config, SQLConfig)
    # Test with custom parameters
    custom_statement_config = SQLConfig()
    config = OracleSyncConfig(
        dsn="localhost:1521/freepdb1",
        user="test_user",
        password="test_password",
        statement_config=custom_statement_config,
    )
    assert config.statement_config is custom_statement_config


def test_oracledb_config_provide_session() -> None:
    """Test OracleDB config provide_session context manager."""
    config = OracleSyncConfig(dsn="localhost:1521/freepdb1", user="test_user", password="test_password")

    # Mock the pool creation to avoid real database connection
    with patch.object(OracleSyncConfig, "create_pool") as mock_create_pool:
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_pool.acquire.return_value = mock_connection
        mock_create_pool.return_value = mock_pool

        # Test session context manager behavior
        with config.provide_session() as session:
            assert isinstance(session, OracleSyncDriver)
            # Check that parameter styles were set
            assert session.config.allowed_parameter_styles == ("named_colon", "positional_colon")
            assert session.config.target_parameter_style == "named_colon"


def test_oracledb_config_driver_type() -> None:
    """Test OracleDB config driver_type property."""
    config = OracleSyncConfig(dsn="localhost:1521/freepdb1", user="test_user", password="test_password")
    assert config.driver_type is OracleSyncDriver


def test_oracledb_config_is_async() -> None:
    """Test OracleDB config is_async attribute."""
    config = OracleSyncConfig(dsn="localhost:1521/freepdb1", user="test_user", password="test_password")
    assert config.is_async is False
    assert OracleSyncConfig.is_async is False


def test_oracledb_config_supports_connection_pooling() -> None:
    """Test OracleDB config supports_connection_pooling attribute."""
    config = OracleSyncConfig(dsn="localhost:1521/freepdb1", user="test_user", password="test_password")
    assert config.supports_connection_pooling is True
    assert OracleSyncConfig.supports_connection_pooling is True
