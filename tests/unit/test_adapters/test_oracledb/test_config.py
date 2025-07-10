"""Unit tests for OracleDB configuration."""

from unittest.mock import MagicMock, patch

from sqlspec.adapters.oracledb import OracleConnectionParams, OraclePoolParams, OracleSyncConfig, OracleSyncDriver
from sqlspec.statement.sql import SQLConfig


def test_oracledb_typed_dict_structure() -> None:
    """Test OracleDB TypedDict structure."""
    # Test that we can create valid connection params
    connection_params: OracleConnectionParams = {
        "dsn": "localhost:1521/freepdb1",
        "user": "test_user",
        "password": "test_password",
        "host": "localhost",
        "port": 1521,
    }
    assert connection_params["dsn"] == "localhost:1521/freepdb1"
    assert connection_params["user"] == "test_user"

    # Test that pool params inherit from connection params and add pool-specific fields
    pool_params: OraclePoolParams = {
        "dsn": "localhost:1521/freepdb1",
        "user": "test_user",
        "password": "test_password",
        "min": 5,
        "max": 20,
        "timeout": 30,
    }
    assert pool_params["dsn"] == "localhost:1521/freepdb1"
    assert pool_params["min"] == 5


def test_oracledb_config_basic_creation() -> None:
    """Test OracleDB config creation with basic parameters."""
    # Test minimal config creation
    pool_config = {"dsn": "localhost:1521/freepdb1", "user": "test_user", "password": "test_password"}
    config = OracleSyncConfig(pool_config=pool_config)
    assert config.pool_config["dsn"] == "localhost:1521/freepdb1"
    assert config.pool_config["user"] == "test_user"
    assert config.pool_config["password"] == "test_password"

    # Test with additional parameters
    pool_config_full = {
        "dsn": "localhost:1521/freepdb1",
        "user": "test_user",
        "password": "test_password",
        "custom": "value",
    }
    config_full = OracleSyncConfig(pool_config=pool_config_full)
    assert config_full.pool_config["dsn"] == "localhost:1521/freepdb1"
    assert config_full.pool_config["user"] == "test_user"
    assert config_full.pool_config["password"] == "test_password"
    assert config_full.pool_config["custom"] == "value"


def test_oracledb_config_with_no_pool_config() -> None:
    """Test OracleDB config with no pool config."""
    config = OracleSyncConfig()

    # Should have empty pool_config
    assert config.pool_config == {}

    # Check base class attributes
    assert isinstance(config.statement_config, SQLConfig)
    assert config.default_row_type is dict


def test_oracledb_config_initialization() -> None:
    """Test OracleDB config initialization."""
    # Test with default parameters
    pool_config = {"dsn": "localhost:1521/freepdb1", "user": "test_user", "password": "test_password"}
    config = OracleSyncConfig(pool_config=pool_config)
    assert isinstance(config.statement_config, SQLConfig)

    # Test with custom parameters
    custom_statement_config = SQLConfig()
    config = OracleSyncConfig(pool_config=pool_config, statement_config=custom_statement_config)
    assert config.statement_config is custom_statement_config


def test_oracledb_config_provide_session() -> None:
    """Test OracleDB config provide_session context manager."""
    pool_config = {"dsn": "localhost:1521/freepdb1", "user": "test_user", "password": "test_password"}
    config = OracleSyncConfig(pool_config=pool_config)

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
            assert session.config.default_parameter_style == "named_colon"


def test_oracledb_config_driver_type() -> None:
    """Test OracleDB config driver_type property."""
    pool_config = {"dsn": "localhost:1521/freepdb1", "user": "test_user", "password": "test_password"}
    config = OracleSyncConfig(pool_config=pool_config)
    assert config.driver_type is OracleSyncDriver


def test_oracledb_config_is_async() -> None:
    """Test OracleDB config is_async attribute."""
    pool_config = {"dsn": "localhost:1521/freepdb1", "user": "test_user", "password": "test_password"}
    config = OracleSyncConfig(pool_config=pool_config)
    assert config.is_async is False
    assert OracleSyncConfig.is_async is False


def test_oracledb_config_supports_connection_pooling() -> None:
    """Test OracleDB config supports_connection_pooling attribute."""
    pool_config = {"dsn": "localhost:1521/freepdb1", "user": "test_user", "password": "test_password"}
    config = OracleSyncConfig(pool_config=pool_config)
    assert config.supports_connection_pooling is True
    assert OracleSyncConfig.supports_connection_pooling is True
