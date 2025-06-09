"""Unit tests for OracleDB configuration."""

from sqlspec.adapters.oracledb import CONNECTION_FIELDS, POOL_FIELDS, OracleSyncConfig, OracleSyncDriver
from sqlspec.config import InstrumentationConfig
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
        dsn="localhost:1521/freepdb1", user="test_user", password="test_password", extras={"custom": "value"}
    )
    assert config.dsn == "localhost:1521/freepdb1"
    assert config.user == "test_user"
    assert config.password == "test_password"
    assert config_full.extras["custom"] == "value"


def test_oracledb_config_extras_handling() -> None:
    """Test OracleDB config extras parameter handling."""
    # Test with explicit extras
    config = OracleSyncConfig(
        dsn="localhost:1521/freepdb1",
        user="test_user",
        password="test_password",
        extras={"custom_param": "value", "debug": True},
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
    assert isinstance(config.instrumentation, InstrumentationConfig)

    # Test with custom parameters
    custom_statement_config = SQLConfig()
    custom_instrumentation = InstrumentationConfig(log_queries=True)

    config = OracleSyncConfig(
        dsn="localhost:1521/freepdb1",
        user="test_user",
        password="test_password",
        statement_config=custom_statement_config,
        instrumentation=custom_instrumentation,
    )
    assert config.statement_config is custom_statement_config
    assert config.instrumentation.log_queries is True


def test_oracledb_config_provide_session() -> None:
    """Test OracleDB config provide_session context manager."""
    config = OracleSyncConfig(dsn="localhost:1521/freepdb1", user="test_user", password="test_password")

    # Test session context manager behavior
    with config.provide_session() as session:
        assert isinstance(session, OracleSyncDriver)
        # Check that parameter styles were set
        assert session.config.allowed_parameter_styles == ("named_colon", "numeric")
        assert session.config.target_parameter_style == "named_colon"
        assert session.instrumentation_config is config.instrumentation


def test_oracledb_config_driver_type() -> None:
    """Test OracleDB config driver_type property."""
    config = OracleSyncConfig(dsn="localhost:1521/freepdb1", user="test_user", password="test_password")
    assert config.driver_type is OracleSyncDriver


def test_oracledb_config_is_async() -> None:
    """Test OracleDB config __is_async__ attribute."""
    config = OracleSyncConfig(dsn="localhost:1521/freepdb1", user="test_user", password="test_password")
    assert config.__is_async__ is False
    assert OracleSyncConfig.__is_async__ is False


def test_oracledb_config_supports_connection_pooling() -> None:
    """Test OracleDB config __supports_connection_pooling__ attribute."""
    config = OracleSyncConfig(dsn="localhost:1521/freepdb1", user="test_user", password="test_password")
    assert config.__supports_connection_pooling__ is True
    assert OracleSyncConfig.__supports_connection_pooling__ is True


def test_oracledb_config_from_pool_config() -> None:
    """Test OracleDB config from_pool_config backward compatibility."""
    # Test basic backward compatibility
    pool_config = {
        "service_name": "test_service_name",
        "port": "test_port",
        "tag": "test_tag",
        "session_callback": 10,
        "max": 10,
    }
    config = OracleSyncConfig.from_pool_config(pool_config)
    # Add specific assertions based on fields
    assert config.extras == {}

    # Test with extra parameters
    pool_config_with_extras = {
        "service_name": "test_service_name",
        "port": "test_port",
        "unknown_param": "test_value",
        "another_param": 42,
    }
    config_extras = OracleSyncConfig.from_pool_config(pool_config_with_extras)
    assert config_extras.extras["unknown_param"] == "test_value"
    assert config_extras.extras["another_param"] == 42
