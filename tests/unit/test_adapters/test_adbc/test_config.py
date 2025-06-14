"""Unit tests for ADBC configuration."""

from sqlspec.adapters.adbc import CONNECTION_FIELDS, AdbcConfig, AdbcDriver
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.sql import SQLConfig


def test_adbc_field_constants() -> None:
    """Test ADBC CONNECTION_FIELDS constants."""
    expected_connection_fields = {
        "uri",
        "driver_name",
        "db_kwargs",
        "conn_kwargs",
        "adbc_driver_manager_entrypoint",
        "autocommit",
        "isolation_level",
        "batch_size",
        "query_timeout",
        "connection_timeout",
        "ssl_mode",
        "ssl_cert",
        "ssl_key",
        "ssl_ca",
        "username",
        "password",
        "token",
        "project_id",
        "dataset_id",
        "account",
        "warehouse",
        "database",
        "schema",
        "role",
        "authorization_header",
        "grpc_options",
    }
    assert CONNECTION_FIELDS == expected_connection_fields


def test_adbc_config_basic_creation() -> None:
    """Test ADBC config creation with basic parameters."""
    # Test minimal config creation
    config = AdbcConfig(driver_name="adbc_driver_sqlite", uri="file::memory:?mode=memory")
    assert config.driver_name == "adbc_driver_sqlite"
    assert config.uri == "file::memory:?mode=memory"

    # Test with all parameters
    config_full = AdbcConfig(
        driver_name="adbc_driver_sqlite", uri="file::memory:?mode=memory", extras={"custom": "value"}
    )
    assert config_full.driver_name == "adbc_driver_sqlite"
    assert config_full.uri == "file::memory:?mode=memory"
    assert config_full.extras["custom"] == "value"


def test_adbc_config_extras_handling() -> None:
    """Test ADBC config extras parameter handling."""
    # Test with explicit extras
    config = AdbcConfig(
        driver_name="adbc_driver_sqlite",
        uri="file::memory:?mode=memory",
        extras={"custom_param": "value", "debug": True},
    )
    assert config.extras["custom_param"] == "value"
    assert config.extras["debug"] is True

    # Test with kwargs going to extras
    config2 = AdbcConfig(
        driver_name="adbc_driver_sqlite", uri="file::memory:?mode=memory", unknown_param="test", another_param=42
    )
    assert config2.extras["unknown_param"] == "test"
    assert config2.extras["another_param"] == 42


def test_adbc_config_initialization() -> None:
    """Test ADBC config initialization."""
    # Test with default parameters
    config = AdbcConfig(driver_name="adbc_driver_sqlite", uri="file::memory:?mode=memory")
    assert isinstance(config.statement_config, SQLConfig)
    assert isinstance(config.instrumentation, InstrumentationConfig)

    # Test with custom parameters
    custom_statement_config = SQLConfig()
    custom_instrumentation = InstrumentationConfig(log_queries=True)

    config = AdbcConfig(
        driver_name="adbc_driver_sqlite",
        uri="file::memory:?mode=memory",
        statement_config=custom_statement_config,
        instrumentation=custom_instrumentation,
    )
    assert config.statement_config is custom_statement_config
    assert config.instrumentation.log_queries is True


def test_adbc_config_provide_session() -> None:
    """Test ADBC config provide_session context manager."""
    config = AdbcConfig(driver_name="adbc_driver_sqlite", uri="file::memory:?mode=memory")

    # Test session context manager behavior
    with config.provide_session() as session:
        assert isinstance(session, AdbcDriver)
        # Check that parameter styles were set
        assert session.config.allowed_parameter_styles == ("qmark", "named_colon")
        assert session.config.target_parameter_style == "qmark"
        assert session.instrumentation_config == config.instrumentation


def test_adbc_config_driver_type() -> None:
    """Test ADBC config driver_type property."""
    config = AdbcConfig(driver_name="adbc_driver_sqlite", uri="file::memory:?mode=memory")
    assert config.driver_type is AdbcDriver


def test_adbc_config_is_async() -> None:
    """Test ADBC config is_async attribute."""
    config = AdbcConfig(driver_name="adbc_driver_sqlite", uri="file::memory:?mode=memory")
    assert config.is_async is False
    assert AdbcConfig.is_async is False


def test_adbc_config_supports_connection_pooling() -> None:
    """Test ADBC config supports_connection_pooling attribute."""
    config = AdbcConfig(driver_name="adbc_driver_sqlite", uri="file::memory:?mode=memory")
    assert config.supports_connection_pooling is False
    assert AdbcConfig.supports_connection_pooling is False


def test_adbc_config_from_connection_config() -> None:
    """Test ADBC config from_connection_config backward compatibility."""
    # Test with driver_name (modern way)
    connection_config = {"db_kwargs": "test_db_kwargs", "driver_name": "test_driver", "uri": "test_uri"}
    config = AdbcConfig.from_connection_config(connection_config)
    assert config.driver_name == "test_driver"
    assert config.uri == "test_uri"

    # Test with driver (backward compatibility)
    connection_config_legacy = {"db_kwargs": "test_db_kwargs", "driver": "test_driver", "uri": "test_uri"}
    config_legacy = AdbcConfig.from_connection_config(connection_config_legacy)
    assert config_legacy.driver_name == "test_driver"  # Should be mapped to driver_name
    assert config_legacy.uri == "test_uri"

    # Test with extra parameters
    connection_config_with_extras = {
        "db_kwargs": "test_db_kwargs",
        "driver_name": "test_driver",
        "unknown_param": "test_value",
        "another_param": 42,
    }
    config_extras = AdbcConfig.from_connection_config(connection_config_with_extras)
    assert config_extras.extras["unknown_param"] == "test_value"
    assert config_extras.extras["another_param"] == 42
