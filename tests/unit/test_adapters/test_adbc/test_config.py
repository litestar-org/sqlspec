"""Unit tests for ADBC configuration."""

from sqlspec.adapters.adbc import AdbcConfig, AdbcConnectionParams, AdbcDriver
from sqlspec.statement.sql import SQLConfig


def test_adbc_typed_dict_structure() -> None:
    """Test ADBC TypedDict structure."""
    # Test that we can create valid connection params
    connection_params: AdbcConnectionParams = {
        "uri": "file::memory:?mode=memory",
        "driver_name": "adbc_driver_sqlite",
        "db_kwargs": {"test_key": "test_value"},
        "autocommit": True,
        "batch_size": 1000,
        "username": "test_user",
        "password": "test_pass",
    }
    assert connection_params["uri"] == "file::memory:?mode=memory"
    assert connection_params["driver_name"] == "adbc_driver_sqlite"
    assert connection_params["db_kwargs"] == {"test_key": "test_value"}
    assert connection_params["autocommit"] is True
    assert connection_params["batch_size"] == 1000
    assert connection_params["username"] == "test_user"
    assert connection_params["password"] == "test_pass"


def test_adbc_config_basic_creation() -> None:
    """Test ADBC config creation with basic parameters."""
    # Test minimal config creation
    connection_config = {"driver_name": "adbc_driver_sqlite", "uri": "file::memory:?mode=memory"}
    config = AdbcConfig(connection_config=connection_config)
    assert config.connection_config["driver_name"] == "adbc_driver_sqlite"
    assert config.connection_config["uri"] == "file::memory:?mode=memory"

    # Test with all parameters
    connection_config_full = {
        "driver_name": "adbc_driver_sqlite",
        "uri": "file::memory:?mode=memory",
        "autocommit": True,
        "batch_size": 1000,
    }
    # Add custom params via extra key in connection_config
    connection_config_full["extra"] = {"custom": "value"}
    config_full = AdbcConfig(connection_config=connection_config_full)
    assert config_full.connection_config["driver_name"] == "adbc_driver_sqlite"
    assert config_full.connection_config["uri"] == "file::memory:?mode=memory"
    assert config_full.connection_config["autocommit"] is True
    assert config_full.connection_config["batch_size"] == 1000
    assert config_full.connection_config["custom"] == "value"


def test_adbc_config_extras_handling() -> None:
    """Test ADBC config extras parameter handling."""
    # Test with extra params in connection_config
    connection_config = {
        "driver_name": "adbc_driver_sqlite",
        "uri": "file::memory:?mode=memory",
        "extra": {"custom_param": "value", "debug": True},
    }
    config = AdbcConfig(connection_config=connection_config)
    assert config.connection_config["custom_param"] == "value"
    assert config.connection_config["debug"] is True

    # Test with more extra params
    connection_config2 = {
        "driver_name": "adbc_driver_sqlite",
        "uri": "file::memory:?mode=memory",
        "extra": {"unknown_param": "test", "another_param": 42},
    }
    config2 = AdbcConfig(connection_config=connection_config2)
    assert config2.connection_config["unknown_param"] == "test"
    assert config2.connection_config["another_param"] == 42


def test_adbc_config_initialization() -> None:
    """Test ADBC config initialization."""
    # Test with default parameters
    connection_config = {"driver_name": "adbc_driver_sqlite", "uri": "file::memory:?mode=memory"}
    config = AdbcConfig(connection_config=connection_config)
    assert isinstance(config.statement_config, SQLConfig)
    # Test with custom parameters
    custom_statement_config = SQLConfig()
    config = AdbcConfig(connection_config=connection_config, statement_config=custom_statement_config)
    assert config.statement_config is custom_statement_config


def test_adbc_config_provide_session() -> None:
    """Test ADBC config provide_session context manager."""
    connection_config = {"driver_name": "adbc_driver_sqlite", "uri": "file::memory:?mode=memory"}
    config = AdbcConfig(connection_config=connection_config)

    # Test session context manager behavior
    with config.provide_session() as session:
        assert isinstance(session, AdbcDriver)
        # Check that parameter styles were set
        assert session.config.allowed_parameter_styles == ("qmark", "named_colon")
        assert session.config.default_parameter_style == "qmark"


def test_adbc_config_driver_type() -> None:
    """Test ADBC config driver_type property."""
    connection_config = {"driver_name": "adbc_driver_sqlite", "uri": "file::memory:?mode=memory"}
    config = AdbcConfig(connection_config=connection_config)
    assert config.driver_type is AdbcDriver


def test_adbc_config_is_async() -> None:
    """Test ADBC config is_async attribute."""
    connection_config = {"driver_name": "adbc_driver_sqlite", "uri": "file::memory:?mode=memory"}
    config = AdbcConfig(connection_config=connection_config)
    assert config.is_async is False
    assert AdbcConfig.is_async is False


def test_adbc_config_supports_connection_pooling() -> None:
    """Test ADBC config supports_connection_pooling attribute."""
    connection_config = {"driver_name": "adbc_driver_sqlite", "uri": "file::memory:?mode=memory"}
    config = AdbcConfig(connection_config=connection_config)
    assert config.supports_connection_pooling is False
    assert AdbcConfig.supports_connection_pooling is False


def test_adbc_config_from_connection_config() -> None:
    """Test ADBC config initialization with various parameters."""
    # Test basic initialization
    connection_config = {"driver_name": "test_driver", "uri": "test_uri", "db_kwargs": {"test_key": "test_value"}}
    config = AdbcConfig(connection_config=connection_config)
    assert config.connection_config["driver_name"] == "test_driver"
    assert config.connection_config["uri"] == "test_uri"
    assert config.connection_config["db_kwargs"] == {"test_key": "test_value"}

    # Test with extras (passed via extra key)
    connection_config_extras = {
        "driver_name": "test_driver",
        "uri": "test_uri",
        "extra": {"unknown_param": "test_value", "another_param": 42},
    }
    config_extras = AdbcConfig(connection_config=connection_config_extras)
    assert config_extras.connection_config["unknown_param"] == "test_value"
    assert config_extras.connection_config["another_param"] == 42
