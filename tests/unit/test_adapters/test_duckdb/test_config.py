"""Unit tests for DuckDB configuration."""

from sqlspec.adapters.duckdb import CONNECTION_FIELDS, DuckDBConfig, DuckDBDriver
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.sql import SQLConfig


def test_duckdb_field_constants() -> None:
    """Test DuckDB CONNECTION_FIELDS constants."""
    expected_connection_fields = {
        "database",
        "read_only",
        "config",
        "memory_limit",
        "threads",
        "temp_directory",
        "max_temp_directory_size",
        "autoload_known_extensions",
        "autoinstall_known_extensions",
        "allow_community_extensions",
        "allow_unsigned_extensions",
        "extension_directory",
        "custom_extension_repository",
        "autoinstall_extension_repository",
        "allow_persistent_secrets",
        "enable_external_access",
        "secret_directory",
        "enable_object_cache",
        "parquet_metadata_cache",
        "enable_external_file_cache",
        "checkpoint_threshold",
        "enable_progress_bar",
        "progress_bar_time",
        "enable_logging",
        "log_query_path",
        "logging_level",
        "preserve_insertion_order",
        "default_null_order",
        "default_order",
        "ieee_floating_point_ops",
        "binary_as_string",
        "arrow_large_buffer_size",
        "errors_as_json",
    }
    assert CONNECTION_FIELDS == expected_connection_fields


def test_duckdb_config_basic_creation() -> None:
    """Test DuckDB config creation with basic parameters."""
    # Test minimal config creation
    config = DuckDBConfig(database=":memory:")
    assert config.database == ":memory:"

    # Test with all parameters
    config_full = DuckDBConfig(database=":memory:", extras={"custom": "value"})
    assert config.database == ":memory:"
    assert config_full.extras["custom"] == "value"


def test_duckdb_config_extras_handling() -> None:
    """Test DuckDB config extras parameter handling."""
    # Test with explicit extras
    config = DuckDBConfig(database=":memory:", extras={"custom_param": "value", "debug": True})
    assert config.extras["custom_param"] == "value"
    assert config.extras["debug"] is True

    # Test with kwargs going to extras
    config2 = DuckDBConfig(database=":memory:", unknown_param="test", another_param=42)
    assert config2.extras["unknown_param"] == "test"
    assert config2.extras["another_param"] == 42


def test_duckdb_config_initialization() -> None:
    """Test DuckDB config initialization."""
    # Test with default parameters
    config = DuckDBConfig(database=":memory:")
    assert isinstance(config.statement_config, SQLConfig)
    assert isinstance(config.instrumentation, InstrumentationConfig)

    # Test with custom parameters
    custom_statement_config = SQLConfig()
    custom_instrumentation = InstrumentationConfig(log_queries=True)

    config = DuckDBConfig(
        database=":memory:",
        statement_config=custom_statement_config,
        instrumentation=custom_instrumentation,
    )
    assert config.statement_config is custom_statement_config
    assert config.instrumentation.log_queries is True


def test_duckdb_config_provide_session() -> None:
    """Test DuckDB config provide_session context manager."""
    config = DuckDBConfig(database=":memory:")

    # Test session context manager behavior
    with config.provide_session() as session:
        assert isinstance(session, DuckDBDriver)
        # Check that parameter styles were set
        assert session.config.allowed_parameter_styles == ("qmark", "numeric")
        assert session.config.target_parameter_style == "qmark"
        assert session.instrumentation_config is config.instrumentation


def test_duckdb_config_driver_type() -> None:
    """Test DuckDB config driver_type property."""
    config = DuckDBConfig(database=":memory:")
    assert config.driver_type is DuckDBDriver


def test_duckdb_config_is_async() -> None:
    """Test DuckDB config __is_async__ attribute."""
    config = DuckDBConfig(database=":memory:")
    assert config.__is_async__ is False
    assert DuckDBConfig.__is_async__ is False


def test_duckdb_config_supports_connection_pooling() -> None:
    """Test DuckDB config __supports_connection_pooling__ attribute."""
    config = DuckDBConfig(database=":memory:")
    assert config.__supports_connection_pooling__ is False
    assert DuckDBConfig.__supports_connection_pooling__ is False


def test_duckdb_config_from_connection_config() -> None:
    """Test DuckDB config from_connection_config backward compatibility."""
    # Test basic backward compatibility
    connection_config = {"database": "test_database", "read_only": "test_read_only", "config": "test_config"}
    config = DuckDBConfig.from_connection_config(connection_config)
    # Add specific assertions based on fields
    assert config.extras == {}

    # Test with extra parameters
    connection_config_with_extras = {
        "database": "test_database",
        "read_only": "test_read_only",
        "unknown_param": "test_value",
        "another_param": 42,
    }
    config_extras = DuckDBConfig.from_connection_config(connection_config_with_extras)
    assert config_extras.extras["unknown_param"] == "test_value"
    assert config_extras.extras["another_param"] == 42
