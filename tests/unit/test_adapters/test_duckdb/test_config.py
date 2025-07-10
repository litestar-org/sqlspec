"""Unit tests for DuckDB configuration.

This module tests the DuckDBConfig class including:
- Basic configuration initialization
- Connection parameter handling
- Extension management
- Secret management
- Performance settings
- Context manager behavior
- Error handling
- Property accessors
"""

from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import pytest

from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBConnectionParams, DuckDBDriver, DuckDBSecretConfig
from sqlspec.statement.sql import SQLConfig

if TYPE_CHECKING:
    pass


# TypedDict Tests
def test_duckdb_typed_dict_structure() -> None:
    """Test DuckDB TypedDict structure."""
    # Test that we can create valid connection params
    connection_params: DuckDBConnectionParams = {
        "database": ":memory:",
        "read_only": False,
        "memory_limit": "16GB",
        "threads": 8,
        "enable_progress_bar": True,
        "autoload_known_extensions": True,
    }
    assert connection_params["database"] == ":memory:"
    assert connection_params["read_only"] is False
    assert connection_params["memory_limit"] == "16GB"
    assert connection_params["threads"] == 8
    assert connection_params["enable_progress_bar"] is True
    assert connection_params["autoload_known_extensions"] is True


# Initialization Tests
def test_duckdb_config_basic_creation() -> None:
    """Test DuckDB config creation with basic parameters."""
    # Test minimal config creation
    connection_config = {"database": ":memory:", "read_only": False}
    config = DuckDBConfig(connection_config=connection_config)
    assert config.connection_config["database"] == ":memory:"
    assert config.connection_config["read_only"] is False

    # Test with additional parameters
    connection_config_full = {
        "database": "/tmp/test.db",
        "read_only": False,
        "memory_limit": "16GB",
        "threads": 8,
        "enable_progress_bar": True,
    }
    config_full = DuckDBConfig(connection_config=connection_config_full)
    assert config_full.connection_config["database"] == "/tmp/test.db"
    assert config_full.connection_config["read_only"] is False
    assert config_full.connection_config["memory_limit"] == "16GB"
    assert config_full.connection_config["threads"] == 8
    assert config_full.connection_config["enable_progress_bar"] is True


def test_duckdb_config_with_no_connection_config() -> None:
    """Test DuckDB config with no connection config."""
    config = DuckDBConfig()

    # Should have database set to :memory: as default
    assert config.connection_config["database"] == ":memory:"

    # Check base class attributes
    assert isinstance(config.statement_config, SQLConfig)
    assert config.default_row_type is dict


def test_duckdb_config_initialization() -> None:
    """Test DuckDB config initialization."""
    # Test with default parameters
    connection_config = {"database": ":memory:", "threads": 4}
    config = DuckDBConfig(connection_config=connection_config)
    assert isinstance(config.statement_config, SQLConfig)

    # Test with custom parameters
    custom_statement_config = SQLConfig()
    config = DuckDBConfig(connection_config=connection_config, statement_config=custom_statement_config)
    assert config.statement_config is custom_statement_config


@pytest.mark.parametrize(
    "connection_config,expected_extras",
    [
        (
            {"database": ":memory:", "extra": {"custom_param": "value", "debug": True}},
            {"custom_param": "value", "debug": True},
        ),
        (
            {"database": ":memory:", "extra": {"unknown_param": "test", "another_param": 42}},
            {"unknown_param": "test", "another_param": 42},
        ),
        ({"database": "/tmp/test.db"}, {}),
    ],
    ids=["with_custom_params", "with_unknown_params", "no_extras"],
)
def test_extras_handling(connection_config: dict[str, Any], expected_extras: dict[str, Any]) -> None:
    """Test handling of extra parameters."""
    config = DuckDBConfig(connection_config=connection_config)
    for key, value in expected_extras.items():
        assert config.connection_config[key] == value


@pytest.mark.parametrize(
    "statement_config,expected_type",
    [(None, SQLConfig), (SQLConfig(), SQLConfig), (SQLConfig(parse_errors_as_warnings=False), SQLConfig)],
    ids=["default", "empty", "custom"],
)
def test_statement_config_initialization(statement_config: "SQLConfig | None", expected_type: type[SQLConfig]) -> None:
    """Test statement config initialization."""
    connection_config = {"database": ":memory:"}
    config = DuckDBConfig(connection_config=connection_config, statement_config=statement_config)
    assert isinstance(config.statement_config, expected_type)

    if statement_config is not None:
        assert config.statement_config is statement_config


# Extension Management Tests
def test_extension_configuration() -> None:
    """Test extension configuration."""
    from sqlspec.adapters.duckdb.config import DuckDBExtensionConfig

    extensions: list[DuckDBExtensionConfig] = [
        {"name": "httpfs", "version": "0.10.0"},
        {"name": "parquet"},
        {"name": "json", "force_install": True},
    ]

    connection_config = {
        "database": ":memory:",
        "autoinstall_known_extensions": True,
        "allow_community_extensions": True,
    }
    config = DuckDBConfig(connection_config=connection_config, extensions=extensions)

    assert config.extensions == extensions
    assert config.connection_config["autoinstall_known_extensions"] is True
    assert config.connection_config["allow_community_extensions"] is True


@pytest.mark.parametrize(
    "extension_flag,value",
    [
        ("autoload_known_extensions", True),
        ("autoinstall_known_extensions", False),
        ("allow_community_extensions", True),
        ("allow_unsigned_extensions", False),
    ],
    ids=["autoload", "autoinstall", "community", "unsigned"],
)
def test_extension_flags(extension_flag: str, value: bool) -> None:
    """Test extension-related flags."""
    connection_config = {"database": ":memory:", extension_flag: value}
    config = DuckDBConfig(connection_config=connection_config)
    assert config.connection_config[extension_flag] == value


def test_extension_repository_configuration() -> None:
    """Test extension repository configuration."""
    connection_config = {
        "database": ":memory:",
        "custom_extension_repository": "https://custom.repo/extensions",
        "autoinstall_extension_repository": "core",
        "extension_directory": "/custom/extensions",
    }
    config = DuckDBConfig(connection_config=connection_config)

    assert config.connection_config["custom_extension_repository"] == "https://custom.repo/extensions"
    assert config.connection_config["autoinstall_extension_repository"] == "core"
    assert config.connection_config["extension_directory"] == "/custom/extensions"


# Secret Management Tests
def test_secret_configuration() -> None:
    """Test secret configuration."""
    secrets: list[DuckDBSecretConfig] = [
        {"secret_type": "openai", "name": "my_openai_key", "value": {"api_key": "sk-test"}, "scope": "LOCAL"},
        {"secret_type": "aws", "name": "my_aws_creds", "value": {"access_key_id": "test", "secret_access_key": "test"}},
    ]

    connection_config = {"database": ":memory:", "allow_persistent_secrets": True, "secret_directory": "/secrets"}
    config = DuckDBConfig(connection_config=connection_config, secrets=secrets)

    assert config.secrets == secrets
    assert config.connection_config["allow_persistent_secrets"] is True
    assert config.connection_config["secret_directory"] == "/secrets"


# Performance Settings Tests
@pytest.mark.parametrize(
    "perf_setting,value",
    [
        ("memory_limit", "32GB"),
        ("threads", 16),
        ("checkpoint_threshold", "512MB"),
        ("temp_directory", "/fast/ssd/tmp"),
        ("max_temp_directory_size", "100GB"),
    ],
    ids=["memory", "threads", "checkpoint", "temp_dir", "max_temp_size"],
)
def test_performance_settings(perf_setting: str, value: Any) -> None:
    """Test performance-related settings."""
    connection_config = {"database": ":memory:", perf_setting: value}
    config = DuckDBConfig(connection_config=connection_config)
    assert config.connection_config[perf_setting] == value


@pytest.mark.parametrize(
    "cache_setting,value",
    [("enable_object_cache", True), ("parquet_metadata_cache", False), ("enable_external_file_cache", True)],
    ids=["object_cache", "parquet_metadata", "external_file"],
)
def test_cache_settings(cache_setting: str, value: bool) -> None:
    """Test cache-related settings."""
    connection_config = {"database": ":memory:", cache_setting: value}
    config = DuckDBConfig(connection_config=connection_config)
    assert config.connection_config[cache_setting] == value


# Connection Creation Tests
@patch("sqlspec.adapters.duckdb.config.duckdb.connect")
def test_create_connection(mock_connect: MagicMock) -> None:
    """Test connection creation."""
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection

    connection_config = {"database": "/tmp/test.db", "read_only": False, "threads": 4}
    config = DuckDBConfig(connection_config=connection_config)

    connection = config.create_connection()

    # Verify connection creation
    # Note: threads is passed as a separate parameter and gets included in the config dict
    mock_connect.assert_called_once_with(database="/tmp/test.db", read_only=False, config={"threads": 4})
    assert connection is mock_connection


@patch("sqlspec.adapters.duckdb.config.duckdb.connect")
def test_create_connection_with_callbacks(mock_connect: MagicMock) -> None:
    """Test connection creation with callbacks."""
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection
    on_connection_create = MagicMock()

    connection_config = {"database": ":memory:"}
    config = DuckDBConfig(connection_config=connection_config, on_connection_create=on_connection_create)

    connection = config.create_connection()

    # Callback should be called with connection
    on_connection_create.assert_called_once_with(mock_connection)
    assert connection is mock_connection


# Context Manager Tests
@patch("sqlspec.adapters.duckdb.config.duckdb.connect")
def test_provide_connection_success(mock_connect: MagicMock) -> None:
    """Test provide_connection context manager normal flow."""
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection

    connection_config = {"database": ":memory:"}
    config = DuckDBConfig(connection_config=connection_config)

    with config.provide_connection() as conn:
        assert conn is mock_connection
        mock_connection.close.assert_not_called()

    mock_connection.close.assert_called_once()


@patch("sqlspec.adapters.duckdb.config.duckdb.connect")
def test_provide_connection_error_handling(mock_connect: MagicMock) -> None:
    """Test provide_connection context manager error handling."""
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection

    connection_config = {"database": ":memory:"}
    config = DuckDBConfig(connection_config=connection_config)

    with pytest.raises(ValueError, match="Test error"):
        with config.provide_connection() as conn:
            assert conn is mock_connection
            raise ValueError("Test error")

    # Connection should still be closed on error
    mock_connection.close.assert_called_once()


@patch("sqlspec.adapters.duckdb.config.duckdb.connect")
def test_provide_session(mock_connect: MagicMock) -> None:
    """Test provide_session context manager."""
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection

    connection_config = {"database": ":memory:"}
    config = DuckDBConfig(connection_config=connection_config)

    with config.provide_session() as session:
        assert isinstance(session, DuckDBDriver)
        assert session.connection is mock_connection

        # Check parameter style injection
        assert session.config.allowed_parameter_styles == ("qmark", "numeric")
        assert session.config.default_parameter_style == "qmark"

        mock_connection.close.assert_not_called()

    mock_connection.close.assert_called_once()


# Property Tests
def test_driver_type() -> None:
    """Test driver_type class attribute."""
    connection_config = {"database": ":memory:"}
    config = DuckDBConfig(connection_config=connection_config)
    assert config.driver_type is DuckDBDriver


def test_connection_type() -> None:
    """Test connection_type class attribute."""
    import duckdb

    connection_config = {"database": ":memory:"}
    config = DuckDBConfig(connection_config=connection_config)
    assert config.connection_type is duckdb.DuckDBPyConnection


def test_is_async() -> None:
    """Test is_async class attribute."""
    assert DuckDBConfig.is_async is False

    connection_config = {"database": ":memory:"}
    config = DuckDBConfig(connection_config=connection_config)
    assert config.is_async is False


def test_supports_connection_pooling() -> None:
    """Test supports_connection_pooling class attribute."""
    assert DuckDBConfig.supports_connection_pooling is False

    connection_config = {"database": ":memory:"}
    config = DuckDBConfig(connection_config=connection_config)
    assert config.supports_connection_pooling is False


# Parameter Style Tests
def test_supported_parameter_styles() -> None:
    """Test supported parameter styles class attribute."""
    assert DuckDBConfig.supported_parameter_styles == ("qmark", "numeric")


def test_default_parameter_style() -> None:
    """Test preferred parameter style class attribute."""
    assert DuckDBConfig.default_parameter_style == "qmark"


# Database Path Tests
@pytest.mark.parametrize(
    "database,description",
    [(":memory:", "in_memory"), ("/tmp/test.db", "file_path"), ("~/data/duck.db", "home_path"), ("", "empty_string")],
    ids=["memory", "absolute", "home", "empty"],
)
def test_database_paths(database: str, description: str) -> None:
    """Test various database path configurations."""
    connection_config = {"database": database} if database else {}
    config = DuckDBConfig(connection_config=connection_config)
    # Empty string defaults to :memory:
    expected_database = ":memory:" if database == "" else database
    assert config.connection_config["database"] == expected_database


# Logging Configuration Tests
@pytest.mark.parametrize(
    "log_setting,value",
    [("enable_logging", True), ("log_query_path", "/var/log/duckdb/queries.log"), ("logging_level", "INFO")],
    ids=["enable", "path", "level"],
)
def test_logging_configuration(log_setting: str, value: Any) -> None:
    """Test logging configuration."""
    connection_config = {"database": ":memory:", log_setting: value}
    config = DuckDBConfig(connection_config=connection_config)
    assert config.connection_config[log_setting] == value


# Progress Bar Tests
def test_progress_bar_configuration() -> None:
    """Test progress bar configuration."""
    connection_config = {
        "database": ":memory:",
        "enable_progress_bar": True,
        "progress_bar_time": 1000,  # milliseconds
    }
    config = DuckDBConfig(connection_config=connection_config)

    assert config.connection_config["enable_progress_bar"] is True
    assert config.connection_config["progress_bar_time"] == 1000


# Data Type Handling Tests
@pytest.mark.parametrize(
    "type_setting,value",
    [
        ("preserve_insertion_order", True),
        ("default_null_order", "NULLS LAST"),
        ("default_order", "DESC"),
        ("ieee_floating_point_ops", False),
        ("binary_as_string", True),
        ("errors_as_json", True),
    ],
    ids=["insertion_order", "null_order", "default_order", "ieee_fp", "binary", "errors"],
)
def test_data_type_settings(type_setting: str, value: Any) -> None:
    """Test data type handling settings."""
    connection_config = {"database": ":memory:", type_setting: value}
    config = DuckDBConfig(connection_config=connection_config)
    assert config.connection_config[type_setting] == value


# Arrow Integration Tests
def test_arrow_configuration() -> None:
    """Test Arrow integration configuration."""
    connection_config = {"database": ":memory:", "arrow_large_buffer_size": True}
    config = DuckDBConfig(connection_config=connection_config)

    assert config.connection_config["arrow_large_buffer_size"] is True


# Security Tests
def test_security_configuration() -> None:
    """Test security-related configuration."""
    connection_config = {"database": ":memory:", "enable_external_access": False, "allow_persistent_secrets": False}
    config = DuckDBConfig(connection_config=connection_config)

    assert config.connection_config["enable_external_access"] is False
    assert config.connection_config["allow_persistent_secrets"] is False


# Edge Cases
def test_config_with_dict_config() -> None:
    """Test config initialization with dict config parameter."""
    config_dict = {"threads": 8, "memory_limit": "16GB", "temp_directory": "/tmp/duckdb"}

    connection_config = {"database": ":memory:", "config": config_dict}
    config = DuckDBConfig(connection_config=connection_config)
    assert config.connection_config["config"] == config_dict


def test_config_with_empty_database() -> None:
    """Test config with empty database string (defaults to :memory:)."""
    connection_config = {"database": ""}
    config = DuckDBConfig(connection_config=connection_config)
    assert config.connection_config["database"] == ":memory:"  # Empty string defaults to :memory:


def test_config_readonly_memory() -> None:
    """Test read-only in-memory database configuration."""
    connection_config = {"database": ":memory:", "read_only": True}
    config = DuckDBConfig(connection_config=connection_config)
    assert config.connection_config["database"] == ":memory:"
    assert config.connection_config["read_only"] is True
