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

from sqlspec.adapters.duckdb import CONNECTION_FIELDS, DuckDBConfig, DuckDBDriver, DuckDBSecretConfig
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow

if TYPE_CHECKING:
    pass


# Constants Tests
def test_connection_fields_constant() -> None:
    """Test CONNECTION_FIELDS constant contains all expected fields."""
    expected_fields = frozenset(
        {
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
    )
    assert CONNECTION_FIELDS == expected_fields


# Initialization Tests
@pytest.mark.parametrize(
    "kwargs,expected_attrs",
    [
        (
            {"database": ":memory:"},
            {
                "database": ":memory:",
                "read_only": None,
                "config": None,
                "memory_limit": None,
                "threads": None,
                "extras": {},
            },
        ),
        (
            {
                "database": "/tmp/test.db",
                "read_only": False,
                "memory_limit": "16GB",
                "threads": 8,
                "enable_progress_bar": True,
            },
            {
                "database": "/tmp/test.db",
                "read_only": False,
                "memory_limit": "16GB",
                "threads": 8,
                "enable_progress_bar": True,
                "extras": {},
            },
        ),
    ],
    ids=["minimal", "with_options"],
)
def test_config_initialization(kwargs: dict[str, Any], expected_attrs: dict[str, Any]) -> None:
    """Test config initialization with various parameters."""
    config = DuckDBConfig(**kwargs)

    for attr, expected_value in expected_attrs.items():
        assert getattr(config, attr) == expected_value

    # Check base class attributes
    assert isinstance(config.statement_config, SQLConfig)
    assert config.default_row_type is DictRow


@pytest.mark.parametrize(
    "init_kwargs,expected_extras",
    [
        ({"database": ":memory:", "custom_param": "value", "debug": True}, {"custom_param": "value", "debug": True}),
        (
            {"database": ":memory:", "unknown_param": "test", "another_param": 42},
            {"unknown_param": "test", "another_param": 42},
        ),
        ({"database": "/tmp/test.db"}, {}),
    ],
    ids=["with_custom_params", "with_unknown_params", "no_extras"],
)
def test_extras_handling(init_kwargs: dict[str, Any], expected_extras: dict[str, Any]) -> None:
    """Test handling of extra parameters."""
    config = DuckDBConfig(**init_kwargs)
    assert config.extras == expected_extras


@pytest.mark.parametrize(
    "statement_config,expected_type",
    [(None, SQLConfig), (SQLConfig(), SQLConfig), (SQLConfig(strict_mode=True), SQLConfig)],
    ids=["default", "empty", "custom"],
)
def test_statement_config_initialization(statement_config: "SQLConfig | None", expected_type: type[SQLConfig]) -> None:
    """Test statement config initialization."""
    config = DuckDBConfig(database=":memory:", statement_config=statement_config)
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

    config = DuckDBConfig(
        database=":memory:", extensions=extensions, autoinstall_known_extensions=True, allow_community_extensions=True
    )

    assert config.extensions == extensions
    assert config.autoinstall_known_extensions is True
    assert config.allow_community_extensions is True


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
    config = DuckDBConfig(database=":memory:", **{extension_flag: value})  # type: ignore[arg-type]
    assert getattr(config, extension_flag) == value


def test_extension_repository_configuration() -> None:
    """Test extension repository configuration."""
    config = DuckDBConfig(
        database=":memory:",
        custom_extension_repository="https://custom.repo/extensions",
        autoinstall_extension_repository="core",
        extension_directory="/custom/extensions",
    )

    assert config.custom_extension_repository == "https://custom.repo/extensions"
    assert config.autoinstall_extension_repository == "core"
    assert config.extension_directory == "/custom/extensions"


# Secret Management Tests
def test_secret_configuration() -> None:
    """Test secret configuration."""
    secrets: list[DuckDBSecretConfig] = [
        {"secret_type": "openai", "name": "my_openai_key", "value": {"api_key": "sk-test"}, "scope": "LOCAL"},
        {"secret_type": "aws", "name": "my_aws_creds", "value": {"access_key_id": "test", "secret_access_key": "test"}},
    ]

    config = DuckDBConfig(
        database=":memory:", secrets=secrets, allow_persistent_secrets=True, secret_directory="/secrets"
    )

    assert config.secrets == secrets
    assert config.allow_persistent_secrets is True
    assert config.secret_directory == "/secrets"


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
    config = DuckDBConfig(database=":memory:", **{perf_setting: value})
    assert getattr(config, perf_setting) == value


@pytest.mark.parametrize(
    "cache_setting,value",
    [("enable_object_cache", True), ("parquet_metadata_cache", False), ("enable_external_file_cache", True)],
    ids=["object_cache", "parquet_metadata", "external_file"],
)
def test_cache_settings(cache_setting: str, value: bool) -> None:
    """Test cache-related settings."""
    config = DuckDBConfig(database=":memory:", **{cache_setting: value})  # type: ignore[arg-type]
    assert getattr(config, cache_setting) == value


# Connection Creation Tests
@patch("sqlspec.adapters.duckdb.config.duckdb.connect")
def test_create_connection(mock_connect: MagicMock) -> None:
    """Test connection creation."""
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection

    config = DuckDBConfig(database="/tmp/test.db", read_only=False, threads=4)

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

    config = DuckDBConfig(database=":memory:", on_connection_create=on_connection_create)

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

    config = DuckDBConfig(database=":memory:")

    with config.provide_connection() as conn:
        assert conn is mock_connection
        mock_connection.close.assert_not_called()

    mock_connection.close.assert_called_once()


@patch("sqlspec.adapters.duckdb.config.duckdb.connect")
def test_provide_connection_error_handling(mock_connect: MagicMock) -> None:
    """Test provide_connection context manager error handling."""
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection

    config = DuckDBConfig(database=":memory:")

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

    config = DuckDBConfig(database=":memory:")

    with config.provide_session() as session:
        assert isinstance(session, DuckDBDriver)
        assert session.connection is mock_connection

        # Check parameter style injection
        assert session.config.allowed_parameter_styles == ("qmark", "numeric")
        assert session.config.target_parameter_style == "qmark"

        mock_connection.close.assert_not_called()

    mock_connection.close.assert_called_once()


# Property Tests
def test_driver_type() -> None:
    """Test driver_type class attribute."""
    config = DuckDBConfig(database=":memory:")
    assert config.driver_type is DuckDBDriver


def test_connection_type() -> None:
    """Test connection_type class attribute."""
    import duckdb

    config = DuckDBConfig(database=":memory:")
    assert config.connection_type is duckdb.DuckDBPyConnection


def test_is_async() -> None:
    """Test is_async class attribute."""
    assert DuckDBConfig.is_async is False

    config = DuckDBConfig(database=":memory:")
    assert config.is_async is False


def test_supports_connection_pooling() -> None:
    """Test supports_connection_pooling class attribute."""
    assert DuckDBConfig.supports_connection_pooling is False

    config = DuckDBConfig(database=":memory:")
    assert config.supports_connection_pooling is False


# Parameter Style Tests
def test_supported_parameter_styles() -> None:
    """Test supported parameter styles class attribute."""
    assert DuckDBConfig.supported_parameter_styles == ("qmark", "numeric")


def test_preferred_parameter_style() -> None:
    """Test preferred parameter style class attribute."""
    assert DuckDBConfig.preferred_parameter_style == "qmark"


# Database Path Tests
@pytest.mark.parametrize(
    "database,description",
    [(":memory:", "in_memory"), ("/tmp/test.db", "file_path"), ("~/data/duck.db", "home_path"), ("", "empty_string")],
    ids=["memory", "absolute", "home", "empty"],
)
def test_database_paths(database: str, description: str) -> None:
    """Test various database path configurations."""
    config = DuckDBConfig(database=database)
    # Empty string defaults to :memory:
    expected_database = ":memory:" if database == "" else database
    assert config.database == expected_database


# Logging Configuration Tests
@pytest.mark.parametrize(
    "log_setting,value",
    [("enable_logging", True), ("log_query_path", "/var/log/duckdb/queries.log"), ("logging_level", "INFO")],
    ids=["enable", "path", "level"],
)
def test_logging_configuration(log_setting: str, value: Any) -> None:
    """Test logging configuration."""
    config = DuckDBConfig(database=":memory:", **{log_setting: value})
    assert getattr(config, log_setting) == value


# Progress Bar Tests
def test_progress_bar_configuration() -> None:
    """Test progress bar configuration."""
    config = DuckDBConfig(
        database=":memory:",
        enable_progress_bar=True,
        progress_bar_time=1000,  # milliseconds
    )

    assert config.enable_progress_bar is True
    assert config.progress_bar_time == 1000


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
    config = DuckDBConfig(database=":memory:", **{type_setting: value})
    assert getattr(config, type_setting) == value


# Arrow Integration Tests
def test_arrow_configuration() -> None:
    """Test Arrow integration configuration."""
    config = DuckDBConfig(database=":memory:", arrow_large_buffer_size=True)

    assert config.arrow_large_buffer_size is True


# Security Tests
def test_security_configuration() -> None:
    """Test security-related configuration."""
    config = DuckDBConfig(database=":memory:", enable_external_access=False, allow_persistent_secrets=False)

    assert config.enable_external_access is False
    assert config.allow_persistent_secrets is False


# Slots Test
def test_slots_defined() -> None:
    """Test that __slots__ is properly defined."""
    assert hasattr(DuckDBConfig, "__slots__")
    expected_slots = {
        "_dialect",
        "pool_instance",
        "allow_community_extensions",
        "allow_persistent_secrets",
        "allow_unsigned_extensions",
        "arrow_large_buffer_size",
        "autoinstall_extension_repository",
        "autoinstall_known_extensions",
        "autoload_known_extensions",
        "binary_as_string",
        "checkpoint_threshold",
        "config",
        "custom_extension_repository",
        "database",
        "default_null_order",
        "default_order",
        "default_row_type",
        "enable_external_access",
        "enable_external_file_cache",
        "enable_logging",
        "enable_object_cache",
        "enable_progress_bar",
        "errors_as_json",
        "extension_directory",
        "extensions",
        "extras",
        "ieee_floating_point_ops",
        "log_query_path",
        "logging_level",
        "max_temp_directory_size",
        "memory_limit",
        "on_connection_create",
        "parquet_metadata_cache",
        "preserve_insertion_order",
        "progress_bar_time",
        "read_only",
        "secret_directory",
        "secrets",
        "statement_config",
        "temp_directory",
        "threads",
    }
    assert set(DuckDBConfig.__slots__) == expected_slots


# Edge Cases
def test_config_with_dict_config() -> None:
    """Test config initialization with dict config parameter."""
    config_dict = {"threads": 8, "memory_limit": "16GB", "temp_directory": "/tmp/duckdb"}

    config = DuckDBConfig(database=":memory:", config=config_dict)
    assert config.config == config_dict


def test_config_with_empty_database() -> None:
    """Test config with empty database string (defaults to :memory:)."""
    config = DuckDBConfig(database="")
    assert config.database == ":memory:"  # Empty string defaults to :memory:


def test_config_readonly_memory() -> None:
    """Test read-only in-memory database configuration."""
    config = DuckDBConfig(database=":memory:", read_only=True)
    assert config.database == ":memory:"
    assert config.read_only is True
