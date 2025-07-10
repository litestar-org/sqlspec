"""Unit tests for BigQuery configuration.

This module tests the BigQueryConfig class including:
- Basic configuration initialization
- Connection parameter handling
- Context manager behavior
- Feature flags and advanced options
- Error handling
- Property accessors
"""

from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import pytest

from sqlspec.adapters.bigquery import BigQueryConfig, BigQueryConnectionParams, BigQueryDriver
from sqlspec.statement.sql import SQLConfig

if TYPE_CHECKING:
    pass


# TypedDict Tests
def test_bigquery_typed_dict_structure() -> None:
    """Test BigQuery TypedDict structure."""
    # Test that we can create valid connection params
    connection_params: BigQueryConnectionParams = {
        "project": "test-project",
        "location": "us-central1",
        "dataset_id": "test_dataset",
        "use_query_cache": True,
        "maximum_bytes_billed": 1000000,
    }
    assert connection_params["project"] == "test-project"
    assert connection_params["location"] == "us-central1"
    assert connection_params["use_query_cache"] is True


# Initialization Tests
def test_bigquery_config_basic_creation() -> None:
    """Test BigQuery config creation with basic parameters."""
    # Test minimal config creation
    connection_config = {"project": "test-project", "location": "us-central1"}
    config = BigQueryConfig(connection_config=connection_config)
    assert config.connection_config["project"] == "test-project"
    assert config.connection_config["location"] == "us-central1"

    # Test with additional parameters
    connection_config_full = {
        "project": "test-project",
        "dataset_id": "test_dataset",
        "location": "us-central1",
        "use_query_cache": True,
        "maximum_bytes_billed": 1000000,
    }
    config_full = BigQueryConfig(connection_config=connection_config_full)
    assert config_full.connection_config["project"] == "test-project"
    assert config_full.connection_config["dataset_id"] == "test_dataset"
    assert config_full.connection_config["location"] == "us-central1"
    assert config_full.connection_config["use_query_cache"] is True
    assert config_full.connection_config["maximum_bytes_billed"] == 1000000


def test_bigquery_config_with_no_connection_config() -> None:
    """Test BigQuery config with no connection config."""
    config = BigQueryConfig()

    # Should have empty connection_config except for default_query_job_config
    assert len(config.connection_config) == 1
    assert "default_query_job_config" in config.connection_config

    # Check base class attributes
    assert isinstance(config.statement_config, SQLConfig)
    assert config.default_row_type is dict


def test_bigquery_config_initialization() -> None:
    """Test BigQuery config initialization."""
    # Test with default parameters
    connection_config = {"project": "test-project", "location": "us-central1"}
    config = BigQueryConfig(connection_config=connection_config)
    assert isinstance(config.statement_config, SQLConfig)

    # Test with custom parameters
    custom_statement_config = SQLConfig()
    config = BigQueryConfig(connection_config=connection_config, statement_config=custom_statement_config)
    assert config.statement_config is custom_statement_config


@pytest.mark.parametrize(
    "connection_config,expected_extras",
    [
        (
            {"project": "test-project", "extra": {"custom_param": "value", "debug": True}},
            {"custom_param": "value", "debug": True},
        ),
        (
            {"project": "test-project", "extra": {"unknown_param": "test", "another_param": 42}},
            {"unknown_param": "test", "another_param": 42},
        ),
        ({"project": "test-project"}, {}),
    ],
    ids=["with_custom_params", "with_unknown_params", "no_extras"],
)
def test_extras_handling(connection_config: dict[str, Any], expected_extras: dict[str, Any]) -> None:
    """Test handling of extra parameters."""
    config = BigQueryConfig(connection_config=connection_config)
    for key, value in expected_extras.items():
        assert config.connection_config[key] == value


# Feature Flag Tests
@pytest.mark.parametrize(
    "feature_flag,value",
    [
        ("enable_bigquery_ml", True),
        ("enable_gemini_integration", False),
        ("enable_cross_cloud", True),
        ("enable_bigquery_omni", False),
        ("enable_column_level_security", True),
        ("enable_row_level_security", False),
        ("enable_dataframes", True),
        ("enable_continuous_queries", False),
        ("enable_vector_search", True),
    ],
    ids=[
        "bigquery_ml",
        "gemini",
        "cross_cloud",
        "omni",
        "column_security",
        "row_security",
        "dataframes",
        "continuous_queries",
        "vector_search",
    ],
)
def test_feature_flags(feature_flag: str, value: bool) -> None:
    """Test feature flag configuration."""
    connection_config = {"project": "test-project", feature_flag: value}
    config = BigQueryConfig(connection_config=connection_config)
    assert config.connection_config[feature_flag] == value


@pytest.mark.parametrize(
    "statement_config,expected_type",
    [(None, SQLConfig), (SQLConfig(), SQLConfig), (SQLConfig(parse_errors_as_warnings=False), SQLConfig)],
    ids=["default", "empty", "custom"],
)
def test_statement_config_initialization(statement_config: "SQLConfig | None", expected_type: type[SQLConfig]) -> None:
    """Test statement config initialization."""
    connection_config = {"project": "test-project"}
    config = BigQueryConfig(connection_config=connection_config, statement_config=statement_config)
    assert isinstance(config.statement_config, expected_type)

    if statement_config is not None:
        assert config.statement_config is statement_config


# Connection Creation Tests
def test_create_connection() -> None:
    """Test connection creation."""
    with patch.object(BigQueryConfig, "connection_type") as mock_connection_type:
        mock_client = MagicMock()
        mock_connection_type.return_value = mock_client

        connection_config = {"project": "test-project", "dataset_id": "test_dataset", "location": "us-central1"}
        config = BigQueryConfig(connection_config=connection_config)

        connection = config.create_connection()

        # Verify client creation - only client fields are passed
        mock_connection_type.assert_called_once_with(project="test-project", location="us-central1")
        assert connection is mock_client


def test_create_connection_with_credentials_path() -> None:
    """Test connection creation with credentials path."""
    with patch.object(BigQueryConfig, "connection_type") as mock_connection_type:
        mock_client = MagicMock()
        mock_connection_type.return_value = mock_client

        connection_config = {"project": "test-project", "credentials_path": "/path/to/credentials.json"}
        config = BigQueryConfig(connection_config=connection_config)

        # Note: The current implementation doesn't use credentials_path to create service account credentials
        # It just stores the path. The actual credential loading would need to be implemented
        connection = config.create_connection()

        # Should create client with basic config (credentials_path not directly used)
        # Only client fields are passed
        mock_connection_type.assert_called_once_with(project="test-project")
        assert connection is mock_client


# Context Manager Tests
def test_provide_connection_success() -> None:
    """Test provide_connection context manager normal flow."""
    with patch.object(BigQueryConfig, "connection_type") as mock_connection_type:
        mock_client = MagicMock()
        mock_connection_type.return_value = mock_client

        connection_config = {"project": "test-project"}
        config = BigQueryConfig(connection_config=connection_config)

        with config.provide_connection() as conn:
            assert conn is mock_client
            # BigQuery client doesn't have a close method to assert on


def test_provide_connection_error_handling() -> None:
    """Test provide_connection context manager error handling."""
    with patch.object(BigQueryConfig, "connection_type") as mock_connection_type:
        mock_client = MagicMock()
        mock_connection_type.return_value = mock_client

        connection_config = {"project": "test-project"}
        config = BigQueryConfig(connection_config=connection_config)

        with pytest.raises(ValueError, match="Test error"):
            with config.provide_connection() as conn:
                assert conn is mock_client
                raise ValueError("Test error")

        # BigQuery client doesn't have a close method to assert on


def test_provide_session() -> None:
    """Test provide_session context manager."""
    with patch.object(BigQueryConfig, "connection_type") as mock_connection_type:
        mock_client = MagicMock()
        mock_connection_type.return_value = mock_client

        connection_config = {"project": "test-project", "dataset_id": "test_dataset"}
        config = BigQueryConfig(connection_config=connection_config)

        with config.provide_session() as session:
            assert isinstance(session, BigQueryDriver)
            assert session.connection is mock_client
            # dataset_id is not an attribute of the driver, it's in the config
            assert config.connection_config["dataset_id"] == "test_dataset"

            # Check parameter style injection
            assert session.config.allowed_parameter_styles == ("named_at",)
            assert session.config.default_parameter_style == "named_at"

            # BigQuery client doesn't have a close method to assert on


# Property Tests
def test_driver_type() -> None:
    """Test driver_type class attribute."""
    connection_config = {"project": "test-project"}
    config = BigQueryConfig(connection_config=connection_config)
    assert config.driver_type is BigQueryDriver


def test_connection_type() -> None:
    """Test connection_type class attribute."""
    from google.cloud.bigquery import Client

    connection_config = {"project": "test-project"}
    config = BigQueryConfig(connection_config=connection_config)
    assert config.connection_type is Client


def test_is_async() -> None:
    """Test is_async class attribute."""
    assert BigQueryConfig.is_async is False

    connection_config = {"project": "test-project"}
    config = BigQueryConfig(connection_config=connection_config)
    assert config.is_async is False


def test_supports_connection_pooling() -> None:
    """Test supports_connection_pooling class attribute."""
    assert BigQueryConfig.supports_connection_pooling is False

    connection_config = {"project": "test-project"}
    config = BigQueryConfig(connection_config=connection_config)
    assert config.supports_connection_pooling is False


# Parameter Style Tests
def test_supported_parameter_styles() -> None:
    """Test supported parameter styles class attribute."""
    assert BigQueryConfig.supported_parameter_styles == ("named_at",)


def test_default_parameter_style() -> None:
    """Test preferred parameter style class attribute."""
    assert BigQueryConfig.default_parameter_style == "named_at"


# Advanced Configuration Tests
@pytest.mark.parametrize(
    "timeout_type,value",
    [("query_timeout_ms", 30000), ("job_timeout_ms", 600000)],
    ids=["query_timeout", "job_timeout"],
)
def test_timeout_configuration(timeout_type: str, value: int) -> None:
    """Test timeout configuration."""
    connection_config = {"project": "test-project", timeout_type: value}
    config = BigQueryConfig(connection_config=connection_config)
    assert config.connection_config[timeout_type] == value


def test_reservation_and_edition() -> None:
    """Test reservation and edition configuration."""
    connection_config = {"project": "test-project", "reservation_id": "my-reservation", "edition": "ENTERPRISE"}
    config = BigQueryConfig(connection_config=connection_config)
    assert config.connection_config["reservation_id"] == "my-reservation"
    assert config.connection_config["edition"] == "ENTERPRISE"


def test_dataframes_configuration() -> None:
    """Test DataFrames configuration."""
    connection_config = {"project": "test-project", "enable_dataframes": True, "dataframes_backend": "bigframes"}
    config = BigQueryConfig(connection_config=connection_config)
    assert config.connection_config["enable_dataframes"] is True
    assert config.connection_config["dataframes_backend"] == "bigframes"


# Callback Tests
def test_callback_configuration() -> None:
    """Test callback function configuration."""
    on_connection_create = MagicMock()
    on_job_start = MagicMock()
    on_job_complete = MagicMock()

    connection_config = {"project": "test-project"}
    config = BigQueryConfig(
        connection_config=connection_config,
        on_connection_create=on_connection_create,
        on_job_start=on_job_start,
        on_job_complete=on_job_complete,
    )

    assert config.on_connection_create is on_connection_create
    assert config.on_job_start is on_job_start
    assert config.on_job_complete is on_job_complete


# Job Configuration Tests
def test_job_config_objects() -> None:
    """Test job configuration objects."""
    mock_query_config = MagicMock(spec="QueryJobConfig")
    mock_load_config = MagicMock(spec="LoadJobConfig")

    connection_config = {
        "project": "test-project",
        "default_query_job_config": mock_query_config,
        "default_load_job_config": mock_load_config,
    }
    config = BigQueryConfig(connection_config=connection_config)

    assert config.connection_config["default_query_job_config"] is mock_query_config
    assert config.connection_config["default_load_job_config"] is mock_load_config


# Storage Format Options Tests
@pytest.mark.parametrize(
    "option,value",
    [("use_avro_logical_types", True), ("parquet_enable_list_inference", False)],
    ids=["avro_logical_types", "parquet_list_inference"],
)
def test_storage_format_options(option: str, value: bool) -> None:
    """Test storage format options."""
    connection_config = {"project": "test-project", option: value}
    config = BigQueryConfig(connection_config=connection_config)
    assert config.connection_config[option] == value


# Edge Cases
def test_config_without_project() -> None:
    """Test config initialization without project (should use default from environment)."""
    config = BigQueryConfig()
    assert config.connection_config.get("project") is None  # Will use default from environment


def test_config_with_both_credentials_types() -> None:
    """Test config with both credentials and credentials_path."""
    mock_credentials = MagicMock()

    connection_config = {
        "project": "test-project",
        "credentials": mock_credentials,
        "credentials_path": "/path/to/creds.json",
    }
    config = BigQueryConfig(connection_config=connection_config)

    # Both should be stored
    assert config.connection_config["credentials"] is mock_credentials
    assert config.connection_config["credentials_path"] == "/path/to/creds.json"
    # Note: The actual precedence is handled in create_connection
