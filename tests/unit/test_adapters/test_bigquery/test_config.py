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

from sqlspec.adapters.bigquery import CONNECTION_FIELDS, BigQueryConfig, BigQueryDriver
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow

if TYPE_CHECKING:
    pass


# Constants Tests
def test_connection_fields_constant() -> None:
    """Test CONNECTION_FIELDS constant contains all expected fields."""
    expected_fields = frozenset(
        {
            "project",
            "location",
            "credentials",
            "dataset_id",
            "credentials_path",
            "client_options",
            "client_info",
            "default_query_job_config",
            "default_load_job_config",
            "use_query_cache",
            "maximum_bytes_billed",
            "enable_bigquery_ml",
            "enable_gemini_integration",
            "query_timeout_ms",
            "job_timeout_ms",
            "reservation_id",
            "edition",
            "enable_cross_cloud",
            "enable_bigquery_omni",
            "use_avro_logical_types",
            "parquet_enable_list_inference",
            "enable_column_level_security",
            "enable_row_level_security",
            "enable_dataframes",
            "dataframes_backend",
            "enable_continuous_queries",
            "enable_vector_search",
        }
    )
    assert CONNECTION_FIELDS == expected_fields


# Initialization Tests
@pytest.mark.parametrize(
    "kwargs,expected_attrs",
    [
        (
            {"project": "test-project"},
            {
                "project": "test-project",
                "dataset_id": None,
                "location": None,
                "credentials": None,
                "credentials_path": None,
                "extras": {},
            },
        ),
        (
            {
                "project": "test-project",
                "dataset_id": "test_dataset",
                "location": "us-central1",
                "use_query_cache": True,
                "maximum_bytes_billed": 1000000,
            },
            {
                "project": "test-project",
                "dataset_id": "test_dataset",
                "location": "us-central1",
                "use_query_cache": True,
                "maximum_bytes_billed": 1000000,
                "extras": {},
            },
        ),
    ],
    ids=["minimal", "with_options"],
)
def test_config_initialization(kwargs: dict[str, Any], expected_attrs: dict[str, Any]) -> None:
    """Test config initialization with various parameters."""
    config = BigQueryConfig(**kwargs)

    for attr, expected_value in expected_attrs.items():
        assert getattr(config, attr) == expected_value

    # Check base class attributes
    assert isinstance(config.statement_config, SQLConfig)
    assert config.default_row_type is DictRow


@pytest.mark.parametrize(
    "init_kwargs,expected_extras",
    [
        ({"project": "test-project", "custom_param": "value", "debug": True}, {"custom_param": "value", "debug": True}),
        (
            {"project": "test-project", "unknown_param": "test", "another_param": 42},
            {"unknown_param": "test", "another_param": 42},
        ),
        ({"project": "test-project"}, {}),
    ],
    ids=["with_custom_params", "with_unknown_params", "no_extras"],
)
def test_extras_handling(init_kwargs: dict[str, Any], expected_extras: dict[str, Any]) -> None:
    """Test handling of extra parameters."""
    config = BigQueryConfig(**init_kwargs)
    assert config.extras == expected_extras


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
    config = BigQueryConfig(project="test-project", **{feature_flag: value})  # type: ignore[arg-type]
    assert getattr(config, feature_flag) == value


@pytest.mark.parametrize(
    "statement_config,expected_type",
    [(None, SQLConfig), (SQLConfig(), SQLConfig), (SQLConfig(strict_mode=True), SQLConfig)],
    ids=["default", "empty", "custom"],
)
def test_statement_config_initialization(statement_config: "SQLConfig | None", expected_type: type[SQLConfig]) -> None:
    """Test statement config initialization."""
    config = BigQueryConfig(project="test-project", statement_config=statement_config)
    assert isinstance(config.statement_config, expected_type)

    if statement_config is not None:
        assert config.statement_config is statement_config


# Connection Creation Tests
def test_create_connection() -> None:
    """Test connection creation."""
    with patch.object(BigQueryConfig, "connection_type") as mock_connection_type:
        mock_client = MagicMock()
        mock_connection_type.return_value = mock_client

        config = BigQueryConfig(project="test-project", dataset_id="test_dataset", location="us-central1")

        connection = config.create_connection()

        # Verify client creation - only non-None fields are passed
        mock_connection_type.assert_called_once_with(project="test-project", location="us-central1")
        assert connection is mock_client


def test_create_connection_with_credentials_path() -> None:
    """Test connection creation with credentials path."""
    with patch.object(BigQueryConfig, "connection_type") as mock_connection_type:
        mock_client = MagicMock()
        mock_connection_type.return_value = mock_client

        config = BigQueryConfig(project="test-project", credentials_path="/path/to/credentials.json")

        # Note: The current implementation doesn't use credentials_path to create service account credentials
        # It just stores the path. The actual credential loading would need to be implemented
        connection = config.create_connection()

        # Should create client with basic config (credentials_path not directly used)
        # Only non-None fields are passed
        mock_connection_type.assert_called_once_with(project="test-project")
        assert connection is mock_client


# Context Manager Tests
def test_provide_connection_success() -> None:
    """Test provide_connection context manager normal flow."""
    with patch.object(BigQueryConfig, "connection_type") as mock_connection_type:
        mock_client = MagicMock()
        mock_connection_type.return_value = mock_client

        config = BigQueryConfig(project="test-project")

        with config.provide_connection() as conn:
            assert conn is mock_client
            # BigQuery client doesn't have a close method to assert on


def test_provide_connection_error_handling() -> None:
    """Test provide_connection context manager error handling."""
    with patch.object(BigQueryConfig, "connection_type") as mock_connection_type:
        mock_client = MagicMock()
        mock_connection_type.return_value = mock_client

        config = BigQueryConfig(project="test-project")

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

        config = BigQueryConfig(project="test-project", dataset_id="test_dataset")

        with config.provide_session() as session:
            assert isinstance(session, BigQueryDriver)
            assert session.connection is mock_client
            # dataset_id is not an attribute of the driver, it's in the config
            assert config.dataset_id == "test_dataset"

            # Check parameter style injection
            assert session.config.allowed_parameter_styles == ("named_at",)
            assert session.config.target_parameter_style == "named_at"

            # BigQuery client doesn't have a close method to assert on


# Property Tests
def test_driver_type() -> None:
    """Test driver_type class attribute."""
    config = BigQueryConfig(project="test-project")
    assert config.driver_type is BigQueryDriver


def test_connection_type() -> None:
    """Test connection_type class attribute."""
    from google.cloud.bigquery import Client

    config = BigQueryConfig(project="test-project")
    assert config.connection_type is Client


def test_is_async() -> None:
    """Test is_async class attribute."""
    assert BigQueryConfig.is_async is False

    config = BigQueryConfig(project="test-project")
    assert config.is_async is False


def test_supports_connection_pooling() -> None:
    """Test supports_connection_pooling class attribute."""
    assert BigQueryConfig.supports_connection_pooling is False

    config = BigQueryConfig(project="test-project")
    assert config.supports_connection_pooling is False


# Parameter Style Tests
def test_supported_parameter_styles() -> None:
    """Test supported parameter styles class attribute."""
    assert BigQueryConfig.supported_parameter_styles == ("named_at",)


def test_preferred_parameter_style() -> None:
    """Test preferred parameter style class attribute."""
    assert BigQueryConfig.preferred_parameter_style == "named_at"


# Advanced Configuration Tests
@pytest.mark.parametrize(
    "timeout_type,value",
    [("query_timeout_ms", 30000), ("job_timeout_ms", 600000)],
    ids=["query_timeout", "job_timeout"],
)
def test_timeout_configuration(timeout_type: str, value: int) -> None:
    """Test timeout configuration."""
    config = BigQueryConfig(project="test-project", **{timeout_type: value})  # type: ignore[arg-type]
    assert getattr(config, timeout_type) == value


def test_reservation_and_edition() -> None:
    """Test reservation and edition configuration."""
    config = BigQueryConfig(project="test-project", reservation_id="my-reservation", edition="ENTERPRISE")
    assert config.reservation_id == "my-reservation"
    assert config.edition == "ENTERPRISE"


def test_dataframes_configuration() -> None:
    """Test DataFrames configuration."""
    config = BigQueryConfig(project="test-project", enable_dataframes=True, dataframes_backend="bigframes")
    assert config.enable_dataframes is True
    assert config.dataframes_backend == "bigframes"


# Callback Tests
def test_callback_configuration() -> None:
    """Test callback function configuration."""
    on_connection_create = MagicMock()
    on_job_start = MagicMock()
    on_job_complete = MagicMock()

    config = BigQueryConfig(
        project="test-project",
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

    config = BigQueryConfig(
        project="test-project", default_query_job_config=mock_query_config, default_load_job_config=mock_load_config
    )

    assert config.default_query_job_config is mock_query_config
    assert config.default_load_job_config is mock_load_config


# Storage Format Options Tests
@pytest.mark.parametrize(
    "option,value",
    [("use_avro_logical_types", True), ("parquet_enable_list_inference", False)],
    ids=["avro_logical_types", "parquet_list_inference"],
)
def test_storage_format_options(option: str, value: bool) -> None:
    """Test storage format options."""
    config = BigQueryConfig(project="test-project", **{option: value})  # type: ignore[arg-type]
    assert getattr(config, option) == value


# Slots Test
def test_slots_defined() -> None:
    """Test that __slots__ is properly defined."""
    assert hasattr(BigQueryConfig, "__slots__")
    expected_slots = {
        "_dialect",
        "pool_instance",
        "_connection_instance",
        "client_info",
        "client_options",
        "credentials",
        "credentials_path",
        "dataframes_backend",
        "dataset_id",
        "default_load_job_config",
        "default_query_job_config",
        "default_row_type",
        "edition",
        "enable_bigquery_ml",
        "enable_bigquery_omni",
        "enable_column_level_security",
        "enable_continuous_queries",
        "enable_cross_cloud",
        "enable_dataframes",
        "enable_gemini_integration",
        "enable_row_level_security",
        "enable_vector_search",
        "extras",
        "job_timeout_ms",
        "location",
        "maximum_bytes_billed",
        "on_connection_create",
        "on_job_complete",
        "on_job_start",
        "parquet_enable_list_inference",
        "project",
        "query_timeout_ms",
        "reservation_id",
        "statement_config",
        "use_avro_logical_types",
        "use_query_cache",
    }
    assert set(BigQueryConfig.__slots__) == expected_slots


# Edge Cases
def test_config_without_project() -> None:
    """Test config initialization without project (should use default from environment)."""
    config = BigQueryConfig()
    assert config.project is None  # Will use default from environment


def test_config_with_both_credentials_types() -> None:
    """Test config with both credentials and credentials_path."""
    mock_credentials = MagicMock()

    config = BigQueryConfig(
        project="test-project", credentials=mock_credentials, credentials_path="/path/to/creds.json"
    )

    # Both should be stored
    assert config.credentials is mock_credentials
    assert config.credentials_path == "/path/to/creds.json"
    # Note: The actual precedence is handled in create_connection
