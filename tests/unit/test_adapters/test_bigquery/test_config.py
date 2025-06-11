"""Unit tests for BigQuery configuration."""

from unittest.mock import MagicMock, patch

from sqlspec.adapters.bigquery import CONNECTION_FIELDS, BigQueryConfig, BigQueryDriver
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.sql import SQLConfig


def test_bigquery_field_constants() -> None:
    """Test BigQuery CONNECTION_FIELDS constants."""
    expected_connection_fields = {
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
    assert CONNECTION_FIELDS == expected_connection_fields


def test_bigquery_config_basic_creation() -> None:
    """Test BigQuery config creation with basic parameters."""
    # Test minimal config creation
    config = BigQueryConfig(project="test-project", dataset_id="test_dataset")
    assert config.project == "test-project"
    assert config.dataset_id == "test_dataset"

    # Test with all parameters
    config_full = BigQueryConfig(project="test-project", dataset_id="test_dataset", extras={"custom": "value"})
    assert config_full.project == "test-project"
    assert config_full.dataset_id == "test_dataset"
    assert config_full.extras["custom"] == "value"


def test_bigquery_config_extras_handling() -> None:
    """Test BigQuery config extras parameter handling."""
    # Test with explicit extras
    config = BigQueryConfig(
        project="test-project", dataset_id="test_dataset", extras={"custom_param": "value", "debug": True}
    )
    assert config.extras["custom_param"] == "value"
    assert config.extras["debug"] is True

    # Test with kwargs going to extras
    config2 = BigQueryConfig(project="test-project", dataset_id="test_dataset", unknown_param="test", another_param=42)
    assert config2.extras["unknown_param"] == "test"
    assert config2.extras["another_param"] == 42


def test_bigquery_config_initialization() -> None:
    """Test BigQuery config initialization."""
    # Test with default parameters
    config = BigQueryConfig(project="test-project", dataset_id="test_dataset")
    assert isinstance(config.statement_config, SQLConfig)
    assert isinstance(config.instrumentation, InstrumentationConfig)

    # Test with custom parameters
    custom_statement_config = SQLConfig()
    custom_instrumentation = InstrumentationConfig(log_queries=True)

    config = BigQueryConfig(
        project="test-project",
        dataset_id="test_dataset",
        statement_config=custom_statement_config,
        instrumentation=custom_instrumentation,
    )
    assert config.statement_config is custom_statement_config
    assert config.instrumentation.log_queries is True


def test_bigquery_config_provide_session() -> None:
    """Test BigQuery config provide_session context manager."""
    config = BigQueryConfig(project="test-project", dataset_id="test_dataset")

    # Mock the connection creation to avoid requiring real credentials
    mock_connection = MagicMock()
    with patch.object(config, "create_connection", return_value=mock_connection):
        # Test session context manager behavior
        with config.provide_session() as session:
            assert isinstance(session, BigQueryDriver)
            # Check that parameter styles were set
            assert session.config.allowed_parameter_styles == ("named_at",)
            assert session.config.target_parameter_style == "named_at"
            assert session.instrumentation_config is config.instrumentation


def test_bigquery_config_driver_type() -> None:
    """Test BigQuery config driver_type property."""
    config = BigQueryConfig(project="test-project", dataset_id="test_dataset")
    assert config.driver_type is BigQueryDriver


def test_bigquery_config_is_async() -> None:
    """Test BigQuery config __is_async__ attribute."""
    config = BigQueryConfig(project="test-project", dataset_id="test_dataset")
    assert config.__is_async__ is False
    assert BigQueryConfig.__is_async__ is False


def test_bigquery_config_supports_connection_pooling() -> None:
    """Test BigQuery config __supports_connection_pooling__ attribute."""
    config = BigQueryConfig(project="test-project", dataset_id="test_dataset")
    assert config.__supports_connection_pooling__ is False
    assert BigQueryConfig.__supports_connection_pooling__ is False
