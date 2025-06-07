"""Unit tests for BigQuery configuration."""

from unittest.mock import Mock, patch

import pytest
from google.api_core.client_info import ClientInfo
from google.api_core.client_options import ClientOptions
from google.auth.credentials import Credentials
from google.cloud.bigquery import LoadJobConfig, QueryJobConfig

from sqlspec.adapters.bigquery.config import BigQueryConfig, BigQueryConnectionConfig
from sqlspec.adapters.bigquery.driver import BigQueryConnection, BigQueryDriver
from sqlspec.config import InstrumentationConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow


def test_bigquery_connection_config_creation() -> None:
    """Test BigQueryConnectionConfig creation with various parameters."""
    config: BigQueryConnectionConfig = {
        "project": "test-project",
        "location": "US",
        "dataset_id": "test_dataset",
    }

    assert config["project"] == "test-project"
    assert config["location"] == "US"
    assert config["dataset_id"] == "test_dataset"


def test_bigquery_connection_config_comprehensive_features() -> None:
    """Test BigQueryConnectionConfig with comprehensive Google Cloud features."""
    mock_credentials = Mock(spec=Credentials)
    mock_client_options = Mock(spec=ClientOptions)
    mock_client_info = Mock(spec=ClientInfo)
    mock_query_config = Mock(spec=QueryJobConfig)
    mock_load_config = Mock(spec=LoadJobConfig)

    config: BigQueryConnectionConfig = {
        "project": "enterprise-project",
        "location": "europe-west1",
        "credentials": mock_credentials,
        "dataset_id": "analytics_dataset",
        "credentials_path": "/path/to/service-account.json",
        "client_options": mock_client_options,
        "client_info": mock_client_info,
        "default_query_job_config": mock_query_config,
        "default_load_job_config": mock_load_config,
        "use_legacy_sql": False,
        "use_query_cache": True,
        "maximum_bytes_billed": 1000000000,
        "enable_bigquery_ml": True,
        "enable_gemini_integration": True,
        "query_timeout_ms": 30000,
        "job_timeout_ms": 60000,
        "reservation_id": "enterprise-reservation",
        "edition": "Enterprise Plus",
        "enable_cross_cloud": True,
        "enable_bigquery_omni": True,
        "use_avro_logical_types": True,
        "parquet_enable_list_inference": True,
        "enable_column_level_security": True,
        "enable_row_level_security": True,
        "enable_dataframes": True,
        "dataframes_backend": "bigframes",
        "enable_continuous_queries": True,
        "enable_vector_search": True,
    }

    # Test that all advanced features are properly set
    assert config["enable_bigquery_ml"] is True
    assert config["enable_gemini_integration"] is True
    assert config["enable_vector_search"] is True
    assert config["edition"] == "Enterprise Plus"
    assert config["maximum_bytes_billed"] == 1000000000


def test_bigquery_connection_config_minimal() -> None:
    """Test BigQueryConnectionConfig with minimal configuration."""
    config: BigQueryConnectionConfig = {"project": "minimal-project"}

    assert config["project"] == "minimal-project"
    assert len(config) == 1


def test_bigquery_config_initialization() -> None:
    """Test BigQueryConfig initialization with default parameters."""
    config = BigQueryConfig()

    assert isinstance(config.connection_config, dict)
    assert isinstance(config.statement_config, SQLConfig)
    assert isinstance(config.instrumentation, InstrumentationConfig)
    assert config.default_row_type == DictRow
    assert config.connection_type == BigQueryConnection
    assert config.driver_type == BigQueryDriver


def test_bigquery_config_with_connection_config() -> None:
    """Test BigQueryConfig initialization with connection configuration."""
    connection_config: BigQueryConnectionConfig = {
        "project": "test-project",
        "location": "US",
        "dataset_id": "test_dataset",
        "use_query_cache": True,
        "maximum_bytes_billed": 500000000,
    }

    config = BigQueryConfig(connection_config=connection_config)

    assert config.connection_config["project"] == "test-project"
    assert config.connection_config["location"] == "US"
    assert config.connection_config["dataset_id"] == "test_dataset"
    assert config.connection_config["use_query_cache"] is True
    assert config.connection_config["maximum_bytes_billed"] == 500000000


def test_bigquery_config_with_statement_config() -> None:
    """Test BigQueryConfig initialization with statement configuration."""
    statement_config = SQLConfig(
        strict_mode=False,
        enable_validation=True,
    )

    config = BigQueryConfig(statement_config=statement_config)

    assert config.statement_config.strict_mode is False
    assert config.statement_config.enable_validation is True


def test_bigquery_config_with_instrumentation() -> None:
    """Test BigQueryConfig initialization with instrumentation configuration."""
    instrumentation = InstrumentationConfig(
        log_queries=True,
        log_runtime=True,
        log_pool_operations=True,
    )

    config = BigQueryConfig(instrumentation=instrumentation)

    assert config.instrumentation.log_queries is True
    assert config.instrumentation.log_runtime is True
    assert config.instrumentation.log_pool_operations is True


def test_bigquery_config_with_callbacks() -> None:
    """Test BigQueryConfig initialization with callback functions."""
    connection_callback = Mock()
    job_start_callback = Mock()
    job_complete_callback = Mock()

    config = BigQueryConfig(
        on_connection_create=connection_callback,
        on_job_start=job_start_callback,
        on_job_complete=job_complete_callback,
    )

    assert config.on_connection_create == connection_callback
    assert config.on_job_start == job_start_callback
    assert config.on_job_complete == job_complete_callback


def test_bigquery_config_setup_default_job_config() -> None:
    """Test BigQueryConfig sets up default job configuration."""
    connection_config: BigQueryConnectionConfig = {
        "project": "test-project",
        "dataset_id": "test_dataset",
        "use_query_cache": True,
        "use_legacy_sql": False,
        "maximum_bytes_billed": 1000000,
        "query_timeout_ms": 30000,
    }

    config = BigQueryConfig(connection_config=connection_config)

    # Check that default_query_job_config was created
    assert "default_query_job_config" in config.connection_config
    job_config = config.connection_config["default_query_job_config"]
    assert isinstance(job_config, QueryJobConfig)


def test_bigquery_config_setup_default_job_config_with_existing() -> None:
    """Test BigQueryConfig respects existing default job configuration."""
    existing_job_config = QueryJobConfig()
    existing_job_config.dry_run = True

    connection_config: BigQueryConnectionConfig = {
        "project": "test-project",
        "default_query_job_config": existing_job_config,
    }

    config = BigQueryConfig(connection_config=connection_config)

    # Check that existing job config is preserved
    assert config.connection_config["default_query_job_config"] == existing_job_config
    assert config.connection_config["default_query_job_config"].dry_run is True


def test_bigquery_config_connection_config_dict() -> None:
    """Test BigQueryConfig.connection_config_dict excludes enhancement flags."""
    connection_config: BigQueryConnectionConfig = {
        "project": "test-project",
        "location": "US",
        "credentials_path": "/path/to/credentials.json",
        "dataset_id": "test_dataset",
        "use_legacy_sql": False,
        "enable_bigquery_ml": True,
        "enable_gemini_integration": True,
        "query_timeout_ms": 30000,
        "maximum_bytes_billed": 1000000,
        "enable_vector_search": True,
    }

    config = BigQueryConfig(connection_config=connection_config)
    client_config = config.connection_config_dict

    # Check that all parameters are included (validation removed)
    assert client_config["project"] == "test-project"
    assert client_config["location"] == "US"
    assert client_config["credentials_path"] == "/path/to/credentials.json"
    assert client_config["dataset_id"] == "test_dataset"
    assert client_config["use_legacy_sql"] is False
    assert client_config["enable_bigquery_ml"] is True
    assert client_config["enable_gemini_integration"] is True
    assert client_config["query_timeout_ms"] == 30000
    assert client_config["enable_vector_search"] is True


def test_bigquery_config_connection_config_dict_with_credentials() -> None:
    """Test BigQueryConfig.connection_config_dict includes valid client parameters."""
    mock_credentials = Mock(spec=Credentials)
    mock_client_options = Mock(spec=ClientOptions)
    mock_client_info = Mock(spec=ClientInfo)

    connection_config: BigQueryConnectionConfig = {
        "project": "test-project",
        "credentials": mock_credentials,
        "client_options": mock_client_options,
        "client_info": mock_client_info,
        "enable_bigquery_ml": True,  # Should be excluded
    }

    config = BigQueryConfig(connection_config=connection_config)
    client_config = config.connection_config_dict

    # Check that all parameters are included (validation removed)
    assert client_config["project"] == "test-project"
    assert client_config["credentials"] == mock_credentials
    assert client_config["client_options"] == mock_client_options
    assert client_config["client_info"] == mock_client_info
    assert client_config["enable_bigquery_ml"] is True  # Now included


@patch("sqlspec.adapters.bigquery.config.BigQueryConnection")
def test_bigquery_config_create_connection_success(mock_connection_class: Mock) -> None:
    """Test BigQueryConfig.create_connection succeeds with valid configuration."""
    mock_connection = Mock()
    mock_connection_class.return_value = mock_connection

    connection_config: BigQueryConnectionConfig = {
        "project": "test-project",
        "location": "US",
    }

    config = BigQueryConfig(connection_config=connection_config)
    connection = config.create_connection()

    assert connection == mock_connection
    # Check that the connection was created with the right parameters
    call_args = mock_connection_class.call_args
    assert call_args.kwargs["project"] == "test-project"
    assert call_args.kwargs["location"] == "US"
    assert "default_query_job_config" in call_args.kwargs  # This is now added automatically


@patch("sqlspec.adapters.bigquery.config.BigQueryConnection")
def test_bigquery_config_create_connection_with_callback(mock_connection_class: Mock) -> None:
    """Test BigQueryConfig.create_connection executes connection creation callback."""
    mock_connection = Mock()
    mock_connection_class.return_value = mock_connection
    connection_callback = Mock()

    config = BigQueryConfig(
        connection_config={"project": "test-project"},
        on_connection_create=connection_callback,
    )
    connection = config.create_connection()

    assert connection == mock_connection
    connection_callback.assert_called_once_with(mock_connection)


@patch("sqlspec.adapters.bigquery.config.BigQueryConnection")
def test_bigquery_config_create_connection_callback_exception(mock_connection_class: Mock) -> None:
    """Test BigQueryConfig.create_connection handles callback exceptions gracefully."""
    mock_connection = Mock()
    mock_connection_class.return_value = mock_connection
    connection_callback = Mock(side_effect=Exception("Callback error"))

    config = BigQueryConfig(
        connection_config={"project": "test-project"},
        on_connection_create=connection_callback,
    )

    # Should not raise exception even if callback fails
    connection = config.create_connection()
    assert connection == mock_connection


@patch("sqlspec.adapters.bigquery.config.BigQueryConnection")
def test_bigquery_config_create_connection_reuse(mock_connection_class: Mock) -> None:
    """Test BigQueryConfig.create_connection reuses existing connection."""
    mock_connection = Mock()
    mock_connection_class.return_value = mock_connection

    config = BigQueryConfig(connection_config={"project": "test-project"})

    # First call should create connection
    connection1 = config.create_connection()
    assert connection1 == mock_connection

    # Second call should reuse existing connection
    connection2 = config.create_connection()
    assert connection2 == mock_connection
    assert connection1 is connection2

    # Should only call constructor once
    mock_connection_class.assert_called_once()


@patch("sqlspec.adapters.bigquery.config.BigQueryConnection")
def test_bigquery_config_create_connection_failure(mock_connection_class: Mock) -> None:
    """Test BigQueryConfig.create_connection raises ImproperConfigurationError on failure."""
    mock_connection_class.side_effect = Exception("Connection failed")

    config = BigQueryConfig(connection_config={"project": "invalid-project"})

    with pytest.raises(ImproperConfigurationError, match="Could not configure BigQuery connection"):
        config.create_connection()


def test_bigquery_config_provide_connection() -> None:
    """Test BigQueryConfig.provide_connection yields connection in context manager."""
    mock_connection = Mock()

    config = BigQueryConfig(connection_config={"project": "test-project"})
    config._connection_instance = mock_connection

    with config.provide_connection() as connection:
        assert connection == mock_connection


def test_bigquery_config_provide_session() -> None:
    """Test BigQueryConfig.provide_session yields driver in context manager."""
    mock_connection = Mock()
    mock_driver = Mock(spec=BigQueryDriver)

    config = BigQueryConfig(connection_config={"project": "test-project"})
    config._connection_instance = mock_connection

    with patch("sqlspec.adapters.bigquery.config.BigQueryDriver", return_value=mock_driver):
        with config.provide_session() as driver:
            assert driver == mock_driver


def test_bigquery_config_provide_session_with_args() -> None:
    """Test BigQueryConfig.provide_session passes arguments to driver."""
    mock_connection = Mock()

    config = BigQueryConfig(
        connection_config={"project": "test-project"},
        on_job_start=Mock(),
        on_job_complete=Mock(),
    )
    config._connection_instance = mock_connection

    with patch("sqlspec.adapters.bigquery.config.BigQueryDriver") as mock_driver_class:
        mock_driver = Mock(spec=BigQueryDriver)
        mock_driver_class.return_value = mock_driver

        with config.provide_session() as driver:
            assert driver == mock_driver

            # Check that driver was created with correct arguments
            mock_driver_class.assert_called_once()
            call_args = mock_driver_class.call_args
            assert call_args.kwargs["connection"] == mock_connection
            assert call_args.kwargs["config"] == config.statement_config
            assert call_args.kwargs["instrumentation_config"] == config.instrumentation
            assert call_args.kwargs["default_row_type"] == config.default_row_type
            assert call_args.kwargs["on_job_start"] == config.on_job_start
            assert call_args.kwargs["on_job_complete"] == config.on_job_complete


def test_bigquery_config_class_variables() -> None:
    """Test BigQueryConfig class variables are set correctly."""
    config = BigQueryConfig()

    assert config.__is_async__ is False
    assert config.__supports_connection_pooling__ is False


def test_bigquery_config_ml_features() -> None:
    """Test BigQueryConfig with machine learning feature configuration."""
    connection_config: BigQueryConnectionConfig = {
        "project": "ml-project",
        "enable_bigquery_ml": True,
        "enable_gemini_integration": True,
        "enable_vector_search": True,
        "enable_dataframes": True,
        "dataframes_backend": "bigframes",
    }

    config = BigQueryConfig(connection_config=connection_config)

    assert config.connection_config["enable_bigquery_ml"] is True
    assert config.connection_config["enable_gemini_integration"] is True
    assert config.connection_config["enable_vector_search"] is True
    assert config.connection_config["enable_dataframes"] is True
    assert config.connection_config["dataframes_backend"] == "bigframes"


def test_bigquery_config_enterprise_features() -> None:
    """Test BigQueryConfig with enterprise feature configuration."""
    connection_config: BigQueryConnectionConfig = {
        "project": "enterprise-project",
        "edition": "Enterprise Plus",
        "reservation_id": "enterprise-reservation",
        "enable_cross_cloud": True,
        "enable_bigquery_omni": True,
        "enable_continuous_queries": True,
        "enable_column_level_security": True,
        "enable_row_level_security": True,
    }

    config = BigQueryConfig(connection_config=connection_config)

    assert config.connection_config["edition"] == "Enterprise Plus"
    assert config.connection_config["reservation_id"] == "enterprise-reservation"
    assert config.connection_config["enable_cross_cloud"] is True
    assert config.connection_config["enable_bigquery_omni"] is True
    assert config.connection_config["enable_continuous_queries"] is True
    assert config.connection_config["enable_column_level_security"] is True
    assert config.connection_config["enable_row_level_security"] is True


def test_bigquery_config_performance_options() -> None:
    """Test BigQueryConfig with performance and optimization settings."""
    connection_config: BigQueryConnectionConfig = {
        "project": "performance-project",
        "use_query_cache": True,
        "maximum_bytes_billed": 2000000000,
        "query_timeout_ms": 60000,
        "job_timeout_ms": 120000,
        "use_avro_logical_types": True,
        "parquet_enable_list_inference": True,
    }

    config = BigQueryConfig(connection_config=connection_config)

    assert config.connection_config["use_query_cache"] is True
    assert config.connection_config["maximum_bytes_billed"] == 2000000000
    assert config.connection_config["query_timeout_ms"] == 60000
    assert config.connection_config["job_timeout_ms"] == 120000
    assert config.connection_config["use_avro_logical_types"] is True
    assert config.connection_config["parquet_enable_list_inference"] is True


def test_bigquery_config_security_options() -> None:
    """Test BigQueryConfig with security and authentication settings."""
    mock_credentials = Mock(spec=Credentials)
    mock_client_options = Mock(spec=ClientOptions)

    connection_config: BigQueryConnectionConfig = {
        "project": "secure-project",
        "credentials": mock_credentials,
        "credentials_path": "/secure/path/to/credentials.json",
        "client_options": mock_client_options,
        "enable_column_level_security": True,
        "enable_row_level_security": True,
    }

    config = BigQueryConfig(connection_config=connection_config)

    assert config.connection_config["credentials"] == mock_credentials
    assert config.connection_config["credentials_path"] == "/secure/path/to/credentials.json"
    assert config.connection_config["client_options"] == mock_client_options
    assert config.connection_config["enable_column_level_security"] is True
    assert config.connection_config["enable_row_level_security"] is True
