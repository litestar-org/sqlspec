"""Unit tests for BigQuery driver."""

import datetime
import math
from decimal import Decimal
from typing import Union
from unittest.mock import Mock, patch

import pytest
from google.cloud.bigquery import (
    ArrayQueryParameter,
    Client,
    QueryJob,
    QueryJobConfig,
    ScalarQueryParameter,
)

from sqlspec.adapters.bigquery.driver import BigQueryDriver
from sqlspec.config import InstrumentationConfig
from sqlspec.exceptions import SQLSpecError
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow


@pytest.fixture
def mock_bigquery_connection() -> Mock:
    """Create a mock BigQuery connection."""
    mock_conn = Mock(spec=Client)
    mock_conn.project = "test-project"
    mock_conn.location = "US"
    return mock_conn


@pytest.fixture
def bigquery_driver(mock_bigquery_connection: Mock) -> BigQueryDriver:
    """Create a BigQuery driver with mock connection."""
    return BigQueryDriver(
        connection=mock_bigquery_connection,
        config=SQLConfig(strict_mode=False),
        instrumentation_config=InstrumentationConfig(),
    )


@pytest.fixture
def mock_query_job() -> Mock:
    """Create a mock BigQuery QueryJob."""
    mock_job = Mock(spec=QueryJob)
    mock_job.job_id = "test-job-123"
    mock_job.num_dml_affected_rows = 5
    return mock_job


def test_bigquery_driver_initialization(mock_bigquery_connection: Mock) -> None:
    """Test BigQueryDriver initialization with default parameters."""
    driver = BigQueryDriver(connection=mock_bigquery_connection)

    assert driver.connection == mock_bigquery_connection
    assert driver.dialect == "bigquery"
    assert driver.__supports_arrow__ is True
    assert driver.__supports_parquet__ is True
    assert driver.default_row_type == DictRow
    assert isinstance(driver.config, SQLConfig)
    assert isinstance(driver.instrumentation_config, InstrumentationConfig)


def test_bigquery_driver_initialization_with_config(mock_bigquery_connection: Mock) -> None:
    """Test BigQueryDriver initialization with custom configuration."""
    config = SQLConfig(strict_mode=False)
    instrumentation = InstrumentationConfig(log_queries=True)

    driver = BigQueryDriver(
        connection=mock_bigquery_connection,
        config=config,
        instrumentation_config=instrumentation,
    )

    assert driver.config == config
    assert driver.instrumentation_config == instrumentation


def test_bigquery_driver_initialization_with_callbacks(mock_bigquery_connection: Mock) -> None:
    """Test BigQueryDriver initialization with callback functions."""
    job_start_callback = Mock()
    job_complete_callback = Mock()

    driver = BigQueryDriver(
        connection=mock_bigquery_connection,
        on_job_start=job_start_callback,
        on_job_complete=job_complete_callback,
    )

    assert driver.on_job_start == job_start_callback
    assert driver.on_job_complete == job_complete_callback


def test_bigquery_driver_initialization_with_job_config(mock_bigquery_connection: Mock) -> None:
    """Test BigQueryDriver initialization with default query job config."""
    job_config = QueryJobConfig()
    job_config.dry_run = True

    driver = BigQueryDriver(
        connection=mock_bigquery_connection,
        default_query_job_config=job_config,
    )

    assert driver._default_query_job_config == job_config


def test_bigquery_driver_get_placeholder_style(bigquery_driver: BigQueryDriver) -> None:
    """Test BigQueryDriver placeholder style is NAMED_AT."""
    style = bigquery_driver._get_placeholder_style()
    assert style == ParameterStyle.NAMED_AT


def test_bigquery_driver_get_bq_param_type_bool(bigquery_driver: BigQueryDriver) -> None:
    """Test BigQuery parameter type detection for boolean values."""
    param_type, array_type = bigquery_driver._get_bq_param_type(True)
    assert param_type == "BOOL"
    assert array_type is None


def test_bigquery_driver_get_bq_param_type_int(bigquery_driver: BigQueryDriver) -> None:
    """Test BigQuery parameter type detection for integer values."""
    param_type, array_type = bigquery_driver._get_bq_param_type(42)
    assert param_type == "INT64"
    assert array_type is None


def test_bigquery_driver_get_bq_param_type_float(bigquery_driver: BigQueryDriver) -> None:
    """Test BigQuery parameter type detection for float values."""
    param_type, array_type = bigquery_driver._get_bq_param_type(math.pi)
    assert param_type == "FLOAT64"
    assert array_type is None


def test_bigquery_driver_get_bq_param_type_decimal(bigquery_driver: BigQueryDriver) -> None:
    """Test BigQuery parameter type detection for decimal values."""
    param_type, array_type = bigquery_driver._get_bq_param_type(Decimal("123.45"))
    assert param_type == "BIGNUMERIC"
    assert array_type is None


def test_bigquery_driver_get_bq_param_type_string(bigquery_driver: BigQueryDriver) -> None:
    """Test BigQuery parameter type detection for string values."""
    param_type, array_type = bigquery_driver._get_bq_param_type("test string")
    assert param_type == "STRING"
    assert array_type is None


def test_bigquery_driver_get_bq_param_type_bytes(bigquery_driver: BigQueryDriver) -> None:
    """Test BigQuery parameter type detection for bytes values."""
    param_type, array_type = bigquery_driver._get_bq_param_type(b"test bytes")
    assert param_type == "BYTES"
    assert array_type is None


def test_bigquery_driver_get_bq_param_type_date(bigquery_driver: BigQueryDriver) -> None:
    """Test BigQuery parameter type detection for date values."""
    param_type, array_type = bigquery_driver._get_bq_param_type(datetime.date(2023, 1, 1))
    assert param_type == "DATE"
    assert array_type is None


def test_bigquery_driver_get_bq_param_type_datetime_with_tz(bigquery_driver: BigQueryDriver) -> None:
    """Test BigQuery parameter type detection for datetime with timezone."""
    dt = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
    param_type, array_type = bigquery_driver._get_bq_param_type(dt)
    assert param_type == "TIMESTAMP"
    assert array_type is None


def test_bigquery_driver_get_bq_param_type_datetime_without_tz(bigquery_driver: BigQueryDriver) -> None:
    """Test BigQuery parameter type detection for datetime without timezone."""
    dt = datetime.datetime(2023, 1, 1, 12, 0, 0)
    param_type, array_type = bigquery_driver._get_bq_param_type(dt)
    assert param_type == "DATETIME"
    assert array_type is None


def test_bigquery_driver_get_bq_param_type_time(bigquery_driver: BigQueryDriver) -> None:
    """Test BigQuery parameter type detection for time values."""
    param_type, array_type = bigquery_driver._get_bq_param_type(datetime.time(12, 30, 0))
    assert param_type == "TIME"
    assert array_type is None


def test_bigquery_driver_get_bq_param_type_array_string(bigquery_driver: BigQueryDriver) -> None:
    """Test BigQuery parameter type detection for string arrays."""
    param_type, array_type = bigquery_driver._get_bq_param_type(["a", "b", "c"])
    assert param_type == "ARRAY"
    assert array_type == "STRING"


def test_bigquery_driver_get_bq_param_type_array_int(bigquery_driver: BigQueryDriver) -> None:
    """Test BigQuery parameter type detection for integer arrays."""
    param_type, array_type = bigquery_driver._get_bq_param_type([1, 2, 3])
    assert param_type == "ARRAY"
    assert array_type == "INT64"


def test_bigquery_driver_get_bq_param_type_array_empty(bigquery_driver: BigQueryDriver) -> None:
    """Test BigQuery parameter type detection raises error for empty arrays."""
    with pytest.raises(SQLSpecError, match="Cannot determine BigQuery ARRAY type for empty sequence"):
        bigquery_driver._get_bq_param_type([])


def test_bigquery_driver_get_bq_param_type_unsupported(bigquery_driver: BigQueryDriver) -> None:
    """Test BigQuery parameter type detection for unsupported types."""
    param_type, array_type = bigquery_driver._get_bq_param_type(object())
    assert param_type is None
    assert array_type is None


def test_bigquery_driver_prepare_bq_query_parameters_scalar(bigquery_driver: BigQueryDriver) -> None:
    """Test BigQuery query parameter preparation for scalar values."""
    params_dict = {
        "@name": "John",
        "@age": 30,
        "@active": True,
        "@score": 95.5,
    }

    bq_params = bigquery_driver._prepare_bq_query_parameters(params_dict)

    assert len(bq_params) == 4
    assert all(isinstance(p, ScalarQueryParameter) for p in bq_params)

    # Check parameter names (@ prefix should be stripped)
    param_names = [p.name for p in bq_params]
    assert "name" in param_names
    assert "age" in param_names
    assert "active" in param_names
    assert "score" in param_names


def test_bigquery_driver_prepare_bq_query_parameters_array(bigquery_driver: BigQueryDriver) -> None:
    """Test BigQuery query parameter preparation for array values."""
    params_dict = {
        "@tags": ["python", "sql", "bigquery"],
        "@numbers": [1, 2, 3, 4, 5],
    }

    bq_params = bigquery_driver._prepare_bq_query_parameters(params_dict)

    assert len(bq_params) == 2
    assert all(isinstance(p, ArrayQueryParameter) for p in bq_params)

    # Find the tags parameter
    tags_param = next(p for p in bq_params if p.name == "tags")
    assert not isinstance(tags_param, ScalarQueryParameter)
    assert tags_param.array_type == "STRING"  # pyright: ignore
    assert tags_param.values == ["python", "sql", "bigquery"]  # pyright: ignore

    # Find the numbers parameter
    numbers_param = next(p for p in bq_params if p.name == "numbers")
    assert not isinstance(numbers_param, ScalarQueryParameter)
    assert numbers_param.array_type == "INT64"  # pyright: ignore
    assert numbers_param.values == [1, 2, 3, 4, 5]  # pyright: ignore


def test_bigquery_driver_prepare_bq_query_parameters_empty(bigquery_driver: BigQueryDriver) -> None:
    """Test BigQuery query parameter preparation with empty parameters."""
    bq_params = bigquery_driver._prepare_bq_query_parameters({})
    assert bq_params == []


def test_bigquery_driver_prepare_bq_query_parameters_unsupported(bigquery_driver: BigQueryDriver) -> None:
    """Test BigQuery query parameter preparation raises error for unsupported types."""
    params_dict = {"@obj": object()}

    with pytest.raises(SQLSpecError, match="Unsupported BigQuery parameter type"):
        bigquery_driver._prepare_bq_query_parameters(params_dict)


@patch("sqlspec.adapters.bigquery.driver.datetime")
def test_bigquery_driver_run_query_job(
    mock_datetime: Mock, bigquery_driver: BigQueryDriver, mock_bigquery_connection: Mock, mock_query_job: Mock
) -> None:
    """Test BigQueryDriver._run_query_job execution."""
    mock_datetime.datetime.now.return_value.strftime.return_value = "20231201-120000"
    mock_bigquery_connection.query.return_value = mock_query_job

    sql_str = "SELECT * FROM users WHERE id = @user_id"
    bq_params: list[Union[ScalarQueryParameter, ArrayQueryParameter]] = [ScalarQueryParameter("user_id", "INT64", 123)]

    result = bigquery_driver._run_query_job(sql_str, bq_params)

    assert result == mock_query_job
    mock_bigquery_connection.query.assert_called_once()

    # Check that query was called with correct SQL and job config
    call_args = mock_bigquery_connection.query.call_args
    assert call_args[0][0] == sql_str
    assert isinstance(call_args.kwargs["job_config"], QueryJobConfig)
    assert call_args.kwargs["job_config"].query_parameters == bq_params


def test_bigquery_driver_run_query_job_with_callbacks(
    bigquery_driver: BigQueryDriver, mock_bigquery_connection: Mock, mock_query_job: Mock
) -> None:
    """Test BigQueryDriver._run_query_job with job callbacks."""
    job_start_callback = Mock()
    job_complete_callback = Mock()
    bigquery_driver.on_job_start = job_start_callback
    bigquery_driver.on_job_complete = job_complete_callback

    mock_bigquery_connection.query.return_value = mock_query_job

    sql_str = "SELECT * FROM users"
    result = bigquery_driver._run_query_job(sql_str, [])

    assert result == mock_query_job
    job_start_callback.assert_called_once()
    job_complete_callback.assert_called_once_with(mock_query_job.job_id, mock_query_job)


def test_bigquery_driver_run_query_job_callback_exceptions(
    bigquery_driver: BigQueryDriver, mock_bigquery_connection: Mock, mock_query_job: Mock
) -> None:
    """Test BigQueryDriver._run_query_job handles callback exceptions gracefully."""
    bigquery_driver.on_job_start = Mock(side_effect=Exception("Start callback error"))
    bigquery_driver.on_job_complete = Mock(side_effect=Exception("Complete callback error"))

    mock_bigquery_connection.query.return_value = mock_query_job

    # Should not raise exception even if callbacks fail
    result = bigquery_driver._run_query_job("SELECT 1", [])
    assert result == mock_query_job


def test_bigquery_driver_rows_to_results(bigquery_driver: BigQueryDriver) -> None:
    """Test BigQueryDriver._rows_to_results conversion."""
    # Create mock BigQuery rows
    # Don't use spec to allow setting __iter__
    mock_row1 = Mock()
    mock_row1.__iter__ = Mock(return_value=iter([("id", 1), ("name", "John")]))
    mock_row1.keys.return_value = ["id", "name"]
    mock_row1.values.return_value = [1, "John"]

    mock_row2 = Mock()
    mock_row2.__iter__ = Mock(return_value=iter([("id", 2), ("name", "Jane")]))
    mock_row2.keys.return_value = ["id", "name"]
    mock_row2.values.return_value = [2, "Jane"]

    # Mock dict() constructor for BigQuery rows
    with patch("builtins.dict") as mock_dict:
        mock_dict.side_effect = [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]

        rows_iterator = iter([mock_row1, mock_row2])
        result = bigquery_driver._rows_to_results(rows_iterator)

        assert result == [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]


def test_bigquery_driver_execute_statement_select(
    bigquery_driver: BigQueryDriver, mock_bigquery_connection: Mock, mock_query_job: Mock
) -> None:
    """Test BigQueryDriver._execute_statement for SELECT statements."""
    mock_bigquery_connection.query.return_value = mock_query_job

    # Mock query job result to iterate properly
    mock_result = Mock()
    mock_result.__iter__ = Mock(return_value=iter([]))  # No rows
    mock_field = Mock()
    mock_field.name = "id"
    mock_result.schema = [mock_field]

    mock_query_job.result.return_value = mock_result
    mock_query_job.num_dml_affected_rows = None

    parameters = {"user_id": 123}
    statement = SQL("SELECT * FROM users WHERE id = @user_id", parameters=parameters)
    result = bigquery_driver._execute_statement(statement)

    # Should return a QueryJob
    assert result == mock_query_job
    mock_bigquery_connection.query.assert_called_once()


def test_bigquery_driver_from_query_builder(bigquery_driver: BigQueryDriver) -> None:
    """Test BigQueryDriver with query builder integration."""
    from sqlspec.statement.builder import QueryBuilder

    mock_builder = Mock(spec=QueryBuilder)
    mock_builder.to_statement.return_value = SQL("SELECT * FROM users")

    # Should be able to handle QueryBuilder objects
    result = bigquery_driver._build_statement(mock_builder, None, None, *())
    assert isinstance(result, SQL)


def test_bigquery_driver_parameter_processing_dict(bigquery_driver: BigQueryDriver) -> None:
    """Test BigQuery driver processes dictionary parameters correctly."""
    params_dict = {"user_id": 123, "name": "John"}

    bq_params = bigquery_driver._prepare_bq_query_parameters(params_dict)

    assert len(bq_params) == 2
    param_names = [p.name for p in bq_params]
    assert "user_id" in param_names
    assert "name" in param_names


def test_bigquery_driver_parameter_processing_mixed_types(bigquery_driver: BigQueryDriver) -> None:
    """Test BigQuery driver processes mixed parameter types correctly."""
    params_dict = {
        "@string_param": "test",
        "@int_param": 42,
        "@bool_param": True,
        "@float_param": math.pi,
        "@array_param": [1, 2, 3],
        "@date_param": datetime.date(2023, 1, 1),
    }

    bq_params = bigquery_driver._prepare_bq_query_parameters(params_dict)

    assert len(bq_params) == 6

    # Check that array parameter is correctly identified
    array_params = [p for p in bq_params if isinstance(p, ArrayQueryParameter)]
    assert len(array_params) == 1
    assert array_params[0].name == "array_param"
    assert array_params[0].array_type == "INT64"


def test_bigquery_driver_connection_override(bigquery_driver: BigQueryDriver) -> None:
    """Test BigQuery driver with connection override."""
    override_connection = Mock(spec=Client)
    override_connection.query.return_value = Mock()

    statement = SQL("SELECT 1")

    # Should use override connection instead of driver's connection
    bigquery_driver._execute_statement(statement, connection=override_connection)

    override_connection.query.assert_called_once()
    # Original connection should not be called
    bigquery_driver.connection.query.assert_not_called()  # pyright: ignore


def test_bigquery_driver_job_config_inheritance(mock_bigquery_connection: Mock) -> None:
    """Test BigQuery driver inherits job config from connection."""
    # Mock connection with default job config
    default_job_config = QueryJobConfig()
    default_job_config.use_query_cache = True
    mock_bigquery_connection.default_query_job_config = default_job_config

    driver = BigQueryDriver(connection=mock_bigquery_connection)

    assert driver._default_query_job_config == default_job_config


def test_bigquery_driver_job_config_precedence(mock_bigquery_connection: Mock) -> None:
    """Test BigQuery driver job config override takes precedence."""
    # Mock connection with default job config
    connection_job_config = QueryJobConfig()
    connection_job_config.use_query_cache = True
    mock_bigquery_connection.default_query_job_config = connection_job_config

    # Override with driver-specific job config
    driver_job_config = QueryJobConfig()
    driver_job_config.dry_run = True

    driver = BigQueryDriver(
        connection=mock_bigquery_connection,
        default_query_job_config=driver_job_config,
    )

    assert driver._default_query_job_config == driver_job_config


def test_to_parquet_raises_if_not_gcs(bigquery_driver: BigQueryDriver) -> None:
    """Test that to_parquet raises if the path is not a gs:// URI."""
    # Skip this test - unified storage mixin supports any path, not just GCS
    pytest.skip("Unified storage mixin supports any path, not just GCS")


def test_to_parquet_raises_if_bucket_not_registered(
    bigquery_driver: BigQueryDriver, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that to_parquet raises if the GCS bucket is not registered as a storage backend."""
    # Skip this test - unified storage mixin handles unregistered paths differently
    pytest.skip("Unified storage mixin handles unregistered paths differently")


def test_to_parquet_calls_extract_table(monkeypatch: pytest.MonkeyPatch, bigquery_driver: BigQueryDriver) -> None:
    """Test that to_parquet calls extract_table with correct arguments if path and bucket are valid."""
    # Skip this test - unified storage mixin doesn't use extract_table anymore
    pytest.skip("Unified storage mixin doesn't use BigQuery extract_table")


def test_bigquery_driver_fetch_arrow_table_native(bigquery_driver: BigQueryDriver, mock_query_job: Mock) -> None:
    """Test BigQueryDriver._fetch_arrow_table uses native QueryJob.to_arrow()."""
    import pyarrow as pa

    from sqlspec.statement.result import ArrowResult

    # Setup mock arrow table for native fetch
    mock_arrow_table = pa.table({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
    mock_query_job.to_arrow.return_value = mock_arrow_table

    # Mock the job result to complete successfully
    mock_query_job.result.return_value = None

    # Mock _execute to return the query job
    bigquery_driver.connection.query.return_value = mock_query_job

    statement = SQL("SELECT * FROM users")
    result = bigquery_driver.fetch_arrow_table(statement)

    assert isinstance(result, ArrowResult)
    assert result.data is mock_arrow_table  # Should be the exact same table
    assert result.data.num_rows == 3
    assert result.data.column_names == ["id", "name"]

    # Verify native to_arrow was called
    mock_query_job.to_arrow.assert_called_once()
    # Verify query job was waited on
    mock_query_job.result.assert_called_once()


def test_bigquery_driver_fetch_arrow_table_with_options(bigquery_driver: BigQueryDriver, mock_query_job: Mock) -> None:
    """Test BigQueryDriver._fetch_arrow_table passes through BigQuery-specific options."""
    import pyarrow as pa

    from sqlspec.statement.result import ArrowResult

    # Setup mock arrow table for native fetch
    mock_arrow_table = pa.table({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
    mock_query_job.to_arrow.return_value = mock_arrow_table
    mock_query_job.result.return_value = None

    # Mock _execute to return the query job
    bigquery_driver.connection.query.return_value = mock_query_job

    statement = SQL("SELECT * FROM users")

    # Test with BigQuery Storage API disabled
    result = bigquery_driver.fetch_arrow_table(statement, use_bqstorage_api=False, bq_job_timeout=30)

    assert isinstance(result, ArrowResult)

    # Verify to_arrow was called with correct options
    mock_query_job.to_arrow.assert_called_once_with(create_bqstorage_client=False)
    # Verify timeout was passed to result()
    mock_query_job.result.assert_called_once_with(timeout=30)
