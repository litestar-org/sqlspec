"""Unit tests for BigQuery driver.

This module tests the BigQueryDriver class including:
- Driver initialization and configuration
- Statement execution (single, many, script)
- Result wrapping and formatting
- Parameter style handling
- Type coercion overrides
- Storage functionality
- Error handling
- BigQuery-specific features (job callbacks, parameter types)
"""

import datetime
import math
from decimal import Decimal
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import pytest

from sqlspec.adapters.bigquery import BigQueryDriver
from sqlspec.exceptions import SQLSpecError
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow

if TYPE_CHECKING:
    pass


# Test Fixtures
@pytest.fixture
def mock_connection() -> MagicMock:
    """Create a mock BigQuery connection."""
    mock_conn = MagicMock()

    # Set up connection attributes
    mock_conn.project = "test-project"
    mock_conn.location = "US"
    mock_conn.default_query_job_config = None

    # Mock query method
    mock_job = MagicMock()
    mock_job.job_id = "test-job-123"
    mock_job.num_dml_affected_rows = 0
    mock_job.state = "DONE"
    mock_job.errors = None
    mock_job.schema = []
    mock_job.statement_type = "SELECT"
    mock_job.result.return_value = iter([])
    mock_job.to_arrow.return_value = None

    mock_conn.query.return_value = mock_job

    return mock_conn


@pytest.fixture
def driver(mock_connection: MagicMock) -> BigQueryDriver:
    """Create a BigQuery driver with mocked connection."""
    config = SQLConfig(
        allowed_parameter_styles=("named_at", "named_colon", "qmark"),
        target_parameter_style="named_at",
        strict_mode=False,
    )
    return BigQueryDriver(connection=mock_connection, config=config)


# Initialization Tests
def test_driver_initialization() -> None:
    """Test driver initialization with various parameters."""
    mock_conn = MagicMock()
    config = SQLConfig()

    driver = BigQueryDriver(connection=mock_conn, config=config)

    assert driver.connection is mock_conn
    assert driver.config is config
    assert driver.dialect == "bigquery"
    assert driver.default_parameter_style == ParameterStyle.NAMED_AT
    assert driver.supported_parameter_styles == (ParameterStyle.NAMED_AT,)


def test_driver_default_row_type() -> None:
    """Test driver default row type."""
    mock_conn = MagicMock()

    # Default row type - BigQuery uses a string type hint
    driver = BigQueryDriver(connection=mock_conn)
    assert driver.default_row_type == DictRow

    # Custom row type
    custom_type: type[DictRow] = dict
    driver = BigQueryDriver(connection=mock_conn, default_row_type=custom_type)
    assert driver.default_row_type is custom_type


def test_driver_initialization_with_callbacks() -> None:
    """Test driver initialization with job callback functions."""
    mock_conn = MagicMock()
    job_start_callback = MagicMock()
    job_complete_callback = MagicMock()

    driver = BigQueryDriver(
        connection=mock_conn, on_job_start=job_start_callback, on_job_complete=job_complete_callback
    )

    assert driver.on_job_start is job_start_callback
    assert driver.on_job_complete is job_complete_callback


def test_driver_initialization_with_job_config() -> None:
    """Test driver initialization with default query job config."""
    from google.cloud.bigquery import QueryJobConfig

    mock_conn = MagicMock()
    job_config = QueryJobConfig()
    job_config.dry_run = True

    driver = BigQueryDriver(connection=mock_conn, default_query_job_config=job_config)

    assert driver._default_query_job_config is job_config


# Arrow Support Tests
def test_arrow_support_flags() -> None:
    """Test driver Arrow support flags."""
    mock_conn = MagicMock()
    driver = BigQueryDriver(connection=mock_conn)

    assert driver.supports_native_arrow_export is True
    assert driver.supports_native_arrow_import is True
    assert BigQueryDriver.supports_native_arrow_export is True
    assert BigQueryDriver.supports_native_arrow_import is True


# Parameter Type Detection Tests
@pytest.mark.parametrize(
    "value,expected_type,expected_array_type",
    [
        (True, "BOOL", None),
        (False, "BOOL", None),
        (42, "INT64", None),
        (math.pi, "FLOAT64", None),
        (Decimal("123.45"), "BIGNUMERIC", None),
        ("test string", "STRING", None),
        (b"test bytes", "BYTES", None),
        (datetime.date(2023, 1, 1), "DATE", None),
        (datetime.time(12, 30, 0), "TIME", None),
        (datetime.datetime(2023, 1, 1, 12, 0, 0), "DATETIME", None),
        (datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc), "TIMESTAMP", None),
        (["a", "b", "c"], "ARRAY", "STRING"),
        ([1, 2, 3], "ARRAY", "INT64"),
        ({"key": "value"}, "JSON", None),
    ],
    ids=[
        "bool_true",
        "bool_false",
        "int",
        "float",
        "decimal",
        "string",
        "bytes",
        "date",
        "time",
        "datetime_naive",
        "datetime_tz",
        "array_string",
        "array_int",
        "json",
    ],
)
def test_get_bq_param_type(
    driver: BigQueryDriver, value: Any, expected_type: str, expected_array_type: "str | None"
) -> None:
    """Test BigQuery parameter type detection."""
    param_type, array_type = driver._get_bq_param_type(value)
    assert param_type == expected_type
    assert array_type == expected_array_type


def test_get_bq_param_type_empty_array(driver: BigQueryDriver) -> None:
    """Test BigQuery parameter type detection raises error for empty arrays."""
    with pytest.raises(SQLSpecError, match="Cannot determine BigQuery ARRAY type for empty sequence"):
        driver._get_bq_param_type([])


def test_get_bq_param_type_unsupported(driver: BigQueryDriver) -> None:
    """Test BigQuery parameter type detection for unsupported types."""
    param_type, array_type = driver._get_bq_param_type(object())
    assert param_type is None
    assert array_type is None


# Parameter Preparation Tests
def test_prepare_bq_query_parameters_scalar(driver: BigQueryDriver) -> None:
    """Test BigQuery query parameter preparation for scalar values."""
    from google.cloud.bigquery import ScalarQueryParameter

    params_dict = {"@name": "John", "@age": 30, "@active": True, "@score": 95.5}

    bq_params = driver._prepare_bq_query_parameters(params_dict)

    assert len(bq_params) == 4
    assert all(isinstance(p, ScalarQueryParameter) for p in bq_params)

    # Check parameter names (@ prefix should be stripped)
    param_names = [p.name for p in bq_params]
    assert "name" in param_names
    assert "age" in param_names
    assert "active" in param_names
    assert "score" in param_names


def test_prepare_bq_query_parameters_array(driver: BigQueryDriver) -> None:
    """Test BigQuery query parameter preparation for array values."""
    from google.cloud.bigquery import ArrayQueryParameter

    params_dict = {"@tags": ["python", "sql", "bigquery"], "@numbers": [1, 2, 3, 4, 5]}

    bq_params = driver._prepare_bq_query_parameters(params_dict)

    assert len(bq_params) == 2
    assert all(isinstance(p, ArrayQueryParameter) for p in bq_params)

    # Find the tags parameter
    tags_param = next(p for p in bq_params if p.name == "tags")
    assert isinstance(tags_param, ArrayQueryParameter)
    assert tags_param.array_type == "STRING"
    assert tags_param.values == ["python", "sql", "bigquery"]

    # Find the numbers parameter
    numbers_param = next(p for p in bq_params if p.name == "numbers")
    assert isinstance(numbers_param, ArrayQueryParameter)
    assert numbers_param.array_type == "INT64"
    assert numbers_param.values == [1, 2, 3, 4, 5]


def test_prepare_bq_query_parameters_empty(driver: BigQueryDriver) -> None:
    """Test BigQuery query parameter preparation with empty parameters."""
    bq_params = driver._prepare_bq_query_parameters({})
    assert bq_params == []


def test_prepare_bq_query_parameters_unsupported(driver: BigQueryDriver) -> None:
    """Test BigQuery query parameter preparation raises error for unsupported types."""
    params_dict = {"@obj": object()}

    with pytest.raises(SQLSpecError, match="Unsupported BigQuery parameter type"):
        driver._prepare_bq_query_parameters(params_dict)


# Execute Statement Tests
@pytest.mark.parametrize(
    "sql_text,is_script,is_many,expected_method",
    [
        ("SELECT * FROM users", False, False, "_execute"),
        ("INSERT INTO users VALUES (@id)", False, True, "_execute_many"),
        ("CREATE TABLE test; INSERT INTO test;", True, False, "_execute_script"),
    ],
    ids=["select", "execute_many", "script"],
)
def test_execute_statement_routing(
    driver: BigQueryDriver,
    mock_connection: MagicMock,
    sql_text: str,
    is_script: bool,
    is_many: bool,
    expected_method: str,
) -> None:
    """Test that _execute_statement routes to correct method."""
    from sqlspec.statement.sql import SQLConfig

    # Create config that allows DDL if needed
    config = SQLConfig(enable_validation=False) if "CREATE" in sql_text else SQLConfig()
    statement = SQL(sql_text, _config=config)
    statement._is_script = is_script
    statement._is_many = is_many

    with patch.object(BigQueryDriver, expected_method, return_value={"rows_affected": 0}) as mock_method:
        driver._execute_statement(statement)
        mock_method.assert_called_once()


def test_execute_select_statement(driver: BigQueryDriver, mock_connection: MagicMock) -> None:
    """Test executing a SELECT statement."""
    # Set up mock job with schema
    mock_job = mock_connection.query.return_value
    mock_field = MagicMock()
    mock_field.name = "id"
    mock_job.schema = [mock_field]
    mock_job.statement_type = "SELECT"
    mock_job.result.return_value = iter([])

    statement = SQL("SELECT * FROM users")
    result = driver._execute_statement(statement)

    assert result == {"data": [], "column_names": ["id"], "rows_affected": 0}

    mock_connection.query.assert_called_once()


def test_execute_dml_statement(driver: BigQueryDriver, mock_connection: MagicMock) -> None:
    """Test executing a DML statement (INSERT/UPDATE/DELETE)."""
    mock_job = mock_connection.query.return_value
    mock_job.num_dml_affected_rows = 1
    mock_job.job_id = "test-job-123"
    mock_job.schema = None
    mock_job.state = "DONE"
    mock_job.errors = None
    mock_job.statement_type = "INSERT"  # This is the key - identify it as a DML statement

    statement = SQL("INSERT INTO users (name) VALUES (@name)", {"name": "Alice"})
    result = driver._execute_statement(statement)

    assert result == {"rows_affected": 1, "status_message": "OK - job_id: test-job-123"}

    mock_connection.query.assert_called_once()


# Parameter Style Handling Tests
@pytest.mark.parametrize(
    "sql_text,params,expected_placeholder",
    [
        ("SELECT * FROM users WHERE id = @user_id", {"user_id": 123}, "@"),
        ("SELECT * FROM users WHERE id = :user_id", {"user_id": 123}, "@"),  # Should be converted
        ("SELECT * FROM users WHERE id = ?", [123], "@"),  # Should be converted
    ],
    ids=["named_at", "named_colon_converted", "qmark_converted"],
)
def test_parameter_style_handling(
    driver: BigQueryDriver, mock_connection: MagicMock, sql_text: str, params: Any, expected_placeholder: str
) -> None:
    """Test parameter style detection and conversion."""
    statement = SQL(sql_text, params, _config=driver.config)

    # Mock the query to return empty result
    mock_job = mock_connection.query.return_value
    mock_job.result.return_value = iter([])
    mock_job.schema = []
    mock_job.num_dml_affected_rows = None

    driver._execute_statement(statement)

    # Check that query was called with SQL containing expected parameter style
    mock_connection.query.assert_called_once()
    query_sql = mock_connection.query.call_args[0][0]

    # BigQuery should always use @ style
    assert expected_placeholder in query_sql


# Execute Many Tests
def test_execute_many(driver: BigQueryDriver, mock_connection: MagicMock) -> None:
    """Test executing a statement multiple times."""
    mock_job = mock_connection.query.return_value
    mock_job.num_dml_affected_rows = 3
    mock_job.job_id = "batch-job-123"

    sql = "INSERT INTO users (name) VALUES (@name)"
    params = [{"name": "Alice"}, {"name": "Bob"}, {"name": "Charlie"}]

    result = driver._execute_many(sql, params)

    assert result == {"rows_affected": 3, "status_message": "OK - executed batch job batch-job-123"}

    mock_connection.query.assert_called_once()


def test_execute_many_with_non_dict_parameters(driver: BigQueryDriver, mock_connection: MagicMock) -> None:
    """Test execute_many handles non-dict parameters by converting them."""
    sql = "INSERT INTO users VALUES (@param_0)"
    params = [["Alice"], ["Bob"]]  # List parameters will be converted to dicts

    # Mock the query job
    mock_job = mock_connection.query.return_value
    mock_job.job_id = "batch-job-123"
    mock_job.result.return_value = None
    mock_job.num_dml_affected_rows = 2

    result = driver._execute_many(sql, params)

    # Verify the script was created with converted parameters
    assert mock_connection.query.called
    executed_sql = mock_connection.query.call_args[0][0]
    # Should create a multi-statement script with remapped parameters
    assert "INSERT INTO users VALUES (@p_0)" in executed_sql
    assert "INSERT INTO users VALUES (@p_1)" in executed_sql

    # Verify result
    assert result["rows_affected"] == 2
    assert "batch-job-123" in result["status_message"]


# Execute Script Tests
def test_execute_script(driver: BigQueryDriver, mock_connection: MagicMock) -> None:
    """Test executing a SQL script."""
    mock_job = mock_connection.query.return_value
    mock_job.job_id = "script-job-123"

    script = """
    CREATE TABLE test (id INTEGER);
    INSERT INTO test VALUES (1);
    INSERT INTO test VALUES (2);
    """

    result = driver._execute_script(script)

    assert result == {"statements_executed": 3, "status_message": "SCRIPT EXECUTED"}

    # Should be called once for each non-empty statement
    assert mock_connection.query.call_count == 3


# Result Wrapping Tests
def test_wrap_select_result(driver: BigQueryDriver) -> None:
    """Test wrapping SELECT results."""
    statement = SQL("SELECT * FROM users")
    result = {
        "data": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
        "column_names": ["id", "name"],
        "rows_affected": 2,
    }

    wrapped = driver._wrap_select_result(statement, result)  # type: ignore[arg-type]

    assert isinstance(wrapped, SQLResult)
    assert wrapped.statement is statement
    assert len(wrapped.data) == 2
    assert wrapped.column_names == ["id", "name"]
    assert wrapped.rows_affected == 2
    assert wrapped.operation_type == "SELECT"


def test_wrap_select_result_with_schema(driver: BigQueryDriver) -> None:
    """Test wrapping SELECT results with schema type."""
    from dataclasses import dataclass

    @dataclass
    class User:
        id: int
        name: str

    statement = SQL("SELECT * FROM users")
    result = {
        "data": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
        "column_names": ["id", "name"],
        "rows_affected": 2,
    }

    wrapped = driver._wrap_select_result(statement, result, schema_type=User)  # type: ignore[arg-type]

    assert isinstance(wrapped, SQLResult)
    assert all(isinstance(item, User) for item in wrapped.data)
    assert wrapped.data[0].id == 1
    assert wrapped.data[0].name == "Alice"


def test_wrap_execute_result_dml(driver: BigQueryDriver) -> None:
    """Test wrapping DML results."""
    statement = SQL("INSERT INTO users VALUES (@id)")

    result = {"rows_affected": 1, "status_message": "OK - job_id: test-job"}

    wrapped = driver._wrap_execute_result(statement, result)  # type: ignore[arg-type]

    assert isinstance(wrapped, SQLResult)
    assert wrapped.data == []
    assert wrapped.rows_affected == 1
    assert wrapped.operation_type == "INSERT"
    assert wrapped.metadata["status_message"] == "OK - job_id: test-job"


def test_wrap_execute_result_script(driver: BigQueryDriver) -> None:
    """Test wrapping script results."""
    from sqlspec.statement.sql import SQLConfig

    config = SQLConfig(enable_validation=False)  # Allow DDL
    statement = SQL("CREATE TABLE test; INSERT INTO test;", _config=config)

    result = {"statements_executed": 2, "status_message": "SCRIPT EXECUTED"}

    wrapped = driver._wrap_execute_result(statement, result)  # type: ignore[arg-type]

    assert isinstance(wrapped, SQLResult)
    assert wrapped.data == []
    assert wrapped.rows_affected == 0
    assert wrapped.operation_type == "SCRIPT"
    assert wrapped.metadata["status_message"] == "SCRIPT EXECUTED"
    assert wrapped.metadata["statements_executed"] == 2


# Connection Tests
def test_connection_method(driver: BigQueryDriver, mock_connection: MagicMock) -> None:
    """Test _connection method."""
    # Test default connection return
    assert driver._connection() is mock_connection

    # Test connection override
    override_connection = MagicMock()
    assert driver._connection(override_connection) is override_connection


# Storage Mixin Tests
def test_storage_methods_available(driver: BigQueryDriver) -> None:
    """Test that driver has all storage methods from SyncStorageMixin."""
    storage_methods = ["fetch_arrow_table", "ingest_arrow_table", "export_to_storage", "import_from_storage"]

    for method in storage_methods:
        assert hasattr(driver, method)
        assert callable(getattr(driver, method))


def test_translator_mixin_integration(driver: BigQueryDriver) -> None:
    """Test SQLTranslatorMixin integration."""
    assert hasattr(driver, "returns_rows")

    # Test with SELECT statement
    select_stmt = SQL("SELECT * FROM users")
    assert driver.returns_rows(select_stmt.expression) is True

    # Test with INSERT statement
    insert_stmt = SQL("INSERT INTO users VALUES (1, 'test')")
    assert driver.returns_rows(insert_stmt.expression) is False


# Job Configuration Tests
def test_job_config_inheritance() -> None:
    """Test BigQuery driver inherits job config from connection."""
    from google.cloud.bigquery import QueryJobConfig

    mock_conn = MagicMock()
    default_job_config = QueryJobConfig()
    default_job_config.use_query_cache = True
    mock_conn.default_query_job_config = default_job_config

    driver = BigQueryDriver(connection=mock_conn)

    assert driver._default_query_job_config is default_job_config


def test_job_config_precedence() -> None:
    """Test BigQuery driver job config override takes precedence."""
    from google.cloud.bigquery import QueryJobConfig

    mock_conn = MagicMock()
    connection_job_config = QueryJobConfig()
    connection_job_config.use_query_cache = True
    mock_conn.default_query_job_config = connection_job_config

    # Override with driver-specific job config
    driver_job_config = QueryJobConfig()
    driver_job_config.dry_run = True

    driver = BigQueryDriver(connection=mock_conn, default_query_job_config=driver_job_config)

    assert driver._default_query_job_config is driver_job_config


# Job Callback Tests
def test_run_query_job_with_callbacks(driver: BigQueryDriver, mock_connection: MagicMock) -> None:
    """Test BigQuery job execution with callbacks."""
    job_start_callback = MagicMock()
    job_complete_callback = MagicMock()
    driver.on_job_start = job_start_callback
    driver.on_job_complete = job_complete_callback

    mock_job = mock_connection.query.return_value
    mock_job.job_id = "test-job-123"

    sql_str = "SELECT * FROM users"
    result = driver._run_query_job(sql_str, [])

    assert result is mock_job
    job_start_callback.assert_called_once()
    job_complete_callback.assert_called_once()
    # Check that the callback was called with any job ID and the mock job
    assert job_complete_callback.call_args[0][1] is mock_job


def test_run_query_job_callback_exceptions(driver: BigQueryDriver, mock_connection: MagicMock) -> None:
    """Test BigQuery job execution handles callback exceptions gracefully."""
    driver.on_job_start = MagicMock(side_effect=Exception("Start callback error"))
    driver.on_job_complete = MagicMock(side_effect=Exception("Complete callback error"))

    mock_job = mock_connection.query.return_value
    mock_job.job_id = "test-job-123"

    # Should not raise exception even if callbacks fail
    result = driver._run_query_job("SELECT 1", [])
    assert result is mock_job


# Edge Cases
def test_execute_with_no_parameters(driver: BigQueryDriver, mock_connection: MagicMock) -> None:
    """Test executing statement with no parameters."""
    mock_job = mock_connection.query.return_value
    mock_job.num_dml_affected_rows = 0
    mock_job.job_id = "test-job"
    mock_job.schema = None
    mock_job.state = "DONE"
    mock_job.errors = None

    from sqlspec.statement.sql import SQLConfig

    config = SQLConfig(enable_validation=False)  # Allow DDL
    statement = SQL("CREATE TABLE test (id INTEGER)", _config=config)
    driver._execute_statement(statement)

    mock_connection.query.assert_called_once()


def test_execute_select_with_empty_result(driver: BigQueryDriver, mock_connection: MagicMock) -> None:
    """Test SELECT with empty result set."""
    mock_job = mock_connection.query.return_value
    mock_field = MagicMock()
    mock_field.name = "id"
    mock_job.schema = [mock_field]
    mock_job.statement_type = "SELECT"
    mock_job.result.return_value = iter([])

    statement = SQL("SELECT * FROM users WHERE 1=0")
    result = driver._execute_statement(statement)

    assert result == {"data": [], "column_names": ["id"], "rows_affected": 0}


def test_rows_to_results_conversion(driver: BigQueryDriver) -> None:
    """Test BigQuery rows to results conversion."""
    # Create mock BigQuery rows
    mock_row1 = MagicMock()
    mock_row1.__iter__ = MagicMock(return_value=iter([("id", 1), ("name", "John")]))

    mock_row2 = MagicMock()
    mock_row2.__iter__ = MagicMock(return_value=iter([("id", 2), ("name", "Jane")]))

    # Mock dict() constructor for BigQuery rows
    with patch("builtins.dict") as mock_dict:
        mock_dict.side_effect = [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]

        rows_iterator = iter([mock_row1, mock_row2])
        result = driver._rows_to_results(rows_iterator)

        assert result == [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]


def test_connection_override(driver: BigQueryDriver) -> None:
    """Test BigQuery driver with connection override."""
    override_connection = MagicMock()
    override_connection.query.return_value = MagicMock()

    statement = SQL("SELECT 1")

    # Should use override connection instead of driver's connection
    driver._execute_statement(statement, connection=override_connection)

    override_connection.query.assert_called_once()
    # Original connection should not be called
    driver.connection.query.assert_not_called()  # pyright: ignore


def test_fetch_arrow_table_native(driver: BigQueryDriver, mock_connection: MagicMock) -> None:
    """Test BigQuery native Arrow table fetch."""
    import pyarrow as pa

    from sqlspec.statement.result import ArrowResult

    # Setup mock arrow table for native fetch
    mock_arrow_table = pa.table({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
    mock_job = mock_connection.query.return_value
    mock_job.to_arrow.return_value = mock_arrow_table
    mock_job.result.return_value = None

    statement = SQL("SELECT * FROM users")
    result = driver.fetch_arrow_table(statement)

    assert isinstance(result, ArrowResult)
    assert result.data is mock_arrow_table
    assert result.data.num_rows == 3
    assert result.data.column_names == ["id", "name"]

    # Verify native to_arrow was called
    mock_job.to_arrow.assert_called_once()
    # Verify query job was waited on
    mock_job.result.assert_called_once()
