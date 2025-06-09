"""Storage tests for BigQuery driver."""

from typing import Any
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from sqlspec.adapters.bigquery import BigQueryDriver
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.result import ArrowResult
from sqlspec.statement.sql import SQL, SQLConfig
from tests.unit.test_adapters.storage_test_helpers import (
    create_mock_arrow_result,
    create_mock_arrow_table,
    create_mock_sql_result,
)


@pytest.fixture
def mock_bigquery_client() -> MagicMock:
    """Create a mock BigQuery client."""
    mock_client = MagicMock()
    mock_query_job = MagicMock()
    mock_client.query.return_value = mock_query_job
    mock_query_job.result.return_value = []
    mock_query_job.to_dataframe.return_value = MagicMock()
    mock_query_job.to_arrow.return_value = create_mock_arrow_table()
    mock_query_job.num_dml_affected_rows = 0
    mock_query_job.total_bytes_processed = 1000
    mock_query_job.slot_millis = 500
    return mock_client


@pytest.fixture
def bigquery_driver(mock_bigquery_client: MagicMock) -> BigQueryDriver:
    """Create a BigQuery driver with mocked client."""
    config = SQLConfig(strict_mode=False)
    instrumentation_config = InstrumentationConfig()
    return BigQueryDriver(
        connection=mock_bigquery_client,
        config=config,
        instrumentation_config=instrumentation_config,
    )


class TestBigQueryStorageOperations:
    """Test storage operations for BigQuery driver."""

    def test_fetch_arrow_table(self, bigquery_driver: BigQueryDriver, mock_bigquery_client: MagicMock) -> None:
        """Test fetch_arrow_table method using BigQuery native Arrow support."""
        # BigQuery has native Arrow support via QueryJob.to_arrow()
        mock_arrow_table = create_mock_arrow_table()
        mock_query_job = MagicMock()
        mock_query_job.to_arrow.return_value = mock_arrow_table
        mock_query_job.result.return_value = None  # Job completion

        # Mock the _execute method to return a QueryJob directly
        # Set the spec to make isinstance() work
        from google.cloud.bigquery.job.query import QueryJob

        mock_query_job.__class__ = QueryJob
        bigquery_driver._execute = MagicMock(return_value=mock_query_job)

        # Test fetch_arrow_table
        statement = SQL("SELECT * FROM users")
        result = bigquery_driver.fetch_arrow_table(statement)

        # Verify result
        assert isinstance(result, ArrowResult)
        assert result.num_rows() == 2
        assert "id" in result.column_names
        assert "name" in result.column_names

        # Verify _execute was called
        bigquery_driver._execute.assert_called_once()
        # Verify to_arrow was called
        mock_query_job.to_arrow.assert_called_once()

    def test_ingest_arrow_table(self, bigquery_driver: BigQueryDriver, mock_bigquery_client: MagicMock) -> None:
        """Test ingest_arrow_table method using fallback."""
        # Create test Arrow table
        table = create_mock_arrow_table()

        # Ensure BigQuery client doesn't have register method to force fallback
        del mock_bigquery_client.register

        # Mock execute_many result
        mock_result = MagicMock()
        mock_result.rows_affected = 2
        bigquery_driver.execute_many = MagicMock(return_value=mock_result)

        # Test ingest
        result = bigquery_driver.ingest_arrow_table(table, "test_table")

        # Verify result
        assert result == 2

        # Verify execute_many was called with INSERT statement
        bigquery_driver.execute_many.assert_called_once()
        call_args = bigquery_driver.execute_many.call_args
        sql_obj = call_args[0][0]
        assert isinstance(sql_obj, SQL)
        assert "INSERT INTO test_table" in sql_obj.to_sql()
        assert sql_obj.is_many is True

        # Check parameters were converted from Arrow table
        assert sql_obj.parameters == [{"id": 1, "name": "name_1"}, {"id": 2, "name": "name_2"}]

    def test_export_to_storage_parquet_gcs(self, bigquery_driver: BigQueryDriver, tmp_path: Any) -> None:
        """Test export_to_storage with Parquet format to GCS."""
        # BigQuery has native export to GCS for Parquet format
        gcs_path = "gs://bucket/output.parquet"

        # Mock the native export method
        bigquery_driver._export_native = MagicMock(return_value=2)

        # Test export to GCS (should use native path)
        result = bigquery_driver.export_to_storage("SELECT * FROM users", gcs_path)

        # Verify native export was called
        bigquery_driver._export_native.assert_called_once_with("SELECT * FROM users", gcs_path, "parquet")

        # Should return row count
        assert result == 2

    def test_export_to_storage_parquet_local(self, bigquery_driver: BigQueryDriver, tmp_path: Any) -> None:
        """Test export_to_storage with Parquet format to local file."""
        # Mock fetch_arrow_table
        mock_arrow_result = create_mock_arrow_result()
        bigquery_driver.fetch_arrow_table = MagicMock(return_value=mock_arrow_result)

        # Mock storage backend
        mock_backend = MagicMock()
        mock_backend.write_arrow = MagicMock()

        # Patch the _resolve_backend_and_path method to return our mock
        bigquery_driver._resolve_backend_and_path = MagicMock(
            return_value=(mock_backend, str(tmp_path / "output.parquet"))
        )

        # Test export to local file (should use backend path)
        output_path = tmp_path / "output.parquet"
        result = bigquery_driver.export_to_storage("SELECT * FROM users", str(output_path))

        # Verify backend was called
        mock_backend.write_arrow.assert_called_once()
        call_args = mock_backend.write_arrow.call_args
        assert call_args[0][0] == str(output_path)
        assert isinstance(call_args[0][1], pa.Table)

        # Should return row count
        assert result == 2

    def test_export_to_storage_csv(self, bigquery_driver: BigQueryDriver, tmp_path: Any) -> None:
        """Test export_to_storage with CSV format."""
        # Mock execute
        mock_result = create_mock_sql_result()
        bigquery_driver.execute = MagicMock(return_value=mock_result)

        # Mock _export_via_backend since CSV goes through that path
        bigquery_driver._export_via_backend = MagicMock(return_value=2)

        # Test export
        output_path = tmp_path / "output.csv"
        result = bigquery_driver.export_to_storage("SELECT * FROM users", str(output_path), format="csv")

        # Verify _export_via_backend was called
        bigquery_driver._export_via_backend.assert_called_once()

        # Should return row count
        assert result == 2

    def test_export_to_storage_json(self, bigquery_driver: BigQueryDriver, tmp_path: Any) -> None:
        """Test export_to_storage with JSON format."""
        # Mock execute
        mock_result = create_mock_sql_result()
        bigquery_driver.execute = MagicMock(return_value=mock_result)

        # Mock _export_via_backend since JSON goes through that path
        bigquery_driver._export_via_backend = MagicMock(return_value=2)

        # Test export
        output_path = tmp_path / "output.json"
        result = bigquery_driver.export_to_storage("SELECT * FROM users", str(output_path), format="json")

        # Verify _export_via_backend was called
        bigquery_driver._export_via_backend.assert_called_once()

        # Should return row count
        assert result == 2

    def test_import_from_storage_parquet(self, bigquery_driver: BigQueryDriver, tmp_path: Any) -> None:
        """Test import_from_storage with Parquet format."""
        # Create test Arrow table
        table = create_mock_arrow_table()

        # Mock storage backend
        mock_backend = MagicMock()
        mock_backend.read_arrow = MagicMock(return_value=table)

        # Mock ingest_arrow_table
        bigquery_driver.ingest_arrow_table = MagicMock(return_value=2)

        # Patch the _resolve_backend_and_path method to return our mock
        bigquery_driver._resolve_backend_and_path = MagicMock(
            return_value=(mock_backend, str(tmp_path / "input.parquet"))
        )

        # Test import
        input_path = tmp_path / "input.parquet"
        result = bigquery_driver.import_from_storage(str(input_path), "test_table")

        # Verify backend was called
        mock_backend.read_arrow.assert_called_once_with(str(input_path))

        # Verify ingest was called (default mode is "create")
        bigquery_driver.ingest_arrow_table.assert_called_once_with(table, "test_table", mode="create")

        # Should return row count
        assert result == 2

    def test_import_from_storage_csv(self, bigquery_driver: BigQueryDriver, tmp_path: Any) -> None:
        """Test import_from_storage with CSV format."""
        # Mock _import_via_backend since CSV goes through that path
        bigquery_driver._import_via_backend = MagicMock(return_value=2)

        # Test import
        input_path = tmp_path / "input.csv"
        result = bigquery_driver.import_from_storage(str(input_path), "test_table", format="csv")

        # Verify _import_via_backend was called
        bigquery_driver._import_via_backend.assert_called_once()

        # Should return row count
        assert result == 2

    def test_fetch_arrow_table_with_filters(
        self, bigquery_driver: BigQueryDriver, mock_bigquery_client: MagicMock
    ) -> None:
        """Test fetch_arrow_table with filters."""
        # Mock BigQuery query job
        mock_arrow_table = create_mock_arrow_table()
        mock_query_job = MagicMock()
        mock_query_job.to_arrow.return_value = mock_arrow_table
        mock_query_job.result.return_value = None  # Job completion

        # Track what gets passed to the filter
        filter_called = False
        filtered_sql = None

        # Create a custom filter function
        def active_filter(statement: SQL) -> SQL:
            """Filter to add WHERE active = TRUE clause."""
            nonlocal filter_called, filtered_sql
            filter_called = True
            # The statement object might have the SQL as a property
            new_sql = statement.to_sql() + " WHERE active = TRUE"
            filtered_sql = new_sql
            return SQL(new_sql, parameters=statement.parameters, config=statement._config)

        # Mock the _execute method to return a QueryJob directly
        bigquery_driver._execute = MagicMock(return_value=mock_query_job)

        # Test with filter - note that filters come after parameters
        statement = SQL("SELECT * FROM users")
        bigquery_driver.fetch_arrow_table(statement, None, active_filter)

        # Verify filter was called
        assert filter_called, "Filter was not called"
        assert filtered_sql == "SELECT * FROM users WHERE active = TRUE"

        # Verify _execute was called with filtered SQL
        # BigQuery calls _execute twice due to internal implementation
        assert bigquery_driver._execute.call_count >= 1
        call_args = bigquery_driver._execute.call_args
        executed_sql = call_args[0][0]
        assert "WHERE active = TRUE" in executed_sql, f"Expected filtered SQL but got: {executed_sql}"

    def test_storage_operations_with_connection_override(self, bigquery_driver: BigQueryDriver) -> None:
        """Test storage operations with connection override."""
        # Create override client
        override_client = MagicMock()
        mock_arrow_table = create_mock_arrow_table()
        mock_query_job = MagicMock()
        mock_query_job.to_arrow.return_value = mock_arrow_table
        mock_query_job.result.return_value = None  # Job completion

        # Mock the _execute method to return a QueryJob directly
        # Set the spec to make isinstance() work
        from google.cloud.bigquery.job.query import QueryJob

        mock_query_job.__class__ = QueryJob
        bigquery_driver._execute = MagicMock(return_value=mock_query_job)

        # Test fetch_arrow_table with client override
        statement = SQL("SELECT * FROM users")
        result = bigquery_driver.fetch_arrow_table(statement, connection=override_client)

        # Verify _execute was called with the override connection
        bigquery_driver._execute.assert_called_once()
        call_args = bigquery_driver._execute.call_args
        assert call_args[1].get("connection") is override_client

        # Verify result
        assert result.num_rows() == 2

    def test_ingest_arrow_table_with_mode_replace(
        self, bigquery_driver: BigQueryDriver, mock_bigquery_client: MagicMock
    ) -> None:
        """Test ingest_arrow_table with replace mode."""
        # Create test Arrow table
        table = create_mock_arrow_table()

        # Ensure BigQuery client doesn't have register method to force fallback
        del mock_bigquery_client.register

        # Mock execute for TRUNCATE
        bigquery_driver.execute = MagicMock(return_value=MagicMock())

        # Mock execute_many for INSERT
        bigquery_driver.execute_many = MagicMock(return_value=MagicMock(rows_affected=2))

        # Test ingest with replace mode
        result = bigquery_driver.ingest_arrow_table(table, "test_table", mode="replace")

        # Verify TRUNCATE was called
        bigquery_driver.execute.assert_called_once()
        truncate_call = bigquery_driver.execute.call_args
        truncate_sql = truncate_call[0][0]
        assert "TRUNCATE TABLE test_table" in truncate_sql.to_sql()

        # Verify INSERT was called
        bigquery_driver.execute_many.assert_called_once()

        # Should return row count
        assert result == 2

    def test_arrow_table_conversion_with_nulls(
        self, bigquery_driver: BigQueryDriver, mock_bigquery_client: MagicMock
    ) -> None:
        """Test Arrow table conversion with null values."""
        # Create Arrow table with nulls
        import pyarrow as pa

        data = {"id": [1, 2], "name": ["test1", None], "email": [None, "test@example.com"]}
        arrow_table = pa.table(data)

        # Mock BigQuery query job
        mock_query_job = MagicMock()
        mock_query_job.to_arrow.return_value = arrow_table
        mock_query_job.result.return_value = None  # Job completion

        # Mock the _execute method to return a QueryJob directly
        # Set the spec to make isinstance() work
        from google.cloud.bigquery.job.query import QueryJob

        mock_query_job.__class__ = QueryJob
        bigquery_driver._execute = MagicMock(return_value=mock_query_job)

        # Test fetch_arrow_table
        statement = SQL("SELECT * FROM users")
        result = bigquery_driver.fetch_arrow_table(statement)

        # Verify result handles nulls properly
        assert isinstance(result, ArrowResult)
        assert result.num_rows() == 2
        assert set(result.column_names) == {"id", "name", "email"}

        # Verify the data contains nulls using PyArrow
        assert arrow_table.column("name")[1].as_py() is None
        assert arrow_table.column("email")[0].as_py() is None
