"""Storage tests for DuckDB driver."""

from typing import Any
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from sqlspec.adapters.duckdb import DuckDBDriver
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.result import ArrowResult
from sqlspec.statement.sql import SQL, SQLConfig
from tests.unit.test_adapters.storage_test_helpers import create_mock_arrow_table


@pytest.fixture
def mock_duckdb_connection() -> MagicMock:
    """Create a mock DuckDB connection."""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.__exit__.return_value = None
    mock_cursor.execute.return_value = mock_cursor
    mock_cursor.executemany.return_value = mock_cursor
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = None
    mock_cursor.description = [(col,) for col in ["id", "name"]]
    mock_cursor.rowcount = 0
    # DuckDB specific methods
    mock_connection.register = MagicMock()
    mock_connection.unregister = MagicMock()
    return mock_connection


@pytest.fixture
def duckdb_driver(mock_duckdb_connection: MagicMock) -> DuckDBDriver:
    """Create a DuckDB driver with mocked connection."""
    config = SQLConfig(strict_mode=False)
    instrumentation_config = InstrumentationConfig()
    return DuckDBDriver(connection=mock_duckdb_connection, config=config, instrumentation_config=instrumentation_config)


class TestDuckDBStorageOperations:
    """Test storage operations for DuckDB driver."""

    def test_fetch_arrow_table(self, duckdb_driver: DuckDBDriver, mock_duckdb_connection: MagicMock) -> None:
        """Test fetch_arrow_table method."""
        # DuckDB uses native cursor.arrow() method
        mock_arrow_table = create_mock_arrow_table()
        mock_cursor = MagicMock()
        mock_cursor.arrow.return_value = mock_arrow_table
        mock_duckdb_connection.execute.return_value = mock_cursor

        # Test fetch_arrow_table
        statement = SQL("SELECT * FROM users")
        result = duckdb_driver.fetch_arrow_table(statement)

        # Verify result
        assert isinstance(result, ArrowResult)
        assert result.num_rows == 2
        assert "id" in result.column_names
        assert "name" in result.column_names

        # Verify execute was called on the connection
        mock_duckdb_connection.execute.assert_called_once()
        # Verify arrow() was called on the cursor
        mock_cursor.arrow.assert_called_once()

    def test_ingest_arrow_table(self, duckdb_driver: DuckDBDriver, mock_duckdb_connection: MagicMock) -> None:
        """Test ingest_arrow_table method using native DuckDB registration."""
        # Create test Arrow table
        table = create_mock_arrow_table()
        assert table.num_rows == 2, f"Expected table to have 2 rows, but got {table.num_rows}"

        # Mock execute to return a result for the CREATE TABLE AS
        mock_result = MagicMock()
        mock_result.rows_affected = 2
        duckdb_driver.execute = MagicMock(return_value=mock_result)

        # Test ingest (DuckDB should use native registration)
        result = duckdb_driver.ingest_arrow_table(table, "test_table", mode="create")

        # Verify result
        assert result == 2

        # Verify register was called
        mock_duckdb_connection.register.assert_called_once()
        register_args = mock_duckdb_connection.register.call_args[0]
        assert isinstance(register_args[0], str)  # Temp table name
        assert register_args[1] is table  # The Arrow table

        # Verify execute was called with CREATE TABLE ... AS SELECT
        duckdb_driver.execute.assert_called_once()
        executed_sql = duckdb_driver.execute.call_args[0][0]
        assert "CREATE TABLE" in executed_sql.to_sql()
        assert "test_table" in executed_sql.to_sql()

        # Verify unregister was called
        mock_duckdb_connection.unregister.assert_called_once()

    def test_export_to_storage_parquet(
        self, duckdb_driver: DuckDBDriver, mock_duckdb_connection: MagicMock, tmp_path: Any
    ) -> None:
        """Test export_to_storage with Parquet format."""
        # DuckDB uses native COPY TO for Parquet export
        # Mock the connection execute for COPY TO
        mock_result = MagicMock()
        # COPY TO returns number of rows exported
        mock_result.fetchone.return_value = (2,)
        mock_duckdb_connection.execute.return_value = mock_result

        # Test export
        output_path = tmp_path / "output.parquet"
        result = duckdb_driver.export_to_storage("SELECT * FROM users", str(output_path))

        # Verify COPY TO was executed
        mock_duckdb_connection.execute.assert_called()
        copy_sql = mock_duckdb_connection.execute.call_args[0][0]
        assert "COPY (" in copy_sql
        assert "SELECT * FROM users" in copy_sql
        assert str(output_path) in copy_sql
        assert "FORMAT PARQUET" in copy_sql

        # Should return row count
        assert result == 2

    def test_export_to_storage_csv(
        self, duckdb_driver: DuckDBDriver, mock_duckdb_connection: MagicMock, tmp_path: Any
    ) -> None:
        """Test export_to_storage with CSV format."""
        # DuckDB uses native COPY TO for CSV export
        # Mock the connection execute for COPY TO
        mock_result = MagicMock()
        # COPY TO returns number of rows exported
        mock_result.fetchone.return_value = (2,)
        mock_duckdb_connection.execute.return_value = mock_result

        # Test export
        output_path = tmp_path / "output.csv"
        result = duckdb_driver.export_to_storage("SELECT * FROM users", str(output_path), format="csv")

        # Verify COPY TO was executed
        mock_duckdb_connection.execute.assert_called()
        copy_sql = mock_duckdb_connection.execute.call_args[0][0]
        assert "COPY (" in copy_sql
        assert "SELECT * FROM users" in copy_sql
        assert str(output_path) in copy_sql
        assert "FORMAT CSV" in copy_sql
        assert "HEADER" in copy_sql  # DuckDB adds HEADER by default for CSV

        # Should return row count
        assert result == 2

    def test_import_from_storage_parquet(
        self, duckdb_driver: DuckDBDriver, mock_duckdb_connection: MagicMock, tmp_path: Any
    ) -> None:
        """Test import_from_storage with Parquet format."""
        # DuckDB uses native read_parquet for import
        # Mock the connection execute for CREATE TABLE AS
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (2,)  # Number of rows imported
        mock_duckdb_connection.execute.return_value = mock_result

        # Test import
        input_path = tmp_path / "input.parquet"
        result = duckdb_driver.import_from_storage(str(input_path), "test_table")

        # Verify CREATE TABLE AS was executed
        mock_duckdb_connection.execute.assert_called()
        import_sql = mock_duckdb_connection.execute.call_args[0][0]
        assert "CREATE TABLE test_table AS" in import_sql
        assert f"read_parquet('{input_path}')" in import_sql

        # Should return row count
        assert result == 2

    def test_export_to_storage_json(
        self, duckdb_driver: DuckDBDriver, mock_duckdb_connection: MagicMock, tmp_path: Any
    ) -> None:
        """Test export_to_storage with JSON format."""
        # DuckDB uses native COPY TO for JSON export
        # Mock the connection execute for COPY TO
        mock_result = MagicMock()
        # COPY TO returns number of rows exported
        mock_result.fetchone.return_value = (2,)
        mock_duckdb_connection.execute.return_value = mock_result

        # Test export
        output_path = tmp_path / "output.json"
        result = duckdb_driver.export_to_storage("SELECT * FROM users", str(output_path), format="json")

        # Verify COPY TO was executed
        mock_duckdb_connection.execute.assert_called()
        copy_sql = mock_duckdb_connection.execute.call_args[0][0]
        assert "COPY (" in copy_sql
        assert "SELECT * FROM users" in copy_sql
        assert str(output_path) in copy_sql
        assert "FORMAT JSON" in copy_sql

        # Should return row count
        assert result == 2

    def test_import_from_storage_csv(
        self, duckdb_driver: DuckDBDriver, mock_duckdb_connection: MagicMock, tmp_path: Any
    ) -> None:
        """Test import_from_storage with CSV format."""
        # DuckDB uses native read_csv_auto for import
        # Mock the connection execute for CREATE TABLE AS
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (2,)  # Number of rows imported
        mock_duckdb_connection.execute.return_value = mock_result

        # Test import
        input_path = tmp_path / "input.csv"
        result = duckdb_driver.import_from_storage(str(input_path), "test_table", format="csv")

        # Verify CREATE TABLE AS was executed
        mock_duckdb_connection.execute.assert_called()
        import_sql = mock_duckdb_connection.execute.call_args[0][0]
        assert "CREATE TABLE test_table AS" in import_sql
        assert f"read_csv_auto('{input_path}')" in import_sql

        # Should return row count
        assert result == 2

    def test_fetch_arrow_table_with_filters(
        self, duckdb_driver: DuckDBDriver, mock_duckdb_connection: MagicMock
    ) -> None:
        """Test fetch_arrow_table with filters."""
        # DuckDB uses native cursor.arrow() method
        mock_arrow_table = create_mock_arrow_table()
        mock_cursor = MagicMock()
        mock_cursor.arrow.return_value = mock_arrow_table
        mock_duckdb_connection.execute.return_value = mock_cursor

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

        # Test with filter - note that filters come after parameters
        statement = SQL("SELECT * FROM users")
        duckdb_driver.fetch_arrow_table(statement, None, active_filter)  # type: ignore[arg-type]

        # Verify filter was called
        assert filter_called, "Filter was not called"
        assert filtered_sql == "SELECT * FROM users WHERE active = TRUE"

        # Verify connection execute was called with filtered SQL
        mock_duckdb_connection.execute.assert_called()
        executed_sql = mock_duckdb_connection.execute.call_args[0][0]
        assert "WHERE active = TRUE" in executed_sql, f"Expected filtered SQL but got: {executed_sql}"

    def test_storage_operations_with_connection_override(self, duckdb_driver: DuckDBDriver) -> None:
        """Test storage operations with connection override."""
        # Create override connection
        override_conn = MagicMock()
        mock_arrow_table = create_mock_arrow_table()
        mock_cursor = MagicMock()
        mock_cursor.arrow.return_value = mock_arrow_table
        override_conn.execute.return_value = mock_cursor

        # Test fetch_arrow_table with connection override
        statement = SQL("SELECT * FROM users")
        result = duckdb_driver.fetch_arrow_table(statement, connection=override_conn)

        # Verify override connection was used
        override_conn.execute.assert_called_once()
        # The default connection should not have been used
        assert result.num_rows == 2

    def test_ingest_arrow_table_with_mode_replace(
        self, duckdb_driver: DuckDBDriver, mock_duckdb_connection: MagicMock
    ) -> None:
        """Test ingest_arrow_table with replace mode using native DuckDB registration."""
        # Create test Arrow table
        table = create_mock_arrow_table()

        # Mock execute to return a result for CREATE OR REPLACE TABLE
        mock_result = MagicMock()
        mock_result.rows_affected = 2
        duckdb_driver.execute = MagicMock(return_value=mock_result)

        # Test ingest with replace mode (DuckDB should use native registration)
        result = duckdb_driver.ingest_arrow_table(table, "test_table", mode="replace")

        # Verify result
        assert result == 2

        # Verify register was called
        mock_duckdb_connection.register.assert_called_once()
        register_args = mock_duckdb_connection.register.call_args[0]
        assert isinstance(register_args[0], str)  # Temp table name
        assert register_args[1] is table  # The Arrow table

        # Verify execute was called with CREATE OR REPLACE TABLE ... AS SELECT
        duckdb_driver.execute.assert_called_once()
        executed_sql = duckdb_driver.execute.call_args[0][0]
        sql_text = executed_sql.to_sql()
        assert "CREATE OR REPLACE TABLE" in sql_text or "CREATE TABLE" in sql_text
        assert "test_table" in sql_text

        # Verify unregister was called
        mock_duckdb_connection.unregister.assert_called_once()

    def test_arrow_table_conversion_with_nulls(
        self, duckdb_driver: DuckDBDriver, mock_duckdb_connection: MagicMock
    ) -> None:
        """Test Arrow table conversion with null values."""
        # Create Arrow table with nulls

        data = {"id": [1, 2], "name": ["test1", None], "email": [None, "test@example.com"]}
        arrow_table = pa.table(data)

        # DuckDB uses native cursor.arrow() method
        mock_cursor = MagicMock()
        mock_cursor.arrow.return_value = arrow_table
        mock_duckdb_connection.execute.return_value = mock_cursor

        # Test fetch_arrow_table
        statement = SQL("SELECT * FROM users")
        result = duckdb_driver.fetch_arrow_table(statement)

        # Verify result handles nulls properly
        assert isinstance(result, ArrowResult)
        assert result.num_rows == 2
        assert set(result.column_names) == {"id", "name", "email"}

        # Verify the data contains nulls using PyArrow
        assert arrow_table.column("name")[1].as_py() is None
        assert arrow_table.column("email")[0].as_py() is None

    def test_ingest_arrow_table_native_path(
        self, duckdb_driver: DuckDBDriver, mock_duckdb_connection: MagicMock
    ) -> None:
        """Test ingest_arrow_table using DuckDB's native registration."""
        # Create test Arrow table
        table = create_mock_arrow_table()

        # Mock execute to return a result
        mock_result = MagicMock()
        mock_result.rows_affected = None  # DuckDB might not return rows_affected
        duckdb_driver.execute = MagicMock(return_value=mock_result)

        # Test ingest with create mode
        result = duckdb_driver.ingest_arrow_table(table, "test_table", mode="create")

        # Verify register was called
        mock_duckdb_connection.register.assert_called_once()
        register_args = mock_duckdb_connection.register.call_args[0]
        assert isinstance(register_args[0], str)  # Temp table name
        assert register_args[1] is table  # The Arrow table

        # Verify execute was called with CREATE TABLE ... AS SELECT
        duckdb_driver.execute.assert_called_once()
        executed_sql = duckdb_driver.execute.call_args[0][0]
        assert "CREATE TABLE" in executed_sql.to_sql()
        assert "test_table" in executed_sql.to_sql()

        # Verify unregister was called
        mock_duckdb_connection.unregister.assert_called_once()

        # Should return row count from the table
        assert result == 2  # From table.num_rows
