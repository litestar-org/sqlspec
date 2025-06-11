"""Storage tests for ADBC driver."""

from typing import Any
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from sqlspec.adapters.adbc import AdbcDriver
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.result import ArrowResult
from sqlspec.statement.sql import SQL, SQLConfig
from tests.unit.test_adapters.storage_test_helpers import (
    create_mock_arrow_result,
    create_mock_arrow_table,
    create_mock_sql_result,
)


@pytest.fixture
def mock_adbc_connection() -> MagicMock:
    """Create a mock ADBC connection."""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.__exit__.return_value = None
    mock_cursor.execute.return_value = mock_cursor
    mock_cursor.executemany.return_value = mock_cursor
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = None
    # ADBC has native Arrow support
    mock_cursor.fetch_arrow_table.return_value = create_mock_arrow_table()
    return mock_connection


@pytest.fixture
def adbc_driver(mock_adbc_connection: MagicMock) -> AdbcDriver:
    """Create an ADBC driver with mocked connection."""
    config = SQLConfig(strict_mode=False)
    instrumentation_config = InstrumentationConfig()
    return AdbcDriver(
        connection=mock_adbc_connection,
        config=config,
        instrumentation_config=instrumentation_config,
    )


class TestADBCStorageOperations:
    """Test storage operations for ADBC driver."""

    def test_fetch_arrow_table_native(self, adbc_driver: AdbcDriver, mock_adbc_connection: MagicMock) -> None:
        """Test fetch_arrow_table method using native ADBC implementation."""
        # Mock the cursor's fetch_arrow_table method
        mock_arrow_table = create_mock_arrow_table()
        mock_cursor = mock_adbc_connection.cursor.return_value
        mock_cursor.fetch_arrow_table.return_value = mock_arrow_table

        # Test fetch_arrow_table
        statement = SQL("SELECT * FROM users")
        result = adbc_driver.fetch_arrow_table(statement)

        # Verify result
        assert isinstance(result, ArrowResult)
        assert result.num_rows == 2
        assert "id" in result.column_names
        assert "name" in result.column_names

        # Verify native method was called
        mock_cursor.execute.assert_called_once_with("SELECT * FROM users", [None])
        mock_cursor.fetch_arrow_table.assert_called_once()

    def test_ingest_arrow_table_native(self, adbc_driver: AdbcDriver, mock_adbc_connection: MagicMock) -> None:
        """Test ingest_arrow_table method using native ADBC implementation."""
        # Create test Arrow table
        table = create_mock_arrow_table()

        # Mock ADBC connection with native adbc_ingest method
        mock_adbc_connection.adbc_ingest.return_value = 2

        # Test ingest
        result = adbc_driver.ingest_arrow_table(table, "test_table")

        # Verify result
        assert result == 2

        # Verify native adbc_ingest was called (default mode is "append")
        mock_adbc_connection.adbc_ingest.assert_called_once_with("test_table", table, mode="append")

    def test_ingest_arrow_table_fallback(self, adbc_driver: AdbcDriver, mock_adbc_connection: MagicMock) -> None:
        """Test ingest_arrow_table method using fallback when native not available."""
        # Create test Arrow table
        table = create_mock_arrow_table()

        # Remove adbc_ingest method to force fallback
        del mock_adbc_connection.adbc_ingest

        # Mock execute_many result
        mock_result = MagicMock()
        mock_result.rows_affected = 2
        adbc_driver.execute_many = MagicMock(return_value=mock_result)

        # Test ingest
        result = adbc_driver.ingest_arrow_table(table, "test_table")

        # Verify result
        assert result == 2

        # Verify execute_many was called with INSERT statement
        adbc_driver.execute_many.assert_called_once()
        call_args = adbc_driver.execute_many.call_args
        sql_obj = call_args[0][0]
        assert isinstance(sql_obj, SQL)
        assert "INSERT INTO test_table" in sql_obj.to_sql()
        assert sql_obj.is_many is True

        # Check parameters were converted from Arrow table
        assert sql_obj.parameters == [{"id": 1, "name": "name_1"}, {"id": 2, "name": "name_2"}]

    def test_export_to_storage_parquet(self, adbc_driver: AdbcDriver, tmp_path: Any) -> None:
        """Test export_to_storage with Parquet format."""
        # Mock fetch_arrow_table
        mock_arrow_result = create_mock_arrow_result()
        adbc_driver.fetch_arrow_table = MagicMock(return_value=mock_arrow_result)

        # Mock storage backend
        mock_backend = MagicMock()
        mock_backend.write_arrow = MagicMock()

        # Patch the _resolve_backend_and_path method to return our mock
        adbc_driver._resolve_backend_and_path = MagicMock(return_value=(mock_backend, str(tmp_path / "output.parquet")))

        # Test export
        output_path = tmp_path / "output.parquet"
        result = adbc_driver.export_to_storage("SELECT * FROM users", str(output_path))

        # Verify backend was called
        mock_backend.write_arrow.assert_called_once()
        call_args = mock_backend.write_arrow.call_args
        assert call_args[0][0] == str(output_path)
        assert isinstance(call_args[0][1], pa.Table)

        # Should return row count
        assert result == 2

    def test_export_to_storage_csv(self, adbc_driver: AdbcDriver, tmp_path: Any) -> None:
        """Test export_to_storage with CSV format."""
        # Mock execute
        mock_result = create_mock_sql_result()
        adbc_driver.execute = MagicMock(return_value=mock_result)

        # Mock _export_via_backend since CSV goes through that path
        adbc_driver._export_via_backend = MagicMock(return_value=2)

        # Test export
        output_path = tmp_path / "output.csv"
        result = adbc_driver.export_to_storage("SELECT * FROM users", str(output_path), format="csv")

        # Verify _export_via_backend was called
        adbc_driver._export_via_backend.assert_called_once()

        # Should return row count
        assert result == 2

    def test_export_to_storage_json(self, adbc_driver: AdbcDriver, tmp_path: Any) -> None:
        """Test export_to_storage with JSON format."""
        # Mock execute
        mock_result = create_mock_sql_result()
        adbc_driver.execute = MagicMock(return_value=mock_result)

        # Mock _export_via_backend since JSON goes through that path
        adbc_driver._export_via_backend = MagicMock(return_value=2)

        # Test export
        output_path = tmp_path / "output.json"
        result = adbc_driver.export_to_storage("SELECT * FROM users", str(output_path), format="json")

        # Verify _export_via_backend was called
        adbc_driver._export_via_backend.assert_called_once()

        # Should return row count
        assert result == 2

    def test_import_from_storage_parquet(self, adbc_driver: AdbcDriver, tmp_path: Any) -> None:
        """Test import_from_storage with Parquet format."""
        # Create test Arrow table
        table = create_mock_arrow_table()

        # Mock storage backend
        mock_backend = MagicMock()
        mock_backend.read_arrow = MagicMock(return_value=table)

        # Mock ingest_arrow_table
        adbc_driver.ingest_arrow_table = MagicMock(return_value=2)

        # Patch the _resolve_backend_and_path method to return our mock
        adbc_driver._resolve_backend_and_path = MagicMock(return_value=(mock_backend, str(tmp_path / "input.parquet")))

        # Test import
        input_path = tmp_path / "input.parquet"
        result = adbc_driver.import_from_storage(str(input_path), "test_table")

        # Verify backend was called
        mock_backend.read_arrow.assert_called_once_with(str(input_path))

        # Verify ingest was called (default mode is "create")
        adbc_driver.ingest_arrow_table.assert_called_once_with(table, "test_table", mode="create")

        # Should return row count
        assert result == 2

    def test_import_from_storage_csv(self, adbc_driver: AdbcDriver, tmp_path: Any) -> None:
        """Test import_from_storage with CSV format."""
        # Mock _import_via_backend since CSV goes through that path
        adbc_driver._import_via_backend = MagicMock(return_value=2)

        # Test import
        input_path = tmp_path / "input.csv"
        result = adbc_driver.import_from_storage(str(input_path), "test_table", format="csv")

        # Verify _import_via_backend was called
        adbc_driver._import_via_backend.assert_called_once()

        # Should return row count
        assert result == 2

    def test_fetch_arrow_table_with_filters(self, adbc_driver: AdbcDriver, mock_adbc_connection: MagicMock) -> None:
        """Test fetch_arrow_table with filters using native implementation."""
        # Mock the cursor's fetch_arrow_table method
        mock_arrow_table = create_mock_arrow_table()
        mock_cursor = mock_adbc_connection.cursor.return_value
        mock_cursor.fetch_arrow_table.return_value = mock_arrow_table

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
        adbc_driver.fetch_arrow_table(statement, None, active_filter)  # pyright: ignore

        # Verify filter was called
        assert filter_called, "Filter was not called"
        assert filtered_sql == "SELECT * FROM users WHERE active = TRUE"

        # Verify execute was called with filtered SQL
        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args
        executed_sql = call_args[0][0]
        # Check for the WHERE clause structure
        assert "WHERE active = TRUE" in executed_sql, f"Expected filtered SQL but got: {executed_sql}"

    def test_storage_operations_with_connection_override(self, adbc_driver: AdbcDriver) -> None:
        """Test storage operations with connection override."""
        # Create override connection
        override_conn = MagicMock()
        override_cursor = MagicMock()
        override_cursor.__enter__.return_value = override_cursor
        override_cursor.__exit__.return_value = None
        override_cursor.fetch_arrow_table.return_value = create_mock_arrow_table()
        override_conn.cursor.return_value = override_cursor

        # Test fetch_arrow_table with connection override
        statement = SQL("SELECT * FROM users")
        adbc_driver.fetch_arrow_table(statement, connection=override_conn)

        # Verify override connection was used
        override_conn.cursor.assert_called_once()
        override_cursor.execute.assert_called_once_with("SELECT * FROM users", [None])
        override_cursor.fetch_arrow_table.assert_called_once()

    def test_ingest_arrow_table_with_mode_replace(
        self, adbc_driver: AdbcDriver, mock_adbc_connection: MagicMock
    ) -> None:
        """Test ingest_arrow_table with replace mode using native implementation."""
        # Create test Arrow table
        table = create_mock_arrow_table()

        # Mock ADBC connection with native adbc_ingest method
        mock_adbc_connection.adbc_ingest.return_value = 2

        # Mock execute for DELETE
        adbc_driver.execute = MagicMock(return_value=MagicMock())

        # Test ingest with replace mode
        result = adbc_driver.ingest_arrow_table(table, "test_table", mode="replace")

        # Verify DELETE was called first (ADBC uses DELETE FROM for replace mode)
        adbc_driver.execute.assert_called_once()
        delete_call = adbc_driver.execute.call_args
        delete_sql = delete_call[0][0]
        assert "DELETE FROM test_table" in delete_sql.to_sql()

        # Verify native adbc_ingest was called
        mock_adbc_connection.adbc_ingest.assert_called_once_with("test_table", table, mode="replace")

        # Should return row count
        assert result == 2

    def test_arrow_table_conversion_with_nulls(self, adbc_driver: AdbcDriver, mock_adbc_connection: MagicMock) -> None:
        """Test Arrow table conversion with null values using native implementation."""
        # Create Arrow table with nulls
        import pyarrow as pa

        schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string()), pa.field("email", pa.string())])  # type: ignore[arg-type]
        data = pa.table([[1, 2], ["test1", None], [None, "test@example.com"]], schema=schema)

        # Mock the cursor's fetch_arrow_table method
        mock_cursor = mock_adbc_connection.cursor.return_value
        mock_cursor.fetch_arrow_table.return_value = data

        # Test fetch_arrow_table
        statement = SQL("SELECT * FROM users")
        result = adbc_driver.fetch_arrow_table(statement)

        # Verify result handles nulls properly
        assert isinstance(result, ArrowResult)
        assert result.num_rows == 2
        assert set(result.column_names) == {"id", "name", "email"}

    def test_fetch_arrow_table_native_capability_detection(self, adbc_driver: AdbcDriver) -> None:
        """Test that ADBC driver detects its native Arrow capability."""
        # Test native capability detection
        assert adbc_driver._has_native_capability("arrow") is True
