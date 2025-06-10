"""Storage tests for AIOSQLite driver."""

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pyarrow as pa
import pytest

from sqlspec.adapters.aiosqlite import AiosqliteDriver
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.result import ArrowResult, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from tests.unit.test_adapters.storage_test_helpers import (
    create_mock_arrow_result,
    create_mock_arrow_table,
    create_mock_sql_result,
)


@pytest.fixture
def mock_aiosqlite_connection() -> AsyncMock:
    """Create a mock AIOSQLite connection."""
    mock_connection = AsyncMock()
    mock_cursor = AsyncMock()
    mock_connection.cursor.return_value = mock_cursor
    mock_cursor.__aenter__.return_value = mock_cursor
    mock_cursor.__aexit__.return_value = None
    mock_cursor.execute.return_value = mock_cursor
    mock_cursor.executemany.return_value = mock_cursor
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = None
    mock_cursor.description = [(col,) for col in ["id", "name"]]
    mock_cursor.rowcount = 0
    return mock_connection


@pytest.fixture
def aiosqlite_driver(mock_aiosqlite_connection: AsyncMock) -> AiosqliteDriver:
    """Create an AIOSQLite driver with mocked connection."""
    config = SQLConfig(strict_mode=False)
    instrumentation_config = InstrumentationConfig()
    return AiosqliteDriver(
        connection=mock_aiosqlite_connection,
        config=config,
        instrumentation_config=instrumentation_config,
    )


class TestAIOSQLiteStorageOperations:
    """Test storage operations for AIOSQLite driver."""

    @pytest.mark.asyncio
    async def test_fetch_arrow_table(self, aiosqlite_driver: AiosqliteDriver) -> None:
        """Test fetch_arrow_table method using fallback."""
        # Mock execute to return SQLResult
        mock_result = create_mock_sql_result()
        aiosqlite_driver.execute = AsyncMock(return_value=mock_result)

        # Test fetch_arrow_table
        statement = SQL("SELECT * FROM users")
        result = await aiosqlite_driver.fetch_arrow_table(statement)

        # Verify result
        assert isinstance(result, ArrowResult)
        assert result.num_rows() == 2
        assert "id" in result.column_names
        assert "name" in result.column_names

        # Verify execute was called
        aiosqlite_driver.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_ingest_arrow_table(
        self, aiosqlite_driver: AiosqliteDriver, mock_aiosqlite_connection: AsyncMock
    ) -> None:
        """Test ingest_arrow_table method using fallback."""
        # Create test Arrow table
        table = create_mock_arrow_table()

        # Ensure AIOSQLite connection doesn't have register method to force fallback
        del mock_aiosqlite_connection.register

        # Mock execute_many result
        mock_result = MagicMock()
        mock_result.rows_affected = 2
        aiosqlite_driver.execute_many = AsyncMock(return_value=mock_result)

        # Test ingest
        result = await aiosqlite_driver.ingest_arrow_table(table, "test_table")

        # Verify result
        assert result == 2

        # Verify execute_many was called with INSERT statement
        aiosqlite_driver.execute_many.assert_called_once()
        call_args = aiosqlite_driver.execute_many.call_args
        sql_obj = call_args[0][0]
        assert isinstance(sql_obj, SQL)
        assert "INSERT INTO test_table" in sql_obj.to_sql()
        assert sql_obj.is_many is True

        # Check parameters were converted from Arrow table
        assert sql_obj.parameters == [{"id": 1, "name": "name_1"}, {"id": 2, "name": "name_2"}]

    @pytest.mark.asyncio
    async def test_export_to_storage_parquet(self, aiosqlite_driver: AiosqliteDriver, tmp_path: Any) -> None:
        """Test export_to_storage with Parquet format."""
        # Mock fetch_arrow_table
        mock_arrow_result = create_mock_arrow_result()
        aiosqlite_driver.fetch_arrow_table = AsyncMock(return_value=mock_arrow_result)

        # Mock storage backend
        mock_backend = MagicMock()
        mock_backend.write_arrow_async = AsyncMock()

        # Patch the _resolve_backend_and_path method to return our mock
        aiosqlite_driver._resolve_backend_and_path = MagicMock(
            return_value=(mock_backend, str(tmp_path / "output.parquet"))
        )

        # Test export
        output_path = tmp_path / "output.parquet"
        result = await aiosqlite_driver.export_to_storage("SELECT * FROM users", str(output_path))

        # Verify backend was called
        mock_backend.write_arrow_async.assert_called_once()
        call_args = mock_backend.write_arrow_async.call_args
        assert call_args[0][0] == str(output_path)
        assert isinstance(call_args[0][1], pa.Table)

        # Should return row count
        assert result == 2

    @pytest.mark.asyncio
    async def test_export_to_storage_csv(self, aiosqlite_driver: AiosqliteDriver, tmp_path: Any) -> None:
        """Test export_to_storage with CSV format."""
        # Mock execute
        mock_result = create_mock_sql_result()
        aiosqlite_driver.execute = AsyncMock(return_value=mock_result)

        # Mock _export_via_backend since CSV goes through that path
        aiosqlite_driver._export_via_backend = AsyncMock(return_value=2)

        # Test export
        output_path = tmp_path / "output.csv"
        result = await aiosqlite_driver.export_to_storage("SELECT * FROM users", str(output_path), format="csv")

        # Verify _export_via_backend was called
        aiosqlite_driver._export_via_backend.assert_called_once()

        # Should return row count
        assert result == 2

    @pytest.mark.asyncio
    async def test_export_to_storage_json(self, aiosqlite_driver: AiosqliteDriver, tmp_path: Any) -> None:
        """Test export_to_storage with JSON format."""
        # Mock execute
        mock_result = create_mock_sql_result()
        aiosqlite_driver.execute = AsyncMock(return_value=mock_result)

        # Mock _export_via_backend since JSON goes through that path
        aiosqlite_driver._export_via_backend = AsyncMock(return_value=2)

        # Test export
        output_path = tmp_path / "output.json"
        result = await aiosqlite_driver.export_to_storage("SELECT * FROM users", str(output_path), format="json")

        # Verify _export_via_backend was called
        aiosqlite_driver._export_via_backend.assert_called_once()

        # Should return row count
        assert result == 2

    @pytest.mark.asyncio
    async def test_import_from_storage_parquet(self, aiosqlite_driver: AiosqliteDriver, tmp_path: Any) -> None:
        """Test import_from_storage with Parquet format."""
        # Create test Arrow table
        table = create_mock_arrow_table()

        # Mock storage backend
        mock_backend = MagicMock()
        mock_backend.read_arrow_async = AsyncMock(return_value=table)

        # Mock ingest_arrow_table
        aiosqlite_driver.ingest_arrow_table = AsyncMock(return_value=2)

        # Patch the _resolve_backend_and_path method to return our mock
        aiosqlite_driver._resolve_backend_and_path = MagicMock(
            return_value=(mock_backend, str(tmp_path / "input.parquet"))
        )

        # Test import
        input_path = tmp_path / "input.parquet"
        result = await aiosqlite_driver.import_from_storage(str(input_path), "test_table")

        # Verify backend was called
        mock_backend.read_arrow_async.assert_called_once_with(str(input_path))

        # Verify ingest was called (default mode is "create")
        aiosqlite_driver.ingest_arrow_table.assert_called_once_with(table, "test_table", mode="create")

        # Should return row count
        assert result == 2

    @pytest.mark.asyncio
    async def test_import_from_storage_csv(self, aiosqlite_driver: AiosqliteDriver, tmp_path: Any) -> None:
        """Test import_from_storage with CSV format."""
        # Mock _import_via_backend since CSV goes through that path
        aiosqlite_driver._import_via_backend = AsyncMock(return_value=2)

        # Test import
        input_path = tmp_path / "input.csv"
        result = await aiosqlite_driver.import_from_storage(str(input_path), "test_table", format="csv")

        # Verify _import_via_backend was called
        aiosqlite_driver._import_via_backend.assert_called_once()

        # Should return row count
        assert result == 2

    @pytest.mark.asyncio
    async def test_fetch_arrow_table_with_filters(self, aiosqlite_driver: AiosqliteDriver) -> None:
        """Test fetch_arrow_table with filters."""
        # Mock execute
        mock_result = create_mock_sql_result()
        aiosqlite_driver.execute = AsyncMock(return_value=mock_result)

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
        await aiosqlite_driver.fetch_arrow_table(statement, None, active_filter)  # type: ignore[arg-type]

        # Verify filter was called
        assert filter_called, "Filter was not called"
        assert filtered_sql == "SELECT * FROM users WHERE active = TRUE"

        # Verify execute was called with filtered SQL
        aiosqlite_driver.execute.assert_called_once()
        call_args = aiosqlite_driver.execute.call_args
        executed_sql = call_args[0][0]
        # Check both the SQL string and to_sql() method
        sql_str = executed_sql.to_sql() if hasattr(executed_sql, "to_sql") else str(executed_sql)
        assert "WHERE active = TRUE" in sql_str, f"Expected filtered SQL but got: {sql_str}"

    @pytest.mark.asyncio
    async def test_storage_operations_with_connection_override(self, aiosqlite_driver: AiosqliteDriver) -> None:
        """Test storage operations with connection override."""
        # Create override connection
        override_conn = AsyncMock()
        override_cursor = AsyncMock()
        override_cursor.__aenter__.return_value = override_cursor
        override_cursor.__aexit__.return_value = None
        override_cursor.fetchall.return_value = []
        override_conn.cursor.return_value = override_cursor

        # Mock execute
        mock_result = create_mock_sql_result()
        aiosqlite_driver.execute = AsyncMock(return_value=mock_result)

        # Test fetch_arrow_table with connection override
        statement = SQL("SELECT * FROM users")
        await aiosqlite_driver.fetch_arrow_table(statement, connection=override_conn)

        # Verify execute was called with override connection
        aiosqlite_driver.execute.assert_called_once()
        call_args = aiosqlite_driver.execute.call_args
        assert call_args[1].get("connection") is override_conn

    @pytest.mark.asyncio
    async def test_ingest_arrow_table_with_mode_replace(
        self, aiosqlite_driver: AiosqliteDriver, mock_aiosqlite_connection: AsyncMock
    ) -> None:
        """Test ingest_arrow_table with replace mode."""
        # Create test Arrow table
        table = create_mock_arrow_table()

        # Ensure AIOSQLite connection doesn't have register method to force fallback
        del mock_aiosqlite_connection.register

        # Mock execute for TRUNCATE
        aiosqlite_driver.execute = AsyncMock(return_value=MagicMock())

        # Mock execute_many for INSERT
        aiosqlite_driver.execute_many = AsyncMock(return_value=MagicMock(rows_affected=2))

        # Test ingest with replace mode
        result = await aiosqlite_driver.ingest_arrow_table(table, "test_table", mode="replace")

        # Verify TRUNCATE was called
        aiosqlite_driver.execute.assert_called_once()
        truncate_call = aiosqlite_driver.execute.call_args
        truncate_sql = truncate_call[0][0]
        assert "TRUNCATE TABLE test_table" in truncate_sql.to_sql()

        # Verify INSERT was called
        aiosqlite_driver.execute_many.assert_called_once()

        # Should return row count
        assert result == 2

    @pytest.mark.asyncio
    async def test_arrow_table_conversion_with_nulls(self, aiosqlite_driver: AiosqliteDriver) -> None:
        """Test Arrow table conversion with null values."""
        # Create data with nulls
        data = [{"id": 1, "name": "test1", "email": None}, {"id": 2, "name": None, "email": "test@example.com"}]

        # Mock execute
        mock_result = MagicMock(spec=SQLResult)
        mock_result.data = data
        mock_result.column_names = ["id", "name", "email"]
        mock_result.num_rows = 2

        aiosqlite_driver.execute = AsyncMock(return_value=mock_result)

        # Test fetch_arrow_table
        statement = SQL("SELECT * FROM users")
        result = await aiosqlite_driver.fetch_arrow_table(statement)

        # Verify result handles nulls properly
        assert isinstance(result, ArrowResult)
        assert result.num_rows() == 2
        assert set(result.column_names) == {"id", "name", "email"}
