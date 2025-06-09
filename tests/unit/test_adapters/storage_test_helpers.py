"""Helper functions and fixtures for driver storage tests."""

from typing import Any, Optional, Type
from unittest.mock import MagicMock, Mock, patch

import pyarrow as pa
import pytest

from sqlspec.base import DriverT
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.result import ArrowResult, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig


def create_mock_arrow_table(num_rows: int = 2, columns: Optional[list[str]] = None) -> pa.Table:
    """Create a mock Arrow table for testing."""
    if columns is None:
        columns = ["id", "name"]
    
    data = {}
    for col in columns:
        if col == "id":
            data[col] = list(range(1, num_rows + 1))
        else:
            data[col] = [f"{col}_{i}" for i in range(1, num_rows + 1)]
    
    return pa.table(data)


def create_mock_sql_result(
    data: Optional[list[dict[str, Any]]] = None,
    columns: Optional[list[str]] = None,
) -> SQLResult:
    """Create a mock SQLResult for testing."""
    if data is None:
        data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
    if columns is None:
        columns = list(data[0].keys()) if data else []
    
    # Create a real SQLResult instance instead of a mock
    from sqlspec.statement.sql import SQL
    
    statement = SQL("SELECT * FROM test")
    result = SQLResult(
        statement=statement,
        data=data,
        column_names=columns,
        rows_affected=len(data),
        operation_type="SELECT",
        total_count=len(data)
    )
    
    # Add helper properties for compatibility
    result.num_rows = len(data)  # type: ignore[attr-defined]
    result.rowcount = len(data)  # type: ignore[attr-defined]
    
    return result


def create_mock_arrow_result(
    table: Optional[pa.Table] = None,
    statement: Optional[SQL] = None
) -> ArrowResult:
    """Create a mock ArrowResult for testing."""
    if table is None:
        table = create_mock_arrow_table()
    if statement is None:
        statement = SQL("SELECT * FROM test")
    
    # Create a real ArrowResult instance
    result = ArrowResult(
        statement=statement,
        data=table
    )
    
    return result


class StorageTestMixin:
    """Mixin class providing common storage test methods."""
    
    driver_class: Type[DriverT]
    mock_connection_fixture: str  # Name of the fixture providing mock connection
    
    def test_fetch_arrow_table(self, request: pytest.FixtureRequest) -> None:
        """Test fetch_arrow_table method."""
        mock_connection = request.getfixturevalue(self.mock_connection_fixture)
        driver = self._create_driver(mock_connection)
        
        # Mock execute to return SQLResult
        mock_result = create_mock_sql_result()
        driver.execute = Mock(return_value=mock_result)
        
        # Test fetch_arrow_table
        statement = SQL("SELECT * FROM users")
        result = driver.fetch_arrow_table(statement)
        
        # Verify result
        assert isinstance(result, ArrowResult)
        assert result.num_rows() == 2
        assert "id" in result.column_names
        assert "name" in result.column_names
        
        # Verify execute was called
        driver.execute.assert_called_once()
    
    def test_ingest_arrow_table(self, request: pytest.FixtureRequest) -> None:
        """Test ingest_arrow_table method."""
        mock_connection = request.getfixturevalue(self.mock_connection_fixture)
        driver = self._create_driver(mock_connection)
        
        # Create test Arrow table
        table = create_mock_arrow_table()
        
        # Mock execute_many
        driver.execute_many = Mock(return_value=MagicMock(rowcount=2))
        
        # Test ingest
        result = driver.ingest_arrow_table(table, "test_table")
        
        # Verify result
        assert result == 2
        
        # Verify execute_many was called with INSERT statement
        driver.execute_many.assert_called_once()
        call_args = driver.execute_many.call_args
        sql_obj = call_args[0][0]
        assert "INSERT INTO test_table" in sql_obj.to_sql()
    
    def test_export_to_storage_parquet(self, request: pytest.FixtureRequest, tmp_path) -> None:
        """Test export_to_storage with Parquet format."""
        mock_connection = request.getfixturevalue(self.mock_connection_fixture)
        driver = self._create_driver(mock_connection)
        
        # Mock fetch_arrow_table
        mock_arrow_result = create_mock_arrow_result()
        driver.fetch_arrow_table = Mock(return_value=mock_arrow_result)
        
        # Mock storage backend
        mock_backend = MagicMock()
        mock_backend.write_parquet = Mock()
        
        with patch("sqlspec.storage.registry.get_storage_backend", return_value=mock_backend):
            # Test export
            output_path = tmp_path / "output.parquet"
            result = driver.export_to_storage("SELECT * FROM users", str(output_path))
            
            # Verify backend was called
            mock_backend.write_parquet.assert_called_once()
            call_args = mock_backend.write_parquet.call_args
            assert call_args[0][0] == str(output_path)
            assert isinstance(call_args[0][1], pa.Table)
            
            # Should return row count
            assert result == 2
    
    def test_export_to_storage_csv(self, request: pytest.FixtureRequest, tmp_path) -> None:
        """Test export_to_storage with CSV format."""
        mock_connection = request.getfixturevalue(self.mock_connection_fixture)
        driver = self._create_driver(mock_connection)
        
        # Mock execute
        mock_result = create_mock_sql_result()
        driver.execute = Mock(return_value=mock_result)
        
        # Mock storage backend
        mock_backend = MagicMock()
        mock_backend.write_text = Mock()
        
        with patch("sqlspec.storage.registry.get_storage_backend", return_value=mock_backend):
            # Test export
            output_path = tmp_path / "output.csv"
            result = driver.export_to_storage("SELECT * FROM users", str(output_path), format="csv")
            
            # Verify backend was called
            mock_backend.write_text.assert_called_once()
            call_args = mock_backend.write_text.call_args
            assert call_args[0][0] == str(output_path)
            assert "id,name" in call_args[0][1]  # CSV header
            
            # Should return row count
            assert result == 2
    
    def test_import_from_storage_parquet(self, request: pytest.FixtureRequest, tmp_path) -> None:
        """Test import_from_storage with Parquet format."""
        mock_connection = request.getfixturevalue(self.mock_connection_fixture)
        driver = self._create_driver(mock_connection)
        
        # Create test Arrow table
        table = create_mock_arrow_table()
        
        # Mock storage backend
        mock_backend = MagicMock()
        mock_backend.read_parquet = Mock(return_value=table)
        
        # Mock ingest_arrow_table
        driver.ingest_arrow_table = Mock(return_value=2)
        
        with patch("sqlspec.storage.registry.get_storage_backend", return_value=mock_backend):
            # Test import
            input_path = tmp_path / "input.parquet"
            result = driver.import_from_storage(str(input_path), "test_table")
            
            # Verify backend was called
            mock_backend.read_parquet.assert_called_once_with(str(input_path))
            
            # Verify ingest was called
            driver.ingest_arrow_table.assert_called_once_with(table, "test_table", mode="append")
            
            # Should return row count
            assert result == 2
    
    def test_copy_from(self, request: pytest.FixtureRequest) -> None:
        """Test copy_from method."""
        mock_connection = request.getfixturevalue(self.mock_connection_fixture)
        driver = self._create_driver(mock_connection)
        
        # For drivers without native copy_from, it should use import_from_storage
        driver.import_from_storage = Mock(return_value=10)
        
        result = driver.copy_from("s3://bucket/data.parquet", "test_table")
        
        driver.import_from_storage.assert_called_once_with(
            "s3://bucket/data.parquet", "test_table", format="parquet", mode="append"
        )
        assert result == 10
    
    def test_copy_to(self, request: pytest.FixtureRequest) -> None:
        """Test copy_to method."""
        mock_connection = request.getfixturevalue(self.mock_connection_fixture)
        driver = self._create_driver(mock_connection)
        
        # For drivers without native copy_to, it should use export_to_storage
        driver.export_to_storage = Mock(return_value=5)
        
        result = driver.copy_to("SELECT * FROM test", "gs://bucket/output.csv")
        
        driver.export_to_storage.assert_called_once_with(
            "SELECT * FROM test", "gs://bucket/output.csv", format="csv"
        )
        assert result == 5
    
    def test_read_parquet_direct(self, request: pytest.FixtureRequest) -> None:
        """Test read_parquet_direct method."""
        mock_connection = request.getfixturevalue(self.mock_connection_fixture)
        driver = self._create_driver(mock_connection)
        
        # Create test table
        table = create_mock_arrow_table()
        
        # Mock storage backend
        mock_backend = MagicMock()
        mock_backend.read_parquet = Mock(return_value=table)
        
        with patch("sqlspec.storage.registry.get_storage_backend", return_value=mock_backend):
            result = driver.read_parquet_direct("s3://bucket/data.parquet")
            
            assert isinstance(result, pa.Table)
            assert result.num_rows == 2
            mock_backend.read_parquet.assert_called_once_with("s3://bucket/data.parquet")
    
    def test_write_parquet_direct(self, request: pytest.FixtureRequest) -> None:
        """Test write_parquet_direct method."""
        mock_connection = request.getfixturevalue(self.mock_connection_fixture)
        driver = self._create_driver(mock_connection)
        
        # Create test table
        table = create_mock_arrow_table()
        
        # Mock storage backend
        mock_backend = MagicMock()
        mock_backend.write_parquet = Mock()
        
        with patch("sqlspec.storage.registry.get_storage_backend", return_value=mock_backend):
            driver.write_parquet_direct(table, "gs://bucket/output.parquet", compression="snappy")
            
            mock_backend.write_parquet.assert_called_once_with(
                "gs://bucket/output.parquet", table, compression="snappy"
            )
    
    def _create_driver(self, mock_connection: Mock) -> DriverT:
        """Create driver instance with mocked connection."""
        config = SQLConfig()
        instrumentation_config = InstrumentationConfig()
        return self.driver_class(
            connection=mock_connection,
            config=config,
            instrumentation_config=instrumentation_config,
        )