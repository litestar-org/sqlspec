"""Unit tests for SQLite driver."""

import tempfile
from typing import Any, Union
from unittest.mock import MagicMock, Mock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sqlspec.adapters.sqlite import SqliteConnection, SqliteDriver
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.sql import SQL, SQLConfig


@pytest.fixture
def mock_sqlite_connection() -> Mock:
    """Create a mock SQLite connection with context manager support for cursor."""
    mock_connection = Mock(spec=SqliteConnection)
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    mock_connection.execute.return_value = mock_cursor
    mock_connection.executemany.return_value = mock_cursor
    mock_connection.executescript.return_value = mock_cursor
    # Patch context manager protocol for cursor
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.__exit__.return_value = None
    # Ensure the cursor returned by __enter__ has all necessary methods
    mock_cursor.execute = MagicMock()
    mock_cursor.executemany = MagicMock()
    mock_cursor.executescript = MagicMock()
    mock_cursor.fetchall = MagicMock()
    mock_cursor.close = MagicMock()
    return mock_connection


@pytest.fixture
def sqlite_driver(mock_sqlite_connection: Mock) -> SqliteDriver:
    """Create a SQLite driver with mocked connection."""
    config = SQLConfig()
    instrumentation_config = InstrumentationConfig()
    return SqliteDriver(connection=mock_sqlite_connection, config=config, instrumentation_config=instrumentation_config)


def test_sqlite_driver_initialization(mock_sqlite_connection: Mock) -> None:
    """Test SQLite driver initialization."""
    config = SQLConfig()
    instrumentation_config = InstrumentationConfig(log_queries=True)

    driver = SqliteDriver(
        connection=mock_sqlite_connection, config=config, instrumentation_config=instrumentation_config
    )

    # Test driver attributes are set correctly
    assert driver.connection is mock_sqlite_connection
    assert driver.config is config
    assert driver.instrumentation_config is instrumentation_config
    assert driver.dialect == "sqlite"
    from sqlspec.statement.parameters import ParameterStyle

    assert driver.default_parameter_style == ParameterStyle.QMARK


def test_sqlite_driver_dialect_property(sqlite_driver: SqliteDriver) -> None:
    """Test SQLite driver dialect property."""
    assert sqlite_driver.dialect == "sqlite"


def test_sqlite_driver_parameter_style(sqlite_driver: SqliteDriver) -> None:
    """Test SQLite driver parameter style."""
    from sqlspec.statement.parameters import ParameterStyle

    assert sqlite_driver.default_parameter_style == ParameterStyle.QMARK


def test_sqlite_config_dialect_property() -> None:
    """Test SQLite config dialect property."""
    from sqlspec.adapters.sqlite import SqliteConfig

    config = SqliteConfig(database=":memory:")
    assert config.dialect == "sqlite"


def test_sqlite_driver_execute_statement_select(sqlite_driver: SqliteDriver, mock_sqlite_connection: Mock) -> None:
    """Test SQLite driver _execute_statement for SELECT statements."""
    # Setup mock cursor
    mock_cursor = mock_sqlite_connection.cursor.return_value.__enter__.return_value
    mock_cursor.description = [(col,) for col in ["id", "name", "email"]]
    mock_cursor.fetchall.return_value = []

    # Create SQL statement
    statement = SQL("SELECT * FROM users WHERE id = 1")

    # Call execute_statement which will handle the mock setup
    result = sqlite_driver._execute_statement(statement)

    # Verify the mock was called correctly
    mock_cursor.execute.assert_called_once()
    mock_cursor.fetchall.assert_called_once()

    # The result should be a dict with expected structure
    assert isinstance(result, dict)
    assert "column_names" in result
    assert "data" in result
    assert result["column_names"] == ["id", "name", "email"]


def test_sqlite_driver_to_parquet(
    sqlite_driver: SqliteDriver, mock_sqlite_connection: Mock, monkeypatch: "pytest.MonkeyPatch"
) -> None:
    """Test to_parquet writes correct data to a Parquet file."""
    mock_cursor = mock_sqlite_connection.cursor.return_value.__enter__.return_value
    mock_cursor.description = [(col,) for col in ["id", "name"]]

    # Create mock Row objects that behave like sqlite3.Row
    class MockRow:
        def __init__(self, data: dict[str, Any]) -> None:
            self._data = data

        def keys(self) -> list[Any]:
            return list(self._data.keys())

        def __iter__(self) -> Any:
            return iter(self._data.values())

        def __getitem__(self, key: Union[int, str]) -> Any:
            if isinstance(key, int):
                return list(self._data.values())[key]
            return self._data[key]

    mock_cursor.fetchall.return_value = [MockRow({"id": 1, "name": "Alice"}), MockRow({"id": 2, "name": "Bob"})]

    statement = SQL("SELECT id, name FROM users")
    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmpfile:
        # Patch pyarrow.parquet.write_table to actually write
        orig_write_table = pq.write_table

        def patched_write_table(table: pa.Table, where: str, **kwargs: "Any") -> None:
            # Actually write using the real function
            return orig_write_table(table, where, **kwargs)

        monkeypatch.setattr(pq, "write_table", patched_write_table)
        sqlite_driver.export_to_storage(statement.to_sql(), tmpfile.name)
        table = pq.read_table(tmpfile.name)
        assert table.num_rows == 2
        assert table.column_names == ["id", "name"]
        assert table.column("id").to_pylist() == [1, 2]
        assert table.column("name").to_pylist() == ["Alice", "Bob"]
