# pyright: reportMissingParameterType=false, reportAttributeAccessIssue=false, reportArgumentType=false
"""Unit tests for sqlspec.statement.result module.

Tests the result wrapper classes for handling different types of SQL operations
including SELECT, INSERT/UPDATE/DELETE, script execution, and Arrow results.
"""

from typing import Any
from unittest.mock import Mock

import pytest

from sqlspec.statement.result import (
    ArrowResult,
    SQLResult,
    StatementResult,
)
from sqlspec.typing import RowT


def test_statement_result_is_abstract() -> None:
    """Test that StatementResult cannot be instantiated directly."""
    with pytest.raises(TypeError, match="abstract"):
        StatementResult(statement="test", data=["test"])  # type: ignore[abstract]


def test_statement_result_metadata_methods() -> None:
    """Test metadata getter and setter methods."""

    # Create a concrete implementation for testing
    class ConcreteResult(StatementResult[dict[str, Any]]):
        def is_success(self) -> bool:
            return True

        def get_data(self) -> list[dict[str, Any]]:
            return self.data  # type: ignore

    result = ConcreteResult(statement="test", data=[{"test": "data"}], metadata={"key1": "value1"})

    # Test metadata access
    assert result.get_metadata("key1") == "value1"
    assert result.get_metadata("missing", "default") == "default"
    assert result.get_metadata("missing") is None

    # Test metadata setting
    result.set_metadata("key2", "value2")
    assert result.get_metadata("key2") == "value2"


@pytest.fixture
def sample_rows() -> list[dict[str, Any]]:
    """Sample row data for testing."""
    return [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": "bob@example.com"},
        {"id": 3, "name": "Charlie", "email": "charlie@example.com"},
    ]


@pytest.fixture
def basic_sql_result_select(sample_rows: list[dict[str, Any]]) -> SQLResult[RowT]:
    """Basic SQLResult for SELECT-like operations for testing."""
    return SQLResult(
        statement="SELECT * FROM users",
        data=sample_rows,
        column_names=["id", "name", "email"],
        rows_affected=len(sample_rows),  # For SELECT, this can be count of rows returned
        operation_type="SELECT",
    )


def test_sql_result_initialization_select(sample_rows: list[dict[str, Any]]) -> None:
    """Test SQLResult initialization for SELECT-like operations."""
    result = SQLResult[dict[str, Any]](
        statement="SELECT * FROM users",
        data=sample_rows,
        column_names=["id", "name", "email"],
        rows_affected=len(sample_rows),
        operation_type="SELECT",
        execution_time=0.5,
        metadata={"query_id": "123"},
    )

    assert result.statement == "SELECT * FROM users"
    assert result.data == sample_rows
    assert result.column_names == ["id", "name", "email"]
    assert result.rows_affected == len(sample_rows)
    assert result.operation_type == "SELECT"
    # These attributes are part of the unified SQLResult class
    assert result.total_count == len(sample_rows)  # Should be set in __post_init__
    assert result.has_more is False  # Default value
    assert result.execution_time == 0.5
    assert result.metadata == {"query_id": "123"}


def test_sql_result_is_success_select(basic_sql_result_select: SQLResult[RowT]) -> None:
    """Test is_success method for SELECT-like SQLResult."""
    assert basic_sql_result_select.is_success() is True

    # Test with empty data list (still a successful select)
    empty_result = SQLResult[RowT](statement="SELECT * FROM empty", data=[], operation_type="SELECT")
    assert empty_result.is_success() is True

    # Test with rows_affected = -1 (indicates an issue with execution)
    failed_result = SQLResult[RowT](statement="SELECT * FROM empty", data=[], operation_type="SELECT", rows_affected=-1)
    assert failed_result.is_success() is False


def test_sql_result_get_data_select(basic_sql_result_select: SQLResult[RowT]) -> None:
    """Test get_data method returns data for SELECT-like SQLResult."""
    data = basic_sql_result_select.get_data()
    assert data == basic_sql_result_select.data
    assert len(data) == 3


def test_sql_result_get_first_select(sample_rows: list[dict[str, Any]]) -> None:
    """Test get_first method for SELECT-like SQLResult."""
    result = SQLResult[dict[str, Any]](statement="SELECT * FROM users", data=sample_rows, operation_type="SELECT")
    first_row = result.get_first()

    assert first_row == sample_rows[0]
    if first_row is not None:
        assert first_row["name"] == "Alice"

    # Test with empty data
    empty_result = SQLResult[dict[str, Any]](statement="SELECT * FROM empty", data=[], operation_type="SELECT")
    assert empty_result.get_first() is None


def test_sql_result_get_count_select(basic_sql_result_select: SQLResult[RowT]) -> None:
    """Test get_count method for SELECT-like SQLResult."""
    assert basic_sql_result_select.get_count() == 3

    # Test with empty data
    empty_result = SQLResult[RowT](statement="SELECT * FROM empty", data=[], operation_type="SELECT")
    assert empty_result.get_count() == 0


def test_sql_result_is_empty_select(basic_sql_result_select: SQLResult[RowT]) -> None:
    """Test is_empty method for SELECT-like SQLResult."""
    assert basic_sql_result_select.is_empty() is False

    # Test with empty data
    empty_result = SQLResult[RowT](statement="SELECT * FROM empty", data=[], operation_type="SELECT")
    assert empty_result.is_empty() is True


@pytest.mark.parametrize(
    ("rows", "expected_count", "expected_empty"),
    [
        ([], 0, True),
        ([{"id": 1}], 1, False),
        ([{"id": 1}, {"id": 2}], 2, False),
    ],
    ids=["empty_rows", "single_row", "multiple_rows"],
)
def test_sql_result_row_operations_select(
    rows: list[dict[str, Any]], expected_count: int, expected_empty: bool
) -> None:
    """Test row operations with different row counts for SELECT-like SQLResult."""
    result = SQLResult[dict[str, Any]](statement="SELECT * FROM test", data=rows, operation_type="SELECT")

    assert result.get_count() == expected_count
    assert result.is_empty() == expected_empty

    if rows:
        assert result.get_first() == rows[0]
    else:
        assert result.get_first() is None


@pytest.fixture
def basic_sql_result_execute() -> SQLResult[RowT]:  # Changed from None to dict[str, Any]
    """Basic SQLResult for EXECUTE-like operations for testing."""
    return SQLResult(
        statement="INSERT INTO users VALUES (1, 'test')",
        data=[],  # Empty list instead of None for execute operations
        rows_affected=5,
        last_inserted_id=123,  # This might be in metadata or a specific attribute if needed
        operation_type="INSERT",
        metadata={"last_inserted_id": 123},  # Store last_inserted_id in metadata
    )


def test_sql_result_initialization_execute() -> None:
    """Test SQLResult initialization for EXECUTE-like operations."""
    result = SQLResult[dict[str, Any]](
        statement="UPDATE users SET name = 'John'",
        data=[],  # Empty list instead of None
        rows_affected=10,
        operation_type="UPDATE",
        execution_time=0.25,
        metadata={"transaction_id": "tx-456", "last_inserted_id": "uuid-123"},
    )

    assert result.statement == "UPDATE users SET name = 'John'"
    assert result.rows_affected == 10
    assert result.get_metadata("last_inserted_id") == "uuid-123"
    assert result.operation_type == "UPDATE"
    assert result.execution_time == 0.25
    assert result.metadata == {"transaction_id": "tx-456", "last_inserted_id": "uuid-123"}
    assert result.data == []  # data should be empty list for execute


def test_sql_result_is_success_execute() -> None:
    """Test is_success method for EXECUTE-like SQLResult."""
    # Successful operation
    success_result = SQLResult[dict[str, Any]](
        statement="INSERT INTO test", data=[], rows_affected=5, operation_type="INSERT"
    )
    assert success_result.is_success() is True

    # Zero rows affected (still success for some operations like UPDATE that might not change anything)
    zero_result = SQLResult[dict[str, Any]](statement="UPDATE test", data=[], rows_affected=0, operation_type="UPDATE")
    assert zero_result.is_success() is True

    # None rows affected (can indicate failure or that it's not applicable)
    # SQLResult.is_success() primarily checks rows_affected >= 0
    none_result = SQLResult[dict[str, Any]](
        statement="UPDATE test", data=[], rows_affected=None, operation_type="UPDATE"
    )
    assert none_result.is_success() is False  # rows_affected is None, so it's not >= 0

    # Negative rows affected (failure)
    negative_result = SQLResult[dict[str, Any]](
        statement="UPDATE test", data=[], rows_affected=-1, operation_type="UPDATE"
    )
    assert negative_result.is_success() is False


def test_sql_result_get_data_execute(basic_sql_result_execute: SQLResult[RowT]) -> None:
    """Test get_data method returns empty list for EXECUTE-like SQLResult."""
    data = basic_sql_result_execute.get_data()
    assert data == []  # For execute-like results, data is empty list


def test_sql_result_get_affected_count_execute(basic_sql_result_execute: SQLResult[RowT]) -> None:
    """Test get_affected_count method for EXECUTE-like SQLResult."""
    assert basic_sql_result_execute.get_affected_count() == 5

    # Test with None rows_affected
    none_result = SQLResult[RowT](statement="UPDATE test", data=[], rows_affected=None, operation_type="UPDATE")
    assert none_result.get_affected_count() == 0  # get_affected_count returns 0 if rows_affected is None


def test_sql_result_get_inserted_id_from_metadata(basic_sql_result_execute: SQLResult[RowT]) -> None:
    """Test getting last_inserted_id from metadata for EXECUTE-like SQLResult."""
    assert basic_sql_result_execute.get_metadata("last_inserted_id") == 123

    # Test with no last_inserted_id in metadata
    no_id_result = SQLResult[RowT](statement="INSERT INTO test", data=[], operation_type="INSERT")
    assert no_id_result.get_metadata("last_inserted_id") is None


@pytest.mark.parametrize(
    ("operation_type", "expected_insert", "expected_update", "expected_delete"),
    [
        ("INSERT", True, False, False),
        ("UPDATE", False, True, False),
        ("DELETE", False, False, True),
        ("MERGE", False, False, False),
        ("insert", True, False, False),  # Case insensitive
        ("update", False, True, False),
        ("delete", False, False, True),
    ],
    ids=[
        "insert_upper",
        "update_upper",
        "delete_upper",
        "merge_upper",
        "insert_lower",
        "update_lower",
        "delete_lower",
    ],
)
def test_execute_result_operation_type_checks(
    operation_type: str, expected_insert: bool, expected_update: bool, expected_delete: bool
) -> None:
    """Test operation type checking methods."""
    result = SQLResult[dict[str, Any]](statement="SQL", data=[], operation_type=operation_type)

    assert result.was_inserted() == expected_insert
    assert result.was_updated() == expected_update
    assert result.was_deleted() == expected_delete


@pytest.fixture
def sample_statement_results_for_script() -> list[SQLResult[Any]]:  # Now uses SQLResult
    """Sample statement results for script testing."""
    return [
        SQLResult(statement="INSERT INTO test", data=[], rows_affected=2, operation_type="INSERT"),
        SQLResult(statement="UPDATE test", data=[], rows_affected=1, operation_type="UPDATE"),
        SQLResult(statement="DELETE FROM test", data=[], rows_affected=3, operation_type="DELETE"),
    ]


@pytest.fixture
def basic_script_result(sample_statement_results_for_script: list[SQLResult[Any]]) -> SQLResult[Any]:
    """Basic SQLResult[Any] for testing."""
    result = SQLResult[Any](
        statement="SCRIPT",
        data=[],
        operation_type="SCRIPT",
    )
    # Add the sample statement results to simulate script execution
    for stmt_result in sample_statement_results_for_script:
        result.add_statement_result(stmt_result)
    return result


def test_script_result_initialization() -> None:
    """Test SQLResult[Any] initialization."""
    result = SQLResult[Any](
        statement="SCRIPT", data=[], operation_type="SCRIPT", execution_time=1.5, metadata={"script_id": "script-123"}
    )

    assert result.statement == "SCRIPT"
    assert result.operation_type == "SCRIPT"
    assert result.execution_time == 1.5
    assert result.metadata == {"script_id": "script-123"}


def test_script_result_is_success(basic_script_result: SQLResult[Any]) -> None:
    """Test is_success method."""
    # All statements successful, no errors
    assert basic_script_result.is_success() is True

    # Add an error
    basic_script_result.add_error("Test error")
    assert basic_script_result.is_success() is False

    # Reset errors but make total != successful
    error_result = SQLResult[Any](statement="SCRIPT", data=[], operation_type="SCRIPT")
    error_result.total_statements = 2
    error_result.successful_statements = 1
    assert error_result.is_success() is False


def test_script_result_get_data(basic_script_result: SQLResult[Any]) -> None:
    """Test get_data method returns SQLResult[Any]Data."""
    data = basic_script_result.get_data()

    assert isinstance(data, dict)
    assert data["total_statements"] == 3
    assert data["successful_statements"] == 3
    assert data["failed_statements"] == 0
    assert data["errors"] == []
    assert len(data["statement_results"]) == 3
    assert data["total_rows_affected"] == 6  # 2 + 1 + 3


def test_script_result_add_statement_result() -> None:
    """Test add_statement_result method."""
    script_result = SQLResult[Any](statement="SCRIPT", data=[], operation_type="SCRIPT")

    # Add successful statement
    success_stmt = SQLResult[None](statement="INSERT", data=[], rows_affected=5, operation_type="INSERT")
    script_result.add_statement_result(success_stmt)

    assert script_result.total_statements == 1
    assert script_result.successful_statements == 1
    assert len(script_result.statement_results) == 1

    # Add failed statement (rows_affected is None)
    failed_stmt = SQLResult[None](statement="UPDATE", data=[], rows_affected=None, operation_type="UPDATE")
    script_result.add_statement_result(failed_stmt)

    assert script_result.total_statements == 2
    assert script_result.successful_statements == 1  # Still 1, as second one failed
    assert len(script_result.statement_results) == 2


def test_script_result_add_error(basic_script_result: SQLResult[Any]) -> None:
    """Test add_error method."""
    basic_script_result.add_error("First error")
    basic_script_result.add_error("Second error")

    assert len(basic_script_result.errors) == 2
    assert basic_script_result.errors == ["First error", "Second error"]


def test_script_result_get_statement_result(basic_script_result: SQLResult[Any]) -> None:
    """Test get_statement_result method."""
    # Valid indices
    assert basic_script_result.get_statement_result(0) is not None
    assert basic_script_result.get_statement_result(1) is not None
    assert basic_script_result.get_statement_result(2) is not None

    # Invalid indices
    assert basic_script_result.get_statement_result(-1) is None
    assert basic_script_result.get_statement_result(3) is None
    assert basic_script_result.get_statement_result(100) is None


def test_script_result_get_total_rows_affected(basic_script_result: SQLResult[Any]) -> None:
    """Test get_total_rows_affected method."""
    assert basic_script_result.get_total_rows_affected() == 6  # 2 + 1 + 3

    # Test with statements that have None rows_affected
    script_with_none = SQLResult[Any](statement="SCRIPT", data=[], operation_type="SCRIPT")
    script_with_none.add_statement_result(
        SQLResult[None](statement="INSERT", data=[], rows_affected=5, operation_type="INSERT")
    )
    script_with_none.add_statement_result(
        SQLResult[None](statement="UPDATE", data=[], rows_affected=None, operation_type="UPDATE")
    )
    script_with_none.add_statement_result(
        SQLResult[None](statement="DELETE", data=[], rows_affected=3, operation_type="DELETE")
    )

    assert script_with_none.get_total_rows_affected() == 8  # 5 + 0 + 3


def test_script_result_get_errors(basic_script_result: SQLResult[Any]) -> None:
    """Test get_errors method."""
    # Initially no errors
    assert basic_script_result.get_errors() == []

    # Add errors
    basic_script_result.add_error("Error 1")
    basic_script_result.add_error("Error 2")

    errors = basic_script_result.get_errors()
    assert errors == ["Error 1", "Error 2"]


def test_script_result_has_errors(basic_script_result: SQLResult[Any]) -> None:
    """Test has_errors method."""
    # Initially no errors
    assert basic_script_result.has_errors() is False

    # Add error
    basic_script_result.add_error("Test error")
    assert basic_script_result.has_errors() is True


@pytest.fixture
def mock_arrow_table() -> Mock:
    """Mock Apache Arrow table for testing."""
    mock_table = Mock()
    mock_table.column_names = ["id", "name", "value"]
    mock_table.num_rows = 100
    mock_table.num_columns = 3
    return mock_table


@pytest.fixture
def basic_arrow_result(mock_arrow_table: Mock) -> ArrowResult:
    """Basic ArrowResult for testing."""
    return ArrowResult(statement="SELECT * FROM users", data=mock_arrow_table, schema={"version": "1.0"})


def test_arrow_result_initialization(mock_arrow_table: Mock) -> None:
    """Test ArrowResult initialization."""
    result = ArrowResult(
        statement="SELECT * FROM users",
        data=mock_arrow_table,
        schema={"version": "1.0", "metadata": {"created": "2023-01-01"}},
        execution_time=0.75,
        metadata={"query_id": "arrow-123"},
    )

    assert result.statement == "SELECT * FROM users"
    assert result.data == mock_arrow_table
    assert result.schema == {"version": "1.0", "metadata": {"created": "2023-01-01"}}
    assert result.execution_time == 0.75
    assert result.metadata == {"query_id": "arrow-123"}


def test_arrow_result_is_success(mock_arrow_table: Mock) -> None:
    """Test is_success method."""
    # With arrow table
    success_result = ArrowResult(statement="SELECT * FROM users", data=mock_arrow_table)
    assert success_result.is_success() is True

    # Without arrow table - create a mock that returns None for data
    none_mock = Mock()
    none_mock.return_value = None
    none_result = ArrowResult(statement="SELECT * FROM users", data=none_mock)
    # Override the data attribute to be None for this test
    none_result.data = None  # type: ignore[assignment]
    assert none_result.is_success() is False


def test_arrow_result_get_data(basic_arrow_result: ArrowResult, mock_arrow_table: Mock) -> None:
    """Test get_data method."""
    data = basic_arrow_result.get_data()
    assert data is mock_arrow_table

    # Test with None data - create a result with None data
    none_result = ArrowResult(statement="SELECT * FROM users", data=mock_arrow_table)
    none_result.data = None  # type: ignore[assignment]
    with pytest.raises(ValueError, match="No Arrow table available"):
        none_result.get_data()


def test_arrow_result_column_names(basic_arrow_result: ArrowResult) -> None:
    """Test column_names method."""
    columns = basic_arrow_result.column_names()
    assert columns == ["id", "name", "value"]

    # Test with None data - create a result with None data
    none_result = ArrowResult(statement="SELECT * FROM users", data=basic_arrow_result.data)
    none_result.data = None  # type: ignore[assignment]
    with pytest.raises(ValueError, match="No Arrow table available"):
        none_result.column_names()


def test_arrow_result_num_rows(basic_arrow_result: ArrowResult) -> None:
    """Test num_rows method."""
    rows = basic_arrow_result.num_rows()
    assert rows == 100  # From mock_arrow_table.num_rows

    # Test with None data
    none_result = ArrowResult(statement="SELECT * FROM users", data=basic_arrow_result.data)
    none_result.data = None  # type: ignore[assignment]
    with pytest.raises(ValueError, match="No Arrow table available"):
        none_result.num_rows()


def test_arrow_result_num_columns(basic_arrow_result: ArrowResult) -> None:
    """Test num_columns method."""
    columns = basic_arrow_result.num_columns()
    assert columns == 3  # From mock_arrow_table.num_columns

    # Test with None data
    none_result = ArrowResult(statement="SELECT * FROM users", data=basic_arrow_result.data)
    none_result.data = None  # type: ignore[assignment]
    with pytest.raises(ValueError, match="No Arrow table available"):
        none_result.num_columns()


@pytest.mark.parametrize(
    ("method_name", "expected_error"),
    [
        ("get_data", "No Arrow table available for this result"),
        ("column_names", "No Arrow table available"),
        ("num_rows", "No Arrow table available"),
        ("num_columns", "No Arrow table available"),
    ],
    ids=["get_data", "column_names", "num_rows", "num_columns"],
)
def test_arrow_result_methods_with_none_table(method_name: str, expected_error: str) -> None:
    """Test ArrowResult methods raise appropriate errors when table is None."""
    # Create a result with None data
    mock_table = Mock()
    none_result = ArrowResult(statement="SELECT * FROM users", data=mock_table)
    none_result.data = None  # type: ignore[assignment]

    method = getattr(none_result, method_name)
    with pytest.raises(ValueError, match=expected_error):
        method()


def test_result_inheritance_chain() -> None:
    """Test that all result classes properly inherit from StatementResult."""
    # Check inheritance
    assert issubclass(SQLResult, StatementResult)
    assert issubclass(ArrowResult, StatementResult)

    # Check they all implement required methods
    for result_class in [SQLResult, ArrowResult]:
        assert hasattr(result_class, "is_success")
        assert hasattr(result_class, "get_data")
        assert hasattr(result_class, "get_metadata")
        assert hasattr(result_class, "set_metadata")


@pytest.mark.parametrize(
    ("result_class", "init_args"),
    [
        (SQLResult, {"statement": "SELECT", "data": [], "operation_type": "SELECT"}),
        (SQLResult, {"statement": "INSERT", "data": [], "rows_affected": 1, "operation_type": "INSERT"}),
        (ArrowResult, {"statement": "SELECT", "data": Mock()}),
        (SQLResult, {"statement": "SCRIPT", "data": [], "operation_type": "SCRIPT"}),
    ],
    ids=["sql_result_select", "sql_result_execute", "arrow_result", "sql_result_script"],
)
def test_metadata_operations_all_result_types(result_class: type, init_args: dict[str, Any]) -> None:
    """Test metadata operations work for all result types."""
    # Initialize with metadata
    result = result_class(**init_args, metadata={"initial": "value"})

    # Test initial metadata
    assert result.get_metadata("initial") == "value"
    assert result.get_metadata("missing", "default") == "default"

    # Test setting metadata
    result.set_metadata("new_key", "new_value")
    assert result.get_metadata("new_key") == "new_value"

    # Test overwriting metadata
    result.set_metadata("initial", "updated_value")
    assert result.get_metadata("initial") == "updated_value"
