"""Unit tests for sqlspec.statement.result module.

Tests the result wrapper classes for handling different types of SQL operations
including SELECT, INSERT/UPDATE/DELETE, script execution, and Arrow results.
"""

from typing import Any
from unittest.mock import Mock

import pytest

from sqlspec.statement.result import (
    ArrowResult,
    ExecuteResult,
    ExecuteResultData,
    ScriptResult,
    ScriptResultData,
    SelectResult,
    StatementResult,
)
from sqlspec.typing import DictRow


def test_statement_result_is_abstract() -> None:
    """Test that StatementResult cannot be instantiated directly."""
    with pytest.raises(TypeError, match="abstract"):
        StatementResult(statement="test", data="test")  # type: ignore[abstract]


def test_statement_result_metadata_methods() -> None:
    """Test metadata getter and setter methods."""

    # Create a concrete implementation for testing
    class ConcreteResult(StatementResult[str]):
        def is_success(self) -> bool:
            return True

        def get_data(self) -> str:
            return self.data  # type: ignore[no-any-return]

    result = ConcreteResult(statement="test", data="test_data", metadata={"key1": "value1"})

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
def basic_select_result(sample_rows: list[dict[str, Any]]) -> SelectResult[list[dict[str, Any]]]:
    """Basic SelectResult for testing."""
    return SelectResult(
        statement="SELECT * FROM users",
        data=sample_rows,
        column_names=["id", "name", "email"],
        rows_affected=len(sample_rows),
    )


def test_select_result_initialization(sample_rows: list[dict[str, Any]]) -> None:
    """Test SelectResult initialization with various parameters."""
    result = SelectResult[dict[str, Any]](
        statement="SELECT * FROM users",
        data=sample_rows,
        column_names=["id", "name", "email"],
        total_count=10,
        has_more=True,
        execution_time=0.5,
        metadata={"query_id": "123"},
    )

    assert result.statement == "SELECT * FROM users"
    assert result.data == sample_rows
    assert result.column_names == ["id", "name", "email"]
    assert result.total_count == 10
    assert result.has_more is True
    assert result.execution_time == 0.5
    assert result.metadata == {"query_id": "123"}


def test_select_result_is_success(basic_select_result: SelectResult[list[dict[str, Any]]]) -> None:
    """Test is_success method."""
    assert basic_select_result.is_success() is True

    # Test with None data
    empty_result = SelectResult[DictRow](statement="SELECT * FROM empty", data=None)
    assert empty_result.is_success() is False


def test_select_result_get_data(basic_select_result: SelectResult[list[dict[str, Any]]]) -> None:
    """Test get_data method returns data."""
    data = basic_select_result.get_data()
    assert data == basic_select_result.data
    assert len(data) == 3


def test_select_result_get_first(sample_rows: list[dict[str, Any]]) -> None:
    """Test get_first method."""
    result = SelectResult[DictRow](statement="SELECT * FROM users", data=sample_rows)
    first_row = result.get_first()

    assert first_row == sample_rows[0]
    if first_row is not None:
        assert first_row["name"] == "Alice"

    # Test with empty data
    empty_result = SelectResult[DictRow](statement="SELECT * FROM empty", data=[])
    assert empty_result.get_first() is None


def test_select_result_get_count(basic_select_result: SelectResult[list[dict[str, Any]]]) -> None:
    """Test get_count method."""
    assert basic_select_result.get_count() == 3

    # Test with empty data
    empty_result = SelectResult[DictRow](statement="SELECT * FROM empty", data=[])
    assert empty_result.get_count() == 0


def test_select_result_is_empty(basic_select_result: SelectResult[list[dict[str, Any]]]) -> None:
    """Test is_empty method."""
    assert basic_select_result.is_empty() is False

    # Test with empty data
    empty_result = SelectResult[DictRow](statement="SELECT * FROM empty", data=[])
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
def test_select_result_row_operations(rows: list[dict[str, Any]], expected_count: int, expected_empty: bool) -> None:
    """Test row operations with different row counts."""
    result = SelectResult[DictRow](statement="SELECT * FROM test", data=rows)

    assert result.get_count() == expected_count
    assert result.is_empty() == expected_empty

    if rows:
        assert result.get_first() == rows[0]
    else:
        assert result.get_first() is None


@pytest.fixture
def basic_execute_result() -> ExecuteResult:
    """Basic ExecuteResult for testing."""
    return ExecuteResult(
        statement="INSERT INTO users VALUES (1, 'test')",
        data=None,
        rows_affected=5,
        last_inserted_id=123,
        operation_type="INSERT",
    )


def test_execute_result_initialization() -> None:
    """Test ExecuteResult initialization with various parameters."""
    result = ExecuteResult(
        statement="UPDATE users SET name = 'John'",
        data=None,
        rows_affected=10,
        last_inserted_id="uuid-123",
        operation_type="UPDATE",
        execution_time=0.25,
        metadata={"transaction_id": "tx-456"},
    )

    assert result.statement == "UPDATE users SET name = 'John'"
    assert result.rows_affected == 10
    assert result.last_inserted_id == "uuid-123"
    assert result.operation_type == "UPDATE"
    assert result.execution_time == 0.25
    assert result.metadata == {"transaction_id": "tx-456"}


def test_execute_result_is_success() -> None:
    """Test is_success method."""
    # Successful operation
    success_result = ExecuteResult(statement="INSERT INTO test", data=None, rows_affected=5)
    assert success_result.is_success() is True

    # Zero rows affected (still success)
    zero_result = ExecuteResult(statement="UPDATE test", data=None, rows_affected=0)
    assert zero_result.is_success() is True

    # None rows affected (failure)
    none_result = ExecuteResult(statement="UPDATE test", data=None, rows_affected=None)
    assert none_result.is_success() is False

    # Negative rows affected (failure)
    negative_result = ExecuteResult(statement="UPDATE test", data=None, rows_affected=-1)
    assert negative_result.is_success() is False


def test_execute_result_get_data(basic_execute_result: ExecuteResult) -> None:
    """Test get_data method returns ExecuteResultData."""
    data = basic_execute_result.get_data()

    assert isinstance(data, dict)
    assert data["rows_affected"] == 5
    assert data["last_inserted_id"] == 123
    assert data["operation_type"] == "INSERT"
    assert data["inserted_ids"] == []
    assert data["returning_data"] is None


def test_execute_result_get_affected_count(basic_execute_result: ExecuteResult) -> None:
    """Test get_affected_count method."""
    assert basic_execute_result.get_affected_count() == 5

    # Test with None rows_affected
    none_result = ExecuteResult(statement="UPDATE test", data=None, rows_affected=None)
    assert none_result.get_affected_count() == 0


def test_execute_result_get_inserted_id(basic_execute_result: ExecuteResult) -> None:
    """Test get_inserted_id method."""
    assert basic_execute_result.get_inserted_id() == 123

    # Test with None last_inserted_id
    none_result = ExecuteResult(statement="INSERT INTO test", data=None)
    assert none_result.get_inserted_id() is None


def test_execute_result_get_inserted_ids(basic_execute_result: ExecuteResult) -> None:
    """Test get_inserted_ids method."""
    # Default should be empty list
    assert basic_execute_result.get_inserted_ids() == []

    # Test after setting inserted_ids
    basic_execute_result.inserted_ids = [1, 2, 3]
    assert basic_execute_result.get_inserted_ids() == [1, 2, 3]


def test_execute_result_get_returning_data(basic_execute_result: ExecuteResult) -> None:
    """Test get_returning_data method."""
    # Default should be None
    assert basic_execute_result.get_returning_data() is None

    # Test after setting returning_data
    returning_data = [{"id": 1, "name": "test"}]
    basic_execute_result.returning_data = returning_data
    assert basic_execute_result.get_returning_data() == returning_data


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
    result = ExecuteResult(statement="SQL", data=None, operation_type=operation_type)

    assert result.was_inserted() == expected_insert
    assert result.was_updated() == expected_update
    assert result.was_deleted() == expected_delete


@pytest.fixture
def sample_statement_results() -> list[ExecuteResult]:
    """Sample statement results for script testing."""
    return [
        ExecuteResult(statement="INSERT INTO test", data=None, rows_affected=2, operation_type="INSERT"),
        ExecuteResult(statement="UPDATE test", data=None, rows_affected=1, operation_type="UPDATE"),
        ExecuteResult(statement="DELETE FROM test", data=None, rows_affected=3, operation_type="DELETE"),
    ]


@pytest.fixture
def basic_script_result(sample_statement_results: list[ExecuteResult]) -> ScriptResult:
    """Basic ScriptResult for testing."""
    # Cast to the expected type since ExecuteResult is a subclass of StatementResult
    statement_results: list[StatementResult[Any]] = sample_statement_results  # type: ignore[assignment]
    result = ScriptResult(
        statement="SCRIPT",
        data=None,
        statement_results=statement_results,
    )
    # Manually set the computed fields since they're normally set by add_statement_result
    result.total_statements = len(sample_statement_results)
    result.successful_statements = len(sample_statement_results)
    return result


def test_script_result_initialization() -> None:
    """Test ScriptResult initialization."""
    result = ScriptResult(statement="SCRIPT", data=None, execution_time=1.5, metadata={"script_id": "script-123"})

    assert result.statement == "SCRIPT"
    assert result.execution_time == 1.5
    assert result.metadata == {"script_id": "script-123"}
    assert result.statement_results == []
    assert result.total_statements == 0
    assert result.successful_statements == 0
    assert result.errors == []


def test_script_result_is_success(basic_script_result: ScriptResult) -> None:
    """Test is_success method."""
    # All statements successful, no errors
    assert basic_script_result.is_success() is True

    # Add an error
    basic_script_result.add_error("Test error")
    assert basic_script_result.is_success() is False

    # Reset errors but make total != successful
    error_result = ScriptResult(statement="SCRIPT", data=None)
    error_result.total_statements = 2
    error_result.successful_statements = 1
    assert error_result.is_success() is False


def test_script_result_get_data(basic_script_result: ScriptResult) -> None:
    """Test get_data method returns ScriptResultData."""
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
    script_result = ScriptResult(statement="SCRIPT", data=None)

    # Add successful statement
    success_stmt = ExecuteResult(statement="INSERT", data=None, rows_affected=5)
    script_result.add_statement_result(success_stmt)

    assert script_result.total_statements == 1
    assert script_result.successful_statements == 1
    assert len(script_result.statement_results) == 1

    # Add failed statement
    failed_stmt = ExecuteResult(statement="UPDATE", data=None, rows_affected=None)
    script_result.add_statement_result(failed_stmt)

    assert script_result.total_statements == 2
    assert script_result.successful_statements == 1  # Still 1
    assert len(script_result.statement_results) == 2


def test_script_result_add_error(basic_script_result: ScriptResult) -> None:
    """Test add_error method."""
    basic_script_result.add_error("First error")
    basic_script_result.add_error("Second error")

    assert len(basic_script_result.errors) == 2
    assert basic_script_result.errors == ["First error", "Second error"]


def test_script_result_get_statement_result(basic_script_result: ScriptResult) -> None:
    """Test get_statement_result method."""
    # Valid indices
    assert basic_script_result.get_statement_result(0) is not None
    assert basic_script_result.get_statement_result(1) is not None
    assert basic_script_result.get_statement_result(2) is not None

    # Invalid indices
    assert basic_script_result.get_statement_result(-1) is None
    assert basic_script_result.get_statement_result(3) is None
    assert basic_script_result.get_statement_result(100) is None


def test_script_result_get_total_rows_affected(basic_script_result: ScriptResult) -> None:
    """Test get_total_rows_affected method."""
    assert basic_script_result.get_total_rows_affected() == 6  # 2 + 1 + 3

    # Test with statements that have None rows_affected
    script_with_none = ScriptResult(statement="SCRIPT", data=None)
    script_with_none.add_statement_result(ExecuteResult(statement="INSERT", data=None, rows_affected=5))
    script_with_none.add_statement_result(ExecuteResult(statement="UPDATE", data=None, rows_affected=None))
    script_with_none.add_statement_result(ExecuteResult(statement="DELETE", data=None, rows_affected=3))

    assert script_with_none.get_total_rows_affected() == 8  # 5 + 0 + 3


def test_script_result_get_errors(basic_script_result: ScriptResult) -> None:
    """Test get_errors method."""
    # Initially no errors
    assert basic_script_result.get_errors() == []

    # Add errors
    basic_script_result.add_error("Error 1")
    basic_script_result.add_error("Error 2")

    errors = basic_script_result.get_errors()
    assert errors == ["Error 1", "Error 2"]


def test_script_result_has_errors(basic_script_result: ScriptResult) -> None:
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

    # Without arrow table
    none_result = ArrowResult(statement="SELECT * FROM users", data=None)
    assert none_result.is_success() is False


def test_arrow_result_get_data(basic_arrow_result: ArrowResult, mock_arrow_table: Mock) -> None:
    """Test get_data method."""
    data = basic_arrow_result.get_data()
    assert data is mock_arrow_table

    # Test with None data
    none_result = ArrowResult(statement="SELECT * FROM users", data=None)
    with pytest.raises(ValueError, match="No Arrow table available"):
        none_result.get_data()


def test_arrow_result_column_names(basic_arrow_result: ArrowResult) -> None:
    """Test column_names method."""
    columns = basic_arrow_result.column_names()
    assert columns == ["id", "name", "value"]

    # Test with None data
    none_result = ArrowResult(statement="SELECT * FROM users", data=None)
    with pytest.raises(ValueError, match="No Arrow table available"):
        none_result.column_names()


def test_arrow_result_num_rows(basic_arrow_result: ArrowResult) -> None:
    """Test num_rows method."""
    rows = basic_arrow_result.num_rows()
    assert rows == 100

    # Test with None data
    none_result = ArrowResult(statement="SELECT * FROM users", data=None)
    with pytest.raises(ValueError, match="No Arrow table available"):
        none_result.num_rows()


def test_arrow_result_num_columns(basic_arrow_result: ArrowResult) -> None:
    """Test num_columns method."""
    columns = basic_arrow_result.num_columns()
    assert columns == 3

    # Test with None data
    none_result = ArrowResult(statement="SELECT * FROM users", data=None)
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
    """Test ArrowResult methods when data is None."""
    result = ArrowResult(statement="SELECT * FROM users", data=None)
    method = getattr(result, method_name)

    with pytest.raises(ValueError, match=expected_error):
        method()


def test_execute_result_data_type_hints() -> None:
    """Test ExecuteResultData TypedDict structure."""
    # This test ensures the TypedDict structure is correct
    data: ExecuteResultData = {
        "rows_affected": 5,
        "last_inserted_id": 123,
        "inserted_ids": [1, 2, 3],
        "returning_data": [{"id": 1}],
        "operation_type": "INSERT",
    }

    assert data["rows_affected"] == 5
    assert data["last_inserted_id"] == 123
    assert data["inserted_ids"] == [1, 2, 3]
    assert data["returning_data"] == [{"id": 1}]
    assert data["operation_type"] == "INSERT"


def test_script_result_data_type_hints() -> None:
    """Test ScriptResultData TypedDict structure."""
    # This test ensures the TypedDict structure is correct
    data: ScriptResultData = {
        "total_statements": 3,
        "successful_statements": 2,
        "failed_statements": 1,
        "errors": ["Error message"],
        "statement_results": [{"rows_affected": 5}],
        "total_rows_affected": 10,
    }

    assert data["total_statements"] == 3
    assert data["successful_statements"] == 2
    assert data["failed_statements"] == 1
    assert data["errors"] == ["Error message"]
    assert data["statement_results"] == [{"rows_affected": 5}]
    assert data["total_rows_affected"] == 10


def test_result_inheritance_chain() -> None:
    """Test that all result classes properly inherit from StatementResult."""
    # Check inheritance
    assert issubclass(SelectResult, StatementResult)
    assert issubclass(ExecuteResult, StatementResult)
    assert issubclass(ScriptResult, StatementResult)
    assert issubclass(ArrowResult, StatementResult)

    # Check they all implement required methods
    for result_class in [SelectResult, ExecuteResult, ScriptResult, ArrowResult]:
        assert hasattr(result_class, "is_success")
        assert hasattr(result_class, "get_data")
        assert hasattr(result_class, "get_metadata")
        assert hasattr(result_class, "set_metadata")


@pytest.mark.parametrize(
    ("result_class", "init_args"),
    [
        (SelectResult, {"statement": "SELECT", "data": []}),
        (ExecuteResult, {"statement": "INSERT", "data": None, "rows_affected": 1}),
        (ScriptResult, {"statement": "SCRIPT", "data": None}),
        (ArrowResult, {"statement": "SELECT", "data": None}),
    ],
    ids=["select_result", "execute_result", "script_result", "arrow_result"],
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
