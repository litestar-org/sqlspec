"""Unit tests for sqlspec.statement.result module."""

from typing import TYPE_CHECKING, Any, Optional
from unittest.mock import Mock

import pytest

from sqlspec.statement.result import ArrowResult, SQLResult, StatementResult
from sqlspec.statement.sql import SQL

if TYPE_CHECKING:
    pass


# Test StatementResult abstract base class
def test_statement_result_is_abstract() -> None:
    """Test that StatementResult cannot be instantiated directly."""
    with pytest.raises(TypeError, match="abstract"):
        StatementResult(statement=SQL("test"), data=["test"])  # type: ignore[abstract]


def test_statement_result_metadata_operations() -> None:
    """Test metadata getter and setter methods on concrete implementation."""

    # Create a concrete implementation for testing
    class ConcreteResult(StatementResult[dict[str, Any]]):
        def is_success(self) -> bool:
            return True

        def get_data(self) -> Any:
            return self.data

    result = ConcreteResult(
        statement=SQL("test"), data=[{"test": "data"}], metadata={"key1": "value1", "key2": "value2"}
    )

    # Test metadata access
    assert result.get_metadata("key1") == "value1"
    assert result.get_metadata("key2") == "value2"
    assert result.get_metadata("missing", "default") == "default"
    assert result.get_metadata("missing") is None

    # Test metadata setting
    result.set_metadata("key3", "value3")
    assert result.get_metadata("key3") == "value3"

    # Test overwriting metadata
    result.set_metadata("key1", "updated")
    assert result.get_metadata("key1") == "updated"


# Test SQLResult for SELECT operations
@pytest.mark.parametrize(
    "data,column_names,rows_affected,expected_success",
    [
        ([{"id": 1}, {"id": 2}], ["id"], 2, True),
        ([], ["id"], 0, True),  # Empty result set is still successful
        ([{"id": 1}], ["id"], None, True),  # None rows_affected is ok for SELECT
        ([{"id": 1}], ["id"], -1, False),  # Negative rows_affected indicates failure
        (None, [], None, False),  # None data indicates failure
    ],
    ids=["normal_select", "empty_select", "none_rows_affected", "negative_rows_affected", "none_data"],
)
def test_sql_result_select_is_success(
    data: Optional[list[dict[str, Any]]], column_names: list[str], rows_affected: Optional[int], expected_success: bool
) -> None:
    """Test is_success method for SELECT operations."""
    result = SQLResult[dict[str, Any]](
        statement=SQL("SELECT * FROM users"),
        data=data,
        column_names=column_names,
        rows_affected=rows_affected or 0,
        operation_type="SELECT",
    )
    assert result.is_success() == expected_success


def test_sql_result_select_initialization() -> None:
    """Test SQLResult initialization for SELECT operations."""
    data = [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": "bob@example.com"},
    ]

    result = SQLResult[dict[str, Any]](
        statement=SQL("SELECT * FROM users"),
        data=data,
        column_names=["id", "name", "email"],
        rows_affected=2,
        operation_type="SELECT",
        total_count=100,  # Total rows in table (for pagination context)
        has_more=True,  # More pages available
        execution_time=0.5,
        metadata={"query_id": "123"},
    )

    assert result.statement.sql == "SELECT * FROM users"
    assert result.data == data
    assert result.column_names == ["id", "name", "email"]
    assert result.rows_affected == 2
    assert result.operation_type == "SELECT"
    assert result.total_count == 100
    assert result.has_more is True
    assert result.execution_time == 0.5
    assert result.metadata == {"query_id": "123"}


def test_sql_result_select_auto_column_names() -> None:
    """Test automatic column name inference from dict data."""
    data = [{"id": 1, "name": "Test", "active": True}]

    result = SQLResult[dict[str, Any]](statement=SQL("SELECT * FROM users"), data=data, operation_type="SELECT")

    # Column names should be inferred from first row
    assert result.column_names == ["id", "name", "active"]


def test_sql_result_select_methods() -> None:
    """Test SELECT-specific methods."""
    data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}, {"id": 3, "name": "Charlie"}]

    result = SQLResult[dict[str, Any]](
        statement=SQL("SELECT * FROM users"), data=data, column_names=["id", "name"], operation_type="SELECT"
    )

    # Test get_data
    assert result.get_data() == data

    # Test get_first
    assert result.get_first() == {"id": 1, "name": "Alice"}

    # Test get_count
    assert result.get_count() == 3

    # Test is_empty
    assert result.is_empty() is False

    # Test num_columns
    assert result.num_columns == 2


@pytest.mark.parametrize(
    "data,expected_first,expected_count,expected_empty",
    [([], None, 0, True), ([{"id": 1}], {"id": 1}, 1, False), ([{"id": 1}, {"id": 2}], {"id": 1}, 2, False)],
    ids=["empty", "single_row", "multiple_rows"],
)
def test_sql_result_select_row_operations(
    data: list[dict[str, Any]], expected_first: Optional[dict[str, Any]], expected_count: int, expected_empty: bool
) -> None:
    """Test row operations with different data sets."""
    result = SQLResult[dict[str, Any]](statement=SQL("SELECT * FROM test"), data=data, operation_type="SELECT")

    assert result.get_first() == expected_first
    assert result.get_count() == expected_count
    assert result.is_empty() == expected_empty


# Test SQLResult for DML operations
@pytest.mark.parametrize(
    "operation_type,rows_affected,expected_success",
    [
        ("INSERT", 1, True),
        ("UPDATE", 5, True),
        ("DELETE", 3, True),
        ("INSERT", 0, True),  # 0 rows affected is still success
        ("DELETE", -1, False),  # Negative rows_affected is failure
    ],
)
def test_sql_result_dml_is_success(operation_type: str, rows_affected: int, expected_success: bool) -> None:
    """Test is_success method for DML operations."""
    result = SQLResult[dict[str, Any]](
        statement=SQL(f"{operation_type} ..."),
        data=[],  # DML typically has empty data unless RETURNING
        rows_affected=rows_affected,
        operation_type=operation_type,
    )
    assert result.is_success() == expected_success


def test_sql_result_dml_operations() -> None:
    """Test DML-specific methods and attributes."""
    result = SQLResult[dict[str, Any]](
        statement=SQL("INSERT INTO users VALUES (?, ?)"),
        data=[],
        rows_affected=1,
        operation_type="INSERT",
        last_inserted_id=123,
        inserted_ids=[123],
        metadata={"transaction_id": "tx-456"},
    )

    assert result.get_affected_count() == 1
    assert result.get_inserted_id() == 123
    assert result.get_inserted_ids() == [123]
    assert result.was_inserted() is True
    assert result.was_updated() is False
    assert result.was_deleted() is False


@pytest.mark.parametrize(
    "operation_type,expected_insert,expected_update,expected_delete",
    [
        ("INSERT", True, False, False),
        ("insert", True, False, False),  # Case insensitive
        ("UPDATE", False, True, False),
        ("update", False, True, False),
        ("DELETE", False, False, True),
        ("delete", False, False, True),
        ("MERGE", False, False, False),
        ("SELECT", False, False, False),
    ],
)
def test_sql_result_operation_type_checks(
    operation_type: str, expected_insert: bool, expected_update: bool, expected_delete: bool
) -> None:
    """Test operation type checking methods."""
    result = SQLResult[dict[str, Any]](statement=SQL("..."), data=[], operation_type=operation_type)

    assert result.was_inserted() == expected_insert
    assert result.was_updated() == expected_update
    assert result.was_deleted() == expected_delete


def test_sql_result_dml_with_returning() -> None:
    """Test DML operations with RETURNING clause."""
    returned_data = [
        {"id": 1, "name": "Alice", "created_at": "2023-01-01"},
        {"id": 2, "name": "Bob", "created_at": "2023-01-01"},
    ]

    result = SQLResult[dict[str, Any]](
        statement=SQL("INSERT INTO users ... RETURNING *"),
        data=returned_data,
        column_names=["id", "name", "created_at"],
        rows_affected=2,
        operation_type="INSERT",
        inserted_ids=[1, 2],
    )

    assert result.is_success() is True
    assert result.get_data() == returned_data
    assert result.get_returning_data() == returned_data
    assert result.get_affected_count() == 2
    assert result.get_inserted_ids() == [1, 2]


# Test SQLResult for script execution
def test_sql_result_script_initialization() -> None:
    """Test SQLResult initialization for script execution."""
    result = SQLResult[Any](
        statement=SQL("-- Script\nINSERT ...; UPDATE ...;"),
        data=[],
        operation_type="SCRIPT",
        execution_time=1.5,
        metadata={"script_id": "script-123"},
    )

    assert result.operation_type == "SCRIPT"
    assert result.statement_results == []
    assert result.errors == []
    assert result.total_statements == 0
    assert result.successful_statements == 0


def test_sql_result_script_execution() -> None:
    """Test script execution tracking."""
    script_result = SQLResult[Any](statement=SQL("SCRIPT"), data=[], operation_type="SCRIPT")

    # Add successful statements
    stmt1 = SQLResult[Any](statement=SQL("INSERT INTO test"), data=[], rows_affected=2, operation_type="INSERT")
    script_result.add_statement_result(stmt1)

    stmt2 = SQLResult[Any](statement=SQL("UPDATE test"), data=[], rows_affected=3, operation_type="UPDATE")
    script_result.add_statement_result(stmt2)

    # Add failed statement
    stmt3 = SQLResult[Any](
        statement=SQL("DELETE FROM test"),
        data=[],
        rows_affected=-1,  # Indicates failure
        operation_type="DELETE",
    )
    script_result.add_statement_result(stmt3)

    assert script_result.total_statements == 3
    assert script_result.successful_statements == 2
    assert script_result.is_success() is False  # Not all statements succeeded


def test_sql_result_script_errors() -> None:
    """Test script error tracking."""
    script_result = SQLResult[Any](statement=SQL("SCRIPT"), data=[], operation_type="SCRIPT")

    # Add errors
    script_result.add_error("Syntax error in statement 1")
    script_result.add_error("Connection lost")

    assert script_result.has_errors() is True
    assert script_result.get_errors() == ["Syntax error in statement 1", "Connection lost"]
    assert script_result.is_success() is False


def test_sql_result_script_get_data() -> None:
    """Test get_data returns summary for script execution."""
    script_result = SQLResult[Any](statement=SQL("SCRIPT"), data=[], operation_type="SCRIPT")

    # Add some statement results
    for i in range(3):
        stmt = SQLResult[Any](statement=SQL(f"Statement {i}"), data=[], rows_affected=i + 1, operation_type="INSERT")
        script_result.add_statement_result(stmt)

    data = script_result.get_data()
    assert isinstance(data, dict)
    assert data["total_statements"] == 3
    assert data["successful_statements"] == 3
    assert data["failed_statements"] == 0
    assert data["errors"] == []
    assert len(data["statement_results"]) == 3
    assert data["total_rows_affected"] == 6  # 1 + 2 + 3


def test_sql_result_script_statement_access() -> None:
    """Test accessing individual statement results."""
    script_result = SQLResult[Any](statement=SQL("SCRIPT"), data=[], operation_type="SCRIPT")

    # Add statements
    stmts = []
    for i in range(3):
        stmt = SQLResult[Any](statement=SQL(f"Statement {i}"), data=[], rows_affected=1, operation_type="INSERT")
        stmts.append(stmt)
        script_result.add_statement_result(stmt)

    # Test valid access
    assert script_result.get_statement_result(0) == stmts[0]
    assert script_result.get_statement_result(1) == stmts[1]
    assert script_result.get_statement_result(2) == stmts[2]

    # Test invalid access
    assert script_result.get_statement_result(-1) is None
    assert script_result.get_statement_result(3) is None


def test_sql_result_script_total_rows_affected() -> None:
    """Test calculating total rows affected across all statements."""
    script_result = SQLResult[Any](statement=SQL("SCRIPT"), data=[], operation_type="SCRIPT")

    # Add statements with various rows_affected values
    script_result.add_statement_result(
        SQLResult[Any](statement=SQL("INSERT"), data=[], rows_affected=5, operation_type="INSERT")
    )
    script_result.add_statement_result(
        SQLResult[Any](statement=SQL("UPDATE"), data=[], rows_affected=3, operation_type="UPDATE")
    )
    script_result.add_statement_result(
        SQLResult[Any](statement=SQL("DELETE"), data=[], rows_affected=-1, operation_type="DELETE")  # Failed
    )

    assert script_result.get_total_rows_affected() == 8  # 5 + 3 + 0
    assert script_result.num_rows == 8  # Property alias


# Test ArrowResult
def test_arrow_result_initialization() -> None:
    """Test ArrowResult initialization."""
    mock_table = Mock()
    mock_table.column_names = ["id", "name", "value"]
    mock_table.num_rows = 100
    mock_table.num_columns = 3

    result = ArrowResult(
        statement=SQL("SELECT * FROM users"),
        data=mock_table,
        schema={"version": "1.0", "fields": ["id", "name", "value"]},
        execution_time=0.75,
        metadata={"query_id": "arrow-123"},
    )

    assert result.statement.sql == "SELECT * FROM users"
    assert result.data == mock_table
    assert result.schema == {"version": "1.0", "fields": ["id", "name", "value"]}
    assert result.execution_time == 0.75
    assert result.metadata == {"query_id": "arrow-123"}


@pytest.mark.parametrize("has_data,expected_success", [(True, True), (False, False)])
def test_arrow_result_is_success(has_data: bool, expected_success: bool) -> None:
    """Test is_success method for ArrowResult."""
    mock_table = Mock() if has_data else None
    result = ArrowResult(
        statement=SQL("SELECT * FROM users"),
        data=mock_table,  # type: ignore[arg-type]
    )
    assert result.is_success() == expected_success


def test_arrow_result_get_data() -> None:
    """Test get_data method for ArrowResult."""
    mock_table = Mock()
    result = ArrowResult(statement=SQL("SELECT * FROM users"), data=mock_table)

    assert result.get_data() == mock_table

    # Test with None data
    none_result = ArrowResult(
        statement=SQL("SELECT * FROM users"),
        data=None,  # type: ignore[arg-type]
    )
    with pytest.raises(ValueError, match="No Arrow table available"):
        none_result.get_data()


def test_arrow_result_properties() -> None:
    """Test ArrowResult properties."""
    mock_table = Mock()
    mock_table.column_names = ["id", "name", "value"]
    mock_table.num_rows = 100
    mock_table.num_columns = 3

    result = ArrowResult(statement=SQL("SELECT * FROM users"), data=mock_table)

    assert result.column_names == ["id", "name", "value"]
    assert result.num_rows == 100
    assert result.num_columns == 3


@pytest.mark.parametrize("property_name", ["column_names", "num_rows", "num_columns"])
def test_arrow_result_properties_with_none_data(property_name: str) -> None:
    """Test ArrowResult properties raise error when data is None."""
    result = ArrowResult(
        statement=SQL("SELECT * FROM users"),
        data=None,  # type: ignore[arg-type]
    )

    with pytest.raises(ValueError, match="No Arrow table available"):
        _ = getattr(result, property_name)


# Test inheritance and common functionality
def test_result_inheritance() -> None:
    """Test that all result classes properly inherit from StatementResult."""
    assert issubclass(SQLResult, StatementResult)
    assert issubclass(ArrowResult, StatementResult)

    # Check required methods are implemented
    for result_class in [SQLResult, ArrowResult]:
        assert hasattr(result_class, "is_success")
        assert hasattr(result_class, "get_data")
        assert hasattr(result_class, "get_metadata")
        assert hasattr(result_class, "set_metadata")


@pytest.mark.parametrize(
    "result_factory",
    [
        lambda: SQLResult(statement=SQL("SELECT"), data=[], operation_type="SELECT"),
        lambda: SQLResult(statement=SQL("INSERT"), data=[], rows_affected=1, operation_type="INSERT"),
        lambda: SQLResult(statement=SQL("SCRIPT"), data=[], operation_type="SCRIPT"),
        lambda: ArrowResult(statement=SQL("SELECT"), data=Mock()),
    ],
    ids=["sql_select", "sql_dml", "sql_script", "arrow"],
)
def test_common_metadata_operations(result_factory: Any) -> None:
    """Test metadata operations work consistently across all result types."""
    result = result_factory()

    # Test setting and getting metadata
    result.set_metadata("test_key", "test_value")
    assert result.get_metadata("test_key") == "test_value"

    # Test default value
    assert result.get_metadata("missing", "default") == "default"
    assert result.get_metadata("missing") is None

    # Test overwriting
    result.set_metadata("test_key", "updated")
    assert result.get_metadata("test_key") == "updated"


def test_sql_result_edge_cases() -> None:
    """Test edge cases and special scenarios."""
    # Test with None data but operation type SELECT
    result = SQLResult[dict[str, Any]](
        statement=SQL("SELECT * FROM empty"),
        data=None,  # type: ignore[arg-type]
        operation_type="SELECT",
    )
    assert result.is_success() is False
    assert result.get_count() == 0
    assert result.is_empty() is True

    # Test total_count inference
    result_with_data = SQLResult[dict[str, Any]](
        statement=SQL("SELECT * FROM users"), data=[{"id": 1}, {"id": 2}], operation_type="SELECT"
    )
    assert result_with_data.total_count == 2  # Should be inferred from data length


def test_sql_result_returning_data_alias() -> None:
    """Test that get_returning_data is an alias for get_data."""
    data = [{"id": 1, "name": "Test"}]
    result = SQLResult[dict[str, Any]](
        statement=SQL("INSERT INTO users ... RETURNING *"), data=data, operation_type="INSERT"
    )

    assert result.get_returning_data() == result.get_data()
    assert result.get_returning_data() == data
