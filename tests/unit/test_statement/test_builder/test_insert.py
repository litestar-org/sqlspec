# ruff: noqa: RUF043
"""Comprehensive unit tests for InsertBuilder.

This module tests all InsertBuilder functionality including:
- Basic INSERT statement construction
- Column specification and value insertion
- Multi-row inserts
- Dictionary-based value insertion
- INSERT from SELECT statements
- Conflict resolution clauses
- Parameter binding and SQL injection prevention
- Error handling and edge cases
"""

from typing import Any
from unittest.mock import Mock

import pytest
from sqlglot import exp

from sqlspec.exceptions import SQLBuilderError
from sqlspec.statement.builder import InsertBuilder
from sqlspec.statement.builder._select import SelectBuilder


# Fixtures for common test data
@pytest.fixture
def sample_user_data() -> dict[str, Any]:
    """Sample user data for testing."""
    return {"name": "John Doe", "email": "john@example.com", "age": 30, "status": "active"}


@pytest.fixture
def sample_users_list() -> list[dict[str, Any]]:
    """Sample list of user dictionaries."""
    return [
        {"name": "John", "email": "john@example.com", "age": 25},
        {"name": "Jane", "email": "jane@example.com", "age": 28},
        {"name": "Bob", "email": "bob@example.com", "age": 35},
    ]


@pytest.fixture
def mock_select_builder() -> Mock:
    """Mock SelectBuilder for testing INSERT from SELECT."""
    mock_builder = Mock(spec=SelectBuilder)
    mock_builder._parameters = {"param_1": "active"}
    mock_expression = Mock(spec=exp.Select)
    mock_builder._expression = mock_expression
    mock_expression.copy.return_value = mock_expression
    return mock_builder


# Basic INSERT statement construction tests
def test_insert_basic_construction() -> None:
    """Test basic INSERT builder instantiation."""
    builder = InsertBuilder()
    assert isinstance(builder, InsertBuilder)
    assert builder._table is None
    assert builder._columns == []
    assert builder._values_added_count == 0


def test_insert_into_sets_table() -> None:
    """Test that into() method sets the target table."""
    builder = InsertBuilder().into("users")
    assert builder._table == "users"


def test_insert_into_returns_self_for_chaining() -> None:
    """Test that into() returns builder instance for method chaining."""
    builder = InsertBuilder()
    result = builder.into("users")
    assert result is builder


def test_insert_columns_sets_column_list() -> None:
    """Test that columns() method sets the column names."""
    builder = InsertBuilder().columns("name", "email", "age")
    assert builder._columns == ["name", "email", "age"]


def test_insert_columns_returns_self_for_chaining() -> None:
    """Test that columns() returns builder instance for method chaining."""
    builder = InsertBuilder()
    result = builder.columns("name", "email")
    assert result is builder


def test_insert_columns_empty_clears_column_list() -> None:
    """Test that calling columns() with no arguments clears column list."""
    builder = InsertBuilder().columns("name", "email").columns()
    assert builder._columns == []


# Values insertion tests
def test_insert_values_basic_insertion() -> None:
    """Test basic values insertion with single row."""
    builder = InsertBuilder().into("users").columns("name", "email").values("John", "john@example.com")
    query = builder.build()

    assert "INSERT INTO users" in query.sql
    assert "VALUES" in query.sql
    assert isinstance(query.parameters, dict)
    assert len(query.parameters) == 2
    assert "John" in query.parameters.values()
    assert "john@example.com" in query.parameters.values()


def test_insert_values_returns_self_for_chaining() -> None:
    """Test that values() returns builder instance for method chaining."""
    builder = InsertBuilder().into("users")
    result = builder.values("John", "john@example.com")
    assert result is builder


def test_insert_values_increments_counter() -> None:
    """Test that values() increments the values added counter."""
    builder = InsertBuilder().into("users").values("John", "john@example.com")
    assert builder._values_added_count == 1

    builder.values("Jane", "jane@example.com")
    assert builder._values_added_count == 2


def test_insert_values_multiple_rows() -> None:
    """Test multiple values() calls create multi-row INSERT."""
    builder = (
        InsertBuilder()
        .into("users")
        .columns("name", "email")
        .values("John", "john@example.com")
        .values("Jane", "jane@example.com")
    )
    query = builder.build()

    assert "INSERT INTO users" in query.sql
    assert "VALUES" in query.sql
    assert isinstance(query.parameters, dict)
    assert len(query.parameters) == 4  # 2 values × 2 rows


def test_insert_values_without_columns() -> None:
    """Test values insertion without explicitly setting columns."""
    builder = InsertBuilder().into("users").values("John", "john@example.com", 30)
    query = builder.build()

    assert "INSERT INTO users" in query.sql
    assert "VALUES" in query.sql
    assert len(query.parameters) == 3


# Values validation tests
def test_insert_values_requires_table() -> None:
    """Test that values() raises error when table not set."""
    builder = InsertBuilder()
    with pytest.raises(SQLBuilderError, match="target table must be set"):
        builder.values("John", "john@example.com")


def test_insert_values_validates_column_count() -> None:
    """Test that values() validates count matches columns when columns are set."""
    builder = InsertBuilder().into("users").columns("name", "email")

    with pytest.raises(SQLBuilderError, match="Number of values.*does not match.*columns"):
        builder.values("John")  # Only 1 value for 2 columns


def test_insert_values_allows_matching_column_count() -> None:
    """Test that values() succeeds when value count matches column count."""
    builder = InsertBuilder().into("users").columns("name", "email")
    # Should not raise an exception
    builder.values("John", "john@example.com")


@pytest.mark.parametrize(
    ("columns", "values", "should_succeed"),
    [
        ([], ["John"], True),  # No columns specified
        (["name"], ["John"], True),  # Matching count
        (["name", "email"], ["John", "john@example.com"], True),  # Matching count
        (["name"], ["John", "extra"], False),  # Too many values
        (["name", "email"], ["John"], False),  # Too few values
    ],
    ids=["no_columns", "single_match", "multiple_match", "too_many", "too_few"],
)
def test_insert_values_column_count_validation(columns: list[str], values: list[str], should_succeed: bool) -> None:
    """Test values() column count validation with various scenarios."""
    builder = InsertBuilder().into("users")
    if columns:
        builder.columns(*columns)

    if should_succeed:
        builder.values(*values)  # Should not raise
    else:
        with pytest.raises(SQLBuilderError):
            builder.values(*values)


# Dictionary-based insertion tests
def test_insert_values_from_dict_basic(sample_user_data: dict[str, Any]) -> None:
    """Test basic dictionary-based value insertion."""
    builder = InsertBuilder().into("users").values_from_dict(sample_user_data)
    query = builder.build()

    assert "INSERT INTO users" in query.sql
    assert "VALUES" in query.sql
    assert len(query.parameters) == len(sample_user_data)
    for value in sample_user_data.values():
        assert value in query.parameters.values()


def test_insert_values_from_dict_sets_columns(sample_user_data: dict[str, Any]) -> None:
    """Test that values_from_dict() automatically sets columns from dictionary keys."""
    builder = InsertBuilder().into("users").values_from_dict(sample_user_data)
    assert set(builder._columns) == set(sample_user_data.keys())


def test_insert_values_from_dict_validates_existing_columns() -> None:
    """Test that values_from_dict() validates against existing columns."""
    builder = InsertBuilder().into("users").columns("name", "email")

    with pytest.raises(SQLBuilderError, match="Dictionary keys.*do not match.*columns"):
        builder.values_from_dict({"name": "John", "age": 30})  # Missing email, extra age


def test_insert_values_from_dict_requires_table() -> None:
    """Test that values_from_dict() requires table to be set."""
    builder = InsertBuilder()
    with pytest.raises(SQLBuilderError, match="target table must be set"):
        builder.values_from_dict({"name": "John"})


def test_insert_values_from_dict_returns_self_for_chaining() -> None:
    """Test that values_from_dict() returns builder for method chaining."""
    builder = InsertBuilder().into("users")
    result = builder.values_from_dict({"name": "John"})
    assert result is builder


# Multiple dictionaries insertion tests
def test_insert_values_from_dicts_basic(sample_users_list: list[dict[str, Any]]) -> None:
    """Test basic multiple dictionary insertion."""
    builder = InsertBuilder().into("users").values_from_dicts(sample_users_list)
    query = builder.build()

    assert "INSERT INTO users" in query.sql
    assert "VALUES" in query.sql
    assert len(query.parameters) == len(sample_users_list) * len(sample_users_list[0])


def test_insert_values_from_dicts_empty_list() -> None:
    """Test values_from_dicts() with empty list."""
    builder = InsertBuilder().into("users")
    result = builder.values_from_dicts([])
    assert result is builder
    assert builder._values_added_count == 0


def test_insert_values_from_dicts_sets_columns_from_first_dict() -> None:
    """Test that values_from_dicts() sets columns from first dictionary."""
    sample_data = [{"name": "John", "email": "john@example.com"}, {"name": "Jane", "email": "jane@example.com"}]
    builder = InsertBuilder().into("users").values_from_dicts(sample_data)
    assert set(builder._columns) == {"name", "email"}


def test_insert_values_from_dicts_validates_consistent_keys() -> None:
    """Test that values_from_dicts() validates all dictionaries have same keys."""
    inconsistent_data = [
        {"name": "John", "email": "john@example.com"},
        {"name": "Jane", "age": 25},  # Different keys
    ]
    builder = InsertBuilder().into("users")

    with pytest.raises(SQLBuilderError, match="Dictionary at index.*do not match"):
        builder.values_from_dicts(inconsistent_data)


def test_insert_values_from_dicts_validates_against_existing_columns() -> None:
    """Test values_from_dicts() validation against pre-set columns."""
    builder = InsertBuilder().into("users").columns("name", "email")
    invalid_data = [{"name": "John", "age": 25}]  # Missing email, extra age

    with pytest.raises(SQLBuilderError, match="do not match expected keys"):
        builder.values_from_dicts(invalid_data)


# INSERT from SELECT tests
def test_insert_from_select_basic() -> None:
    """Test basic INSERT from SELECT statement."""
    from sqlspec.statement.builder import SelectBuilder

    # Use a real SelectBuilder instead of a mock
    select_builder = SelectBuilder().select("id", "name").from_("temp_users").where(("active", True))

    builder = InsertBuilder().into("users_backup").from_select(select_builder)
    query = builder.build()

    assert "INSERT INTO users_backup" in query.sql
    assert "SELECT" in query.sql
    assert isinstance(query.parameters, dict)
    # Should have the parameter from the WHERE clause
    assert True in query.parameters.values()


def test_insert_from_select_merges_parameters(mock_select_builder: Mock) -> None:
    """Test that from_select() merges parameters from SELECT builder."""
    builder = InsertBuilder().into("users_backup").from_select(mock_select_builder)
    # Parameters from mock_select_builder should be present
    assert "active" in builder._parameters.values()


def test_insert_from_select_requires_table() -> None:
    """Test that from_select() requires table to be set."""
    builder = InsertBuilder()
    mock_select = Mock(spec=SelectBuilder)

    with pytest.raises(SQLBuilderError, match="target table must be set"):
        builder.from_select(mock_select)


def test_insert_from_select_validates_select_expression() -> None:
    """Test that from_select() validates SELECT builder has valid expression."""
    builder = InsertBuilder().into("users_backup")
    invalid_select = Mock(spec=SelectBuilder)
    invalid_select._parameters = {}
    invalid_select._expression = None

    with pytest.raises(SQLBuilderError, match="must have a valid SELECT expression"):
        builder.from_select(invalid_select)


def test_insert_from_select_returns_self_for_chaining() -> None:
    """Test that from_select() returns builder for method chaining."""
    mock_builder = Mock(spec=SelectBuilder)
    mock_builder._parameters = {"param_1": "active"}
    mock_expression = Mock(spec=exp.Select)
    mock_builder._expression = mock_expression
    mock_expression.copy.return_value = mock_expression

    builder = InsertBuilder().into("users_backup")
    result = builder.from_select(mock_builder)
    assert result is builder


# Conflict resolution tests
def test_insert_on_conflict_do_nothing() -> None:
    """Test ON CONFLICT DO NOTHING clause addition."""
    builder = InsertBuilder().into("users").values("John", "john@example.com").on_conflict_do_nothing()
    # Should not raise an error and should return self
    assert isinstance(builder, InsertBuilder)


def test_insert_on_conflict_do_nothing_returns_self() -> None:
    """Test that on_conflict_do_nothing() returns builder for chaining."""
    builder = InsertBuilder().into("users")
    result = builder.on_conflict_do_nothing()
    assert result is builder


def test_insert_on_duplicate_key_update() -> None:
    """Test ON DUPLICATE KEY UPDATE clause addition."""
    builder = InsertBuilder().into("users").on_duplicate_key_update(status="updated", modified_at="NOW()")
    # Should not raise an error and should return self
    assert isinstance(builder, InsertBuilder)


def test_insert_on_duplicate_key_update_returns_self() -> None:
    """Test that on_duplicate_key_update() returns builder for chaining."""
    builder = InsertBuilder().into("users")
    result = builder.on_duplicate_key_update(status="updated")
    assert result is builder


# Error handling tests
def test_insert_expression_not_initialized() -> None:
    """Test error handling when expression is not initialized."""
    builder = InsertBuilder()
    builder._expression = None

    with pytest.raises(SQLBuilderError, match="expression not initialized"):
        builder._get_insert_expression()


def test_insert_wrong_expression_type() -> None:
    """Test error handling when expression is wrong type."""
    builder = InsertBuilder()
    builder._expression = Mock(spec=exp.Select)  # Wrong type

    with pytest.raises(SQLBuilderError, match="not an Insert instance"):
        builder._get_insert_expression()


# SQL injection prevention tests
@pytest.mark.parametrize(
    "malicious_value",
    [
        "'; DROP TABLE users; --",
        "1; DELETE FROM users; --",
        "' OR '1'='1",
        "<script>alert('xss')</script>",
    ],
    ids=["sql_injection_drop", "sql_injection_delete", "sql_injection_or", "xss_attempt"],
)
def test_insert_prevents_sql_injection_in_values(malicious_value: str) -> None:
    """Test that malicious values are properly parameterized."""
    builder = InsertBuilder().into("users").columns("name").values(malicious_value)
    query = builder.build()

    # Malicious SQL should not appear in the query
    assert "DROP TABLE" not in query.sql
    assert "DELETE FROM" not in query.sql
    assert "OR '1'='1'" not in query.sql
    assert "<script>" not in query.sql

    # Value should be parameterized
    assert malicious_value in query.parameters.values()


def test_insert_prevents_sql_injection_in_dict_values() -> None:
    """Test SQL injection prevention in dictionary values."""
    malicious_data = {"name": "'; DROP TABLE users; --", "email": "test@example.com"}
    builder = InsertBuilder().into("users").values_from_dict(malicious_data)
    query = builder.build()

    assert "DROP TABLE" not in query.sql
    assert malicious_data["name"] in query.parameters.values()


# Edge cases and boundary conditions
def test_insert_with_none_values() -> None:
    """Test INSERT with None values."""
    builder = InsertBuilder().into("users").columns("name", "email").values("John", None)
    query = builder.build()

    assert "INSERT INTO users" in query.sql
    assert None in query.parameters.values()


def test_insert_with_zero_values() -> None:
    """Test INSERT with zero/empty values."""
    builder = InsertBuilder().into("users").columns("count").values(0)
    query = builder.build()

    assert "INSERT INTO users" in query.sql
    assert 0 in query.parameters.values()


def test_insert_with_boolean_values() -> None:
    """Test INSERT with boolean values."""
    builder = InsertBuilder().into("users").columns("active", "verified").values(True, False)
    query = builder.build()

    assert "INSERT INTO users" in query.sql
    assert True in query.parameters.values()
    assert False in query.parameters.values()


@pytest.mark.parametrize(
    "test_values",
    [
        ([1, 2, 3, 4, 5]),  # List
        ({"key": "value"}),  # Dict
        ({"a", "b", "c"}),  # Set
    ],
    ids=["list_value", "dict_value", "set_value"],
)
def test_insert_with_complex_python_types(test_values: Any) -> None:
    """Test INSERT with complex Python data types."""
    builder = InsertBuilder().into("data").columns("content").values(test_values)
    query = builder.build()

    assert "INSERT INTO data" in query.sql
    assert test_values in query.parameters.values()


# Large data tests
def test_insert_large_number_of_values() -> None:
    """Test INSERT with large number of parameter values."""
    large_data = [{"id": i, "name": f"user_{i}"} for i in range(100)]
    builder = InsertBuilder().into("users").values_from_dicts(large_data)
    query = builder.build()

    assert "INSERT INTO users" in query.sql
    assert len(query.parameters) == 200  # 100 rows × 2 columns


def test_insert_very_long_string_value() -> None:
    """Test INSERT with very long string values."""
    long_string = "x" * 10000
    builder = InsertBuilder().into("logs").columns("message").values(long_string)
    query = builder.build()

    assert "INSERT INTO logs" in query.sql
    assert long_string in query.parameters.values()


# Method chaining and fluent interface tests
def test_insert_full_method_chaining() -> None:
    """Test complete method chaining workflow."""
    query = (
        InsertBuilder()
        .into("users")
        .columns("name", "email", "status")
        .values("John", "john@example.com", "active")
        .values("Jane", "jane@example.com", "active")
        .on_conflict_do_nothing()
        .build()
    )

    assert "INSERT INTO users" in query.sql
    assert "VALUES" in query.sql
    assert len(query.parameters) == 6  # 3 columns × 2 rows


def test_insert_mixed_value_methods() -> None:
    """Test mixing different value insertion methods."""
    builder = (
        InsertBuilder()
        .into("users")
        .columns("name", "email")
        .values("John", "john@example.com")
        .values_from_dict({"name": "Jane", "email": "jane@example.com"})
    )
    query = builder.build()

    assert "INSERT INTO users" in query.sql
    assert len(query.parameters) == 4  # 2 columns × 2 rows


# Expression type validation
def test_insert_expected_result_type() -> None:
    """Test that _expected_result_type returns correct type."""
    builder = InsertBuilder()
    from sqlspec.statement.result import ExecuteResult

    assert builder._expected_result_type == ExecuteResult


def test_insert_create_base_expression() -> None:
    """Test that _create_base_expression returns Insert expression."""
    builder = InsertBuilder()
    expression = builder._create_base_expression()
    assert isinstance(expression, exp.Insert)


# Build and SQL generation tests
def test_insert_build_returns_safe_query() -> None:
    """Test that build() returns SafeQuery object."""
    builder = InsertBuilder().into("users").values("John", "john@example.com")
    query = builder.build()

    from sqlspec.statement.builder._base import SafeQuery

    assert isinstance(query, SafeQuery)
    assert isinstance(query.sql, str)
    assert isinstance(query.parameters, dict)


def test_insert_to_statement_conversion() -> None:
    """Test conversion to SQL statement object."""
    builder = InsertBuilder().into("users").values("John", "john@example.com")
    statement = builder.to_statement()

    from sqlspec.statement.sql import SQL

    assert isinstance(statement, SQL)
