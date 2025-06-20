"""Unit tests for InsertBuilder functionality.

This module tests all InsertBuilder functionality including:
- Basic INSERT statement construction
- Column specification and value insertion
- Multi-row inserts
- Dictionary-based value insertion
- INSERT from SELECT statements
- Conflict resolution clauses (ON CONFLICT, ON DUPLICATE KEY)
- Parameter binding and SQL injection prevention
- Error handling and edge cases
"""

from typing import TYPE_CHECKING, Any
from unittest.mock import Mock

import pytest
from sqlglot import exp

from sqlspec.exceptions import SQLBuilderError
from sqlspec.statement.builder import InsertBuilder, SelectBuilder
from sqlspec.statement.builder.base import SafeQuery
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL

if TYPE_CHECKING:
    pass


# Test basic INSERT construction
def test_insert_builder_initialization() -> None:
    """Test InsertBuilder initialization."""
    builder = InsertBuilder()
    assert isinstance(builder, InsertBuilder)
    assert builder._table is None
    assert builder._columns == []
    assert builder._values_added_count == 0


def test_insert_into_table() -> None:
    """Test setting target table with into()."""
    builder = InsertBuilder().into("users")
    assert builder._table == "users"


def test_insert_into_returns_self() -> None:
    """Test that into() returns builder for chaining."""
    builder = InsertBuilder()
    result = builder.into("users")
    assert result is builder


# Test column specification
@pytest.mark.parametrize(
    "columns,expected",
    [
        (["name"], ["name"]),
        (["name", "email"], ["name", "email"]),
        (["id", "name", "email", "age"], ["id", "name", "email", "age"]),
        ([], []),  # Empty columns clears list
    ],
    ids=["single_column", "two_columns", "multiple_columns", "empty_columns"],
)
def test_insert_columns_specification(columns: list[str], expected: list[str]) -> None:
    """Test column specification with various inputs."""
    builder = InsertBuilder().columns(*columns)
    assert builder._columns == expected


def test_insert_columns_returns_self() -> None:
    """Test that columns() returns builder for chaining."""
    builder = InsertBuilder()
    result = builder.columns("name", "email")
    assert result is builder


# Test value insertion
@pytest.mark.parametrize(
    "values,expected_param_count",
    [
        (["John"], 1),
        (["John", "john@example.com"], 2),
        (["John", "john@example.com", 30, True], 4),
        ([None], 1),
        ([0], 1),
        ([True, False], 2),
    ],
    ids=["single_value", "two_values", "multiple_values", "none_value", "zero_value", "boolean_values"],
)
def test_insert_values_basic(values: list[Any], expected_param_count: int) -> None:
    """Test basic value insertion with various types."""
    builder = InsertBuilder().into("users").values(*values)
    query = builder.build()

    assert 'INSERT INTO "users"' in query.sql or "INSERT INTO users" in query.sql
    assert "VALUES" in query.sql
    assert len(query.parameters) == expected_param_count
    for value in values:
        assert value in query.parameters.values()


def test_insert_values_increments_counter() -> None:
    """Test that values() increments the row counter."""
    builder = InsertBuilder().into("users")

    builder.values("John", "john@example.com")
    assert builder._values_added_count == 1

    builder.values("Jane", "jane@example.com")
    assert builder._values_added_count == 2


def test_insert_values_returns_self() -> None:
    """Test that values() returns builder for chaining."""
    builder = InsertBuilder().into("users")
    result = builder.values("John", "john@example.com")
    assert result is builder


# Test multi-row inserts
def test_insert_multiple_rows() -> None:
    """Test inserting multiple rows with multiple values() calls."""
    builder = (
        InsertBuilder()
        .into("users")
        .columns("name", "email")
        .values("John", "john@example.com")
        .values("Jane", "jane@example.com")
        .values("Bob", "bob@example.com")
    )
    query = builder.build()

    assert 'INSERT INTO "users"' in query.sql or "INSERT INTO users" in query.sql
    assert "VALUES" in query.sql
    assert len(query.parameters) == 6  # 3 rows x 2 columns


# Test value validation
def test_insert_values_requires_table() -> None:
    """Test that values() requires table to be set."""
    builder = InsertBuilder()
    with pytest.raises(SQLBuilderError, match="target table must be set"):
        builder.values("John")


@pytest.mark.parametrize(
    "columns,values,should_succeed",
    [
        ([], ["John"], True),  # No columns specified, any values OK
        (["name"], ["John"], True),  # Matching count
        (["name", "email"], ["John", "john@example.com"], True),  # Matching count
        (["name"], ["John", "extra"], False),  # Too many values
        (["name", "email"], ["John"], False),  # Too few values
        (["name", "email", "age"], ["John", "john@example.com"], False),  # Too few values
    ],
    ids=["no_columns", "single_match", "multiple_match", "too_many", "too_few", "too_few_three"],
)
def test_insert_values_column_validation(columns: list[str], values: list[str], should_succeed: bool) -> None:
    """Test column count validation in values()."""
    builder = InsertBuilder().into("users")
    if columns:
        builder.columns(*columns)

    if should_succeed:
        builder.values(*values)  # Should not raise
    else:
        with pytest.raises(SQLBuilderError, match="Number of values.*does not match.*columns"):
            builder.values(*values)


# Test dictionary-based insertion
def test_insert_values_from_dict_basic() -> None:
    """Test basic dictionary value insertion."""
    data = {"name": "John Doe", "email": "john@example.com", "age": 30}
    builder = InsertBuilder().into("users").values_from_dict(data)
    query = builder.build()

    assert 'INSERT INTO "users"' in query.sql or "INSERT INTO users" in query.sql
    assert len(query.parameters) == len(data)
    for value in data.values():
        assert value in query.parameters.values()


def test_insert_values_from_dict_sets_columns() -> None:
    """Test that values_from_dict() automatically sets columns."""
    data = {"name": "John", "email": "john@example.com"}
    builder = InsertBuilder().into("users").values_from_dict(data)
    assert set(builder._columns) == set(data.keys())


def test_insert_values_from_dict_validates_columns() -> None:
    """Test that values_from_dict() validates against existing columns."""
    builder = InsertBuilder().into("users").columns("name", "email")
    data = {"name": "John", "age": 30}  # Wrong keys

    with pytest.raises(SQLBuilderError, match="Dictionary keys.*do not match.*columns"):
        builder.values_from_dict(data)


def test_insert_values_from_dict_returns_self() -> None:
    """Test that values_from_dict() returns builder for chaining."""
    builder = InsertBuilder().into("users")
    result = builder.values_from_dict({"name": "John"})
    assert result is builder


# Test multiple dictionaries insertion
def test_insert_values_from_dicts_basic() -> None:
    """Test inserting multiple rows from list of dicts."""
    data = [
        {"name": "John", "email": "john@example.com"},
        {"name": "Jane", "email": "jane@example.com"},
        {"name": "Bob", "email": "bob@example.com"},
    ]
    builder = InsertBuilder().into("users").values_from_dicts(data)
    query = builder.build()

    assert 'INSERT INTO "users"' in query.sql or "INSERT INTO users" in query.sql
    assert len(query.parameters) == 6  # 3 rows x 2 columns


def test_insert_values_from_dicts_empty_list() -> None:
    """Test values_from_dicts() with empty list."""
    builder = InsertBuilder().into("users")
    result = builder.values_from_dicts([])
    assert result is builder
    assert builder._values_added_count == 0


def test_insert_values_from_dicts_validates_consistency() -> None:
    """Test that all dicts must have same keys."""
    inconsistent_data = [
        {"name": "John", "email": "john@example.com"},
        {"name": "Jane", "age": 25},  # Different keys
    ]
    builder = InsertBuilder().into("users")

    with pytest.raises(SQLBuilderError, match="Dictionary at index.*do not match"):
        builder.values_from_dicts(inconsistent_data)  # type: ignore[arg-type]


# Test INSERT from SELECT
def test_insert_from_select_basic() -> None:
    """Test INSERT from SELECT statement."""
    select_builder = SelectBuilder().select("id", "name").from_("temp_users").where(("active", True))

    builder = InsertBuilder().into("users_backup").from_select(select_builder)
    query = builder.build()

    assert 'INSERT INTO "users_backup"' in query.sql
    assert "SELECT" in query.sql
    assert isinstance(query.parameters, dict)
    assert True in query.parameters.values()


def test_insert_from_select_merges_parameters() -> None:
    """Test that from_select() merges SELECT parameters."""
    select_builder = SelectBuilder().select("*").from_("users").where(("status", "active"))

    builder = InsertBuilder().into("users_backup").from_select(select_builder)
    assert "active" in builder._parameters.values()


def test_insert_from_select_requires_table() -> None:
    """Test that from_select() requires table to be set."""
    builder = InsertBuilder()
    select_builder = SelectBuilder().select("*").from_("users")

    with pytest.raises(SQLBuilderError, match="target table must be set"):
        builder.from_select(select_builder)


def test_insert_from_select_validates_expression() -> None:
    """Test that from_select() validates SELECT has expression."""
    builder = InsertBuilder().into("users_backup")
    invalid_select = Mock(spec=SelectBuilder)
    invalid_select._parameters = {}
    invalid_select._expression = None

    with pytest.raises(SQLBuilderError, match="must have a valid SELECT expression"):
        builder.from_select(invalid_select)


# Test conflict resolution
def test_insert_on_conflict_do_nothing() -> None:
    """Test ON CONFLICT DO NOTHING clause."""
    builder = InsertBuilder().into("users").values("John", "john@example.com").on_conflict_do_nothing()
    assert isinstance(builder, InsertBuilder)


def test_insert_on_duplicate_key_update() -> None:
    """Test ON DUPLICATE KEY UPDATE clause."""
    builder = InsertBuilder().into("users").on_duplicate_key_update(status="updated", modified_at="NOW()")
    assert isinstance(builder, InsertBuilder)


# Test SQL injection prevention
@pytest.mark.parametrize(
    "malicious_value",
    [
        "'; DROP TABLE users; --",
        "1; DELETE FROM users; --",
        "' OR '1'='1",
        "<script>alert('xss')</script>",
        "Robert'); DROP TABLE students;--",
    ],
    ids=["drop_table", "delete_from", "or_condition", "xss_script", "bobby_tables"],
)
def test_insert_sql_injection_prevention(malicious_value: str) -> None:
    """Test that malicious values are properly parameterized."""
    builder = InsertBuilder().into("users").columns("name").values(malicious_value)
    query = builder.build()

    # Malicious SQL should not appear in query
    assert "DROP TABLE" not in query.sql
    assert "DELETE FROM" not in query.sql
    assert "OR '1'='1'" not in query.sql
    assert "<script>" not in query.sql

    # Value should be parameterized
    assert malicious_value in query.parameters.values()


def test_insert_sql_injection_in_dict() -> None:
    """Test SQL injection prevention with dictionary values."""
    malicious_data = {
        "name": "'; DROP TABLE users; --",
        "email": "test@example.com",
        "comment": "1=1; DELETE FROM users WHERE 1=1",
    }
    builder = InsertBuilder().into("users").values_from_dict(malicious_data)
    query = builder.build()

    assert "DROP TABLE" not in query.sql
    assert "DELETE FROM" not in query.sql
    for value in malicious_data.values():
        assert value in query.parameters.values()


# Test edge cases
@pytest.mark.parametrize(
    "special_value,description",
    [
        (None, "null_value"),
        (0, "zero"),
        ("", "empty_string"),
        (" ", "whitespace"),
        (True, "true_boolean"),
        (False, "false_boolean"),
        ([], "empty_list"),
        ({}, "empty_dict"),
        ({"key": "value"}, "dict_as_value"),
        ([1, 2, 3], "list_as_value"),
        (("a", "b", "c"), "tuple_as_value"),
        ("x" * 10000, "very_long_string"),
    ],
    ids=lambda x: x[1] if isinstance(x, tuple) else str(x),
)
def test_insert_special_values(special_value: Any, description: str) -> None:
    """Test INSERT with special values."""
    builder = InsertBuilder().into("data").columns("value").values(special_value)
    query = builder.build()

    assert 'INSERT INTO "data"' in query.sql
    assert special_value in query.parameters.values()


# Test error handling
def test_insert_expression_not_initialized() -> None:
    """Test error when expression not initialized."""
    builder = InsertBuilder()
    builder._expression = None

    with pytest.raises(SQLBuilderError, match="expression not initialized"):
        builder._get_insert_expression()


def test_insert_wrong_expression_type() -> None:
    """Test error when expression is wrong type."""
    builder = InsertBuilder()
    builder._expression = Mock(spec=exp.Select)  # Wrong type

    with pytest.raises(SQLBuilderError, match="not an Insert instance"):
        builder._get_insert_expression()


# Test large data handling
def test_insert_large_batch() -> None:
    """Test INSERT with large number of rows."""
    large_data = [{"id": i, "name": f"user_{i}", "value": i * 10} for i in range(100)]
    builder = InsertBuilder().into("users").values_from_dicts(large_data)
    query = builder.build()

    assert 'INSERT INTO "users"' in query.sql
    assert len(query.parameters) == 300  # 100 rows x 3 columns


# Test method chaining
def test_insert_full_method_chain() -> None:
    """Test complete method chaining workflow."""
    query = (
        InsertBuilder()
        .into("users")
        .columns("name", "email", "status")
        .values("John", "john@example.com", "active")
        .values("Jane", "jane@example.com", "active")
        .values("Bob", "bob@example.com", "pending")
        .on_conflict_do_nothing()
        .build()
    )

    assert 'INSERT INTO "users"' in query.sql
    assert "VALUES" in query.sql
    assert len(query.parameters) == 9  # 3 rows x 3 columns


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

    assert 'INSERT INTO "users"' in query.sql
    assert len(query.parameters) == 4


# Test type information
def test_insert_expected_result_type() -> None:
    """Test that _expected_result_type returns correct type."""
    builder = InsertBuilder()
    import typing

    result_type = builder._expected_result_type
    # Check that it's a SQLResult type
    assert typing.get_origin(result_type) is SQLResult or result_type.__name__ == "SQLResult"


def test_insert_create_base_expression() -> None:
    """Test that _create_base_expression returns Insert expression."""
    builder = InsertBuilder()
    expression = builder._create_base_expression()
    assert isinstance(expression, exp.Insert)


# Test build output
def test_insert_build_returns_safe_query() -> None:
    """Test that build() returns SafeQuery object."""
    builder = InsertBuilder().into("users").values("John", "john@example.com")
    query = builder.build()

    assert isinstance(query, SafeQuery)
    assert isinstance(query.sql, str)
    assert isinstance(query.parameters, dict)


def test_insert_to_statement_conversion() -> None:
    """Test conversion to SQL statement object."""
    builder = InsertBuilder().into("users").values("John", "john@example.com")
    statement = builder.to_statement()

    assert isinstance(statement, SQL)
    # SQL formatting might differ between build() and to_statement()
    assert 'INSERT INTO "users"' in statement.sql
    assert "VALUES" in statement.sql
    assert "param_1" in statement.sql
    assert "param_2" in statement.sql
    # Parameters might be wrapped
    build_params = builder.build().parameters
    if "parameters" in statement.parameters:
        assert statement.parameters["parameters"] == build_params
    else:
        assert statement.parameters == build_params
