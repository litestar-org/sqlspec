"""Tests for UpdateBuilder."""

import pytest
from sqlglot import exp

from sqlspec.exceptions import SQLBuilderError
from sqlspec.statement.builder import UpdateBuilder


def test_basic_update() -> None:
    """Test basic UPDATE statement construction."""
    builder = UpdateBuilder().table("users")
    result = builder.set("name", "John").set("age", 25).build()

    # Verify SQL structure
    assert "UPDATE" in result.sql
    assert "users" in result.sql
    assert "SET" in result.sql

    # Verify parameters
    assert isinstance(result.parameters, dict)
    assert len(result.parameters) == 2
    assert "John" in result.parameters.values()
    assert 25 in result.parameters.values()


def test_update_with_where() -> None:
    """Test UPDATE with WHERE clause."""
    builder = UpdateBuilder().table("users")
    result = builder.set("name", "Jane").where("id = 1").build()

    assert "UPDATE" in result.sql
    assert "SET" in result.sql
    assert "WHERE" in result.sql
    assert isinstance(result.parameters, dict)
    assert len(result.parameters) == 1


def test_update_multiple_sets() -> None:
    """Test UPDATE with multiple SET clauses."""
    builder = UpdateBuilder().table("users")
    result = builder.set("name", "John").set("email", "john@example.com").set("age", 30).where("id = 1").build()

    assert "SET" in result.sql
    assert isinstance(result.parameters, dict)
    assert len(result.parameters) == 3


def test_update_with_from_clause() -> None:
    """Test UPDATE with FROM clause (PostgreSQL style)."""
    builder = UpdateBuilder().table("users")
    result = builder.set("status", "active").from_("user_profiles").where("users.id = user_profiles.user_id").build()

    assert "UPDATE" in result.sql
    assert "FROM" in result.sql


def test_update_without_table_raises_error() -> None:
    """Test that UPDATE without table raises error on build."""
    builder = UpdateBuilder()

    with pytest.raises(SQLBuilderError):
        builder.set("name", "John").build()


def test_update_parameter_binding() -> None:
    """Test that values are properly parameterized."""
    builder = UpdateBuilder().table("users")
    result = builder.set("data", "'; DROP TABLE users; --").build()

    # Verify SQL injection is prevented
    assert "DROP TABLE" not in result.sql
    assert isinstance(result.parameters, dict)
    assert len(result.parameters) == 1


def test_update_with_expression_column() -> None:
    """Test UPDATE with sqlglot expression as column."""
    builder = UpdateBuilder().table("users")
    col_expr = exp.column("name")
    result = builder.set(col_expr, "John").build()

    assert "UPDATE" in result.sql
    assert isinstance(result.parameters, dict)
    assert len(result.parameters) == 1


def test_update_table_method() -> None:
    """Test setting table via table() method."""
    builder = UpdateBuilder()
    result = builder.table("users").set("name", "John").build()

    assert "UPDATE" in result.sql
    assert "users" in result.sql


def test_update_with_complex_where() -> None:
    """Test UPDATE with complex WHERE conditions."""
    builder = UpdateBuilder().table("users")
    result = builder.set("status", "updated").where("age > 18 AND created_at < '2023-01-01'").build()

    assert "WHERE" in result.sql
    assert "AND" in result.sql


def test_update_error_on_non_update_expression() -> None:
    """Test that methods raise errors on non-UPDATE expressions."""
    builder = UpdateBuilder()
    # Intentionally set a non-Update expression to test error handling of other methods
    builder._expression = exp.Select()  # Set wrong expression type

    # table() method should not raise an error here as it will create a new Update expression
    # if one doesn't exist or is of the wrong type. Let's test it separately if needed.
    # For this test, we assume table() has been called correctly or the expression is already an Update one
    # and we are testing other methods.

    with pytest.raises(SQLBuilderError, match="Cannot add SET clause to non-UPDATE expression"):
        builder.set("name", "John")

    with pytest.raises(SQLBuilderError, match="Cannot add WHERE clause to non-UPDATE expression"):
        builder.where("id = 1")

    # Test from_ on a non-Update expression
    with pytest.raises(SQLBuilderError, match="Cannot add FROM clause to non-UPDATE expression"):
        builder.from_("another_table")

    # Test build on a non-Update expression
    with pytest.raises(SQLBuilderError, match="No UPDATE expression to build or expression is of the wrong type"):
        builder.build()

    # Test .table() when expression is already set to something non-Update
    # It should overwrite with a new Update expression, so no error here.
    # builder.table("users") # This would not raise an error.

    # To specifically test the SQLBuilderError for table() if it were to guard against non-Update types:
    # One would need to modify table() to raise an error if self._expression is not None and not exp.Update
    # However, current implementation of table() replaces the expression.


def test_update_table_method_resets_expression_if_not_update() -> None:
    """Test that table() resets the expression if it's not an Update type."""
    builder = UpdateBuilder()
    builder._expression = exp.Select().select("column").from_("some_table")  # Set a non-Update expression
    # Calling table should replace the Select expression with an Update expression
    builder.table("users")
    assert isinstance(builder._expression, exp.Update)  # type: ignore[unreachable]
    assert builder._expression.this.name == "users"  # type: ignore[unreachable]


def test_update_string_representation() -> None:
    """Test string representation of UpdateBuilder."""
    builder = UpdateBuilder().table("users")
    builder.set("name", "John")

    sql_str = str(builder)
    assert "UPDATE" in sql_str
    assert "users" in sql_str
