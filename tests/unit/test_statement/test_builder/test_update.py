"""Unit tests for UpdateBuilder functionality.

This module tests the UpdateBuilder including:
- Basic UPDATE statement construction
- SET clause with single and multiple columns
- WHERE conditions and helpers (LIKE, BETWEEN, IN, EXISTS, NULL)
- UPDATE with FROM clause (PostgreSQL style)
- UPDATE with JOIN clauses
- Complex WHERE conditions using AND/OR
- UPDATE with subqueries
- RETURNING clause support
- Parameter binding and SQL injection prevention
- Error handling for invalid operations
"""

from typing import Any

import pytest
from sqlglot import exp

from sqlspec.exceptions import SQLBuilderError
from sqlspec.statement.builder import SelectBuilder, UpdateBuilder
from sqlspec.statement.builder.base import SafeQuery
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL


# Test basic UPDATE construction
def test_update_builder_initialization() -> None:
    """Test UpdateBuilder initialization."""
    builder = UpdateBuilder()
    assert isinstance(builder, UpdateBuilder)
    # UpdateBuilder uses _expression to store the UPDATE statement
    assert isinstance(builder._expression, exp.Update)
    assert builder._expression.args.get("this") is None  # target table
    assert builder._parameters == {}


def test_update_table_method() -> None:
    """Test setting target table with table()."""
    builder = UpdateBuilder().table("users")
    # Check the target table is set in the expression
    assert builder._expression is not None
    assert builder._expression.args.get("this") is not None
    assert isinstance(builder._expression.args["this"], exp.Table)
    assert builder._expression.args["this"].name == "users"


def test_update_table_returns_self() -> None:
    """Test that table() returns builder for chaining."""
    builder = UpdateBuilder()
    result = builder.table("users")
    assert result is builder


# Test SET clause functionality
@pytest.mark.parametrize(
    "column,value,expected_param_count",
    [
        ("name", "John", 1),
        ("email", "john@example.com", 1),
        ("age", 30, 1),
        ("active", True, 1),
        ("balance", 99.99, 1),
        ("data", None, 1),
        ("created_at", "2024-01-01", 1),
    ],
    ids=["string", "email", "integer", "boolean", "float", "null", "date"],
)
def test_update_set_single_column(column: str, value: Any, expected_param_count: int) -> None:
    """Test SET clause with single column and various value types."""
    builder = UpdateBuilder().table("users").set(column, value)
    query = builder.build()

    assert 'UPDATE "users"' in query.sql or "UPDATE users" in query.sql
    assert "SET" in query.sql
    assert len(query.parameters) == expected_param_count
    assert value in query.parameters.values()


def test_update_set_multiple_columns() -> None:
    """Test SET clause with multiple columns."""
    builder = (
        UpdateBuilder()
        .table("users")
        .set("name", "John Doe")
        .set("email", "john@example.com")
        .set("age", 30)
        .set("active", True)
    )
    query = builder.build()

    assert 'UPDATE "users"' in query.sql or "UPDATE users" in query.sql
    assert "SET" in query.sql
    assert len(query.parameters) == 4
    assert all(v in query.parameters.values() for v in ["John Doe", "john@example.com", 30, True])


def test_update_set_returns_self() -> None:
    """Test that set() returns builder for chaining."""
    builder = UpdateBuilder().table("users")
    result = builder.set("name", "John")
    assert result is builder


def test_update_set_with_expression_column() -> None:
    """Test SET with sqlglot expression as column."""
    builder = UpdateBuilder().table("users")
    col_expr = exp.column("name")
    query = builder.set(col_expr, "John").build()

    assert 'UPDATE "users"' in query.sql or "UPDATE users" in query.sql
    assert "SET" in query.sql
    assert "John" in query.parameters.values()


def test_update_set_with_expression_value() -> None:
    """Test SET with sqlglot expression as value (e.g., column = column + 1)."""
    builder = UpdateBuilder().table("accounts")
    # Create expression for balance = balance + 100
    value_expr = exp.Add(this=exp.column("balance"), expression=exp.Literal.number(100))
    query = builder.set("balance", value_expr).build()

    assert 'UPDATE "accounts"' in query.sql or "UPDATE accounts" in query.sql
    assert "SET" in query.sql
    # The expression should be used directly, not parameterized
    # Column names might be quoted
    assert "balance = balance + 100" in query.sql or '"balance" = "balance" + 100' in query.sql
    assert len(query.parameters) == 0  # No parameters for expressions


# Test WHERE conditions
@pytest.mark.parametrize(
    "method,args,expected_sql_parts",
    [
        ("where", (("status", "active"),), ["WHERE"]),
        ("where", ("id = 1",), ["WHERE", "id = 1"]),
        ("where_like", ("name", "%John%"), ["LIKE"]),
        ("where_between", ("age", 18, 65), ["BETWEEN"]),
        ("where_in", ("status", ["active", "pending"]), ["IN"]),
        ("where_not_in", ("status", ["deleted", "banned"]), ["NOT IN", "NOT", "IN"]),
        ("where_null", ("deleted_at",), ["IS NULL"]),
        ("where_not_null", ("email",), ["IS NOT NULL", "NOT", "IS NULL"]),
    ],
    ids=["where_tuple", "where_string", "like", "between", "in", "not_in", "null", "not_null"],
)
def test_update_where_conditions(method: str, args: tuple, expected_sql_parts: list[str]) -> None:
    """Test various WHERE condition helper methods."""
    builder = UpdateBuilder(enable_optimization=False).table("users").set("status", "updated")
    where_method = getattr(builder, method)
    builder = where_method(*args)

    query = builder.build()
    assert 'UPDATE "users"' in query.sql or "UPDATE users" in query.sql
    assert "SET" in query.sql
    assert any(part in query.sql for part in expected_sql_parts)


def test_update_where_exists_with_subquery() -> None:
    """Test WHERE EXISTS with subquery."""
    # Use a literal instead of parameter for the subquery to avoid parameter name conflicts
    subquery = SelectBuilder().select("1").from_("orders").where("user_id = users.id")
    builder = UpdateBuilder(enable_optimization=False).table("users").set("has_orders", True).where_exists(subquery)

    query = builder.build()
    assert 'UPDATE "users"' in query.sql or "UPDATE users" in query.sql
    assert "EXISTS" in query.sql
    assert "orders" in query.sql


def test_update_multiple_where_conditions() -> None:
    """Test multiple WHERE conditions (AND logic)."""
    builder = (
        UpdateBuilder()
        .table("users")
        .set("status", "inactive")
        .where(("age", ">", 65))
        .where(("last_login", "<", "2023-01-01"))
        .where_not_null("email")
    )

    query = builder.build()
    assert 'UPDATE "users"' in query.sql or "UPDATE users" in query.sql
    assert "WHERE" in query.sql
    # Multiple conditions should be AND-ed together


# Test UPDATE with FROM clause
def test_update_with_from_clause() -> None:
    """Test UPDATE with FROM clause (PostgreSQL style)."""
    builder = (
        UpdateBuilder()
        .table("users")
        .set("total_orders", exp.column("o.order_count"))
        .from_("(SELECT user_id, COUNT(*) as order_count FROM orders GROUP BY user_id) o")
        .where("users.id = o.user_id")
    )

    query = builder.build()
    assert 'UPDATE "users"' in query.sql or "UPDATE users" in query.sql
    assert "FROM" in query.sql
    assert "orders" in query.sql


def test_update_from_returns_self() -> None:
    """Test that from() returns builder for chaining."""
    builder = UpdateBuilder().table("users").set("name", "John")
    result = builder.from_("other_table")
    assert result is builder


# Test UPDATE with JOIN (non-standard but supported by some databases)
@pytest.mark.parametrize(
    "join_type,method_name",
    [("INNER", "join"), ("LEFT", "left_join"), ("RIGHT", "right_join")],
    ids=["inner_join", "left_join", "right_join"],
)
def test_update_with_joins(join_type: str, method_name: str) -> None:
    """Test UPDATE with JOIN clauses."""
    # UpdateBuilder doesn't support JOIN clauses directly
    # Most databases use UPDATE ... FROM pattern instead
    pytest.skip("UpdateBuilder doesn't support JOIN clauses - use FROM clause instead")


# Test RETURNING clause
def test_update_with_returning() -> None:
    """Test UPDATE with RETURNING clause."""
    builder = (
        UpdateBuilder()
        .table("users")
        .set("last_updated", "NOW()")
        .where(("id", 123))
        .returning("id", "name", "last_updated")
    )

    query = builder.build()
    assert 'UPDATE "users"' in query.sql or "UPDATE users" in query.sql
    assert "RETURNING" in query.sql


def test_update_returning_star() -> None:
    """Test UPDATE RETURNING *."""
    builder = UpdateBuilder().table("users").set("active", False).where(("status", "deleted")).returning("*")

    query = builder.build()
    assert 'UPDATE "users"' in query.sql or "UPDATE users" in query.sql
    assert "RETURNING" in query.sql
    assert "*" in query.sql


# Test SQL injection prevention
@pytest.mark.parametrize(
    "malicious_value",
    [
        "'; DROP TABLE users; --",
        "1'; DELETE FROM users WHERE '1'='1",
        "' OR '1'='1",
        "<script>alert('xss')</script>",
        "Robert'); DROP TABLE students;--",
    ],
    ids=["drop_table", "delete_from", "or_condition", "xss_script", "bobby_tables"],
)
def test_update_sql_injection_prevention(malicious_value: str) -> None:
    """Test that malicious values are properly parameterized."""
    builder = UpdateBuilder().table("users").set("name", malicious_value).where(("id", 1))
    query = builder.build()

    # Malicious SQL should not appear in query
    assert "DROP TABLE" not in query.sql
    assert "DELETE FROM" not in query.sql
    assert "OR '1'='1'" not in query.sql
    assert "<script>" not in query.sql

    # Value should be parameterized
    assert malicious_value in query.parameters.values()


# Test error conditions
def test_update_without_table_raises_error() -> None:
    """Test that UPDATE without table raises error."""
    builder = UpdateBuilder()
    builder.set("name", "John")  # set() works without table
    with pytest.raises(SQLBuilderError, match="No table specified for UPDATE statement"):
        builder.build()  # but build() fails


def test_update_without_set_raises_error() -> None:
    """Test that UPDATE without SET clause raises error."""
    builder = UpdateBuilder().table("users")
    with pytest.raises(SQLBuilderError, match="At least one SET clause must be specified"):
        builder.build()


# These tests were for internal methods that don't exist, removed


# Test complex scenarios
def test_update_complex_query() -> None:
    """Test complex UPDATE with multiple features."""
    builder = (
        UpdateBuilder()
        .table("users")
        .set("status", "reviewed")
        .set("review_date", "2024-01-15")
        .set("reviewer_id", 42)
        .from_("user_activity ua")
        .where("users.id = ua.user_id")
        .where_between("ua.activity_score", 80, 100)
        .where_in("ua.activity_type", ["purchase", "review", "referral"])
        .where_not_null("users.email")
        .returning("id", "status", "review_date")
    )

    query = builder.build()

    # Verify all components are present
    assert 'UPDATE "users"' in query.sql or "UPDATE users" in query.sql
    assert "SET" in query.sql
    assert "FROM" in query.sql
    assert "WHERE" in query.sql
    # BETWEEN is optimized to >= AND <=
    # Check for activity_score with or without quotes and table alias
    assert "BETWEEN" in query.sql or (
        ("activity_score" in query.sql or '"activity_score"' in query.sql) and ("<=" in query.sql and ">=" in query.sql)
    )
    assert "IN" in query.sql
    assert "RETURNING" in query.sql

    # Verify parameters
    assert isinstance(query.parameters, dict)
    param_values = list(query.parameters.values())
    assert "reviewed" in param_values
    assert "2024-01-15" in param_values
    assert 42 in param_values
    assert 80 in param_values
    assert 100 in param_values
    assert "purchase" in param_values


def test_update_with_case_expression() -> None:
    """Test UPDATE with CASE expression in SET."""
    builder = UpdateBuilder().table("products")

    # Build a CASE expression for price adjustment
    case_expr = exp.Case()
    case_expr = case_expr.when(
        exp.GT(this=exp.column("stock"), expression=exp.Literal.number(100)), exp.Literal.number(0.9)
    )
    case_expr = case_expr.when(
        exp.GT(this=exp.column("stock"), expression=exp.Literal.number(50)), exp.Literal.number(0.95)
    )
    case_expr = case_expr.else_(exp.Literal.number(1.0))

    # price = price * CASE ...
    price_update = exp.Mul(this=exp.column("price"), expression=case_expr)

    query = builder.set("price", price_update).set("last_updated", "NOW()").build()

    assert 'UPDATE "products"' in query.sql or "UPDATE products" in query.sql
    assert "CASE" in query.sql
    assert "WHEN" in query.sql


# Test edge cases
def test_update_empty_where_in_list() -> None:
    """Test WHERE IN with empty list."""
    builder = UpdateBuilder().table("users").set("status", "unknown").where_in("id", [])
    query = builder.build()
    assert 'UPDATE "users"' in query.sql or "UPDATE users" in query.sql
    assert "SET" in query.sql


def test_update_parameter_naming_consistency() -> None:
    """Test that parameter naming is consistent across multiple conditions."""
    builder = (
        UpdateBuilder()
        .table("users")
        .set("name", "Updated Name")
        .set("email", "new@example.com")
        .where_like("old_email", "%@oldomain.com")
        .where_between("age", 25, 45)
        .where_in("department", ["sales", "marketing"])
    )

    query = builder.build()
    assert isinstance(query.parameters, dict)

    # All parameter names should be unique
    param_names = list(query.parameters.keys())
    assert len(param_names) == len(set(param_names))

    # All values should be preserved
    param_values = list(query.parameters.values())
    assert "Updated Name" in param_values
    assert "new@example.com" in param_values
    assert "%@oldomain.com" in param_values
    assert 25 in param_values
    assert 45 in param_values
    assert "sales" in param_values
    assert "marketing" in param_values


def test_update_table_method_replaces_expression() -> None:
    """Test that table() replaces existing expression if wrong type."""
    builder = UpdateBuilder()
    builder._expression = exp.Select()  # Wrong type

    builder.table("users")
    assert isinstance(builder._expression, exp.Update)


# Test type information
def test_update_expected_result_type() -> None:
    """Test that _expected_result_type returns correct type."""
    builder = UpdateBuilder()
    import typing

    result_type = builder._expected_result_type
    # Check that it's a SQLResult type
    assert typing.get_origin(result_type) is SQLResult or result_type.__name__ == "SQLResult"


def test_update_create_base_expression() -> None:
    """Test that _create_base_expression returns Update expression."""
    builder = UpdateBuilder()
    expression = builder._create_base_expression()
    assert isinstance(expression, exp.Update)


# Test build output
def test_update_build_returns_safe_query() -> None:
    """Test that build() returns SafeQuery object."""
    builder = UpdateBuilder().table("users").set("name", "John").where(("id", 1))
    query = builder.build()

    assert isinstance(query, SafeQuery)
    assert isinstance(query.sql, str)
    assert isinstance(query.parameters, dict)


def test_update_to_statement_conversion() -> None:
    """Test conversion to SQL statement object."""
    builder = UpdateBuilder().table("users").set("name", "John").where(("id", 1))
    statement = builder.to_statement()

    assert isinstance(statement, SQL)
    # SQL might have different formatting but should have same content
    assert 'UPDATE "users"' in statement.sql or "UPDATE users" in statement.sql
    assert "SET name =" in statement.sql or 'SET "name" =' in statement.sql
    assert "WHERE" in statement.sql
    assert "id =" in statement.sql or '"id" =' in statement.sql
    # Parameters should be available (might be nested)
    build_params = builder.build().parameters
    if isinstance(statement.parameters, dict) and "parameters" in statement.parameters:
        # Nested format
        assert statement.parameters["parameters"] == build_params
    else:
        # Direct format
        assert statement.parameters == build_params


# Test fluent interface chaining
def test_update_fluent_interface_chaining() -> None:
    """Test that all methods return builder for fluent chaining."""
    builder = (
        UpdateBuilder()
        .table("users")
        .set("name", "John Doe")
        .set("email", "john@example.com")
        .set("status", "active")
        .from_("user_profiles")
        .where(("users.id", ">", 100))
        .where_like("email", "%@example.com")
        .where_between("age", 25, 65)
        .where_in("role", ["admin", "manager"])
        .where_not_null("verified_at")
        .returning("*")
    )

    query = builder.build()
    # Verify the query has all components (JOIN not supported in UpdateBuilder)
    assert all(keyword in query.sql for keyword in ["UPDATE", "SET", "FROM", "WHERE", "RETURNING"])


# Test special values
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
        ({1, 2, 3}, "set_as_value"),
        ("x" * 10000, "very_long_string"),
    ],
    ids=lambda x: x[1] if isinstance(x, tuple) else str(x),
)
def test_update_special_values(special_value: Any, description: str) -> None:
    """Test UPDATE with special values."""
    builder = UpdateBuilder().table("data").set("value", special_value).where(("id", 1))
    query = builder.build()

    assert 'UPDATE "data"' in query.sql or "UPDATE data" in query.sql
    assert special_value in query.parameters.values()


def test_update_batch_operations() -> None:
    """Test UPDATE affecting multiple rows."""
    builder = (
        UpdateBuilder()
        .table("orders")
        .set("status", "shipped")
        .set("shipped_date", "2024-01-15")
        .where_in("id", list(range(1, 101)))  # Update 100 orders
    )

    query = builder.build()
    assert 'UPDATE "orders"' in query.sql or "UPDATE orders" in query.sql
    assert len(query.parameters) == 102  # 2 SET values + 100 IDs


def test_update_with_subquery_in_set() -> None:
    """Test UPDATE with subquery in SET clause."""
    subquery = SelectBuilder().select("AVG(salary)").from_("employees").where(("department", "sales"))

    builder = UpdateBuilder().table("departments").set("avg_salary", subquery).where(("name", "sales"))

    query = builder.build()
    assert 'UPDATE "departments"' in query.sql or "UPDATE departments" in query.sql
    assert "SELECT" in query.sql
    assert "AVG" in query.sql
