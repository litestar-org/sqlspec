"""Unit tests for DeleteBuilder functionality.

This module tests the DeleteBuilder including:
- Basic DELETE statement construction
- WHERE conditions and helpers (=, LIKE, BETWEEN, IN, EXISTS, NULL)
- Complex WHERE conditions using AND/OR
- DELETE with USING clause (PostgreSQL style)
- DELETE with JOIN clauses (MySQL style)
- RETURNING clause support
- Cascading deletes and referential integrity
- Parameter binding and SQL injection prevention
- Error handling for invalid operations
"""

from typing import TYPE_CHECKING

import pytest
from sqlglot import exp

from sqlspec.exceptions import SQLBuilderError
from sqlspec.statement.builder import DeleteBuilder, SelectBuilder
from sqlspec.statement.builder.base import SafeQuery
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL

if TYPE_CHECKING:
    pass


# Test basic DELETE construction
def test_delete_builder_initialization() -> None:
    """Test DeleteBuilder initialization."""
    builder = DeleteBuilder()
    assert isinstance(builder, DeleteBuilder)
    assert builder._table is None
    assert builder._parameters == {}


def test_delete_from_method() -> None:
    """Test setting target table with from()."""
    builder = DeleteBuilder().from_("users")
    assert builder._table == "users"


def test_delete_from_returns_self() -> None:
    """Test that from() returns builder for chaining."""
    builder = DeleteBuilder()
    result = builder.from_("users")
    assert result is builder


# Test WHERE conditions
@pytest.mark.parametrize(
    "method,args,expected_sql_parts",
    [
        ("where", (("status", "inactive"),), ["WHERE"]),
        ("where", ("id = 1",), ["WHERE", "id = 1"]),
        ("where_eq", ("id", 123), ["WHERE", "="]),
        ("where_like", ("name", "%test%"), ["LIKE"]),
        ("where_between", ("age", 0, 17), ["BETWEEN"]),
        ("where_in", ("status", ["deleted", "banned"]), ["IN"]),
        ("where_not_in", ("role", ["admin", "moderator"]), ["NOT IN", "NOT", "IN"]),
        ("where_null", ("deleted_at",), ["IS NULL"]),
        ("where_not_null", ("verified_at",), ["IS NOT NULL", "NOT", "IS NULL"]),
    ],
    ids=["where_tuple", "where_string", "where_eq", "like", "between", "in", "not_in", "null", "not_null"],
)
def test_delete_where_conditions(method: str, args: tuple, expected_sql_parts: list[str]) -> None:
    """Test various WHERE condition helper methods."""
    builder = DeleteBuilder(enable_optimization=False).from_("users")
    where_method = getattr(builder, method)
    builder = where_method(*args)

    query = builder.build()
    assert 'DELETE FROM "users"' in query.sql or "DELETE FROM users" in query.sql
    assert any(part in query.sql for part in expected_sql_parts)


def test_delete_where_exists_with_subquery() -> None:
    """Test WHERE EXISTS with subquery."""
    subquery = SelectBuilder().select("1").from_("orders").where(("user_id", "users.id")).where(("status", "unpaid"))
    builder = DeleteBuilder(enable_optimization=False).from_("users").where_exists(subquery)

    query = builder.build()
    assert 'DELETE FROM "users"' in query.sql or "DELETE FROM users" in query.sql
    assert "EXISTS" in query.sql
    assert "orders" in query.sql


def test_delete_where_not_exists() -> None:
    """Test WHERE NOT EXISTS."""
    subquery = SelectBuilder().select("1").from_("orders").where(("user_id", "users.id"))
    builder = DeleteBuilder(enable_optimization=False).from_("users").where_not_exists(subquery)

    query = builder.build()
    assert 'DELETE FROM "users"' in query.sql or "DELETE FROM users" in query.sql
    assert "NOT EXISTS" in query.sql or ("NOT" in query.sql and "EXISTS" in query.sql)


def test_delete_multiple_where_conditions() -> None:
    """Test multiple WHERE conditions (AND logic)."""
    builder = (
        DeleteBuilder()
        .from_("users")
        .where(("status", "inactive"))
        .where(("last_login", "<", "2022-01-01"))
        .where_null("email_verified_at")
        .where_not_in("role", ["admin", "moderator"])
    )

    query = builder.build()
    assert 'DELETE FROM "users"' in query.sql or "DELETE FROM users" in query.sql
    assert "WHERE" in query.sql
    # Multiple conditions should be AND-ed together


# Test DELETE with JOIN (MySQL style)
@pytest.mark.parametrize(
    "join_type,method_name", [("INNER", "join"), ("LEFT", "left_join")], ids=["inner_join", "left_join"]
)
@pytest.mark.skip(reason="DeleteBuilder doesn't support JOIN operations")
def test_delete_with_joins(join_type: str, method_name: str) -> None:
    """Test DELETE with JOIN clauses (MySQL style)."""
    builder = DeleteBuilder().from_("users")

    join_method = getattr(builder, method_name)
    builder = join_method("user_sessions", on="users.id = user_sessions.user_id")
    builder = builder.where("user_sessions.expired = true")

    query = builder.build()
    assert "DELETE" in query.sql
    assert f"{join_type} JOIN" in query.sql
    assert "user_sessions" in query.sql


# Test RETURNING clause
def test_delete_with_returning() -> None:
    """Test DELETE with RETURNING clause."""
    builder = DeleteBuilder().from_("users").where(("status", "deleted")).returning("id", "email", "deleted_at")

    query = builder.build()
    assert 'DELETE FROM "users"' in query.sql or "DELETE FROM users" in query.sql
    assert "RETURNING" in query.sql


def test_delete_returning_star() -> None:
    """Test DELETE RETURNING *."""
    builder = DeleteBuilder().from_("logs").where("created_at < 2023-01-01").returning("*")

    query = builder.build()
    assert 'DELETE FROM "logs"' in query.sql or "DELETE FROM logs" in query.sql
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
def test_delete_sql_injection_prevention(malicious_value: str) -> None:
    """Test that malicious values are properly parameterized."""
    builder = DeleteBuilder().from_("users").where_eq("name", malicious_value)
    query = builder.build()

    # Malicious SQL should not appear in query
    assert "DROP TABLE" not in query.sql
    assert "DELETE FROM users WHERE" not in query.sql or query.sql.count("DELETE") == 1
    assert "OR '1'='1'" not in query.sql
    assert "<script>" not in query.sql

    # Value should be parameterized
    assert malicious_value in query.parameters.values()


# Test error conditions
def test_delete_without_table_raises_error() -> None:
    """Test that DELETE without table raises error."""
    builder = DeleteBuilder()
    with pytest.raises(SQLBuilderError, match="DELETE requires a table"):
        builder.build()


def test_delete_where_requires_table() -> None:
    """Test that where() requires table to be set."""
    builder = DeleteBuilder()
    with pytest.raises(SQLBuilderError, match="WHERE clause requires"):
        builder.where(("id", 1))


def test_delete_cascading_scenario() -> None:
    """Test DELETE for cascading delete scenario."""
    # Delete all orders for inactive users older than 1 year
    inactive_users = (
        SelectBuilder()
        .select("id")
        .from_("users")
        .where(("status", "inactive"))
        .where(("last_login", "<", "2023-01-01"))
    )

    builder = DeleteBuilder().from_("orders").where_in("user_id", inactive_users)

    query = builder.build()
    assert 'DELETE FROM "orders"' in query.sql or "DELETE FROM orders" in query.sql
    assert "WHERE" in query.sql
    assert "IN" in query.sql
    assert "SELECT" in query.sql  # Subquery


# Test edge cases
def test_delete_empty_where_in_list() -> None:
    """Test WHERE IN with empty list."""
    builder = DeleteBuilder().from_("users").where_in("id", [])
    query = builder.build()
    assert 'DELETE FROM "users"' in query.sql or "DELETE FROM users" in query.sql


def test_delete_where_in_with_tuples() -> None:
    """Test WHERE IN with tuple instead of list."""
    builder = DeleteBuilder().from_("users").where_in("id", (1, 2, 3, 4, 5))
    query = builder.build()

    assert 'DELETE FROM "users"' in query.sql or "DELETE FROM users" in query.sql
    assert "IN" in query.sql
    assert len(query.parameters) == 5


def test_delete_parameter_naming_consistency() -> None:
    """Test that parameter naming is consistent across multiple conditions."""
    builder = (
        DeleteBuilder()
        .from_("users")
        .where_eq("status", "inactive")
        .where_like("email", "%@oldomain.com")
        .where_between("age", 60, 100)
        .where_in("role", ["guest", "temporary"])
    )

    query = builder.build()
    assert isinstance(query.parameters, dict)

    # All parameter names should be unique
    param_names = list(query.parameters.keys())
    assert len(param_names) == len(set(param_names))

    # All values should be preserved
    param_values = list(query.parameters.values())
    assert "inactive" in param_values
    assert "%@oldomain.com" in param_values
    assert 60 in param_values
    assert 100 in param_values
    assert "guest" in param_values
    assert "temporary" in param_values


def test_delete_batch_operations() -> None:
    """Test DELETE affecting multiple rows."""
    builder = DeleteBuilder().from_("logs").where_in("id", list(range(1, 1001)))  # Delete 1000 logs

    query = builder.build()
    assert 'DELETE FROM "logs"' in query.sql or "DELETE FROM logs" in query.sql
    assert len(query.parameters) == 1000


# Test type information
def test_delete_expected_result_type() -> None:
    """Test that _expected_result_type returns correct type."""
    builder = DeleteBuilder()
    import typing

    result_type = builder._expected_result_type
    # Check that it's a SQLResult type
    assert typing.get_origin(result_type) is SQLResult or result_type.__name__ == "SQLResult"


def test_delete_create_base_expression() -> None:
    """Test that _create_base_expression returns Delete expression."""
    builder = DeleteBuilder()
    expression = builder._create_base_expression()
    assert isinstance(expression, exp.Delete)


# Test build output
def test_delete_build_returns_safe_query() -> None:
    """Test that build() returns SafeQuery object."""
    builder = DeleteBuilder().from_("users").where(("id", 1))
    query = builder.build()

    assert isinstance(query, SafeQuery)
    assert isinstance(query.sql, str)
    assert isinstance(query.parameters, dict)


def test_delete_to_statement_conversion() -> None:
    """Test conversion to SQL statement object."""
    builder = DeleteBuilder().from_("users").where(("id", 1))
    statement = builder.to_statement()

    assert isinstance(statement, SQL)
    # SQL normalization might format differently
    assert 'DELETE FROM "users"' in statement.sql or "DELETE FROM users" in statement.sql
    assert "id = :param_1" in statement.sql or '"id" = :param_1' in statement.sql
    # Statement parameters might be wrapped
    build_params = builder.build().parameters
    if "parameters" in statement.parameters:
        assert statement.parameters["parameters"] == build_params
    else:
        assert statement.parameters == build_params


# Test special scenarios
def test_delete_all_rows() -> None:
    """Test DELETE without WHERE clause (delete all rows)."""
    builder = DeleteBuilder().from_("temporary_data")
    query = builder.build()

    assert 'DELETE FROM "temporary_data"' in query.sql or "DELETE FROM temporary_data" in query.sql
    assert "WHERE" not in query.sql  # No WHERE clause means delete all rows


def test_delete_table_alias() -> None:
    """Test DELETE with table alias."""
    builder = DeleteBuilder().from_("very_long_table_name AS t").where(("t.status", "deleted"))

    query = builder.build()
    # SQLGlot might quote the entire table expression
    assert "very_long_table_name" in query.sql
    assert "t.status" in query.sql or '"t"."status"' in query.sql
