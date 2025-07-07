"""Unit tests for dynamic column creation via __getattr__.

This module tests the dynamic column creation feature that allows
using attributes on the sql factory object to create Column objects
that support method chaining and operator overloading.
"""

import pytest
from sqlglot import exp

from sqlspec import sql
from sqlspec.statement.builder import Column
from sqlspec.statement.builder._column import ColumnExpression, FunctionColumn


def test_dynamic_column_creation() -> None:
    """Test that accessing undefined attributes on sql factory creates Column objects."""
    # Test simple column creation
    user_id = sql.user_id
    assert isinstance(user_id, Column)
    assert user_id.name == "user_id"
    assert user_id.table is None

    # Test various column names
    email = sql.email_address
    assert isinstance(email, Column)
    assert email.name == "email_address"

    created_at = sql.created_at
    assert isinstance(created_at, Column)
    assert created_at.name == "created_at"


def test_dynamic_column_method_chaining() -> None:
    """Test that dynamic columns support method chaining."""
    # Test upper() method
    upper_name = sql.user_name.upper()
    assert isinstance(upper_name, FunctionColumn)
    assert isinstance(upper_name._expression, exp.Upper)

    # Test lower() method
    lower_email = sql.email.lower()
    assert isinstance(lower_email, FunctionColumn)
    assert isinstance(lower_email._expression, exp.Lower)

    # Test alias() method on columns
    aliased_col = sql.user_id.alias("uid")
    assert isinstance(aliased_col, exp.Alias)
    assert aliased_col.alias == "uid"

    # Test alias() method on function columns
    aliased_func = sql.name.upper().alias("upper_name")
    assert isinstance(aliased_func, exp.Alias)
    assert aliased_func.alias == "upper_name"


def test_dynamic_column_operators() -> None:
    """Test that dynamic columns support operator overloading."""
    # Test comparison operators
    eq_expr = sql.user_id == 123
    assert isinstance(eq_expr, ColumnExpression)
    assert isinstance(eq_expr._expression, exp.EQ)

    ne_expr = sql.status != "active"
    assert isinstance(ne_expr, ColumnExpression)
    assert isinstance(ne_expr._expression, exp.NEQ)

    gt_expr = sql.age > 18
    assert isinstance(gt_expr, ColumnExpression)
    assert isinstance(gt_expr._expression, exp.GT)

    # Test logical operators
    and_expr = (sql.age >= 18) & (sql.age <= 65)
    assert isinstance(and_expr, ColumnExpression)
    assert isinstance(and_expr._expression, exp.And)

    or_expr = (sql.status == "active") | (sql.status == "pending")
    assert isinstance(or_expr, ColumnExpression)
    assert isinstance(or_expr._expression, exp.Or)

    # Test NOT operator
    not_expr = ~sql.is_deleted
    assert isinstance(not_expr, ColumnExpression)
    assert isinstance(not_expr._expression, exp.Not)


def test_dynamic_column_sql_methods() -> None:
    """Test that dynamic columns support SQL-specific methods."""
    # Test LIKE
    like_expr = sql.name.like("%john%")
    assert isinstance(like_expr, ColumnExpression)
    assert isinstance(like_expr._expression, exp.Like)

    # Test IN
    in_expr = sql.category.in_(["electronics", "books", "music"])
    assert isinstance(in_expr, ColumnExpression)
    assert isinstance(in_expr._expression, exp.In)

    # Test BETWEEN
    between_expr = sql.price.between(10, 100)
    assert isinstance(between_expr, ColumnExpression)
    assert isinstance(between_expr._expression, exp.Between)

    # Test IS NULL
    null_expr = sql.deleted_at.is_null()
    assert isinstance(null_expr, ColumnExpression)
    assert isinstance(null_expr._expression, exp.Is)

    # Test IS NOT NULL
    not_null_expr = sql.created_at.is_not_null()
    assert isinstance(not_null_expr, ColumnExpression)
    assert isinstance(not_null_expr._expression, exp.Not)


def test_dynamic_columns_in_select_query() -> None:
    """Test using dynamic columns in SELECT queries."""
    # Test simple select with dynamic columns
    query = sql.select(sql.user_id, sql.user_name).from_("users")
    query_sql = query.build().sql

    assert "user_id" in query_sql
    assert "user_name" in query_sql
    assert 'FROM "users"' in query_sql

    # Test select with function columns
    query2 = sql.select(sql.id, sql.name.upper().alias("upper_name"), sql.email.lower().alias("lower_email")).from_(
        "users"
    )
    query2_sql = query2.build().sql

    assert "UPPER" in query2_sql
    assert "LOWER" in query2_sql
    assert "upper_name" in query2_sql
    assert "lower_email" in query2_sql


def test_dynamic_columns_in_where_clause() -> None:
    """Test using dynamic columns in WHERE clauses."""
    # Test simple conditions
    query = sql.select().from_("users").where(sql.age >= 18).where(sql.status == "active")
    query_sql = query.build().sql

    assert "age" in query_sql
    assert "status" in query_sql
    assert ">=" in query_sql
    assert "=" in query_sql

    # Test complex conditions
    query2 = (
        sql.select()
        .from_("products")
        .where((sql.price > 10) & (sql.price < 100))
        .where(sql.category.in_(["electronics", "books"]))
    )
    query2_sql = query2.build().sql

    assert "price" in query2_sql
    assert "category" in query2_sql
    assert "IN" in query2_sql


def test_dynamic_columns_with_other_builders() -> None:
    """Test using dynamic columns with UPDATE, DELETE, etc."""
    # Test with UPDATE
    update_query = sql.update("users").set({"status": "inactive"}).where(sql.last_login < "2023-01-01")
    update_sql = update_query.build().sql

    assert "UPDATE" in update_sql
    assert "last_login" in update_sql

    # Test with DELETE
    delete_query = sql.delete().from_("logs").where(sql.created_at < "2023-01-01")
    delete_sql = delete_query.build().sql

    assert "DELETE" in delete_sql
    assert "created_at" in delete_sql


def test_dynamic_column_edge_cases() -> None:
    """Test edge cases for dynamic column creation."""
    # Test column names that could conflict with SQL factory methods
    # These should still work as long as they're not actual methods
    col1 = sql.select_all  # Not a method, so creates a column
    assert isinstance(col1, Column)
    assert col1.name == "select_all"

    # Test underscored names
    col2 = sql._internal_id
    assert isinstance(col2, Column)
    assert col2.name == "_internal_id"

    # Test NULL comparisons
    null_eq = sql.deleted_at == None  # noqa: E711
    assert isinstance(null_eq, ColumnExpression)
    assert isinstance(null_eq._expression, exp.Is)

    null_ne = sql.deleted_at != None  # noqa: E711
    assert isinstance(null_ne, ColumnExpression)
    assert isinstance(null_ne._expression, exp.Not)


def test_dynamic_column_repr() -> None:
    """Test string representation of dynamic columns."""
    col = sql.user_id
    assert repr(col) == "Column<user_id>"

    # With table (created manually, not via dynamic access)
    col_with_table = Column("name", "users")
    assert repr(col_with_table) == "Column<users.name>"


def test_cannot_use_boolean_operators() -> None:
    """Test that using 'and'/'or' instead of '&'/'|' raises helpful error."""
    cond1 = sql.age > 18
    cond2 = sql.status == "active"

    with pytest.raises(TypeError, match="Cannot use 'and'/'or' operators"):
        # This will trigger __bool__ which should raise
        if cond1 and cond2:  # type: ignore[redundant-expr]
            pass
