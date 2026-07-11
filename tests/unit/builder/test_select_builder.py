# pyright: reportAttributeAccessIssue=false
"""Unit tests for SELECT builder methods."""

from typing import cast

import pytest

from sqlspec import sql
from sqlspec.builder._base import QueryBuilder


def test_select_only_replaces_columns() -> None:
    """Test select_only() replaces existing selected columns."""
    query = sql.select("id", "name").from_("users")
    assert "id" in query.to_sql()
    assert "name" in query.to_sql()
    query.select_only("email")
    sql_str = query.to_sql()
    assert "id" not in sql_str
    assert "name" not in sql_str
    assert "email" in sql_str


def test_select_only_multiple_columns() -> None:
    """Test select_only() handles multiple columns."""
    query = sql.select("id").from_("users")
    query.select_only("name", "email")
    sql_str = query.to_sql()
    assert "id" not in sql_str
    assert "name" in sql_str
    assert "email" in sql_str


def test_select_only_on_new_query() -> None:
    """Test select_only() works on fresh query."""
    query = sql.select("id").from_("users")
    query.select_only("active")
    assert "active" in query.to_sql()


def test_select_only_with_expressions() -> None:
    """Test select_only() works with SQL expressions (strings)."""
    query = sql.select("id").from_("users")
    query.select_only("COUNT(id) AS count")
    sql_str = query.to_sql().upper()
    assert "COUNT" in sql_str
    assert "ID" in sql_str


def test_select_only_not_available_on_update() -> None:
    """Test select_only() is not available on UPDATE statements."""
    query = sql.update("users").set(active=True)
    with pytest.raises(AttributeError):
        query.select_only("id")


def test_order_by_raw_trailing_desc_emits_descending_sort() -> None:
    """sql.raw with trailing DESC must produce ORDER BY ... DESC, not an alias."""
    from sqlglot import exp

    raw_desc = cast("exp.Ordered", sql.raw("COALESCE(a, b, 0) DESC"))
    builder = sql.select("a", "b").from_("things").order_by(raw_desc)
    expr = builder._expression
    assert expr is not None
    order = expr.args.get("order")
    assert order is not None
    assert any(isinstance(o, exp.Ordered) and o.args.get("desc") for o in order.expressions)
    sql_text = builder.to_sql()
    assert "DESC" in sql_text
    assert 'AS "desc"' not in sql_text.lower()


def test_order_by_raw_trailing_asc_emits_ascending_sort() -> None:
    """sql.raw with trailing ASC must produce an ascending Ordered expression."""
    from sqlglot import exp

    raw_asc = cast("exp.Ordered", sql.raw("LOWER(a) ASC"))
    builder = sql.select("a").from_("things").order_by(raw_asc)
    expr = builder._expression
    assert expr is not None
    order = expr.args.get("order")
    assert order is not None
    assert all(isinstance(o, exp.Ordered) and o.args.get("desc") is False for o in order.expressions)


def test_order_by_raw_function_without_direction_unchanged() -> None:
    """sql.raw without trailing direction must continue to work as a sort key."""
    from sqlglot import exp

    raw_expr = cast("exp.Ordered", sql.raw("COALESCE(a, b, 0)"))
    builder = sql.select("a").from_("things").order_by(raw_expr)
    sql_text = builder.to_sql()
    assert "COALESCE" in sql_text.upper()
    assert "ORDER BY" in sql_text.upper()


def test_select_hints_statement_hint_does_not_mutate_select_expression() -> None:
    query = sql.select("*").from_("users").with_hint("MAX_EXECUTION_TIME(1000)")
    result = query.build()
    assert "MAX_EXECUTION_TIME" in result.sql
    assert query._expression is not None
    assert query._expression.args.get("hint") is None


def test_select_hints_statement_hint_build_is_repeatable() -> None:
    query = sql.select("*").from_("users").with_hint("MAX_EXECUTION_TIME(1000)")
    first = query.build().sql
    second = query.build().sql
    assert second == first


def test_select_hints_statement_hint_preserves_cte_rendering() -> None:
    cte = sql.select("id").from_("users")
    query = sql.select("*").from_("active_users").with_cte("active_users", cte).with_hint("MAX_EXECUTION_TIME(1000)")
    query.enable_optimization = False
    rendered = query.build().sql.upper()
    assert rendered.startswith("WITH")
    assert '"ACTIVE_USERS" AS (' in rendered
    assert "MAX_EXECUTION_TIME" in rendered


def test_subquery_no_roundtrip_where_in_query_builder_subquery_does_not_call_build(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    subquery = sql.select("id").from_("users").where_eq("status", "active")

    def fail_build(self: QueryBuilder, *args: object, **kwargs: object) -> object:
        msg = "QueryBuilder.build() should not be needed to normalize a subquery"
        raise AssertionError(msg)

    monkeypatch.setattr(QueryBuilder, "build", fail_build)
    query = sql.select("*").from_("orders").where_in("user_id", subquery)
    assert "status" in query.parameters


def test_subquery_no_roundtrip_where_in_query_builder_subquery_still_renames_colliding_parameters() -> None:
    subquery = sql.select("id").from_("users").where_eq("status", "active")
    query = sql.select("*").from_("orders").where_eq("status", "pending").where_in("user_id", subquery)
    assert query.parameters["status"] == "pending"
    assert query.parameters["status_1"] == "active"
