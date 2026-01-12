"""Unit tests for shared query modification utilities.

This module tests the pure functions in query_modifiers.py that provide
expression factories, condition builders, and expression modifiers for
both the SQL class and builder classes.
"""

import pytest
from sqlglot import exp

from sqlspec.core.query_modifiers import (
    apply_limit,
    apply_offset,
    apply_or_where,
    apply_select_only,
    apply_where,
    create_between_condition,
    create_condition,
    create_exists_condition,
    create_in_condition,
    create_not_exists_condition,
    create_not_in_condition,
    expr_eq,
    expr_gt,
    expr_gte,
    expr_ilike,
    expr_is_not_null,
    expr_is_null,
    expr_like,
    expr_lt,
    expr_lte,
    expr_neq,
    expr_not_like,
    extract_column_name,
    parse_column_for_condition,
    safe_modify_with_cte,
)
from sqlspec.exceptions import SQLSpecError

pytestmark = pytest.mark.xdist_group("core")


# =============================================================================
# Expression Factory Tests
# =============================================================================


class TestExpressionFactories:
    """Tests for expression factory functions."""

    def test_expr_eq_creates_equality(self) -> None:
        """Test expr_eq creates EQ expression."""
        col = exp.column("status")
        placeholder = exp.Placeholder(this="status_param")

        result = expr_eq(col, placeholder)

        assert isinstance(result, exp.EQ)
        assert result.this == col
        assert result.expression == placeholder

    def test_expr_neq_creates_not_equal(self) -> None:
        """Test expr_neq creates NEQ expression."""
        col = exp.column("status")
        placeholder = exp.Placeholder(this="status_param")

        result = expr_neq(col, placeholder)

        assert isinstance(result, exp.NEQ)
        assert result.this == col
        assert result.expression == placeholder

    def test_expr_lt_creates_less_than(self) -> None:
        """Test expr_lt creates LT expression."""
        col = exp.column("age")
        placeholder = exp.Placeholder(this="age_param")

        result = expr_lt(col, placeholder)

        assert isinstance(result, exp.LT)
        assert result.this == col
        assert result.expression == placeholder

    def test_expr_lte_creates_less_than_or_equal(self) -> None:
        """Test expr_lte creates LTE expression."""
        col = exp.column("age")
        placeholder = exp.Placeholder(this="age_param")

        result = expr_lte(col, placeholder)

        assert isinstance(result, exp.LTE)
        assert result.this == col
        assert result.expression == placeholder

    def test_expr_gt_creates_greater_than(self) -> None:
        """Test expr_gt creates GT expression."""
        col = exp.column("score")
        placeholder = exp.Placeholder(this="score_param")

        result = expr_gt(col, placeholder)

        assert isinstance(result, exp.GT)
        assert result.this == col
        assert result.expression == placeholder

    def test_expr_gte_creates_greater_than_or_equal(self) -> None:
        """Test expr_gte creates GTE expression."""
        col = exp.column("score")
        placeholder = exp.Placeholder(this="score_param")

        result = expr_gte(col, placeholder)

        assert isinstance(result, exp.GTE)
        assert result.this == col
        assert result.expression == placeholder

    def test_expr_like_creates_like(self) -> None:
        """Test expr_like creates Like expression."""
        col = exp.column("name")
        placeholder = exp.Placeholder(this="name_param")

        result = expr_like(col, placeholder)

        assert isinstance(result, exp.Like)
        assert result.this == col
        assert result.expression == placeholder

    def test_expr_not_like_creates_not_like(self) -> None:
        """Test expr_not_like creates NOT(Like) expression."""
        col = exp.column("name")
        placeholder = exp.Placeholder(this="name_param")

        result = expr_not_like(col, placeholder)

        assert isinstance(result, exp.Not)
        assert isinstance(result.this, exp.Like)

    def test_expr_ilike_creates_ilike(self) -> None:
        """Test expr_ilike creates ILike expression."""
        col = exp.column("name")
        placeholder = exp.Placeholder(this="name_param")

        result = expr_ilike(col, placeholder)

        assert isinstance(result, exp.ILike)
        assert result.this == col
        assert result.expression == placeholder

    def test_expr_is_null_creates_is_null(self) -> None:
        """Test expr_is_null creates IS NULL expression."""
        col = exp.column("deleted_at")
        placeholder = exp.Placeholder(this="ignored")

        result = expr_is_null(col, placeholder)

        assert isinstance(result, exp.Is)
        assert result.this == col
        assert isinstance(result.expression, exp.Null)

    def test_expr_is_not_null_creates_is_not_null(self) -> None:
        """Test expr_is_not_null creates IS NOT NULL expression."""
        col = exp.column("deleted_at")
        placeholder = exp.Placeholder(this="ignored")

        result = expr_is_not_null(col, placeholder)

        assert isinstance(result, exp.Not)
        assert isinstance(result.this, exp.Is)


# =============================================================================
# Column Parsing Tests
# =============================================================================


class TestColumnParsing:
    """Tests for column parsing utilities."""

    def test_parse_column_string(self) -> None:
        """Test parsing simple column string."""
        result = parse_column_for_condition("name")

        assert isinstance(result, exp.Column)
        assert result.name == "name"

    def test_parse_column_table_qualified(self) -> None:
        """Test parsing table.column notation."""
        result = parse_column_for_condition("users.name")

        assert isinstance(result, exp.Column)
        assert result.name == "name"
        assert result.table == "users"

    def test_parse_column_expression_passthrough(self) -> None:
        """Test that Expression is passed through unchanged."""
        col = exp.column("status")

        result = parse_column_for_condition(col)

        assert result is col

    def test_extract_column_name_simple(self) -> None:
        """Test extracting name from simple string."""
        result = extract_column_name("status")

        assert result == "status"

    def test_extract_column_name_table_qualified(self) -> None:
        """Test extracting name from table.column notation."""
        result = extract_column_name("users.status")

        assert result == "status"

    def test_extract_column_name_from_column_expression(self) -> None:
        """Test extracting name from Column expression."""
        col = exp.column("status")

        result = extract_column_name(col)

        assert result == "status"

    def test_extract_column_name_fallback(self) -> None:
        """Test fallback when column name cannot be determined."""
        # Use exp.true() which returns Boolean with name="" (falsy), triggering fallback
        bool_expr = exp.true()

        result = extract_column_name(bool_expr)

        assert result == "column"


# =============================================================================
# Condition Builder Tests
# =============================================================================


class TestConditionBuilders:
    """Tests for condition building functions."""

    def test_create_condition_with_eq(self) -> None:
        """Test creating equality condition."""
        result = create_condition("status", "status_param", expr_eq)

        assert isinstance(result, exp.EQ)
        assert isinstance(result.this, exp.Column)
        assert isinstance(result.expression, exp.Placeholder)
        assert result.expression.this == "status_param"

    def test_create_condition_with_table_qualified_column(self) -> None:
        """Test creating condition with table.column."""
        result = create_condition("users.status", "status_param", expr_eq)

        assert isinstance(result, exp.EQ)
        assert isinstance(result.this, exp.Column)
        assert result.this.table == "users"
        assert result.this.name == "status"

    def test_create_in_condition(self) -> None:
        """Test creating IN condition."""
        param_names = ["p1", "p2", "p3"]

        result = create_in_condition("status", param_names)

        assert isinstance(result, exp.In)
        assert isinstance(result.this, exp.Column)
        assert len(result.expressions) == 3
        for i, expr in enumerate(result.expressions):
            assert isinstance(expr, exp.Placeholder)
            assert expr.this == param_names[i]

    def test_create_not_in_condition(self) -> None:
        """Test creating NOT IN condition."""
        param_names = ["p1", "p2"]

        result = create_not_in_condition("status", param_names)

        assert isinstance(result, exp.Not)
        assert isinstance(result.this, exp.In)

    def test_create_between_condition(self) -> None:
        """Test creating BETWEEN condition."""
        result = create_between_condition("age", "low_param", "high_param")

        assert isinstance(result, exp.Between)
        assert isinstance(result.this, exp.Column)
        assert isinstance(result.args["low"], exp.Placeholder)
        assert isinstance(result.args["high"], exp.Placeholder)
        assert result.args["low"].this == "low_param"
        assert result.args["high"].this == "high_param"

    def test_create_exists_condition(self) -> None:
        """Test creating EXISTS condition."""
        subquery = exp.select("1").from_("users")

        result = create_exists_condition(subquery)

        assert isinstance(result, exp.Exists)
        assert result.this == subquery

    def test_create_not_exists_condition(self) -> None:
        """Test creating NOT EXISTS condition."""
        subquery = exp.select("1").from_("users")

        result = create_not_exists_condition(subquery)

        assert isinstance(result, exp.Not)
        assert isinstance(result.this, exp.Exists)


# =============================================================================
# Expression Modifier Tests
# =============================================================================


class TestExpressionModifiers:
    """Tests for expression modification functions."""

    def test_apply_where_to_select(self) -> None:
        """Test applying WHERE to SELECT."""
        select_expr = exp.select("*").from_("users")
        condition = exp.EQ(this=exp.column("status"), expression=exp.Literal.string("active"))

        result = apply_where(select_expr, condition)

        assert isinstance(result, exp.Select)
        assert result.args.get("where") is not None
        sql = result.sql()
        assert "WHERE" in sql

    def test_apply_where_to_update(self) -> None:
        """Test applying WHERE to UPDATE."""
        update_expr = exp.update("users", {"name": exp.Literal.string("new_name")})
        condition = exp.EQ(this=exp.column("id"), expression=exp.Literal.number(1))

        result = apply_where(update_expr, condition)

        assert isinstance(result, exp.Update)
        assert result.args.get("where") is not None

    def test_apply_where_to_delete(self) -> None:
        """Test applying WHERE to DELETE."""
        delete_expr = exp.delete("users")
        condition = exp.EQ(this=exp.column("id"), expression=exp.Literal.number(1))

        result = apply_where(delete_expr, condition)

        assert isinstance(result, exp.Delete)
        assert result.args.get("where") is not None

    def test_apply_where_invalid_type(self) -> None:
        """Test applying WHERE to unsupported type raises error."""
        # Create a simple Insert expression (WHERE is not valid for INSERT)
        insert_expr = exp.Insert(
            this=exp.to_table("users"),
            expression=exp.Values(expressions=[exp.Tuple(expressions=[exp.Literal.number(1)])]),
        )

        with pytest.raises(SQLSpecError, match="Cannot apply WHERE"):
            apply_where(insert_expr, exp.true())

    def test_apply_or_where(self) -> None:
        """Test applying WHERE with OR."""
        select_expr = exp.select("*").from_("users")
        first_condition = exp.EQ(this=exp.column("status"), expression=exp.Literal.string("active"))
        select_expr = apply_where(select_expr, first_condition)

        second_condition = exp.EQ(this=exp.column("status"), expression=exp.Literal.string("pending"))
        result = apply_or_where(select_expr, second_condition)

        where_clause = result.args.get("where")
        assert where_clause is not None
        assert isinstance(where_clause.this, exp.Or)

    def test_apply_or_where_without_existing_where_raises(self) -> None:
        """Test OR WHERE without existing WHERE raises error."""
        select_expr = exp.select("*").from_("users")
        condition = exp.EQ(this=exp.column("status"), expression=exp.Literal.string("active"))

        with pytest.raises(SQLSpecError, match="Cannot use OR without existing WHERE"):
            apply_or_where(select_expr, condition)

    def test_apply_limit(self) -> None:
        """Test applying LIMIT."""
        select_expr = exp.select("*").from_("users")

        result = apply_limit(select_expr, 10)

        assert result.args.get("limit") is not None
        sql = result.sql()
        assert "LIMIT 10" in sql

    def test_apply_limit_invalid_type(self) -> None:
        """Test LIMIT on non-SELECT raises error."""
        update_expr = exp.update("users", {"name": exp.Literal.string("test")})

        with pytest.raises(SQLSpecError, match="LIMIT only valid for SELECT"):
            apply_limit(update_expr, 10)

    def test_apply_offset(self) -> None:
        """Test applying OFFSET."""
        select_expr = exp.select("*").from_("users")

        result = apply_offset(select_expr, 20)

        assert result.args.get("offset") is not None
        sql = result.sql()
        assert "OFFSET 20" in sql

    def test_apply_offset_invalid_type(self) -> None:
        """Test OFFSET on non-SELECT raises error."""
        update_expr = exp.update("users", {"name": exp.Literal.string("test")})

        with pytest.raises(SQLSpecError, match="OFFSET only valid for SELECT"):
            apply_offset(update_expr, 20)

    def test_apply_select_only(self) -> None:
        """Test replacing SELECT columns."""
        select_expr = exp.select("*").from_("users")

        result = apply_select_only(select_expr, ("id", "name", "email"))

        sql = result.sql()
        assert "id" in sql
        assert "name" in sql
        assert "email" in sql
        assert "*" not in sql

    def test_apply_select_only_with_expressions(self) -> None:
        """Test replacing SELECT with expression objects."""
        select_expr = exp.select("*").from_("users")
        count_expr = exp.func("COUNT", exp.Star())

        result = apply_select_only(select_expr, (count_expr,))

        sql = result.sql()
        assert "COUNT(*)" in sql

    def test_apply_select_only_invalid_type(self) -> None:
        """Test select_only on non-SELECT raises error."""
        update_expr = exp.update("users", {"name": exp.Literal.string("test")})

        with pytest.raises(SQLSpecError, match="select_only only valid for SELECT"):
            apply_select_only(update_expr, ("id",))


# =============================================================================
# CTE Utility Tests
# =============================================================================


class TestCTEUtilities:
    """Tests for CTE handling utilities."""

    def test_safe_modify_preserves_cte(self) -> None:
        """Test that CTEs are preserved at top level after modification."""
        # Build SELECT with CTE
        cte_select = exp.select("id", "name").from_("base_users")
        main_select = exp.select("*").from_("active_users")
        main_select = main_select.with_("active_users", as_=cte_select)

        # Apply a modification that would normally disrupt CTE
        def add_where(expr: exp.Expression) -> exp.Expression:
            return apply_where(expr, exp.EQ(this=exp.column("id"), expression=exp.Literal.number(1)))

        result = safe_modify_with_cte(main_select, add_where)

        # CTE should be preserved at top level (sqlglot uses "with_" key)
        assert isinstance(result, exp.Select)
        assert result.args.get("with_") is not None
        sql = result.sql()
        assert "WITH" in sql
        assert "WHERE" in sql

    def test_safe_modify_without_cte(self) -> None:
        """Test modification works normally without CTE."""
        select_expr = exp.select("*").from_("users")

        def add_limit(expr: exp.Expression) -> exp.Expression:
            return apply_limit(expr, 10)

        result = safe_modify_with_cte(select_expr, add_limit)

        assert isinstance(result, exp.Select)
        sql = result.sql()
        assert "LIMIT 10" in sql

    def test_safe_modify_non_select_passthrough(self) -> None:
        """Test that non-SELECT expressions pass through unchanged."""
        update_expr = exp.update("users", {"name": exp.Literal.string("test")})

        def add_where(expr: exp.Expression) -> exp.Expression:
            return apply_where(expr, exp.EQ(this=exp.column("id"), expression=exp.Literal.number(1)))

        result = safe_modify_with_cte(update_expr, add_where)

        assert isinstance(result, exp.Update)


# =============================================================================
# Integration Tests
# =============================================================================


class TestIntegration:
    """Integration tests combining multiple utilities."""

    def test_build_complex_where_clause(self) -> None:
        """Test building a complex WHERE with multiple conditions."""
        select_expr = exp.select("*").from_("orders")

        # Add first condition
        cond1 = create_condition("status", "status_p", expr_eq)
        select_expr = apply_where(select_expr, cond1)

        # Add second condition with AND
        cond2 = create_condition("total", "total_p", expr_gt)
        select_expr = apply_where(select_expr, cond2)

        # Add third condition with OR
        cond3 = create_condition("priority", "priority_p", expr_eq)
        select_expr = apply_or_where(select_expr, cond3)

        sql = select_expr.sql()
        assert "WHERE" in sql
        assert "AND" in sql
        assert "OR" in sql

    def test_pagination_with_where(self) -> None:
        """Test combining WHERE, LIMIT, and OFFSET."""
        select_expr = exp.select("*").from_("products")

        condition = create_condition("category", "cat_p", expr_eq)
        select_expr = apply_where(select_expr, condition)
        select_expr = apply_limit(select_expr, 20)
        select_expr = apply_offset(select_expr, 40)

        sql = select_expr.sql()
        assert "WHERE" in sql
        assert "LIMIT 20" in sql
        assert "OFFSET 40" in sql

    def test_select_only_with_cte_preserved(self) -> None:
        """Test select_only preserves CTE (fixes issue #301)."""
        cte_select = exp.select("id", "name", "total").from_("sales")
        main_select = exp.select("*").from_("top_sales")
        main_select = main_select.with_("top_sales", as_=cte_select)

        def select_columns(expr: exp.Expression) -> exp.Expression:
            return apply_select_only(expr, ("id", "name"))

        result = safe_modify_with_cte(main_select, select_columns)

        sql = result.sql()
        assert "WITH" in sql
        assert "id" in sql
        assert "name" in sql
