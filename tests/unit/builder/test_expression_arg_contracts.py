"""Render regressions for expression builders that silently dropped clauses.

Each construct here previously passed an argument key absent from the target
sqlglot expression's ``arg_types``; the generator ignores unknown keys, so the
clause vanished from rendered SQL without an error. Assertions render the
expression and require the full clause text.
"""

from typing import Any, cast

import pytest
from sqlglot import exp

from sqlspec import sql
from sqlspec.builder import Column
from sqlspec.builder._ddl import ColumnDefinition, build_column_expression
from sqlspec.exceptions import SQLBuilderError


def test_count_distinct_renders_distinct() -> None:
    rendered = sql.count_distinct("category").expression.sql(dialect="postgres")
    assert rendered == "COUNT(DISTINCT category)"


def test_count_with_distinct_flag_renders_distinct() -> None:
    rendered = sql.count("category", distinct=True).expression.sql(dialect="postgres")
    assert rendered == "COUNT(DISTINCT category)"


def test_count_star_distinct_raises() -> None:
    with pytest.raises(SQLBuilderError):
        sql.count("*", distinct=True)


def test_sum_distinct_renders_distinct() -> None:
    rendered = sql.sum("amount", distinct=True).expression.sql(dialect="postgres")
    assert rendered == "SUM(DISTINCT amount)"


def test_column_like_escape_renders_escape_clause() -> None:
    rendered = Column("name").like("50!%%", escape="!").sqlglot_expression.sql(dialect="postgres")
    assert rendered == "name LIKE '50!%%' ESCAPE '!'"


def test_where_like_escape_renders_escape_clause() -> None:
    stmt = sql.select("*").from_("t").where_like("name", "50!%%", escape="!").build()
    assert "ESCAPE" in stmt.sql


def test_column_round_renders_decimals() -> None:
    rendered = Column("price").round(2).sqlglot_expression.sql(dialect="postgres")
    assert rendered == "ROUND(price, 2)"


def test_factory_round_renders_decimals() -> None:
    rendered = sql.round("price", 2).expression.sql(dialect="postgres")
    assert rendered == "ROUND(price, 2)"


def test_column_substring_renders_start_and_length() -> None:
    rendered = Column("code").substring(2, 3).sqlglot_expression.sql()
    assert rendered == "SUBSTRING(code, 2, 3)"


def test_column_substring_renders_start_only() -> None:
    rendered = Column("code").substring(2).sqlglot_expression.sql()
    assert rendered == "SUBSTRING(code, 2)"


def test_column_any_renders_operand_array() -> None:
    rendered = Column("status").any_(["a", "b"]).sqlglot_expression.sql(dialect="postgres")
    assert rendered == "status = ANY(ARRAY['a', 'b'])"


def test_column_not_any_renders_operand_array() -> None:
    rendered = Column("status").not_any_(["a", "b"]).sqlglot_expression.sql(dialect="postgres")
    assert rendered == "status <> ANY(ARRAY['a', 'b'])"


def test_builder_expressions_satisfy_required_sqlglot_arguments() -> None:
    ordered_select = cast("Any", sql.select("a").from_("t")).order_by(exp.alias_(exp.column("a"), "asc"))
    ordered_expression = ordered_select.get_expression()
    assert ordered_expression is not None
    expressions = [
        sql.coalesce("a", "b").expression,
        sql.nvl("a", "b").expression,
        Column("a").coalesce("b").sqlglot_expression,
        Column("a").asc(),
        Column("a").desc(),
        next(sql.row_number_.order_by(exp.column("a")).build().find_all(exp.Ordered)),
        next(ordered_expression.find_all(exp.Ordered)),
        next(
            build_column_expression(ColumnDefinition("id", "INT", auto_increment=True)).find_all(
                exp.AutoIncrementColumnConstraint
            )
        ),
        next(
            sql
            .create_materialized_view("mv")
            .as_select("SELECT 1")
            .with_data()
            ._create_base_expression()
            .find_all(exp.Property)
        ),
        sql.in_(sql.select("id").from_("items")),
    ]
    assert [(type(expression).__name__, expression.error_messages()) for expression in expressions] == [
        (type(expression).__name__, []) for expression in expressions
    ]


def test_materialized_view_with_data_renders_postgres_clause() -> None:
    expression = sql.create_materialized_view("mv").as_select("SELECT 1").with_data()._create_base_expression()
    assert expression.sql(dialect="postgres") == "CREATE MATERIALIZED_VIEW mv AS SELECT 1 WITH DATA"


def test_materialized_view_no_data_renders_postgres_clause() -> None:
    expression = sql.create_materialized_view("mv").as_select("SELECT 1").no_data()._create_base_expression()
    assert expression.sql(dialect="postgres") == "CREATE MATERIALIZED_VIEW mv AS SELECT 1 WITH NO DATA"
