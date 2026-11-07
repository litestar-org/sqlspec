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
    assert rendered == "NOT status = ANY(ARRAY['a', 'b'])"


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


@pytest.mark.parametrize(
    ("column_ref", "expected_sql"),
    [
        pytest.param(sql.c("email"), '"email"', id="c-call"),
        pytest.param(sql.c("email", "u"), '"u"."email"', id="c-call-qualified"),
        pytest.param(sql.c("email", table="u"), '"u"."email"', id="c-call-kw-qualified"),
        pytest.param(sql.c.email, '"email"', id="c-attr"),
        pytest.param(sql.c["first-name"], '"first-name"', id="c-subscript-hyphen"),
        pytest.param(sql.c["u.email"], '"u"."email"', id="c-subscript-qualified"),
        pytest.param(sql.c["u", "first.name"], '"u"."first.name"', id="c-subscript-tuple-dotted"),
        pytest.param(sql.col("email"), '"email"', id="col"),
        pytest.param(sql.col("email", "u"), '"u"."email"', id="col-qualified"),
        pytest.param(sql.column("email", "u"), '"u"."email"', id="column-qualified"),
    ],
)
def test_column_factory_aliases_and_namespace_render_identically(column_ref: Column, expected_sql: str) -> None:
    assert column_ref.sqlglot_expression.sql(dialect="postgres", identify=True) == expected_sql
    assert column_ref.expression.sql(dialect="postgres", identify=True) == expected_sql


def test_column_namespace_rejects_dunder_attributes() -> None:
    with pytest.raises(AttributeError):
        _ = sql.c.__wrapped__


def test_column_to_column_comparisons_and_nested_function_columns() -> None:
    query = (
        sql
        .select(sql.upper(sql.c.email.trim()).as_("norm_email"), sql.c.nickname.coalesce(sql.c.email, "anon").as_("dn"))
        .from_("users")
        .where((sql.c.created_at <= sql.c.updated_at) & (sql.c.a != sql.c.b))
        .group_by(sql.c.email.lower())
        .order_by(sql.c.email.lower().desc())
    )
    built = query.build(dialect="postgres")
    rendered = " ".join(built.sql.split())
    assert 'UPPER(TRIM("users"."email")) AS "norm_email"' in rendered
    assert 'COALESCE("users"."nickname", "users"."email", \'anon\') AS "dn"' in rendered
    assert '"users"."created_at" <= "users"."updated_at"' in rendered
    assert '"users"."a" <> "users"."b"' in rendered
    assert 'GROUP BY LOWER("users"."email")' in rendered
    assert 'ORDER BY LOWER("users"."email") DESC' in rendered


@pytest.mark.parametrize(
    ("expr", "expected_sql"),
    [
        pytest.param(sql.func("generate_series", 1, 3), "GENERATE_SERIES(1, 3)", id="int-args"),
        pytest.param(sql.func("read_csv", "a.csv"), "READ_CSV('a.csv')", id="string-literal-arg"),
        pytest.param(sql.func("nullif", sql.c.a, 0), 'NULLIF("a", 0)', id="column-and-literal"),
        pytest.param(
            sql.func("coalesce", sql.c("a", "t"), "fallback"),
            'COALESCE("t"."a", \'fallback\')',
            id="qualified-col-and-str",
        ),
        pytest.param(sql.var("DEFAULT"), "DEFAULT", id="var-default"),
        pytest.param(sql.var("CURRENT_TIMESTAMP"), "CURRENT_TIMESTAMP", id="var-current-timestamp"),
    ],
)
def test_sql_func_and_var_render_expected_expressions(expr: Any, expected_sql: str) -> None:
    assert expr.sqlglot_expression.sql(dialect="postgres", identify=True) == expected_sql


@pytest.mark.parametrize("bad_name", ["", "   "])
def test_sql_func_and_var_reject_empty_names(bad_name: str) -> None:
    with pytest.raises(SQLBuilderError, match="Function name"):
        sql.func(bad_name, 1)
    with pytest.raises(SQLBuilderError, match="Variable name"):
        sql.var(bad_name)
