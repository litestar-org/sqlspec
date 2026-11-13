"""Unit tests for SQL builder parsing utilities.

This module tests the parsing utilities in sqlspec.builder,
specifically focusing on the parameter style conversion functionality that
was added to fix QueryBuilder parameter handling issues.
"""

import contextlib
from datetime import date
from types import SimpleNamespace
from typing import Any, cast

import pytest
from sqlglot import exp
from sqlglot.errors import ParseError

import sqlspec.builder._parsing_utils as parsing_utils
from sqlspec import sql
from sqlspec.builder import (
    Select,
    Values,
    parse_column_expression,
    parse_condition_expression,
    parse_order_expression,
    parse_table_expression,
)
from sqlspec.core import SQL, get_cache
from sqlspec.exceptions import SQLBuilderError


def test_parse_condition_expression_with_dollar_parameters() -> None:
    """Test that parse_condition_expression handles $1 style parameters correctly."""
    condition = "category = $1"

    # Should parse without errors and convert $1 to SQLGlot-compatible format
    expr = parse_condition_expression(condition)
    assert expr is not None
    assert isinstance(expr, exp.Expr)


def test_parse_condition_expression_with_colon_numeric_parameters() -> None:
    """Test that parse_condition_expression handles :1 style parameters correctly."""
    condition = "id = :1"

    # Should parse without errors and convert :1 to SQLGlot-compatible format
    expr = parse_condition_expression(condition)
    assert expr is not None
    assert isinstance(expr, exp.Expr)


def test_parse_condition_expression_with_named_parameters() -> None:
    """Test that parse_condition_expression handles :name style parameters correctly."""
    condition = "status = :status_value"

    # Should parse without errors - named parameters are already SQLGlot-compatible
    expr = parse_condition_expression(condition)
    assert expr is not None
    assert isinstance(expr, exp.Expr)


def test_parse_condition_expression_with_question_mark_parameters() -> None:
    """Test that parse_condition_expression handles ? style parameters correctly."""
    condition = "name = ?"

    # Should parse without errors
    expr = parse_condition_expression(condition)
    assert expr is not None
    assert isinstance(expr, exp.Expr)


def test_parse_condition_expression_no_parameters() -> None:
    """Test that parse_condition_expression handles conditions without parameters."""
    condition = "active = TRUE"

    # Should parse without errors
    expr = parse_condition_expression(condition)
    assert expr is not None
    assert isinstance(expr, exp.Expr)


def test_parse_condition_expression_complex_conditions() -> None:
    """Test that parse_condition_expression handles complex conditions."""
    conditions = [
        "name LIKE '%test%'",
        "age > 18 AND status = 'active'",
        "price BETWEEN 10 AND 100",
        "category IN ('tech', 'science')",
    ]

    for condition in conditions:
        expr = parse_condition_expression(condition)
        assert expr is not None
        assert isinstance(expr, exp.Expr)


def test_parse_condition_expression_tuple_format() -> None:
    """Test that parse_condition_expression handles tuple conditions correctly."""
    # Test 2-tuple format (column, value)
    condition = ("category", "Electronics")

    expr = parse_condition_expression(condition)
    assert expr is not None
    assert isinstance(expr, exp.Expr)


def test_parse_condition_expression_sqlglot_expression_passthrough() -> None:
    """Test that parse_condition_expression passes through SQLGlot expressions unchanged."""
    original_expr = exp.EQ(this=exp.column("name"), expression=exp.convert("test"))

    result_expr = parse_condition_expression(original_expr)
    assert result_expr is original_expr  # Should be the same object


def test_parse_column_expression_basic() -> None:
    """Test that parse_column_expression handles basic column names."""
    column = "name"

    expr = parse_column_expression(column)
    assert expr is not None
    assert isinstance(expr, exp.Expr)


def test_parse_column_expression_qualified() -> None:
    """Test that parse_column_expression handles qualified column names."""
    column = "users.name"

    expr = parse_column_expression(column)
    assert expr is not None
    assert isinstance(expr, exp.Expr)


_COLUMN_PARSING_CORPUS = [
    "name",
    "users.name",
    "db.users.name",
    "col$1",
    "_x",
    "true",
    "null",
    "count",
    "user",
    "MAX(price)",
    "name AS n",
    "price * 2",
    '"Quoted".col',
]

_ORDER_PARSING_CORPUS = [
    "name",
    "users.name",
    "db.users.name",
    "col$1",
    "_x",
    "true",
    "null",
    "count",
    "user",
    "MAX(price)",
    "price * 2",
    '"Quoted".col',
    "name DESC",
    "users.name asc",
    "COUNT(*) DESC",
    "name nulls first",
]


def _parse_column_oracle(value: str) -> exp.Expr:
    parsed = exp.maybe_parse(value) or exp.column(value)
    if isinstance(parsed, exp.Binary):
        return exp.Paren(this=parsed)
    return parsed


def _parse_order_oracle(value: str) -> exp.Expr:
    parsed = parsing_utils.maybe_parse(str(value), into=exp.Ordered)
    if parsed:
        return parsed
    return _parse_column_oracle(value)


def _assert_matches_oracle(value: str, parser, oracle) -> None:
    with contextlib.suppress(ParseError):
        expected = oracle(value)
        actual = parser(value)
        assert type(actual) is type(expected)
        assert actual.sql() == expected.sql()
        return

    with pytest.raises(ParseError):
        parser(value)


@pytest.mark.parametrize("value", _COLUMN_PARSING_CORPUS)
def test_parse_column_expression_matches_parser_oracle(value: str) -> None:
    """Column parser fast paths must preserve the previous parser-backed shape."""
    _assert_matches_oracle(value, parse_column_expression, _parse_column_oracle)


@pytest.mark.parametrize("value", _ORDER_PARSING_CORPUS)
def test_parse_order_expression_matches_parser_oracle(value: str) -> None:
    """Order parser fast paths must preserve the previous parser-backed shape."""
    _assert_matches_oracle(value, parse_order_expression, _parse_order_oracle)


def test_parse_column_expression_simple_identifier_avoids_parser(monkeypatch: pytest.MonkeyPatch) -> None:
    """Simple column identifiers should avoid sqlglot parsing."""
    calls = 0
    original = exp.maybe_parse

    def recorder(*args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(exp, "maybe_parse", recorder)

    expr = parse_column_expression("users.name")

    assert calls == 0
    assert expr.sql() == "users.name"


def test_parse_table_expression_simple_identifier_avoids_parser(monkeypatch: pytest.MonkeyPatch) -> None:
    """Simple table identifiers should avoid SELECT-wrapper parsing."""
    calls = 0
    original = exp.maybe_parse

    def recorder(*args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(exp, "maybe_parse", recorder)

    expr = parse_table_expression("schema.users", explicit_alias="u")

    assert calls == 0
    assert expr.sql() == "schema.users AS u"


def test_parse_order_expression_directional_identifier_avoids_parser(monkeypatch: pytest.MonkeyPatch) -> None:
    """Directional simple ORDER BY identifiers should avoid sqlglot parsing."""
    calls = 0
    original = parsing_utils.maybe_parse

    def recorder(*args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(parsing_utils, "maybe_parse", recorder)

    expr = parse_order_expression("users.name asc")

    assert calls == 0
    assert isinstance(expr, exp.Ordered)
    assert expr.sql() == "users.name ASC"


def test_parse_column_expression_sqlglot_passthrough() -> None:
    """Test that parse_column_expression passes through SQLGlot expressions."""
    original_expr = exp.column("test")

    result_expr = parse_column_expression(original_expr)
    assert result_expr is original_expr  # Should be the same object


def test_parameter_style_conversion_regression() -> None:
    """Regression test for the specific parameter style conversion issue."""
    # This reproduces the exact issue that was fixed: $1 being treated as column
    condition = "category = $1"

    # Should not raise parsing errors
    expr = parse_condition_expression(condition)
    assert expr is not None

    # The expression should be parseable by SQLGlot without treating $1 as a column
    # This verifies our parameter conversion fix works
    assert isinstance(expr, exp.Expr)


def test_cached_static_expression_reuses_factory() -> None:
    cache = get_cache()
    cache.clear()

    factory_calls = {"count": 0}

    def factory() -> exp.Expr:
        factory_calls["count"] += 1
        return exp.select("1")

    builder = sql.select()

    first = builder.build_static_expression(cache_key="static:test", expression_factory=factory, parameters={"p": 1})
    assert cache.get_expression("static:test") is not None
    assert cache.get_statement("static:test") is None

    second = builder.build_static_expression(cache_key="static:test", expression_factory=factory, parameters={"p": 2})

    assert factory_calls["count"] == 1
    assert first.parameters == {"p": 1}
    assert second.parameters == {"p": 2}
    assert first.sql == second.sql


def test_build_static_expression_explicit_optimize_overrides_disabled_builder(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    def fake_optimize(expression: exp.Expr, **kwargs: Any) -> exp.Expr:
        _ = kwargs
        nonlocal calls
        calls += 1
        return expression

    monkeypatch.setattr("sqlglot.optimizer.optimize", fake_optimize)
    builder = Select(enable_optimization=False, simplify_expressions=True)
    builder.build_static_expression(expression=exp.select("1"), optimize_expression=True)
    assert calls == 1


def test_cached_static_expression_respects_copy_flag() -> None:
    cache = get_cache()
    cache.clear()

    base_expr = exp.select(exp.column("a"))

    builder = sql.select()

    result = builder.build_static_expression(
        cache_key="static:copy", expression_factory=lambda: base_expr, copy=True, parameters={"val": 123}
    )
    assert cache.get_expression("static:copy") is not None

    base_expr.set("from", exp.from_("tbl"))

    repeat = builder.build_static_expression(
        cache_key="static:copy", expression_factory=lambda: base_expr, copy=True, parameters={"val": 456}
    )

    assert "tbl" not in result.sql
    assert "tbl" not in repeat.sql
    assert repeat.parameters == {"val": 456}


@pytest.mark.parametrize("static", [False, True])
def test_cross_dialect_render_preserves_owned_and_cached_ast(static: bool) -> None:
    expression = exp.select("a", "b").from_("items").distinct("a").order_by("a", "b")
    builder = Select(dialect="postgres", enable_optimization=False)
    builder.set_expression(expression)
    expected = expression.sql(dialect="postgres")
    cache_key = "cross-dialect-render-isolation"
    cache = get_cache()
    cache.delete_expression(cache_key)
    try:
        if static:
            builder.build_static_expression(cache_key=cache_key, expression_factory=lambda: expression, dialect="mysql")
            cached = cast("exp.Expr", cache.get_expression(cache_key))
            assert cached.sql(dialect="postgres") == expected
            rendered = builder.build_static_expression(cache_key=cache_key, dialect="postgres")
        else:
            builder.build(dialect="mysql")
            rendered = builder.build(dialect="postgres")
        assert expression.sql(dialect="postgres") == expected
        assert "DISTINCT ON" in rendered.sql
    finally:
        cache.delete_expression(cache_key)


def test_extract_sql_object_expression_rejects_sqlglot_node() -> None:
    """The extractor accepts SQL carriers only, so a bare node is rejected."""
    with pytest.raises(TypeError, match="Value is not a SQL object"):
        parsing_utils.extract_sql_object_expression(exp.column("a"))


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(None, id="none"),
        pytest.param("x", id="str"),
        pytest.param(1, id="int"),
        pytest.param(True, id="bool"),
        pytest.param(b"x", id="bytes"),
        pytest.param(date(2026, 1, 1), id="date"),
        pytest.param({"k": 1}, id="dict"),
        pytest.param([1], id="list"),
        pytest.param(
            SimpleNamespace(expression=exp.condition("a = :p"), parameters={"p": 1}), id="parameters-lookalike"
        ),
        pytest.param(SimpleNamespace(expression=None, sql="b + 1"), id="text-lookalike"),
    ],
)
def test_resolve_value_expression_returns_none_for_bindable_values(value: Any) -> None:
    """Values that bind as parameters resolve to no expression."""
    assert parsing_utils.resolve_value_expression(value, sql.update("t"), "a") is None


@pytest.mark.parametrize(
    ("value", "expected_sql", "expected_parameters"),
    [
        pytest.param(sql.column("b"), "b", {}, id="column"),
        pytest.param(sql.count("b"), "COUNT(b)", {}, id="wrapper"),
        pytest.param(sql.column("b").cast("INT"), "CAST(b AS INT)", {}, id="function-column"),
        pytest.param(sql.column("b") > 1, "b > 1", {}, id="column-expression"),
        pytest.param(exp.select("x"), "(SELECT x)", {}, id="select-node"),
        pytest.param(
            exp.maybe_parse("SELECT 1 UNION SELECT 2"), "(SELECT 1 UNION SELECT 2)", {}, id="set-operation-node"
        ),
        pytest.param(
            sql.select("x").from_("y").where_eq("z", 5), "(SELECT x FROM y WHERE z = :z)", {"z": 5}, id="query-builder"
        ),
        pytest.param(sql.raw("b + :p", p=1), "b + :p", {"p": 1}, id="sql-object"),
    ],
)
def test_resolve_value_expression(value: Any, expected_sql: str, expected_parameters: dict[str, Any]) -> None:
    """Expression-carrying values resolve to SQL and merge their parameters into the builder."""
    builder = sql.update("t")

    resolved = parsing_utils.resolve_value_expression(value, builder, "a")

    assert resolved is not None
    assert resolved.sql() == expected_sql
    assert builder.parameters == expected_parameters


def test_resolve_value_expression_copies_sqlglot_node() -> None:
    """A SQLGlot node resolves to an equal copy, leaving the caller's node untouched."""
    node = exp.column("b")

    resolved = parsing_utils.resolve_value_expression(node, sql.update("t"), "a")

    assert resolved == node
    assert resolved is not node


def test_resolve_value_expression_parenthesizes_set_operation_as_subquery() -> None:
    """Set operations wrap in a subquery node, and other queries in parentheses."""
    builder = sql.update("t")

    union = parsing_utils.resolve_value_expression(exp.maybe_parse("SELECT 1 UNION SELECT 2"), builder, "a")
    select = parsing_utils.resolve_value_expression(exp.select("x"), builder, "a")

    assert union is not None
    assert select is not None
    assert union.sql() == "(SELECT 1 UNION SELECT 2)"
    assert select.sql() == "(SELECT x)"


def test_parse_column_expression_without_builder_returns_same_node() -> None:
    """Without a builder, a query node is returned as given."""
    node = exp.select("x")

    assert parse_column_expression(node) is node


class _ExpressionLookalike:
    """Plain object shaped like a SQL carrier, as a decoded request model could be."""

    def __init__(self, expression: Any) -> None:
        self.expression = expression
        self.sql = "injected_sql OR 1 = 1"
        self.parameters = {"injected_parameter": 1}
        self.sqlglot_expression = expression

    def __str__(self) -> str:
        return "injected_str = 1"


def _merge() -> Any:
    return sql.merge(dialect="postgres").into("t").using("s", alias="s").on("t.id = s.id")


def _conflict_insert() -> Any:
    return sql.insert("t", dialect="postgres").columns("id").values(1)


VALUE_SITES = [
    pytest.param(lambda v: sql.select("z").from_("t").where_eq("a", v), id="where-eq"),
    pytest.param(lambda v: sql.select("z").from_("t").where_gt("a", v), id="where-gt"),
    pytest.param(lambda v: sql.select("z").from_("t").where(("a", ">=", v)), id="where-tuple"),
    pytest.param(lambda v: sql.select("z").from_("t").where_in("a", v), id="where-in"),
    pytest.param(lambda v: sql.select("z").from_("t").where_in("a", [v, 7]), id="where-in-list"),
    pytest.param(lambda v: sql.select("z").from_("t").where_between("a", v, 9), id="where-between"),
    pytest.param(lambda v: sql.select("z").from_("t").where(("a", "NOT BETWEEN", (v, 9))), id="not-between"),
    pytest.param(lambda v: sql.update("t", dialect="postgres").set(a=v), id="update-set"),
    pytest.param(lambda v: sql.insert("t", dialect="postgres").columns("a").values(v), id="insert-positional"),
    pytest.param(lambda v: sql.insert("t", dialect="postgres").values(a=v), id="insert-keyword"),
    pytest.param(lambda v: _conflict_insert().on_conflict("id").do_update(a=v), id="conflict-update"),
    pytest.param(
        lambda v: sql.insert("t", dialect="mysql").columns("id").values(1).on_duplicate_key_update(a=v),
        id="duplicate-key-update",
    ),
    pytest.param(lambda v: _merge().when_matched_then_update(a=v), id="merge-matched"),
    pytest.param(lambda v: _merge().when_not_matched_by_source_then_update(a=v), id="merge-by-source"),
    pytest.param(lambda v: _merge().when_not_matched_then_insert(columns=["a"], values=[v]), id="merge-insert"),
    pytest.param(lambda v: Values().add_rows([[v]]), id="values"),
]


@pytest.mark.parametrize(
    "expression",
    [
        pytest.param("injected_expression", id="text-attribute"),
        pytest.param(exp.column("injected_expression"), id="node-attribute"),
        pytest.param(None, id="empty-attribute"),
    ],
)
@pytest.mark.parametrize("site", VALUE_SITES)
def test_value_sites_bind_sql_carrier_lookalikes(site: Any, expression: Any) -> None:
    """An object that only looks like a SQL carrier binds as a parameter and renders none of its attributes."""
    lookalike = _ExpressionLookalike(expression)

    statement = site(lookalike).build()

    assert "injected" not in statement.sql
    assert any(value is lookalike for value in statement.parameters.values())
    assert "injected_parameter" not in statement.parameters


@pytest.mark.parametrize("site", VALUE_SITES)
def test_value_sites_render_sql_objects(site: Any) -> None:
    """A real SQL object renders as SQL and merges its parameters."""
    fragment = sql.raw("rendered_col + :p", p=3)
    assert isinstance(fragment, SQL)

    statement = site(fragment).build()

    assert "rendered_col" in statement.sql
    assert statement.parameters["p"] == 3
    assert all(isinstance(value, int) for value in statement.parameters.values())


LOOKALIKE_EXPRESSIONS = [
    pytest.param("injected_expression", id="text-attribute"),
    pytest.param(exp.column("injected_expression"), id="node-attribute"),
    pytest.param(None, id="empty-attribute"),
]


@pytest.mark.parametrize("expression", LOOKALIKE_EXPRESSIONS)
@pytest.mark.parametrize(
    ("site", "message"),
    [
        pytest.param(lambda q, v: q.where_exists(v), "Unsupported subquery type", id="where-exists"),
        pytest.param(lambda q, v: q.where_not_exists(v), "Unsupported subquery type", id="where-not-exists"),
        pytest.param(lambda q, v: q.where_any("a", v), "Unsupported type for 'values'", id="where-any"),
        pytest.param(lambda q, v: q.where_not_any("a", v), "Unsupported type for 'values'", id="where-not-any"),
        pytest.param(lambda q, v: q.where(v), "Unsupported condition type", id="where"),
        pytest.param(lambda q, v: q.join("u", on=v), "Unsupported join condition type", id="join"),
    ],
)
def test_query_only_sites_reject_sql_carrier_lookalikes(site: Any, message: str, expression: Any) -> None:
    """Positions where a parameter cannot stand in raise for an object that only looks like a SQL carrier."""
    with pytest.raises(SQLBuilderError, match=message):
        site(sql.select("z").from_("t"), _ExpressionLookalike(expression))


@pytest.mark.parametrize(
    ("site", "expected"),
    [
        pytest.param(lambda q, v: q.where_exists(v), 'EXISTS( SELECT "y"."x" AS "x" FROM "y"', id="where-exists"),
        pytest.param(lambda q, v: q.where_not_exists(v), 'NOT EXISTS( SELECT "y"."x" AS "x" FROM "y"', id="not-exists"),
        pytest.param(lambda q, v: q.where_any("a", v), '"y"."x" AS "x"', id="where-any"),
        pytest.param(lambda q, v: q.where_not_any("a", v), '"y"."x" AS "x"', id="where-not-any"),
    ],
)
def test_query_only_sites_render_sql_objects(site: Any, expected: str) -> None:
    """A real SQL object still renders at query-only positions and merges its parameters."""
    fragment = sql.raw("SELECT x FROM y WHERE k = :p", p=3)

    statement = site(sql.select("z").from_("t"), fragment).build()

    normalized = " ".join(statement.sql.split())
    assert expected in normalized
    assert '"y"."k" = :p' in normalized
    assert statement.parameters == {"p": 3}


def test_where_and_join_render_sql_objects() -> None:
    """A real SQL object still renders as a WHERE or JOIN condition."""
    statement = (
        sql
        .select("z")
        .from_("t")
        .join("u", on=sql.raw("t.id = u.id AND u.k = :p", p=3))
        .where(sql.raw("b = :q", q=4))
        .build()
    )

    normalized = " ".join(statement.sql.split())
    assert "t.id = u.id" in normalized.replace('"', "")
    assert statement.parameters == {"p": 3, "q": 4}


def test_extract_sql_object_expression_rejects_sql_carrier_lookalike() -> None:
    """Extraction accepts SQL objects only."""
    with pytest.raises(TypeError, match="Value is not a SQL object"):
        parsing_utils.extract_sql_object_expression(_ExpressionLookalike(exp.column("injected_expression")))


@pytest.mark.parametrize("site", VALUE_SITES)
def test_value_sites_render_sql_objects_without_parameters(site: Any) -> None:
    """A SQL object with no parameters renders, because its empty parameter list merges as nothing."""
    statement = site(SQL("rendered_col + 1")).build()

    assert "rendered_col" in statement.sql
    assert "rendered_col" not in " ".join(str(value) for value in statement.parameters.values())


@pytest.mark.parametrize(
    "fragment",
    [pytest.param(SQL("b = ?", 1), id="positional"), pytest.param(SQL("b = :x AND c = ?", 5, x=1), id="mixed")],
)
@pytest.mark.parametrize("site", VALUE_SITES)
def test_value_sites_reject_positional_sql_objects(site: Any, fragment: SQL) -> None:
    """Positional parameters cannot merge into a builder's named parameters, so they raise instead of misbinding."""
    with pytest.raises(SQLBuilderError, match="Positional parameters"):
        site(fragment)


def test_coerce_column_splits_qualified_dotted_identifiers() -> None:
    """Dotted column strings split into table and column components."""
    col = parsing_utils._coerce_column("t.col")  # pyright: ignore[reportPrivateUsage]
    assert isinstance(col, exp.Column)
    assert col.table == "t"
    assert col.name == "col"


@pytest.mark.parametrize(
    "invalid_expr",
    [
        "",
        "   ",
        "col\x00",
        "a; SELECT 1",
        "name nulls first",
        "a = 1",
        "a AND b",
        "(SELECT 1)",
        "SELECT 1",
        "DROP TABLE t",
    ],
)
def test_parse_column_expression_rejects_non_scalar_inputs(invalid_expr: str) -> None:
    """Column expression parser rejects multi-statement, ordering, predicate, and query AST nodes."""
    with pytest.raises(SQLBuilderError):
        parse_column_expression(invalid_expr)


def test_parse_column_expression_disallows_alias_when_requested() -> None:
    """Predicate and grouping column parsing rejects AS aliases when allow_alias is False."""
    with pytest.raises(SQLBuilderError, match="Invalid column expression"):
        parse_column_expression("name AS alias_name", allow_alias=False)
    with pytest.raises(SQLBuilderError, match="Invalid column expression"):
        parse_column_expression("name DESC", allow_alias=False)


@pytest.mark.parametrize("invalid_order", ["", "col\x00", "a; SELECT 1", "name AS n", "(SELECT 1)", "a = 1"])
def test_parse_order_expression_rejects_invalid_clauses(invalid_order: str) -> None:
    """ORDER BY parser rejects aliases, predicates, subqueries, and statement terminators."""
    with pytest.raises(SQLBuilderError):
        parse_order_expression(invalid_order)


@pytest.mark.parametrize("invalid_cond", ["", "   ", "a = 1\x00", "a = 1; SELECT 1", "DROP TABLE t"])
def test_parse_condition_expression_rejects_multi_statement_and_non_conditions(invalid_cond: str) -> None:
    """Condition expression parser rejects empty strings, NUL bytes, semicolons, and command nodes."""
    with pytest.raises(SQLBuilderError):
        parse_condition_expression(invalid_cond)
