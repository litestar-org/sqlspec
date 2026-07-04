"""Unit tests for SQL builder parsing utilities.

This module tests the parsing utilities in sqlspec.builder,
specifically focusing on the parameter style conversion functionality that
was added to fix QueryBuilder parameter handling issues.
"""

import contextlib
from typing import Any

import pytest
from sqlglot import exp
from sqlglot.errors import ParseError

import sqlspec.builder._parsing_utils as parsing_utils
from sqlspec import sql
from sqlspec.builder import (
    Select,
    parse_column_expression,
    parse_condition_expression,
    parse_order_expression,
    parse_table_expression,
)
from sqlspec.core import get_cache


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


_PARSING_CORPUS = [
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
    "name DESC",
    "users.name asc",
    "COUNT(*) DESC",
    "name nulls first",
]


def _parse_column_oracle(value: str) -> exp.Expr:
    return exp.maybe_parse(value) or exp.column(value)


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


@pytest.mark.parametrize("value", _PARSING_CORPUS)
def test_parse_column_expression_matches_parser_oracle(value: str) -> None:
    """Column parser fast paths must preserve the previous parser-backed shape."""
    _assert_matches_oracle(value, parse_column_expression, _parse_column_oracle)


@pytest.mark.parametrize("value", _PARSING_CORPUS)
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

    monkeypatch.setattr("sqlspec.builder._base.optimize", fake_optimize)
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
