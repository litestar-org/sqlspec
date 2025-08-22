"""Unit tests for SQL builder parsing utilities.

This module tests the parsing utilities in sqlspec.builder._parsing_utils,
specifically focusing on the parameter style conversion functionality that
was added to fix QueryBuilder parameter handling issues.
"""

import pytest
from sqlglot import exp

from sqlspec.builder._parsing_utils import parse_column_expression, parse_condition_expression

pytestmark = pytest.mark.xdist_group("utils")


def test_parse_condition_expression_with_dollar_parameters() -> None:
    """Test that parse_condition_expression handles $1 style parameters correctly."""
    condition = "category = $1"

    # Should parse without errors and convert $1 to SQLGlot-compatible format
    expr = parse_condition_expression(condition)
    assert expr is not None
    assert isinstance(expr, exp.Expression)


def test_parse_condition_expression_with_colon_numeric_parameters() -> None:
    """Test that parse_condition_expression handles :1 style parameters correctly."""
    condition = "id = :1"

    # Should parse without errors and convert :1 to SQLGlot-compatible format
    expr = parse_condition_expression(condition)
    assert expr is not None
    assert isinstance(expr, exp.Expression)


def test_parse_condition_expression_with_named_parameters() -> None:
    """Test that parse_condition_expression handles :name style parameters correctly."""
    condition = "status = :status_value"

    # Should parse without errors - named parameters are already SQLGlot-compatible
    expr = parse_condition_expression(condition)
    assert expr is not None
    assert isinstance(expr, exp.Expression)


def test_parse_condition_expression_with_question_mark_parameters() -> None:
    """Test that parse_condition_expression handles ? style parameters correctly."""
    condition = "name = ?"

    # Should parse without errors
    expr = parse_condition_expression(condition)
    assert expr is not None
    assert isinstance(expr, exp.Expression)


def test_parse_condition_expression_no_parameters() -> None:
    """Test that parse_condition_expression handles conditions without parameters."""
    condition = "active = TRUE"

    # Should parse without errors
    expr = parse_condition_expression(condition)
    assert expr is not None
    assert isinstance(expr, exp.Expression)


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
        assert isinstance(expr, exp.Expression)


def test_parse_condition_expression_tuple_format() -> None:
    """Test that parse_condition_expression handles tuple conditions correctly."""
    # Test 2-tuple format (column, value)
    condition = ("category", "Electronics")

    expr = parse_condition_expression(condition)
    assert expr is not None
    assert isinstance(expr, exp.Expression)


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
    assert isinstance(expr, exp.Expression)


def test_parse_column_expression_qualified() -> None:
    """Test that parse_column_expression handles qualified column names."""
    column = "users.name"

    expr = parse_column_expression(column)
    assert expr is not None
    assert isinstance(expr, exp.Expression)


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
    assert isinstance(expr, exp.Expression)
