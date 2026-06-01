# pyright: reportPrivateUsage = false
"""Tests for SQL dialect helper functions."""

from unittest.mock import PropertyMock, patch

from sqlglot import exp, parse_one

from sqlspec.core import SQL
from sqlspec.driver import _sql_helpers
from sqlspec.driver._sql_helpers import convert_to_dialect


def test_convert_to_dialect_sql_object_skips_parse_one() -> None:
    """SQL objects with cached expressions use the parsed AST directly."""
    statement = SQL("SELECT 1")
    statement.compile()
    assert statement.expression is not None

    with patch("sqlspec.driver._sql_helpers.parse_one") as mock_parse:
        result = convert_to_dialect(statement, source_dialect="sqlite")

    mock_parse.assert_not_called()
    assert "SELECT" in result.upper()


def test_convert_to_dialect_raw_string_calls_parse_one() -> None:
    """Raw strings are documented to parse on every call."""
    with patch("sqlspec.driver._sql_helpers.parse_one", wraps=parse_one) as mock_parse:
        result = convert_to_dialect("SELECT 1", source_dialect="sqlite")

    mock_parse.assert_called_once()
    assert "SELECT" in result.upper()


def test_convert_to_dialect_raw_string_multiple_calls_each_invokes_parse_one() -> None:
    """There is intentionally no parse cache for the raw-string helper path."""
    with patch("sqlspec.driver._sql_helpers.parse_one", wraps=parse_one) as mock_parse:
        convert_to_dialect("SELECT 1", source_dialect="sqlite")
        convert_to_dialect("SELECT 1", source_dialect="sqlite")

    assert mock_parse.call_count == 2


def test_convert_to_dialect_expr_object_skips_parse_one() -> None:
    """SQLGlot expressions are already parsed and should not be re-parsed."""
    expression = exp.select("1")

    with patch("sqlspec.driver._sql_helpers.parse_one") as mock_parse:
        result = convert_to_dialect(expression, source_dialect="sqlite")

    mock_parse.assert_not_called()
    assert "SELECT" in result.upper()


def test_convert_to_dialect_sql_object_reads_expression_once() -> None:
    """The SQL-object fast path should dispatch the expression property once."""
    expression = exp.select("1")

    with patch.object(SQL, "expression", new_callable=PropertyMock, return_value=expression) as mock_expression:
        result = convert_to_dialect(SQL("SELECT 1"), source_dialect="sqlite")

    assert "SELECT" in result.upper()
    mock_expression.assert_called_once()


def test_statement_not_importable_from_sql_helpers_at_runtime() -> None:
    """Statement is TYPE_CHECKING-only in the mypyc-compiled helper module."""
    assert not hasattr(_sql_helpers, "Statement")
