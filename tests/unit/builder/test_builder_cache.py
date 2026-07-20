# pyright: reportPrivateUsage = false
"""Regression tests for structural query-builder cache identity."""

from typing import Any

import pytest
from sqlglot import exp, parse_one

from sqlspec import sql
from sqlspec.builder import Select


def test_builder_cache_key_includes_complete_cte_body() -> None:
    first = sql.select("*").from_("recent").with_cte("recent", "SELECT 1 AS value")
    second = sql.select("*").from_("recent").with_cte("recent", "SELECT 2 AS value")

    assert first._cache_key() != second._cache_key()


def test_builder_cache_key_preserves_placeholder_name_case() -> None:
    first = Select("*").from_("users")
    first.set_expression(parse_one("SELECT * FROM users WHERE id = :UserID"))
    first.load_parameters({"UserID": 1})
    second = Select("*").from_("users")
    second.set_expression(parse_one("SELECT * FROM users WHERE id = :userid"))
    second.load_parameters({"userid": 1})

    assert first._cache_key() != second._cache_key()


def test_insert_values_mutation_invalidates_sqlglot_structural_hash() -> None:
    builder = sql.insert("items").columns("value").values(1)
    expression = builder.get_expression()
    assert expression is not None
    initial_hash = hash(expression)

    builder.values(2)

    assert hash(expression) != initial_hash


def test_optimized_expression_cache_returns_owned_copies(monkeypatch: pytest.MonkeyPatch) -> None:
    cache = _ExpressionCache()
    monkeypatch.setattr("sqlspec.builder._base.get_cache", lambda: cache)
    monkeypatch.setattr("sqlspec.builder._base.optimize", _copy_expression)
    builder = Select("*").from_("users")
    expression = builder.get_expression()
    assert expression is not None

    first = builder._optimize_expression(expression)
    second = builder._optimize_expression(expression)

    assert first is not second
    first.set("limit", exp.Limit(expression=exp.Literal.number(1)))
    assert second.args.get("limit") is None


class _ExpressionCache:
    def __init__(self) -> None:
        self.values: dict[str, exp.Expr] = {}

    def get_optimized(self, key: str) -> exp.Expr | None:
        return self.values.get(key)

    def put_optimized(self, key: str, value: exp.Expr) -> None:
        self.values[key] = value


def _copy_expression(expression: exp.Expr, **_: Any) -> exp.Expr:
    return expression.copy()
