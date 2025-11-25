"""Tests for cached static expression helper on QueryBuilder."""

import pytest
from sqlglot import exp

from sqlspec import sql
from sqlspec.core import get_cache

pytestmark = pytest.mark.xdist_group("builder")


def test_cached_static_expression_reuses_factory() -> None:
    cache = get_cache()
    cache.clear()

    factory_calls = {"count": 0}

    def factory() -> exp.Expression:
        factory_calls["count"] += 1
        return exp.select("1")

    builder = sql.select()

    first = builder.build_static_expression(cache_key="static:test", expression_factory=factory, parameters={"p": 1})

    second = builder.build_static_expression(cache_key="static:test", expression_factory=factory, parameters={"p": 2})

    assert factory_calls["count"] == 1
    assert first.parameters == {"p": 1}
    assert second.parameters == {"p": 2}
    assert first.sql == second.sql


def test_cached_static_expression_respects_copy_flag() -> None:
    cache = get_cache()
    cache.clear()

    base_expr = exp.select(exp.column("a"))

    builder = sql.select()

    result = builder.build_static_expression(
        cache_key="static:copy", expression_factory=lambda: base_expr, copy=True, parameters={"val": 123}
    )

    # Cached expression should remain unchanged when caller mutates the original
    base_expr.set("from", exp.from_("tbl"))

    repeat = builder.build_static_expression(
        cache_key="static:copy", expression_factory=lambda: base_expr, copy=True, parameters={"val": 456}
    )

    assert "tbl" not in result.sql
    assert "tbl" not in repeat.sql
    assert repeat.parameters == {"val": 456}
