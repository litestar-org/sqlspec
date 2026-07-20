# pyright: reportPrivateUsage = false
"""Regression tests for structural query-builder cache identity."""

from typing import Any

import pytest
from sqlglot import exp, parse_one

from sqlspec import sql
from sqlspec.builder import QueryBuilder, Select
from sqlspec.core import CacheConfig, LimitOffsetFilter, NamespacedCache, StatementConfig
from sqlspec.core.parameters import TypedParameter
from sqlspec.core.sqlcommenter import SQLCommenterContext


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


@pytest.mark.parametrize("builder_kind", ["select", "insert", "update", "delete", "merge"])
def test_same_shape_builder_values_rebind_into_distinct_statements(
    monkeypatch: pytest.MonkeyPatch, builder_kind: str
) -> None:
    cache = _install_builder_cache(monkeypatch)

    first = _builder_for_kind(builder_kind, "first").to_statement()
    second = _builder_for_kind(builder_kind, "second").to_statement()

    assert first is not second
    assert first.raw_expression is not second.raw_expression
    assert "first" in first.named_parameters.values()
    assert "second" in second.named_parameters.values()
    stats = cache._caches["builder"].get_stats()
    assert (stats.misses, stats.hits) == (1, 1)
    assert len(cache._caches["builder"]._cache) == 1


def test_same_shape_cte_values_rebind_on_builder_cache_hit(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_builder_cache(monkeypatch)
    first_cte = sql.select("id").from_("events").where_eq("kind", "first")
    second_cte = sql.select("id").from_("events").where_eq("kind", "second")

    first = sql.select("*").from_("recent").with_cte("recent", first_cte).to_statement()
    second = sql.select("*").from_("recent").with_cte("recent", second_cte).to_statement()

    assert first is not second
    assert "first" in first.named_parameters.values()
    assert "second" in second.named_parameters.values()


def test_builder_cache_hit_uses_exact_current_statement_config(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_builder_cache(monkeypatch)
    first_config = StatementConfig(output_transformer=_append_first_marker)
    second_config = StatementConfig(output_transformer=_append_second_marker)

    first = _builder_for_kind("select", "value").to_statement(first_config)
    second = _builder_for_kind("select", "other").to_statement(second_config)

    assert first is not second
    assert first.statement_config is first_config
    assert second.statement_config is second_config
    assert first.compile()[0].endswith("/* first */")
    assert second.compile()[0].endswith("/* second */")


def test_builder_cache_hit_preserves_current_typed_parameter(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_builder_cache(monkeypatch)
    first_value = TypedParameter(1, int, "identifier")
    second_value = TypedParameter("2", str, "identifier")

    first = sql.select("*").from_("items").where_eq("id", first_value).to_statement()
    second = sql.select("*").from_("items").where_eq("id", second_value).to_statement()

    assert next(iter(first.named_parameters.values())) is first_value
    assert next(iter(second.named_parameters.values())) is second_value


def test_builder_cache_bypasses_per_statement_disabled_config(monkeypatch: pytest.MonkeyPatch) -> None:
    cache = _install_builder_cache(monkeypatch)
    config = StatementConfig(enable_caching=False)

    first = _builder_for_kind("select", "first").to_statement(config)
    second = _builder_for_kind("select", "second").to_statement(config)

    assert first is not second
    assert first.statement_config is config
    assert second.statement_config is config
    stats = cache._caches["builder"].get_stats()
    assert (stats.misses, stats.hits) == (0, 0)


def test_builder_cache_bypasses_global_disabled_config(monkeypatch: pytest.MonkeyPatch) -> None:
    cache = _install_builder_cache(monkeypatch)
    monkeypatch.setattr("sqlspec.builder._base.get_cache_config", lambda: CacheConfig(compiled_cache_enabled=False))

    first = _builder_for_kind("select", "first").to_statement()
    second = _builder_for_kind("select", "second").to_statement()

    assert first is not second
    stats = cache._caches["builder"].get_stats()
    assert (stats.misses, stats.hits) == (0, 0)


def test_builder_cache_lru_evicts_distinct_shapes(monkeypatch: pytest.MonkeyPatch) -> None:
    cache = _install_builder_cache(monkeypatch, max_size=1)

    sql.select("id").from_("items").to_statement()
    sql.select("name").from_("items").to_statement()
    sql.select("id").from_("items").to_statement()

    stats = cache._caches["builder"].get_stats()
    assert (stats.misses, stats.hits, stats.evictions) == (3, 0, 2)
    assert len(cache._caches["builder"]._cache) == 1


def test_builder_mutation_after_cache_hit_uses_new_template(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_builder_cache(monkeypatch)
    builder = sql.select("*").from_("items").where_eq("id", 1)
    first = builder.to_statement()
    builder.where_eq("active", True)

    second = builder.to_statement()

    assert first is not second
    assert first.raw_expression != second.raw_expression
    assert "active" not in first.sql
    assert "active" in second.sql


def test_builder_cache_hit_keeps_filters_isolated(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_builder_cache(monkeypatch)
    builder = sql.select("*").from_("items")

    first = builder.apply_filters(LimitOffsetFilter(limit=1, offset=0))
    second = builder.apply_filters(LimitOffsetFilter(limit=2, offset=1))

    assert first is not second
    assert first.raw_expression is not second.raw_expression
    assert first.named_parameters != second.named_parameters


def test_dynamic_sqlcommenter_context_is_late_bound_after_builder_hit(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_builder_cache(monkeypatch)
    config = StatementConfig(enable_sqlcommenter=True, sqlcommenter_enable_context=True)

    with SQLCommenterContext.scope({"route": "/first"}):
        first_sql, _ = _builder_for_kind("select", "value").to_statement(config).compile()
    with SQLCommenterContext.scope({"route": "/second"}):
        second_sql, _ = _builder_for_kind("select", "other").to_statement(config).compile()

    assert "route='%2Ffirst'" in first_sql
    assert "route='%2Fsecond'" in second_sql
    assert "route='%2Ffirst'" not in second_sql


def test_static_script_embedding_uses_current_builder_value(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_builder_cache(monkeypatch)

    first_sql, first_parameters = _builder_for_kind("select", "first").to_statement().as_script().compile()
    second_sql, second_parameters = _builder_for_kind("select", "second").to_statement().as_script().compile()

    assert first_sql != second_sql
    assert "first" in first_sql
    assert "second" in second_sql
    assert first_parameters is None
    assert second_parameters is None


class _ExpressionCache:
    def __init__(self) -> None:
        self.values: dict[str, exp.Expr] = {}

    def get_optimized(self, key: str) -> exp.Expr | None:
        return self.values.get(key)

    def put_optimized(self, key: str, value: exp.Expr) -> None:
        self.values[key] = value


def _copy_expression(expression: exp.Expr, **_: Any) -> exp.Expr:
    return expression.copy()


def _install_builder_cache(monkeypatch: pytest.MonkeyPatch, max_size: int = 32) -> NamespacedCache:
    cache = NamespacedCache(CacheConfig(sql_cache_size=max_size), ttl_seconds=None)
    monkeypatch.setattr("sqlspec.builder._base.get_cache", lambda: cache)
    return cache


def _builder_for_kind(builder_kind: str, value: Any) -> QueryBuilder:
    if builder_kind == "select":
        return sql.select("*").from_("items").where_eq("value", value)
    if builder_kind == "insert":
        return sql.insert("items").columns("value").values(value)
    if builder_kind == "update":
        return sql.update("items").set(value=value).where_eq("id", 1)
    if builder_kind == "delete":
        return sql.delete("items").where_eq("value", value)
    return (
        sql
        .merge("items")
        .using("incoming", alias="source")
        .on("items.id = source.id")
        .when_matched_then_update(value=value)
    )


def _append_first_marker(sql_text: str, parameters: Any) -> tuple[str, Any]:
    return f"{sql_text} /* first */", parameters


def _append_second_marker(sql_text: str, parameters: Any) -> tuple[str, Any]:
    return f"{sql_text} /* second */", parameters
