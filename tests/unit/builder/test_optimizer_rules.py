"""Regression tests for query builder optimizer rule wiring."""

from collections.abc import Callable
from typing import cast

import pytest
from sqlglot.optimizer import RULES
from sqlglot.optimizer.optimize_joins import optimize_joins as _optimize_joins_rule
from sqlglot.optimizer.pushdown_predicates import pushdown_predicates as _pushdown_predicates_rule
from sqlglot.optimizer.simplify import simplify as _simplify_rule

from sqlspec.builder import Select


class _NoOpCache:
    def get_optimized(self, _cache_key: object) -> None:
        return None

    def put_optimized(self, _cache_key: object, _optimized: object) -> None:
        return None


def _build_join_where_query(**builder_kwargs: object) -> Select:
    return (
        Select("users.id", "orders.total", **builder_kwargs)
        .from_("users")
        .join("orders", "users.id = orders.user_id")
        .where("orders.status = 'open'")
    )


def _capture_optimize_kwargs(monkeypatch: pytest.MonkeyPatch) -> dict[str, object]:
    captured_kwargs: dict[str, object] = {}

    def fake_optimize(expression: object, **kwargs: object) -> object:
        captured_kwargs.update(kwargs)
        return expression

    monkeypatch.setattr("sqlspec.builder._base.get_cache", lambda: _NoOpCache())
    monkeypatch.setattr("sqlspec.builder._base.optimize", fake_optimize)
    return captured_kwargs


def test_default_builder_renders_stable_optimized_sql() -> None:
    query = _build_join_where_query()

    assert query.build().sql == (
        "SELECT\n"
        '  "users"."id" AS "id",\n'
        '  "orders"."total" AS "total"\n'
        'FROM "users" AS "users"\n'
        'JOIN "orders" AS "orders"\n'
        '  ON "orders"."status" = \'open\' AND "orders"."user_id" = "users"."id"'
    )


def test_optimizer_passes_rules_tuple_and_omits_optimizer_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_kwargs = _capture_optimize_kwargs(monkeypatch)

    _build_join_where_query().build()

    assert captured_kwargs["rules"] is RULES
    rules = cast(tuple[Callable[..., object], ...], captured_kwargs["rules"])
    assert RULES[0] in rules
    assert "optimizer_settings" not in captured_kwargs


@pytest.mark.parametrize(
    ("builder_kwargs", "excluded_rule"),
    [
        ({"optimize_joins": False}, _optimize_joins_rule),
        ({"optimize_predicates": False}, _pushdown_predicates_rule),
        ({"simplify_expressions": False}, _simplify_rule),
    ],
)
def test_disabled_optimizer_flag_removes_only_its_rule(
    monkeypatch: pytest.MonkeyPatch, builder_kwargs: dict[str, object], excluded_rule: Callable[..., object]
) -> None:
    captured_kwargs = _capture_optimize_kwargs(monkeypatch)

    _build_join_where_query(**builder_kwargs).build()

    rules = cast(tuple[Callable[..., object], ...], captured_kwargs["rules"])
    expected_rules = tuple(rule for rule in RULES if rule is not excluded_rule)

    assert isinstance(rules, tuple)
    assert rules == expected_rules
    assert RULES[0] in rules
    assert "optimizer_settings" not in captured_kwargs
