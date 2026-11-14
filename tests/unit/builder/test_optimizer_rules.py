"""Regression tests for query builder optimizer rule wiring."""

from collections.abc import Callable
from typing import cast

import pytest
from sqlglot.optimizer import RULES
from sqlglot.optimizer.optimize_joins import optimize_joins as _optimize_joins_rule
from sqlglot.optimizer.pushdown_predicates import pushdown_predicates as _pushdown_predicates_rule
from sqlglot.optimizer.simplify import simplify as _simplify_rule

from sqlspec import sql
from sqlspec.builder import Select
from sqlspec.builder._base import _pushdown_predicates_without_simplify


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
    monkeypatch.setattr("sqlglot.optimizer.optimize", fake_optimize)
    return captured_kwargs


def test_default_builder_renders_stable_optimized_sql() -> None:
    query = _build_join_where_query()

    assert query.build().sql == (
        "SELECT\n"
        '  "users"."id" AS "id",\n'
        '  "orders"."total" AS "total"\n'
        'FROM "users" AS "users"\n'
        'JOIN "orders" AS "orders"\n'
        '  ON "users"."id" = "orders"."user_id" AND "orders"."status" = \'open\''
    )

    simplified_query = _build_join_where_query(simplify_expressions=True)
    assert simplified_query.build().sql == (
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

    rules = cast(tuple[Callable[..., object], ...], captured_kwargs["rules"])
    expected_default_rules = tuple(
        (_pushdown_predicates_without_simplify if rule is _pushdown_predicates_rule else rule)
        for rule in RULES
        if rule is not _simplify_rule
    )
    assert rules == expected_default_rules
    assert _simplify_rule not in rules
    assert _pushdown_predicates_without_simplify in rules
    assert RULES[0] in rules
    assert "optimizer_settings" not in captured_kwargs

    captured_kwargs.clear()
    _build_join_where_query(simplify_expressions=True).build()

    assert captured_kwargs["rules"] is RULES
    assert "optimizer_settings" not in captured_kwargs


@pytest.mark.parametrize(
    ("builder_kwargs", "expected_rules"),
    [
        (
            {"optimize_joins": False, "simplify_expressions": True},
            tuple(rule for rule in RULES if rule is not _optimize_joins_rule),
        ),
        (
            {"optimize_predicates": False, "simplify_expressions": True},
            tuple(rule for rule in RULES if rule is not _pushdown_predicates_rule),
        ),
        (
            {"simplify_expressions": False},
            tuple(
                (_pushdown_predicates_without_simplify if rule is _pushdown_predicates_rule else rule)
                for rule in RULES
                if rule is not _simplify_rule
            ),
        ),
        (
            {"optimize_predicates": False, "simplify_expressions": False},
            tuple(rule for rule in RULES if rule not in {_pushdown_predicates_rule, _simplify_rule}),
        ),
    ],
)
def test_disabled_optimizer_flag_removes_only_its_rule(
    monkeypatch: pytest.MonkeyPatch,
    builder_kwargs: dict[str, object],
    expected_rules: tuple[Callable[..., object], ...],
) -> None:
    captured_kwargs = _capture_optimize_kwargs(monkeypatch)

    _build_join_where_query(**builder_kwargs).build()

    rules = cast(tuple[Callable[..., object], ...], captured_kwargs["rules"])

    assert isinstance(rules, tuple)
    assert rules == expected_rules
    assert RULES[0] in rules
    assert "optimizer_settings" not in captured_kwargs


def test_simplify_expressions_defaults_to_false() -> None:
    """QueryBuilder disables expression simplification unless opted in."""
    assert Select("*").simplify_expressions is False
    assert sql.select("*").simplify_expressions is False


@pytest.mark.parametrize(
    ("condition", "expected_fragment"),
    [
        ("NOT b < 1 AND NOT b <= 1", 'NOT "t"."b" < 1 AND NOT "t"."b" <= 1'),
        ("NOT (a < 1 AND b <= 2)", 'NOT ( "t"."a" < 1 AND "t"."b" <= 2 )'),
    ],
    ids=["negated-inequalities", "negated-conjunction"],
)
def test_default_builder_preserves_unsimplified_boolean_conditions(condition: str, expected_fragment: str) -> None:
    """Compound negated predicates keep their structure when simplify_expressions is False."""
    built = sql.select("*").from_("t").where(condition).build()

    assert expected_fragment in " ".join(built.sql.split())
    assert '"t"."b" >= 1' not in built.sql


def test_opt_in_simplify_expressions_simplifies_and_uses_distinct_cache_entry() -> None:
    """Opting in to simplify_expressions=True simplifies predicates without polluting default cache."""
    unsimplified = Select("*").from_("t").where("NOT b < 1 AND NOT b <= 1").build()
    simplified = Select("*", simplify_expressions=True).from_("t").where("NOT b < 1 AND NOT b <= 1").build()
    unsimplified_again = Select("*").from_("t").where("NOT b < 1 AND NOT b <= 1").build()

    assert 'NOT "t"."b" < 1 AND NOT "t"."b" <= 1' in " ".join(unsimplified.sql.split())
    assert '"t"."b" >= 1' in simplified.sql
    assert unsimplified_again.sql == unsimplified.sql


def test_predicate_pushdown_without_simplify_removes_pushed_where_true_and_keeps_unpushed() -> None:
    """Predicate pushdown strips its pushed WHERE TRUE placeholder while preserving remaining predicates."""
    built = (
        Select("users.id", "orders.total")
        .from_("users")
        .join("orders", "users.id = orders.user_id")
        .where("orders.status = 'open'")
        .where("NOT users.b < 1 AND NOT users.b <= 1")
        .build()
    )
    rendered = " ".join(built.sql.split())

    assert '"orders"."status" = \'open\'' in rendered
    assert 'NOT "users"."b" < 1 AND NOT "users"."b" <= 1' in rendered
    assert "TRUE" not in rendered
