# pyright: reportAttributeAccessIssue=false
"""Unit tests for SELECT builder methods."""

from typing import Any, cast

import pytest
from sqlglot import exp

from sqlspec import sql
from sqlspec.builder._base import QueryBuilder
from sqlspec.exceptions import SQLBuilderError


def test_select_only_replaces_columns() -> None:
    """Test select_only() replaces existing selected columns."""
    query = sql.select("id", "name").from_("users")
    assert "id" in query.to_sql()
    assert "name" in query.to_sql()
    query.select_only("email")
    sql_str = query.to_sql()
    assert "id" not in sql_str
    assert "name" not in sql_str
    assert "email" in sql_str


def test_select_only_multiple_columns() -> None:
    """Test select_only() handles multiple columns."""
    query = sql.select("id").from_("users")
    query.select_only("name", "email")
    sql_str = query.to_sql()
    assert "id" not in sql_str
    assert "name" in sql_str
    assert "email" in sql_str


def test_select_only_on_new_query() -> None:
    """Test select_only() works on fresh query."""
    query = sql.select("id").from_("users")
    query.select_only("active")
    assert "active" in query.to_sql()


def test_select_only_with_expressions() -> None:
    """Test select_only() works with SQL expressions (strings)."""
    query = sql.select("id").from_("users")
    query.select_only("COUNT(id) AS count")
    sql_str = query.to_sql().upper()
    assert "COUNT" in sql_str
    assert "ID" in sql_str


def test_select_only_not_available_on_update() -> None:
    """Test select_only() is not available on UPDATE statements."""
    query = sql.update("users").set(active=True)
    with pytest.raises(AttributeError):
        query.select_only("id")


def test_order_by_raw_trailing_desc_emits_descending_sort() -> None:
    """sql.raw with trailing DESC must produce ORDER BY ... DESC, not an alias."""
    from sqlglot import exp

    raw_desc = cast("exp.Ordered", sql.raw("COALESCE(a, b, 0) DESC"))
    builder = sql.select("a", "b").from_("things").order_by(raw_desc)
    expr = builder._expression
    assert expr is not None
    order = expr.args.get("order")
    assert order is not None
    assert any(isinstance(o, exp.Ordered) and o.args.get("desc") for o in order.expressions)
    sql_text = builder.to_sql()
    assert "DESC" in sql_text
    assert 'AS "desc"' not in sql_text.lower()


def test_order_by_raw_trailing_asc_emits_ascending_sort() -> None:
    """sql.raw with trailing ASC must produce an ascending Ordered expression."""
    from sqlglot import exp

    raw_asc = cast("exp.Ordered", sql.raw("LOWER(a) ASC"))
    builder = sql.select("a").from_("things").order_by(raw_asc)
    expr = builder._expression
    assert expr is not None
    order = expr.args.get("order")
    assert order is not None
    assert all(isinstance(o, exp.Ordered) and o.args.get("desc") is False for o in order.expressions)


def test_order_by_raw_function_without_direction_unchanged() -> None:
    """sql.raw without trailing direction must continue to work as a sort key."""

    raw_expr = cast("exp.Ordered", sql.raw("COALESCE(a, b, 0)"))
    builder = sql.select("a").from_("things").order_by(raw_expr)
    sql_text = builder.to_sql()
    assert "COALESCE" in sql_text.upper()
    assert "ORDER BY" in sql_text.upper()


def test_select_hints_statement_hint_does_not_mutate_select_expression() -> None:
    query = sql.select("*").from_("users").with_hint("MAX_EXECUTION_TIME(1000)")
    result = query.build()
    assert "MAX_EXECUTION_TIME" in result.sql
    assert query._expression is not None
    assert query._expression.args.get("hint") is None


def test_select_hints_statement_hint_build_is_repeatable() -> None:
    query = sql.select("*").from_("users").with_hint("MAX_EXECUTION_TIME(1000)")
    first = query.build().sql
    second = query.build().sql
    assert second == first


def test_select_hints_statement_hint_preserves_cte_rendering() -> None:
    cte = sql.select("id").from_("users")
    query = sql.select("*").from_("active_users").with_cte("active_users", cte).with_hint("MAX_EXECUTION_TIME(1000)")
    query.enable_optimization = False
    rendered = query.build().sql.upper()
    assert rendered.startswith("WITH")
    assert '"ACTIVE_USERS" AS (' in rendered
    assert "MAX_EXECUTION_TIME" in rendered


def test_subquery_no_roundtrip_where_in_query_builder_subquery_does_not_call_build(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    subquery = sql.select("id").from_("users").where_eq("status", "active")

    def fail_build(self: QueryBuilder, *args: object, **kwargs: object) -> object:
        msg = "QueryBuilder.build() should not be needed to normalize a subquery"
        raise AssertionError(msg)

    monkeypatch.setattr(QueryBuilder, "build", fail_build)
    query = sql.select("*").from_("orders").where_in("user_id", subquery)
    assert "status" in query.parameters


def test_subquery_no_roundtrip_where_in_query_builder_subquery_still_renames_colliding_parameters() -> None:
    subquery = sql.select("id").from_("users").where_eq("status", "active")
    query = sql.select("*").from_("orders").where_eq("status", "pending").where_in("user_id", subquery)
    assert query.parameters["status"] == "pending"
    assert query.parameters["status_1"] == "active"


@pytest.mark.parametrize(
    "hint",
    ["INDEX(users idx_users_email)", "FULL(u) PARALLEL(u, 4)", "NO_INDEX(users)"],
    ids=["index", "multi", "no_index"],
)
def test_select_statement_hint_renders_verbatim(hint: str) -> None:
    query = sql.select("*", dialect="oracle").from_("users").with_hint(hint, dialect="oracle")
    assert query.build().sql.startswith(f"SELECT /*+ {hint} */")


def _normalized_sql(statement: Any) -> str:
    return " ".join(statement.sql.split())


def _union() -> exp.Expr:
    return cast("exp.Expr", exp.maybe_parse("SELECT 1 UNION SELECT 2"))


QUALIFIED_PREFIX = 'SELECT "t"."z" AS "z" FROM "t" AS "t" WHERE '


@pytest.mark.parametrize(
    ("apply", "expected"),
    [
        pytest.param(
            lambda q: q.join("u", on=sql.raw("t.id = u.id")),
            'SELECT "z" FROM "t" JOIN "u" ON "t"."id" = "u"."id"',
            id="join-raw",
        ),
        pytest.param(
            lambda q: q.join("u", on=exp.condition("t.a = u.a AND t.b = u.b")),
            'SELECT "z" FROM "t" JOIN "u" ON "t"."a" = "u"."a" AND "t"."b" = "u"."b"',
            id="join-node",
        ),
        pytest.param(
            lambda q: q.where_in("a", sql.raw("b + 1")), QUALIFIED_PREFIX + '"t"."a" IN ("t"."b" + 1)', id="where-in"
        ),
        pytest.param(
            lambda q: q.where_not_in("a", sql.raw("b + 1")),
            QUALIFIED_PREFIX + 'NOT "t"."a" IN ("t"."b" + 1)',
            id="where-not-in",
        ),
        pytest.param(
            lambda q: q.where_exists(_union()),
            QUALIFIED_PREFIX + 'EXISTS( SELECT 1 AS "1" UNION SELECT 2 AS "2" )',
            id="where-exists",
        ),
        pytest.param(
            lambda q: q.where_not_exists(_union()),
            QUALIFIED_PREFIX + 'NOT EXISTS( SELECT 1 AS "1" UNION SELECT 2 AS "2" )',
            id="where-not-exists",
        ),
    ],
)
def test_select_predicates_render_full_expression(apply: Any, expected: str) -> None:
    """Join and subquery predicates render every operand of a fragment or SQLGlot node."""
    assert _normalized_sql(apply(sql.select("z").from_("t")).build()) == expected


def test_join_and_exists_leave_caller_nodes_detached() -> None:
    """Join and EXISTS sites attach a copy of the caller's node."""
    condition = exp.condition("t.id = u.id")
    union = _union()

    sql.select("z").from_("t").join("u", on=condition).where_exists(union).build()

    assert condition.parent is None
    assert union.parent is None


PREDICATE_VALUES = [
    pytest.param(sql.column("b"), '"t"."b"', id="column"),
    pytest.param(sql.column("b").cast("INT"), 'CAST("t"."b" AS INT)', id="function-column"),
    pytest.param(sql.column("b") > 1, '"t"."b" > 1', id="column-expression"),
    pytest.param(sql.case().when("x > 1", 1).else_(0).end(), 'CASE WHEN "t"."x" > 1 THEN 1 ELSE 0 END', id="case"),
    pytest.param(sql.count("b"), 'COUNT("t"."b")', id="aggregate"),
    pytest.param(sql.coalesce("b", "c"), 'COALESCE("t"."b", "t"."c")', id="conversion"),
]


@pytest.mark.parametrize(("value", "fragment"), PREDICATE_VALUES)
@pytest.mark.parametrize(
    ("apply", "predicate", "parameters"),
    [
        pytest.param(lambda q, v: q.where_eq("a", v), '"t"."a" = {fragment}', {}, id="where-eq"),
        pytest.param(lambda q, v: q.where_gt("a", v), '"t"."a" > {fragment}', {}, id="where-gt"),
        pytest.param(lambda q, v: q.where(("a", ">=", v)), '"t"."a" >= {fragment}', {}, id="tuple"),
        pytest.param(lambda q, v: q.where_in("a", v), '"t"."a" IN ({fragment})', {}, id="where-in"),
        pytest.param(
            lambda q, v: q.where_in("a", [v, 7]), '"t"."a" IN ({fragment}, :a_2)', {"a_2": 7}, id="where-in-list"
        ),
        pytest.param(
            lambda q, v: q.where_between("a", v, 9),
            '"t"."a" <= :a_high AND "t"."a" >= {fragment}',
            {"a_high": 9},
            id="where-between",
        ),
    ],
)
def test_comparison_predicates_render_expression_values(
    apply: Any, predicate: str, parameters: dict[str, Any], value: Any, fragment: str
) -> None:
    """Comparison, IN, and BETWEEN predicates render wrappers as SQL."""
    statement = apply(sql.select("z").from_("t"), value).build()

    assert _normalized_sql(statement) == QUALIFIED_PREFIX + predicate.format(fragment=fragment)
    assert statement.parameters == parameters


def test_where_eq_renders_sqlglot_node() -> None:
    """A SQLGlot node compares as SQL instead of binding."""
    statement = sql.select("z").from_("t").where_eq("a", exp.column("b")).build()

    assert _normalized_sql(statement) == QUALIFIED_PREFIX + '"t"."a" = "t"."b"'
    assert statement.parameters == {}


def test_where_eq_keeps_set_operation_parentheses() -> None:
    """A set operation stays parenthesized as a scalar operand."""
    statement = sql.select("z").from_("t").where_eq("a", _union()).build()

    assert _normalized_sql(statement) == QUALIFIED_PREFIX + '"t"."a" = ( SELECT 1 AS "1" UNION SELECT 2 AS "2" )'


def test_where_eq_embeds_query_builder() -> None:
    """A query builder embeds as a scalar subquery with merged parameters."""
    statement = sql.select("z").from_("t").where_eq("a", sql.select("x").from_("y").where_eq("z", 5)).build()

    assert _normalized_sql(statement) == QUALIFIED_PREFIX + (
        '"t"."a" = ( SELECT "y"."x" AS "x" FROM "y" AS "y" WHERE "y"."z" = :z )'
    )
    assert statement.parameters == {"z": 5}


def test_where_in_renders_parameterized_fragment() -> None:
    """A parameterized fragment in IN renders as SQL and merges its parameters."""
    statement = sql.select("z").from_("t").where_in("a", sql.raw("b + :p", p=3)).build()

    assert _normalized_sql(statement) == QUALIFIED_PREFIX + '"t"."a" IN ("t"."b" + :p)'
    assert statement.parameters == {"p": 3}


def test_not_between_renders_expression_bound() -> None:
    """A NOT BETWEEN bound renders as SQL beside a bound parameter."""
    statement = sql.select("z").from_("t").where(("a", "NOT BETWEEN", (sql.column("b"), 9))).build()

    assert _normalized_sql(statement) == QUALIFIED_PREFIX + '"t"."a" < "t"."b" OR "t"."a" > :a_high'
    assert statement.parameters == {"a_high": 9}


@pytest.mark.parametrize(
    ("apply", "predicate", "parameters"),
    [
        pytest.param(lambda q: q.where_eq("a", "b"), '"t"."a" = :a', {"a": "b"}, id="where-eq"),
        pytest.param(lambda q: q.where_in("a", [1, 2]), '"t"."a" IN (:a_1, :a_2)', {"a_1": 1, "a_2": 2}, id="where-in"),
        pytest.param(
            lambda q: q.where_between("a", 1, 9),
            '"t"."a" <= :a_high AND "t"."a" >= :a_low',
            {"a_low": 1, "a_high": 9},
            id="where-between",
        ),
        pytest.param(lambda q: q.where_eq("a", None), '"t"."a" IS NULL', {}, id="where-eq-none"),
    ],
)
def test_between_keeps_plain_values_bound(apply: Any, predicate: str, parameters: dict[str, Any]) -> None:
    """Plain values keep their existing parameter names."""
    statement = apply(sql.select("z").from_("t")).build()

    assert _normalized_sql(statement) == QUALIFIED_PREFIX + predicate
    assert statement.parameters == parameters


@pytest.mark.parametrize(
    ("operator", "message"),
    [
        pytest.param("BETWEEN", "^BETWEEN operator requires a tuple of two values, got int$", id="between"),
        pytest.param("NOT BETWEEN", "^NOT BETWEEN operator requires a tuple of two values, got int$", id="not-between"),
    ],
)
def test_between_rejects_non_pair(operator: str, message: str) -> None:
    """Between predicates reject a value that is not a pair."""
    with pytest.raises(SQLBuilderError, match=message):
        sql.select("z").from_("t").where(("a", operator, 5))


def _projection_query() -> Any:
    return sql.select("x").from_("y").where_eq("z", 5)


def _projection_base() -> Any:
    return sql.select("z").from_("t")


SUBQUERY_PROJECTION = '( SELECT "y"."x" AS "x" FROM "y" AS "y" WHERE "y"."z" = :z )'


@pytest.mark.parametrize(
    ("builder", "expected", "parameters"),
    [
        pytest.param(
            lambda: sql.select(_projection_query()).from_("t"),
            f'SELECT {SUBQUERY_PROJECTION} AS "_col_0" FROM "t" AS "t"',
            {"z": 5},
            id="factory",
        ),
        pytest.param(
            lambda: _projection_base().select(_projection_query()),
            f'SELECT "t"."z" AS "z", {SUBQUERY_PROJECTION} AS "_col_1" FROM "t" AS "t"',
            {"z": 5},
            id="select",
        ),
        pytest.param(
            lambda: _projection_base().select_only(_projection_query()),
            f'SELECT {SUBQUERY_PROJECTION} AS "_col_0" FROM "t" AS "t"',
            {"z": 5},
            id="select-only",
        ),
        pytest.param(
            lambda: sql.select("a", dialect="postgres").from_("t").select(_projection_query()),
            'SELECT "t"."a" AS "a", ( SELECT "y"."x" AS "x" FROM "y" AS "y" WHERE "y"."z" = %(z)s ) AS "_col_1" '
            'FROM "t" AS "t"',
            {"z": 5},
            id="postgres",
        ),
        pytest.param(
            lambda: _projection_base().where_eq("z", 1).select(_projection_query()),
            'SELECT "t"."z" AS "z", ( SELECT "y"."x" AS "x" FROM "y" AS "y" WHERE "y"."z" = :z_1 ) AS "_col_1" '
            'FROM "t" AS "t" WHERE "t"."z" = :z',
            {"z": 1, "z_1": 5},
            id="name-collision",
        ),
        pytest.param(
            lambda: sql.select(exp.select("x").from_("y")).from_("t"),
            'SELECT ( SELECT "y"."x" AS "x" FROM "y" AS "y" ) AS "_col_0" FROM "t" AS "t"',
            {},
            id="select-node",
        ),
        pytest.param(
            lambda: sql.select(_union()).from_("t"),
            'SELECT ( SELECT 1 AS "1" UNION SELECT 2 AS "2" ) AS "_col_0" FROM "t" AS "t"',
            {},
            id="union-node",
        ),
        pytest.param(
            lambda: _projection_base().select(exp.select("x").from_("y").subquery("q")),
            'SELECT "t"."z" AS "z", ( SELECT "y"."x" AS "x" FROM "y" AS "y" ) AS "q" FROM "t" AS "t"',
            {},
            id="aliased-subquery",
        ),
    ],
)
def test_select_renders_query_projection(builder: Any, expected: str, parameters: dict[str, Any]) -> None:
    """Query builders and query nodes project as parenthesized scalar subqueries."""
    statement = builder().build()

    assert _normalized_sql(statement) == expected
    assert statement.parameters == parameters


def test_select_projection_leaves_caller_node_detached() -> None:
    """A projected query node is copied, leaving the caller's node detached."""
    node = exp.select("x").from_("y")

    sql.select(node).from_("t").build()

    assert node.parent is None


def test_where_in_renders_built_statement() -> None:
    """A statement from a query builder embeds with its named parameters."""
    subquery = sql.select("x").from_("y").where_eq("k", 5).to_statement()

    statement = sql.select("z").from_("t").where_in("a", subquery).build()

    assert _normalized_sql(statement) == QUALIFIED_PREFIX + (
        '"t"."a" IN (( SELECT "y"."x" AS "x" FROM "y" AS "y" WHERE "y"."k" = :k ))'
    )
    assert statement.parameters == {"k": 5}


@pytest.mark.parametrize(
    "projection",
    [
        pytest.param(lambda: sql.raw("SELECT x FROM y WHERE z = :z", z=5), id="raw"),
        pytest.param(lambda: sql.select("x").from_("y").where_eq("z", 5).to_statement(), id="statement"),
    ],
)
def test_select_renders_sql_object_query_projection(projection: Any) -> None:
    """A SQL object holding a query projects as a parenthesized scalar subquery."""
    statement = sql.select(projection()).from_("t").build()

    assert _normalized_sql(statement) == (
        'SELECT ( SELECT "y"."x" AS "x" FROM "y" AS "y" WHERE "y"."z" = :z ) AS "_col_0" FROM "t" AS "t"'
    )
    assert statement.parameters == {"z": 5}


@pytest.mark.parametrize(
    ("value", "array", "parameters"),
    [
        pytest.param(sql.column("b"), 'ARRAY("t"."b", :a_any_2)', {"a_any_2": 7}, id="column"),
        pytest.param(sql.raw("b + :p", p=3), 'ARRAY("t"."b" + :p, :a_any_2)', {"p": 3, "a_any_2": 7}, id="sql-object"),
        pytest.param(
            sql.select("x").from_("y").where_eq("k", 5),
            'ARRAY( ( SELECT "y"."x" AS "x" FROM "y" AS "y" WHERE "y"."k" = :k ), :a_any_2 )',
            {"k": 5, "a_any_2": 7},
            id="query-builder",
        ),
    ],
)
def test_where_any_renders_expression_elements(value: Any, array: str, parameters: dict[str, Any]) -> None:
    """Expression elements of a where_any list render as SQL beside bound plain values."""
    statement = sql.select("z").from_("t").where_any("a", [value, 7]).build()

    assert _normalized_sql(statement) == QUALIFIED_PREFIX + f'"t"."a" = ANY( {array} )'
    assert statement.parameters == parameters


def test_where_not_any_renders_expression_elements() -> None:
    """Expression elements of a where_not_any list render as SQL."""
    statement = sql.select("z").from_("t").where_not_any("a", [sql.column("b")]).build()

    assert _normalized_sql(statement) == QUALIFIED_PREFIX + '"t"."a" <> ALL ( ARRAY("t"."b") )'
    assert statement.parameters == {}
