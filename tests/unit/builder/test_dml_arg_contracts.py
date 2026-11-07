"""Render regressions for DML builders that silently dropped clauses.

UPDATE ... FROM lost its FROM clause while keeping the correlated WHERE,
UPDATE ... JOIN lost the join and its condition, DELETE with a CTE lost the
DELETE statement entirely, and every builder CTE rendered with the alias and
query swapped. Each came from passing arguments sqlglot's generator ignores
or from calling ``with_`` with reversed arguments.
"""

import re
from typing import Any

import pytest
from sqlglot import exp

from sqlspec import sql
from sqlspec.builder import Column
from sqlspec.exceptions import SQLBuilderError


def test_update_from_renders_from_clause() -> None:
    stmt = sql.update("t").set("a", 1).from_("other").where("t.id = other.id").build(dialect="postgres")
    assert "FROM" in stmt.sql
    assert "other" in stmt.sql


def test_update_from_multiple_tables_renders_all() -> None:
    stmt = sql.update("t").set("a", 1).from_("x").from_("y").where("t.id = x.id").build(dialect="postgres")
    assert "FROM" in stmt.sql
    assert "x" in stmt.sql
    assert "y" in stmt.sql


def test_update_join_renders_join_and_condition() -> None:
    stmt = sql.update("t").join("j", on="t.id = j.id").set("a", 1).build(dialect="mysql")
    assert "JOIN" in stmt.sql
    assert "j" in stmt.sql.split("JOIN")[-1]


def test_delete_with_cte_renders_delete_statement() -> None:
    cte = sql.select("id").from_("src")
    stmt = sql.delete().from_("t").where_in("id", cte).with_cte("x", cte).build()
    assert "WITH" in stmt.sql
    assert "DELETE FROM" in stmt.sql


def test_cte_renders_alias_before_query() -> None:
    cte = sql.select("id").from_("users")
    builder = sql.select("*").from_("active").with_cte("active", cte)
    builder.enable_optimization = False
    stmt = builder.build()
    assert re.search(r'WITH\s+"?active"?\s+AS\s+\(', stmt.sql), stmt.sql


def test_update_with_cte_renders_alias_before_query() -> None:
    cte = sql.select("id").from_("users")
    builder = sql.update("t").set("a", 1).with_cte("u", cte).where("t.id = 1")
    builder.enable_optimization = False
    stmt = builder.build(dialect="postgres")
    assert re.search(r'WITH\s+"?u"?\s+AS\s+\(', stmt.sql), stmt.sql


@pytest.mark.parametrize(
    ("columns", "expected"),
    [
        pytest.param(("*",), "*", id="star"),
        pytest.param(("a AS x",), '"a" AS "x"', id="alias"),
        pytest.param(("t.a",), '"t"."a"', id="qualified"),
        pytest.param(("a + 1 AS x",), '"a" + 1 AS "x"', id="arithmetic"),
        pytest.param(("a b",), '"a" AS "b"', id="implicit-alias"),
        pytest.param(('"a b"',), '"a b"', id="quoted"),
        pytest.param(("first-name",), '"first" - "name"', id="hyphen-is-subtraction"),
        pytest.param(('"first-name"',), '"first-name"', id="quoted-hyphen"),
        pytest.param((exp.column("first-name"),), '"first-name"', id="explicit-hyphen-column"),
        pytest.param((exp.column("a b"),), '"a b"', id="explicit-space-column"),
        pytest.param(("user", "default", "order"), '"user", "default", "order"', id="keywords"),
        pytest.param((" a ",), '"a"', id="whitespace"),
        pytest.param(("(SELECT 1) AS m",), '(\n  SELECT\n    1 AS "1"\n) AS "m"', id="subquery"),
        pytest.param((Column("a"),), '"a"', id="column-object"),
        pytest.param((Column("a").upper(),), 'UPPER("a")', id="function-column"),
        pytest.param((sql.count(),), "COUNT(*)", id="aggregate-wrapper"),
        pytest.param((exp.Star(),), "*", id="star-node"),
        pytest.param((exp.column("a"),), '"a"', id="column-node"),
    ],
)
def test_returning_string_projections(columns: "tuple[Any, ...]", expected: str) -> None:
    built = sql.update("t").set(a=1).returning(*columns).build(dialect="postgres")
    assert built.sql == f'UPDATE "t" SET "a" = %(a)s\nRETURNING {expected}'


@pytest.mark.parametrize(
    ("dialect", "expected"),
    [
        pytest.param("sqlite", 'UPDATE "t" SET "a" = :a\nRETURNING "a" + 1 AS "x", "t"."a"', id="sqlite"),
        pytest.param("duckdb", 'UPDATE "t" SET "a" = $a\nRETURNING "a" + 1 AS "x", "t"."a"', id="duckdb"),
        pytest.param("mariadb", "UPDATE `t` SET `a` = :a\nRETURNING `a` + 1 AS `x`, `t`.`a`", id="mariadb"),
        pytest.param("oracle", "UPDATE t SET a = :a\nRETURNING a + 1 AS x, t.a", id="oracle"),
    ],
)
def test_returning_strings_render_per_dialect(dialect: str, expected: str) -> None:
    assert sql.update("t").set(a=1).returning("a + 1 AS x", "t.a").build(dialect=dialect).sql == expected


def test_returning_parses_with_builder_dialect() -> None:
    built = sql.update("t", dialect="mssql").set(a=1).returning("[a b] AS c").build(dialect="postgres")
    assert built.sql == 'UPDATE "t" SET "a" = %(a)s\nRETURNING "a b" AS "c"'


@pytest.mark.parametrize(
    "column",
    [
        "'abc",
        '"a',
        "a /* x",
        "",
        " ",
        "a, b",
        "a; DROP TABLE t",
        "DROP TABLE t",
        "a FROM x",
        "DISTINCT a",
        "a +",
        "SELECT 1",
        "[a b] AS c",
    ],
)
def test_returning_rejects_invalid_strings(column: str) -> None:
    with pytest.raises(SQLBuilderError, match="Invalid RETURNING expression"):
        sql.update("t").set(a=1).returning(column)


def test_returning_requires_a_column() -> None:
    with pytest.raises(SQLBuilderError, match="at least one"):
        sql.update("t").set(a=1).returning()


class _SqlCarrierLookalike:
    """Application object whose field names match the attributes of a SQL carrier."""

    def __init__(self) -> None:
        self.expression = exp.column("secret")
        self.sqlglot_expression = exp.column("secret")
        self.sql = "secret"
        self.parameters: dict[str, Any] = {}


@pytest.mark.parametrize("value", [_SqlCarrierLookalike(), {"a": 1}, 1, None], ids=["lookalike", "dict", "int", "none"])
def test_returning_rejects_values_that_are_not_sql_carriers(value: Any) -> None:
    with pytest.raises(SQLBuilderError, match="Unsupported RETURNING value"):
        sql.update("t").set(a=1).returning(value)


def test_returning_merges_subquery_builder_parameters() -> None:
    subquery: Any = sql.select("b").from_("s").where_eq("id", 7)
    built = sql.update("t").set(a=1).returning(subquery).build(dialect="postgres")
    assert built.sql == (
        'UPDATE "t" SET "a" = %(a)s\nRETURNING (\n  SELECT\n    "s"."b" AS "b"\n  FROM "s" AS "s"\n'
        '  WHERE\n    "s"."id" = %(id)s\n)'
    )
    assert built.parameters == {"a": 1, "id": 7}
