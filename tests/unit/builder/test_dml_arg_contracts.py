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
        pytest.param(("2fa",), '"2fa"', id="word-starting-with-digit"),
        pytest.param(("t.2fa",), '"t"."2fa"', id="qualified-word-starting-with-digit"),
        pytest.param(("1e5",), '"1e5"', id="exponent-spelling-is-a-word"),
        pytest.param(("0x1F",), '"0x1F"', id="hex-spelling-is-a-word"),
        pytest.param(("1",), "1", id="integer-literal"),
        pytest.param(("1.5",), "1.5", id="decimal-literal"),
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


@pytest.mark.parametrize(
    ("dialect", "expected"),
    [
        pytest.param("oracle", 'UPDATE t SET a = :a\nRETURNING "2fa"', id="oracle"),
        pytest.param("tsql", "UPDATE [t] SET [a] = :a\nOUTPUT [INSERTED].[2fa]", id="tsql"),
    ],
)
def test_returning_word_starting_with_digit_stays_quoted_column(dialect: str, expected: str) -> None:
    assert sql.update("t").set(a=1).returning("2fa").build(dialect=dialect).sql == expected


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


@pytest.mark.parametrize(
    ("dialect", "update", "delete", "insert"),
    [
        pytest.param(
            "postgres",
            'UPDATE "t" AS "u" SET "a" = %(a)s',
            'DELETE FROM "t" AS "u"',
            'INSERT INTO "t" AS "u" (\n  "a"\n)\nVALUES\n  (%(a)s)',
            id="postgres",
        ),
        pytest.param(
            "sqlite",
            'UPDATE "t" AS "u" SET "a" = :a',
            'DELETE FROM "t" AS "u"',
            'INSERT INTO "t" AS "u" (\n  "a"\n)\nVALUES\n  (:a)',
            id="sqlite",
        ),
        pytest.param(
            "duckdb",
            'UPDATE "t" AS "u" SET "a" = $a',
            'DELETE FROM "t" AS "u"',
            'INSERT INTO "t" AS "u" (\n  "a"\n)\nVALUES\n  ($a)',
            id="duckdb",
        ),
        pytest.param(
            "mysql",
            "UPDATE `t` AS `u` SET `a` = :a",
            "DELETE FROM `t` AS `u`",
            "INSERT INTO `t` AS `u` (\n  `a`\n)\nVALUES\n  (:a)",
            id="mysql",
        ),
        pytest.param(
            "bigquery",
            "UPDATE `t` AS `u` SET `a` = @a",
            "DELETE FROM `t` AS `u`",
            "INSERT INTO `t` AS `u` (\n  `a`\n)\nVALUES\n  (@a)",
            id="bigquery",
        ),
        pytest.param(
            "oracle",
            "UPDATE t u SET a = :a",
            "DELETE FROM t u",
            "INSERT INTO t u (\n  a\n)\nVALUES\n  (:a)",
            id="oracle",
        ),
        pytest.param(
            "sybase",
            "UPDATE [t] AS [u] SET [a] = :a",
            "DELETE FROM [t] AS [u]",
            "INSERT INTO [t] AS [u] (\n  [a]\n)\nVALUES\n  (:a)",
            id="sybase",
        ),
    ],
)
@pytest.mark.parametrize("target", ["t AS u", "t as u", "t u", " t  AS  u "])
def test_dml_table_string_keeps_alias(target: str, dialect: str, update: str, delete: str, insert: str) -> None:
    assert sql.update(target).set(a=1).build(dialect=dialect).sql == update
    assert sql.delete(target).build(dialect=dialect).sql == delete
    assert sql.insert(target).columns("a").values(1).build(dialect=dialect).sql == insert


@pytest.mark.parametrize(
    ("factory", "expected"),
    [
        pytest.param(
            lambda: sql.update().table("t AS u").set(a=1), 'UPDATE "t" AS "u" SET "a" = %(a)s', id="update-table"
        ),
        pytest.param(lambda: sql.delete().from_("t AS u"), 'DELETE FROM "t" AS "u"', id="delete-from"),
        pytest.param(
            lambda: sql.insert().into("t AS u").columns("a").values(1),
            'INSERT INTO "t" AS "u" (\n  "a"\n)\nVALUES\n  (%(a)s)',
            id="insert-into",
        ),
        pytest.param(
            lambda: sql.update("dbo.t AS u").set(a=1), 'UPDATE "dbo"."t" AS "u" SET "a" = %(a)s', id="schema-target"
        ),
        pytest.param(
            lambda: sql.update('"my t" AS u').set(a=1), 'UPDATE "my t" AS "u" SET "a" = %(a)s', id="quoted-target"
        ),
        pytest.param(
            lambda: sql.update("update_log AS u").set(a=1),
            'UPDATE "update_log" AS "u" SET "a" = %(a)s',
            id="name-starts-with-sql-keyword",
        ),
        pytest.param(
            lambda: sql.update("t AS u").set(a=1).where_eq("u.id", 2).returning("u.a"),
            'UPDATE "t" AS "u" SET "a" = %(a)s\nWHERE\n  "u"."id" = %(id)s\nRETURNING "u"."a"',
            id="alias-references",
        ),
        pytest.param(
            lambda: sql.update("t").set(a=1).from_("s AS x").where("t.id = x.id"),
            'UPDATE "t" SET "a" = %(a)s\nFROM "s" AS "x"\nWHERE\n  "t"."id" = "x"."id"',
            id="update-from-as",
        ),
        pytest.param(
            lambda: sql.update("t").set(a=1).from_("s x").where("t.id = x.id"),
            'UPDATE "t" SET "a" = %(a)s\nFROM "s" AS "x"\nWHERE\n  "t"."id" = "x"."id"',
            id="update-from-implicit",
        ),
        pytest.param(
            lambda: sql.update("t").set(a=1).from_("dbo.s AS x").where("t.id = x.id"),
            'UPDATE "t" SET "a" = %(a)s\nFROM "dbo"."s" AS "x"\nWHERE\n  "t"."id" = "x"."id"',
            id="update-from-schema",
        ),
        pytest.param(
            lambda: sql.update("t").set(a=1).from_("x").from_("y AS z"),
            'UPDATE "t" SET "a" = %(a)s\nFROM "x", "y" AS "z"',
            id="update-from-second-source",
        ),
        pytest.param(
            lambda: sql.merge("t AS u").using("s", alias="s").on("u.id = s.id").when_matched_then_delete(),
            'MERGE INTO t AS "u"\nUSING "s" AS s\nON (\n  "u"."id" = "s"."id"\n)\nWHEN MATCHED THEN DELETE',
            id="merge-target",
        ),
    ],
)
def test_dml_table_string_alias_entry_points(factory: Any, expected: str) -> None:
    assert factory().build(dialect="postgres").sql == expected


@pytest.mark.parametrize(
    ("target", "expected"),
    [
        pytest.param("my-table", 'UPDATE "my-table" SET "a" = %(a)s', id="hyphenated-name"),
        pytest.param('"a b"', 'UPDATE "a b" SET "a" = %(a)s', id="quoted-name-with-space"),
        pytest.param("t, s", 'UPDATE "t, s" SET "a" = %(a)s', id="not-one-table"),
        pytest.param("t; DROP TABLE x", 'UPDATE "t; DROP TABLE x" SET "a" = %(a)s', id="trailing-statement"),
        pytest.param("t AS u(a, b)", 'UPDATE "t AS u(a, b)" SET "a" = %(a)s', id="alias-columns"),
        pytest.param("t JOIN s ON 1 = 1", 'UPDATE "t JOIN s ON 1 = 1" SET "a" = %(a)s', id="join"),
    ],
)
def test_dml_table_string_without_a_plain_alias_stays_one_identifier(target: str, expected: str) -> None:
    assert sql.update(target).set(a=1).build(dialect="postgres").sql == expected


@pytest.mark.parametrize(
    ("target", "quoted"),
    [
        pytest.param("f(1).t x", '"f(1)"."t x"', id="function"),
        pytest.param("f((SELECT 1)).t x", '"f((SELECT 1))"."t x"', id="function-with-subquery"),
        pytest.param("f(1).s.t x", '"f(1)"."s"."t x"', id="function-as-catalog"),
    ],
)
def test_dml_table_string_with_a_computed_name_part_renders_only_identifiers(target: str, quoted: str) -> None:
    assert sql.update(target).set(a=1).build(dialect="postgres").sql == f'UPDATE {quoted} SET "a" = %(a)s'
    assert sql.delete(target).build(dialect="postgres").sql == f"DELETE FROM {quoted}"
    from_sql = sql.update("z").set(a=1).from_(target).build(dialect="postgres").sql
    assert "F(" not in from_sql
    assert quoted in from_sql


def test_dml_table_string_alias_parses_with_builder_dialect() -> None:
    built = sql.update("[my t] AS u", dialect="tsql").set(a=1).build(dialect="postgres")
    assert built.sql == 'UPDATE "my t" AS "u" SET "a" = %(a)s'


@pytest.mark.parametrize(
    ("factory", "expected"),
    [
        pytest.param(
            lambda: sql.update("t AS u").set(a=1).where_eq("u.id", 2),
            "UPDATE [u] SET [a] = :a\nFROM [t] AS [u]\nWHERE\n  [u].[id] = :id",
            id="update",
        ),
        pytest.param(
            lambda: sql.update().table("t", alias="u").set(a=1),
            "UPDATE [u] SET [a] = :a\nFROM [t] AS u",
            id="update-explicit-alias",
        ),
        pytest.param(
            lambda: sql.update("dbo.t AS u").set(a=1),
            "UPDATE [u] SET [a] = :a\nFROM [dbo].[t] AS [u]",
            id="update-schema",
        ),
        pytest.param(
            lambda: sql.update("t AS u").set(a=1).where_eq("u.id", 2).returning("a", "u.b", "DELETED.c AS old"),
            "UPDATE [u] SET [a] = :a\nOUTPUT [INSERTED].[a], [INSERTED].[b], [deleted].[c] AS [old]\n"
            "FROM [t] AS [u]\nWHERE\n  [u].[id] = :id",
            id="update-output",
        ),
        pytest.param(
            lambda: sql.update("t AS u").set(a=1).from_("s", alias="s").where("u.id = s.id"),
            "UPDATE [u] SET [a] = :a\nFROM [t] AS [u], [s] AS s\nWHERE\n  [u].[id] = [s].[id]",
            id="update-existing-from",
        ),
        pytest.param(
            lambda: sql.update("t AS u").set(a=1).from_("s AS x").where("u.id = x.id"),
            "UPDATE [u] SET [a] = :a\nFROM [t] AS [u], [s] AS [x]\nWHERE\n  [u].[id] = [x].[id]",
            id="update-from-string-alias",
        ),
        pytest.param(
            lambda: sql.update("t AS u").set(a=1).join("s", on="u.id = s.id"),
            "UPDATE [u] SET [a] = :a\nFROM [t] AS [u]\n  JOIN [s]\n    ON [s].[id] = [u].[id]",
            id="update-join",
        ),
        pytest.param(
            lambda: sql.delete("t AS u").where_eq("u.id", 2),
            "DELETE   [u] FROM [t] AS [u]\nWHERE\n  [u].[id] = :id",
            id="delete",
        ),
        pytest.param(
            lambda: sql.delete("t AS u").where_eq("u.id", 2).returning("a", "u.b"),
            "DELETE   [u]\nOUTPUT [DELETED].[a], [DELETED].[b] FROM [t] AS [u]\nWHERE\n  [u].[id] = :id",
            id="delete-output",
        ),
        pytest.param(
            lambda: sql.merge("t AS u").using("s", alias="s").on("u.id = s.id").when_matched_then_delete(),
            "MERGE INTO [t] AS [u]\nUSING [s] AS s\nON (\n  [u].[id] = [s].[id]\n)\nWHEN MATCHED THEN DELETE",
            id="merge",
        ),
    ],
)
def test_tsql_aliased_dml_target_renders_valid_shape(factory: Any, expected: str) -> None:
    assert factory().build(dialect="tsql").sql == expected


def test_tsql_rejects_aliased_insert_target() -> None:
    with pytest.raises(SQLBuilderError, match="INSERT targets do not support aliases"):
        sql.insert("t AS u").columns("a").values(1).build(dialect="tsql")
