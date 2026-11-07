"""Tests for table and alias text handling across builder call paths."""

from collections.abc import Callable
from typing import Any

import pytest
import sqlglot
from sqlglot import exp

from sqlspec.builder import QueryBuilder, Values, sql
from sqlspec.builder._join import JoinBuilder
from sqlspec.builder._temporal import create_temporal_table
from sqlspec.exceptions import SQLBuilderError

DIALECT_QUOTES = {
    "postgres": ('"', '"'),
    "mysql": ("`", "`"),
    "sqlite": ('"', '"'),
    "oracle": ("", ""),
    "tsql": ("[", "]"),
    "duckdb": ('"', '"'),
}
DIALECTS = tuple(DIALECT_QUOTES)
ALIAS_KEYWORD = {dialect: "" if dialect == "oracle" else "AS " for dialect in DIALECTS}
PLACEHOLDERS = {"postgres": "%({})s", "duckdb": "${}"}

UNSAFE_TABLES = (
    "t x WHERE 1=0 UNION SELECT id, pw FROM secrets --",
    "t x; DROP TABLE z",
    "t; DROP TABLE z",
    "a; b",
    "t -- trailing",
    "t--trailing",
    "t /* trailing */",
    "t # trailing",
    "t u v",
    "t AS u v",
    "t AS",
    "AS u",
    "t AS AS u",
    "t AS u; DROP TABLE z",
    '"t"; DROP TABLE z',
    '"t" x; DROP TABLE z',
    '"unterminated',
    "t.",
    ".t",
    "",
    "   ",
    "(SELECT pw FROM secrets) s",
    "t, secrets",
    "t JOIN secrets ON 1 = 1",
)
UNSAFE_ALIASES = (
    "u x",
    "AS u",
    "AS",
    "u; DROP TABLE z",
    "u WHERE 1=0 UNION SELECT id, pw FROM secrets --",
    "u -- trailing",
    "u /* trailing */",
    '"u"; DROP TABLE z',
    "u.v",
    "(u)",
)


def _render(builder: QueryBuilder, dialect: str) -> str:
    return " ".join(builder.build(dialect=dialect).sql.split())


def _identifier(dialect: str, *parts: str) -> str:
    opening, closing = DIALECT_QUOTES[dialect]
    return ".".join(f"{opening}{part}{closing}" for part in parts)


def _placeholder(dialect: str, name: str) -> str:
    return PLACEHOLDERS.get(dialect, ":{}").format(name)


def _aliased(dialect: str, table: str, alias: str) -> str:
    return f"{table} {ALIAS_KEYWORD[dialect]}{_identifier(dialect, alias)}"


def _select_from(table: str, alias: "str | None" = None) -> QueryBuilder:
    return sql.select("*").from_(table, alias=alias)


def _select_join(table: str, alias: "str | None" = None) -> QueryBuilder:
    return sql.select("*").from_("base").join(table, "base.a = base.b", alias=alias)


def _select_left_join(table: str, alias: "str | None" = None) -> QueryBuilder:
    return sql.select("*").from_("base").left_join(table, "base.a = base.b", alias=alias)


def _select_cross_join(table: str, alias: "str | None" = None) -> QueryBuilder:
    return sql.select("*").from_("base").cross_join(table, alias=alias)


def _join_builder(table: str, alias: "str | None" = None) -> QueryBuilder:
    join = JoinBuilder("inner join")(table, alias=alias).on("base.a = base.b")
    return sql.select("*").from_("base").join(join)


def _update_table(table: str, alias: "str | None" = None) -> QueryBuilder:
    return sql.update().table(table, alias=alias).set(a=1)


def _update_from(table: str, alias: "str | None" = None) -> QueryBuilder:
    return sql.update("base").set(a=1).from_(table, alias=alias)


def _merge_into(table: str, alias: "str | None" = None) -> QueryBuilder:
    return sql.merge().into(table, alias=alias).using("src").on("1 = 1").when_matched_then_delete()


def _merge_using(table: str, alias: "str | None" = None) -> QueryBuilder:
    return sql.merge().into("base").using(table, alias=alias).on("1 = 1").when_matched_then_delete()


def _delete_from(table: str, alias: "str | None" = None) -> QueryBuilder:
    return sql.delete().from_(table)


def _insert_into(table: str, alias: "str | None" = None) -> QueryBuilder:
    return sql.insert().into(table).values(1)


ALIASED_PATHS: tuple[Callable[..., QueryBuilder], ...] = (
    _select_from,
    _select_join,
    _select_left_join,
    _select_cross_join,
    _join_builder,
    _update_table,
    _update_from,
    _merge_into,
    _merge_using,
)
ALL_PATHS: tuple[Callable[..., QueryBuilder], ...] = (*ALIASED_PATHS, _delete_from, _insert_into)
FACTORY_PATHS: tuple[Callable[[str], Any], ...] = (
    lambda table: sql.update(table).set(a=1),
    lambda table: sql.delete(table),
    lambda table: sql.insert(table).values(1),
    lambda table: sql.merge(table).using("src").on("1 = 1").when_matched_then_delete(),
)


@pytest.mark.parametrize("dialect", DIALECTS)
@pytest.mark.parametrize(
    ("text", "alias"), [("t u", None), ("t AS u", None), ("t as u", None), ("  t   AS   u  ", None), ("t", "u")]
)
def test_select_from_renders_alias_as_one_identifier(dialect: str, text: str, alias: "str | None") -> None:
    rendered = _render(_select_from(text, alias), dialect)
    assert rendered == f"SELECT * FROM {_aliased(dialect, _identifier(dialect, 't'), 'u')}"


@pytest.mark.parametrize("dialect", DIALECTS)
def test_select_from_renders_schema_qualified_alias(dialect: str) -> None:
    rendered = _render(_select_from("schema.t AS u"), dialect)
    assert rendered == f"SELECT * FROM {_aliased(dialect, _identifier(dialect, 'schema', 't'), 'u')}"


@pytest.mark.parametrize("dialect", DIALECTS)
@pytest.mark.parametrize(("text", "alias"), [("s x", None), ("s AS x", None), ("s", "x")])
def test_select_join_renders_alias_as_one_identifier(dialect: str, text: str, alias: "str | None") -> None:
    base = _aliased(dialect, _identifier(dialect, "base"), "base")
    target = _aliased(dialect, _identifier(dialect, "s"), "x")
    condition = f"{_identifier(dialect, 'base', 'a')} = {_identifier(dialect, 'base', 'b')}"
    assert _render(_select_join(text, alias), dialect) == f"SELECT * FROM {base} JOIN {target} ON {condition}"
    assert _render(_select_left_join(text, alias), dialect) == f"SELECT * FROM {base} LEFT JOIN {target} ON {condition}"
    assert _render(_select_cross_join(text, alias), dialect) == f"SELECT * FROM {base} CROSS JOIN {target}"
    assert _render(_join_builder(text, alias), dialect) == f"SELECT * FROM {base} JOIN {target} ON {condition}"


@pytest.mark.parametrize("dialect", DIALECTS)
@pytest.mark.parametrize(("text", "alias"), [("t u", None), ("t AS u", None), ("t", "u")])
def test_update_table_renders_alias_as_one_identifier(dialect: str, text: str, alias: "str | None") -> None:
    target = _aliased(dialect, _identifier(dialect, "t"), "u")
    rendered = _render(_update_table(text, alias), dialect)
    assert rendered == f"UPDATE {target} SET {_identifier(dialect, 'a')} = {_placeholder(dialect, 'a')}"


@pytest.mark.parametrize("dialect", ["postgres", "sqlite", "duckdb"])
@pytest.mark.parametrize(("text", "alias"), [("s x", None), ("s AS x", None), ("s", "x")])
def test_update_from_renders_alias_as_one_identifier(dialect: str, text: str, alias: "str | None") -> None:
    source = _aliased(dialect, _identifier(dialect, "s"), "x")
    rendered = _render(_update_from(text, alias), dialect)
    assert (
        rendered
        == f"UPDATE {_identifier(dialect, 'base')} SET {_identifier(dialect, 'a')} = {_placeholder(dialect, 'a')} FROM {source}"
    )


@pytest.mark.parametrize("dialect", DIALECTS)
@pytest.mark.parametrize("text", ["t u", "t AS u"])
def test_delete_renders_alias_as_one_identifier(dialect: str, text: str) -> None:
    assert _render(_delete_from(text), dialect) == f"DELETE FROM {_aliased(dialect, _identifier(dialect, 't'), 'u')}"
    assert _render(sql.delete(text), dialect) == f"DELETE FROM {_aliased(dialect, _identifier(dialect, 't'), 'u')}"


@pytest.mark.parametrize("dialect", DIALECTS)
@pytest.mark.parametrize("text", ["t u", "t AS u"])
def test_insert_renders_alias_as_one_identifier(dialect: str, text: str) -> None:
    target = _aliased(dialect, _identifier(dialect, "t"), "u")
    assert _render(_insert_into(text), dialect) == f"INSERT INTO {target} VALUES ({_placeholder(dialect, 'value_1')})"


@pytest.mark.parametrize(
    ("text", "alias", "expected"),
    [
        ("t", None, 'MERGE INTO t USING "src" ON ( 1 = 1 ) WHEN MATCHED THEN DELETE'),
        ("t u", None, 'MERGE INTO t AS "u" USING "src" ON ( 1 = 1 ) WHEN MATCHED THEN DELETE'),
        ("t AS u", None, 'MERGE INTO t AS "u" USING "src" ON ( 1 = 1 ) WHEN MATCHED THEN DELETE'),
        ("t", "u", 'MERGE INTO t AS "u" USING "src" ON ( 1 = 1 ) WHEN MATCHED THEN DELETE'),
        ('"my t"', None, 'MERGE INTO "my t" USING "src" ON ( 1 = 1 ) WHEN MATCHED THEN DELETE'),
        ("schema.t", None, 'MERGE INTO "schema"."t" USING "src" ON ( 1 = 1 ) WHEN MATCHED THEN DELETE'),
    ],
)
def test_merge_into_renders_target(text: str, alias: "str | None", expected: str) -> None:
    assert _render(_merge_into(text, alias), "postgres") == expected


@pytest.mark.parametrize(("text", "alias"), [("s x", None), ("s AS x", None), ("s", "x")])
def test_merge_using_renders_alias_as_one_identifier(text: str, alias: "str | None") -> None:
    assert _render(_merge_using(text, alias), "postgres") == (
        'MERGE INTO base USING "s" AS "x" ON ( 1 = 1 ) WHEN MATCHED THEN DELETE'
    )


@pytest.mark.parametrize("path", ALL_PATHS, ids=lambda path: path.__name__)
@pytest.mark.parametrize("text", UNSAFE_TABLES)
def test_unsafe_table_text_raises(path: Callable[..., QueryBuilder], text: str) -> None:
    with pytest.raises(SQLBuilderError, match=r"(?i)table"):
        path(text)


@pytest.mark.parametrize("path", ALIASED_PATHS, ids=lambda path: path.__name__)
@pytest.mark.parametrize("alias", UNSAFE_ALIASES)
def test_unsafe_explicit_alias_raises(path: Callable[..., QueryBuilder], alias: str) -> None:
    with pytest.raises(SQLBuilderError, match="alias"):
        path("t", alias)


@pytest.mark.parametrize("path", ALIASED_PATHS, ids=lambda path: path.__name__)
def test_inline_and_explicit_alias_together_raise(path: Callable[..., QueryBuilder]) -> None:
    with pytest.raises(SQLBuilderError, match="alias"):
        path("t u", "v")


@pytest.mark.parametrize("path", FACTORY_PATHS)
@pytest.mark.parametrize("text", ["t x; DROP TABLE z", "t; DROP TABLE z", "a; b", "t -- trailing", "t u v"])
def test_factory_table_argument_rejects_unsafe_text(path: Callable[[str], Any], text: str) -> None:
    with pytest.raises(SQLBuilderError, match="table reference"):
        path(text)


@pytest.mark.parametrize("dialect", DIALECTS)
@pytest.mark.parametrize(
    "build",
    [
        lambda: sql.select("*").from_('"t; drop table z"'),
        lambda: sql.select("*").from_(exp.to_table('"t; drop table z"')),
    ],
)
def test_quoted_table_text_stays_one_identifier(dialect: str, build: Callable[[], QueryBuilder]) -> None:
    opening, closing = DIALECT_QUOTES[dialect]
    opening, closing = (opening, closing) if opening else ('"', '"')
    name = f"{opening}t; drop table z{closing}"
    assert _render(build(), dialect) == f"SELECT * FROM {name} {ALIAS_KEYWORD[dialect]}{name}"


@pytest.mark.parametrize(
    ("dialect", "text", "expected"),
    [
        ("postgres", 't "u; x""y"', 'SELECT * FROM "t" AS "u; x""y"'),
        ("mysql", "t `u; x``y`", "SELECT * FROM `t` AS `u; x``y`"),
        ("tsql", "t [u; x]]y]", "SELECT * FROM [t] AS [u; x]]y]"),
        ("oracle", 't "Mixed"', 'SELECT * FROM t "Mixed"'),
    ],
)
def test_quoted_inline_alias_stays_one_identifier(dialect: str, text: str, expected: str) -> None:
    assert _render(sql.select("*").from_(text), dialect) == expected


@pytest.mark.parametrize(
    ("build", "expected"),
    [
        (lambda: sql.merge("t; DROP TABLE x"), None),
        (
            lambda: sql.merge('"t; DROP TABLE x"').using("s").on("1 = 1").when_matched_then_delete(),
            'MERGE INTO "t; DROP TABLE x" USING "s" ON ( 1 = 1 ) WHEN MATCHED THEN DELETE',
        ),
        (
            lambda: (
                sql.merge().into(exp.to_table('"t; DROP TABLE x"')).using("s").on("1 = 1").when_matched_then_delete()
            ),
            'MERGE INTO "t; DROP TABLE x" USING "s" ON ( 1 = 1 ) WHEN MATCHED THEN DELETE',
        ),
        (
            lambda: (
                sql
                .merge()
                .into(exp.Table(this=exp.to_identifier('a"b; DROP TABLE x', quoted=True)))
                .using("s")
                .on("1 = 1")
                .when_matched_then_delete()
            ),
            'MERGE INTO "a""b; DROP TABLE x" USING "s" ON ( 1 = 1 ) WHEN MATCHED THEN DELETE',
        ),
    ],
)
def test_merge_target_never_loses_required_quotes(build: Callable[[], QueryBuilder], expected: "str | None") -> None:
    if expected is None:
        with pytest.raises(SQLBuilderError, match="table reference"):
            build()
        return
    assert _render(build(), "postgres") == expected


@pytest.mark.parametrize("name", ["a; DROP TABLE z", "a b", "a -- c", "a.b", "AS a", ""])
@pytest.mark.parametrize("method", ["with_", "with_cte"])
def test_cte_alias_rejects_non_identifier(name: str, method: str) -> None:
    builder = sql.select("*").from_("a")
    with pytest.raises(SQLBuilderError, match="alias"):
        getattr(builder, method)(name, sql.select("1"))


@pytest.mark.parametrize("dialect", DIALECTS)
@pytest.mark.parametrize("columns", [None, ["x"]])
def test_cte_alias_renders_one_identifier(dialect: str, columns: "list[str] | None") -> None:
    rendered = _render(sql.select("x").from_("c").with_cte("c", sql.select("1 AS x"), columns=columns), dialect)
    assert rendered.startswith(f"WITH {_identifier(dialect, 'c')} AS (")


@pytest.mark.parametrize(
    ("build", "expected"),
    [
        (
            lambda alias: sql.select("*").from_(sql.select("a").from_("t"), alias=alias),
            'SELECT * FROM ( SELECT "t"."a" AS "a" FROM "t" AS "t" ) AS "q; DROP TABLE z"',
        ),
        (lambda alias: sql.select("*").from_(exp.to_table("t"), alias=alias), 'SELECT * FROM "t" AS "q; DROP TABLE z"'),
        (
            lambda alias: sql.select("*").from_(Values([{"x": 1}]).as_(alias)),
            'SELECT "q; DROP TABLE z"."x" AS "x" FROM (VALUES (%(subquery_x)s)) AS "q; DROP TABLE z"("x")',
        ),
        (
            lambda alias: sql.select(sql.column("a").as_(alias)).from_("t"),
            'SELECT "t"."a" AS "q; DROP TABLE z" FROM "t" AS "t"',
        ),
    ],
)
def test_non_string_source_alias_renders_one_quoted_identifier(
    build: Callable[[str], QueryBuilder], expected: str
) -> None:
    assert _render(build("q; DROP TABLE z"), "postgres") == expected


@pytest.mark.parametrize(
    ("text", "alias", "expected"),
    [
        ("UNNEST(t.tags)", "arr", 'CROSS JOIN LATERAL UNNEST("t"."tags") AS "arr"'),
        ("generate_series(1, 3) g", None, 'CROSS JOIN LATERAL GENERATE_SERIES(1, 3) AS "g"'),
        ("plain_table", "p", 'CROSS JOIN LATERAL "plain_table" AS "p"'),
    ],
)
def test_lateral_join_accepts_one_function_call(text: str, alias: "str | None", expected: str) -> None:
    rendered = _render(sql.select("*").from_("t").cross_lateral_join(text, alias=alias), "postgres")
    assert rendered == f'SELECT * FROM "t" AS "t" {expected}'


@pytest.mark.parametrize(
    "text",
    [
        "UNNEST(t.tags); DROP TABLE z",
        "UNNEST(t.tags), secrets",
        "UNNEST(t.tags) x WHERE 1 = 1",
        "UNNEST(t.tags) -- trailing",
        "UNNEST(t.tags) /* trailing */",
        "f((SELECT pw FROM secrets))",
        "f(1) x JOIN secrets ON 1 = 1",
        "(SELECT pw FROM secrets) s",
        "f(",
    ],
)
def test_lateral_join_rejects_anything_but_one_function_call(text: str) -> None:
    with pytest.raises(SQLBuilderError, match=r"table (function|reference)"):
        sql.select("*").from_("t").cross_lateral_join(text)


def test_lateral_join_rejects_unsafe_function_alias() -> None:
    with pytest.raises(SQLBuilderError, match="alias"):
        sql.select("*").from_("t").cross_lateral_join("UNNEST(t.tags)", alias="u; DROP TABLE z")


@pytest.mark.parametrize("name", ["t x", "t AS x", "t; DROP TABLE z", "a; b", "t -- trailing"])
@pytest.mark.parametrize(
    "build",
    [
        lambda name: sql.create_table(name).column("id", "INT"),
        lambda name: sql.drop_table(name),
        lambda name: sql.alter_table(name).add_column("c", "INT"),
        lambda name: sql.create_view(name).as_select("SELECT 1"),
        lambda name: sql.create_index("i").on_table(name).columns("id"),
    ],
)
def test_ddl_table_names_reject_anything_but_a_name(name: str, build: Callable[[str], QueryBuilder]) -> None:
    with pytest.raises(SQLBuilderError, match="table reference"):
        build(name).build()


@pytest.mark.parametrize("text", ["t x; DROP TABLE z", "a; b"])
def test_temporal_table_rejects_unsafe_text(text: str) -> None:
    with pytest.raises(SQLBuilderError, match="table reference"):
        create_temporal_table(text, "2020-01-01")


@pytest.mark.parametrize(
    "build",
    [
        lambda alias, source: sql.select("*").from_(source, alias=alias),
        lambda alias, source: sql.update("b").set(x=1).from_(source, alias=alias),
    ],
)
def test_subquery_alias_text_never_reaches_placeholder_names(
    build: Callable[[str, QueryBuilder], QueryBuilder],
) -> None:
    source = sql.select("a").from_("t").where_eq("a", 1)
    built = build("q; DROP TABLE z--", source).build(dialect="sqlite")
    assert ":q_DROP_TABLE_z_a" in built.sql
    assert "DROP TABLE" not in built.sql.replace(":q_DROP_TABLE_z_a", "")
    assert '"q; drop table z--"' in built.sql


def test_quoted_cte_alias_never_reaches_placeholder_names() -> None:
    source = sql.select("a").from_("t").where_eq("a", 1)
    built = sql.select("a").from_("c").with_cte('"c; x"', source).build(dialect="sqlite")
    assert '"c; x"' in built.sql
    assert ":c_x_a" in built.sql


QUOTE_STYLES = ('"{}"', "`{}`", "[{}]")
QUOTED_BODIES = (
    ".json_each((SELECT json_quote(pw) FROM secrets)) --",
    ".json_each((SELECT json_quote(pw) FROM secrets)) /*",
    ".json_each((SELECT json_quote(pw) FROM secrets)) #",
    "\njson_each((SELECT json_quote(pw) FROM secrets)) --",
    "\nEXTERNAL_QUERY('c', (SELECT 1)) --",
    "x\n; DROP TABLE z --",
    "x; DROP TABLE z /*",
    "# \nf((SELECT 1))",
    "-- \nf((SELECT 1))",
    "/* */ f((SELECT 1))",
    "f((SELECT 1))",
)
PREFIXES = ("", "#.", "--.", "/*.", "a.", "a ")
QUOTE_PAYLOADS = tuple(
    prefix + style.format(body) for prefix in PREFIXES for style in QUOTE_STYLES for body in QUOTED_BODIES
)
BARE_PAYLOADS = (
    "#\nf((SELECT 1))",
    "--\nf((SELECT 1))",
    "/* */ f((SELECT 1))",
    "a.f((SELECT 1))",
    "a /*\n*/ b",
    "a #\n, secrets",
    "a --\n, secrets",
)
PAYLOAD_DIALECTS = (None, *DIALECTS, "bigquery", "snowflake", "spark", "clickhouse")


def _update_from_literal(table: str, alias: "str | None" = None) -> QueryBuilder:
    return sql.update("base").set(a=exp.Literal.number(1)).from_(table, alias=alias)


PAYLOAD_PATHS = (_select_from, _select_join, _update_from_literal, _merge_using)


@pytest.mark.parametrize("dialect", PAYLOAD_DIALECTS)
@pytest.mark.parametrize("builder_has_dialect", [True, False], ids=["builder_dialect", "default_builder"])
@pytest.mark.parametrize("path", PAYLOAD_PATHS, ids=lambda path: path.__name__)
def test_payloads_never_render_a_call_or_a_second_statement(
    dialect: "str | None", builder_has_dialect: bool, path: Callable[..., QueryBuilder]
) -> None:
    for payload in (*QUOTE_PAYLOADS, *BARE_PAYLOADS):
        try:
            builder = _rebuild_with_dialect(path, payload, dialect) if builder_has_dialect else path(payload)
            rendered = builder.build(dialect=dialect).sql
        except SQLBuilderError:
            continue
        statements = sqlglot.parse(rendered, dialect=dialect)
        assert len(statements) == 1, (payload, rendered)
        statement = statements[0]
        assert statement is not None
        assert statement.find(exp.Func) is None, (payload, rendered)
        assert len(list(statement.find_all(exp.Query))) <= 1, (payload, rendered)
        assert not any(node.comments for node in statement.walk()), (payload, rendered)


def _rebuild_with_dialect(path: Callable[..., QueryBuilder], payload: str, dialect: "str | None") -> QueryBuilder:
    if path is _select_from:
        return sql.select("*", dialect=dialect).from_(payload)
    if path is _select_join:
        return sql.select("*", dialect=dialect).from_("base").join(payload, "base.a = base.b")
    if path is _update_from_literal:
        return sql.update("base", dialect=dialect).set(a=exp.Literal.number(1)).from_(payload)
    return sql.merge("base", dialect=dialect).using(payload).on("1 = 1").when_matched_then_delete()


@pytest.mark.parametrize(
    ("dialect", "text"),
    [
        (None, "`.json_each((SELECT json_quote(pw) FROM secrets)) --`"),
        ("bigquery", "#.\"\nEXTERNAL_QUERY('c', (SELECT 1)) --\""),
        ("mysql", '#."\nf((SELECT 1)) --"'),
    ],
)
def test_text_read_as_comment_or_call_by_its_dialect_raises(dialect: "str | None", text: str) -> None:
    with pytest.raises(SQLBuilderError, match="table reference"):
        sql.select("*", dialect=dialect).from_(text)


@pytest.mark.parametrize(
    ("dialect", "text", "expected"),
    [
        ("duckdb", "'a.parquet'", 'SELECT * FROM "a.parquet" AS "a.parquet"'),
        ("duckdb", "'a.parquet' p", 'SELECT * FROM "a.parquet" AS "p"'),
        ("bigquery", "p.d.t_*", "SELECT * FROM `p`.`d`.`t_*` AS `t_*`"),
        ("bigquery", "`p.d.t` x", "SELECT * FROM `p.d.t` AS `x`"),
        (None, "[dbo].[t] x", 'SELECT * FROM "dbo"."t" AS "x"'),
        (None, "`my t` x", 'SELECT * FROM "my t" AS "x"'),
        (None, "a-b.c", 'SELECT * FROM "a-b"."c" AS "c"'),
        (None, "a.b.c.d", 'SELECT * FROM "a"."b"."c"."d" AS "d"'),
        (None, "db..t", 'SELECT * FROM "db".."t" AS "t"'),
    ],
)
def test_select_from_keeps_supported_table_text(dialect: "str | None", text: str, expected: str) -> None:
    built = sql.select("*", dialect=dialect).from_(text).build()
    assert " ".join(built.sql.split()) == expected


def test_text_quoted_natively_by_its_dialect_stays_one_identifier() -> None:
    built = sql.select("*", dialect="sqlite").from_("`a; DROP TABLE z --`").build()
    name = '"a; drop table z --"'
    assert " ".join(built.sql.split()) == f"SELECT * FROM {name} AS {name}"


def test_lateral_join_keeps_function_schema() -> None:
    query = sql.select("*").from_("t").lateral_join("myschema.f(t.id) x")
    assert " ".join(query.build().sql.split()) == (
        'SELECT * FROM "t" AS "t" CROSS JOIN LATERAL "myschema".F("t"."id") AS "x"'
    )


@pytest.mark.parametrize(
    "text",
    [
        "json_each(?)",
        "generate_series(1,3) g",
        "read_csv('a.csv')",
        "range(3) r",
        "a.f(1) x",
        "query('SELECT pw FROM secrets')",
        "read_text('/etc/hostname')",
        "f((SELECT pw FROM secrets))",
        "(SELECT pw FROM secrets) s",
        '"a"(1)',
        '"a(b)".f(1)',
    ],
)
@pytest.mark.parametrize("path", ALL_PATHS, ids=lambda path: path.__name__)
def test_parenthesis_in_table_text_names_the_supported_path(path: Callable[..., QueryBuilder], text: str) -> None:
    with pytest.raises(SQLBuilderError, match=r"Pass an expression or builder"):
        path(text)


@pytest.mark.parametrize(
    "text",
    [
        "f((SELECT pw FROM secrets))",
        "f(1 IN (SELECT pw FROM secrets))",
        "f(EXISTS(SELECT 1))",
        "f(1); DROP TABLE z",
        "f(1) -- trailing",
        "f(/* inner */ 1)",
        "f(1) x, secrets",
        "f(1) UNION SELECT pw FROM secrets",
    ],
)
def test_lateral_function_text_rejects_subqueries_statements_and_comments(text: str) -> None:
    with pytest.raises(SQLBuilderError, match="table function"):
        sql.select("*").from_("t").lateral_join(text)


def test_lateral_function_text_rejects_second_alias() -> None:
    with pytest.raises(SQLBuilderError, match="alias"):
        sql.select("*").from_("t").lateral_join("f(1) x", alias="y")


@pytest.mark.parametrize(
    "source",
    [lambda: exp.func("generate_series", 1, 3), lambda: sql.raw("generate_series(1, 3)")],
    ids=["exp_func", "raw"],
)
def test_expression_function_sources_render_with_aliases(source: Callable[[], Any]) -> None:
    call = "GENERATE_SERIES(1, 3)"
    expected = {
        "from": (sql.select("*").from_(source(), alias="g"), f'SELECT * FROM {call} AS "g"'),
        "join": (
            sql.select("*").from_("t").join(source(), "t.a = g.a", alias="g"),
            f'SELECT * FROM "t" JOIN {call} AS "g" ON "t"."a" = "g"."a"',
        ),
        "update": (
            sql.update("t").set(a=exp.Literal.number(1)).from_(source(), alias="g"),
            f'UPDATE "t" SET "a" = 1 FROM {call} AS "g"',
        ),
        "merge": (
            sql.merge("t").using(source(), alias="g").on("t.a = g.a").when_matched_then_delete(),
            f'MERGE INTO t USING {call} AS "g" ON ( "t"."a" = "g"."a" ) WHEN MATCHED THEN DELETE',
        ),
    }
    for builder, rendered in expected.values():
        assert " ".join(builder.build().sql.split()) == rendered


@pytest.mark.parametrize(
    ("build", "expected"),
    [
        (lambda: sql.select("*").from_(sql.raw("my_fn(:n)", n=3), alias="g"), 'SELECT * FROM MY_FN(:n) AS "g"'),
        (
            lambda: sql.select("*").from_("t").join(sql.raw("my_fn(:n)", n=3), "t.a = g.a", alias="g"),
            'SELECT * FROM "t" JOIN MY_FN(:n) AS "g" ON "t"."a" = "g"."a"',
        ),
    ],
    ids=["from", "join"],
)
def test_parameterized_raw_function_source_binds_parameters(build: Callable[[], QueryBuilder], expected: str) -> None:
    built = build().build()
    assert " ".join(built.sql.split()) == expected
    assert built.parameters == {"n": 3}


def test_placeholder_function_source_binds_added_parameter() -> None:
    builder = sql.select("*").from_(exp.func("my_fn", exp.Placeholder(this="n")), alias="g")
    builder.add_parameter(3, name="n")
    built = builder.build()
    assert " ".join(built.sql.split()) == 'SELECT * FROM MY_FN(:n) AS "g"'
    assert built.parameters == {"n": 3}


@pytest.mark.parametrize(
    ("build", "expected"),
    [
        (lambda: sql.select("*", dialect="tsql").from_("@tv"), "SELECT * FROM @tv AS [tv]"),
        (lambda: sql.select("*", dialect="tsql").from_("@tv u"), "SELECT * FROM @tv AS [u]"),
        (lambda: sql.select("*", dialect="mssql").from_("@tv AS u"), "SELECT * FROM @tv AS [u]"),
        (lambda: sql.delete("@tv", dialect="tsql"), "DELETE FROM @tv"),
    ],
)
def test_tsql_table_variable_is_accepted(build: Callable[[], QueryBuilder], expected: str) -> None:
    assert " ".join(build().build().sql.split()) == expected


@pytest.mark.parametrize(
    "text", ["@tv; DROP TABLE z", "@tv -- trailing", "@tv /* c */", "@tv u v", "@tv AS", "@", "@1", "@a.b", "@`a b`"]
)
def test_tsql_table_variable_rejects_anything_else(text: str) -> None:
    with pytest.raises(SQLBuilderError, match=r"table reference|alias"):
        sql.select("*", dialect="tsql").from_(text)


@pytest.mark.parametrize("dialect", [None, "postgres", "mysql", "sqlite", "oracle", "duckdb"])
def test_table_variable_raises_outside_tsql(dialect: "str | None") -> None:
    with pytest.raises(SQLBuilderError, match="table reference"):
        sql.select("*", dialect=dialect).from_("@tv")


@pytest.mark.parametrize(
    ("dialect", "text", "expected"),
    [
        ("postgres", '"a(b)"', '"a(b)"'),
        ("sqlite", '"a(b)"', '"a(b)"'),
        ("duckdb", '"a(b)"', '"a(b)"'),
        ("oracle", '"a(b)"', '"a(b)"'),
        ("mysql", "`a(b)`", "`a(b)`"),
        ("tsql", "[a(b)]", "[a(b)]"),
        ("bigquery", "`a(b)`", "`a(b)`"),
    ],
)
def test_parenthesis_inside_quoted_name_renders_one_identifier(dialect: str, text: str, expected: str) -> None:
    keyword = "" if dialect == "oracle" else "AS "
    built = sql.select("*", dialect=dialect).from_(text).build()
    assert " ".join(built.sql.split()) == f"SELECT * FROM {expected} {keyword}{expected}"


def test_parenthesis_inside_quoted_name_with_alias_and_schema() -> None:
    assert _render(sql.select("*").from_('s."a(b)" AS u'), "postgres") == 'SELECT * FROM "s"."a(b)" AS "u"'


def test_ddl_keeps_parenthesis_inside_quoted_name() -> None:
    assert sql.drop_table('"a(b"').build().sql == 'DROP TABLE "a(b"'


@pytest.mark.parametrize("text", ["`a(b)`", "[a(b)]"])
def test_parenthesis_inside_quotes_the_dialect_does_not_define_raises(text: str) -> None:
    with pytest.raises(SQLBuilderError, match="table reference"):
        sql.select("*", dialect="postgres").from_(text)


INVISIBLE_CHARACTERS = ("\ufeff", "\u200b", "\u00ad", "\u00a0", "\u3000", "\u2028", "\u2029", "\x00", "\x0b", "\x85")


@pytest.mark.parametrize("dialect", ["oracle", "db2", None])
@pytest.mark.parametrize("character", INVISIBLE_CHARACTERS)
@pytest.mark.parametrize(
    "template", ["x{0}UNION{0}SELECT{0}1", "t{0}u", "t {0}u", "t AS u{0}", "{0}t", "a{0}.b", 'a."b"{0}', 't "u"{0}']
)
def test_invisible_characters_in_unquoted_text_raise(dialect: "str | None", character: str, template: str) -> None:
    text = template.format(character)
    for build in (lambda: sql.select("*", dialect=dialect).from_(text), lambda: sql.delete(text, dialect=dialect)):
        with pytest.raises(SQLBuilderError, match="table reference"):
            build()


@pytest.mark.parametrize("dialect", ["oracle", "db2"])
@pytest.mark.parametrize("character", INVISIBLE_CHARACTERS)
def test_invisible_characters_in_explicit_alias_raise(dialect: str, character: str) -> None:
    with pytest.raises(SQLBuilderError, match="alias"):
        sql.select("*", dialect=dialect).from_("t", alias=f"u{character}v")


@pytest.mark.parametrize(
    ("dialect", "expected"),
    [("oracle", 'SELECT * FROM "x\ufeffy" "x\ufeffy"'), ("db2", 'SELECT * FROM "x\ufeffy" AS "x\ufeffy"')],
)
def test_invisible_characters_inside_quotes_stay_one_identifier(dialect: str, expected: str) -> None:
    built = sql.select("*", dialect=dialect).from_('"x\ufeffy"').build()
    assert " ".join(built.sql.split()) == expected
