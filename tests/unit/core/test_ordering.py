"""Database default and explicit NULL ordering contracts."""

import pickle

import pytest
from sqlglot import exp, parse_one

from sqlspec import sql
from sqlspec.builder import Column
from sqlspec.core import SQL, OrderByFilter, StatementConfig
from sqlspec.core._ordering import NullsPlacement, apply_direction, default_nulls, ordered
from sqlspec.core.hashing import hash_expression

DIALECTS = ["postgres", "mysql", "oracle", "tsql", "sqlite", "duckdb"]


@pytest.mark.parametrize("dialect", DIALECTS)
@pytest.mark.parametrize(("desc", "suffix"), [(None, ""), (False, " ASC"), (True, " DESC")])
def test_ordered_emits_no_null_placement(dialect: str, desc: bool | None, suffix: str) -> None:
    assert ordered(exp.column("id"), desc=desc).sql(dialect=dialect) == "id" + suffix


@pytest.mark.parametrize(
    ("dialect", "expected"),
    [
        ("postgres", "id DESC"),
        ("oracle", "id DESC"),
        ("snowflake", "id DESC"),
        ("sqlite", "id DESC NULLS FIRST"),
        ("duckdb", "id DESC NULLS FIRST"),
        ("bigquery", "id DESC NULLS FIRST"),
        ("mysql", "CASE WHEN id IS NULL THEN 1 ELSE 0 END DESC, id DESC"),
        ("tsql", "CASE WHEN id IS NULL THEN 1 ELSE 0 END DESC, id DESC"),
    ],
)
def test_ordered_explicit_nulls(dialect: str, expected: str) -> None:
    assert ordered(exp.column("id"), desc=True, nulls="first").sql(dialect=dialect) == expected


def test_default_nulls_and_hashing() -> None:
    implicit = default_nulls(parse_one("id", into=exp.Ordered), "id")
    explicit = default_nulls(parse_one("id NULLS FIRST", into=exp.Ordered), "id NULLS FIRST")
    assert implicit.sql(dialect="postgres") == "id"
    assert explicit.sql(dialect="postgres") == "id NULLS FIRST"
    assert hash_expression(implicit) != hash_expression(explicit)
    assert exp.column("id").desc().sql(dialect="postgres") == "id DESC NULLS LAST"


def test_apply_direction_never_nests() -> None:
    item = ordered(exp.column("id"))
    assert apply_direction(item, True) is item
    assert item.sql(dialect="postgres") == "id DESC"
    assert apply_direction(ordered(exp.column("id"), desc=False), True).sql(dialect="postgres") == "id ASC"
    assert apply_direction(exp.column("id"), True).sql(dialect="postgres") == "id DESC"
    assert apply_direction(exp.column("id"), False).sql(dialect="postgres") == "id"


def test_marker_survives_copy_and_pickle() -> None:
    item = ordered(exp.column("id"))
    for restored in [item.copy(), pickle.loads(pickle.dumps(item))]:
        assert restored.sql(dialect="postgres") == "id"


@pytest.mark.parametrize("dialect", DIALECTS)
def test_raw_sql_order_by_round_trips_unchanged(dialect: str) -> None:
    assert (
        parse_one("select * from t order by id desc", dialect=dialect).sql(dialect=dialect)
        == "SELECT * FROM t ORDER BY id DESC"
    )


@pytest.mark.parametrize("dialect", DIALECTS)
@pytest.mark.parametrize("desc", [False, True])
def test_core_sites_emit_no_null_placement(dialect: str, desc: bool) -> None:
    expected = "SELECT * FROM t ORDER BY id" + (" DESC" if desc else "")
    assert (
        SQL("SELECT * FROM t", statement_config=StatementConfig(dialect=dialect)).order_by("id", desc=desc).compile()[0]
        == expected
    )
    statement = OrderByFilter("id", "desc" if desc else "asc").append_to_statement(
        SQL("SELECT * FROM t", statement_config=StatementConfig(dialect=dialect))
    )
    assert statement.compile()[0] == expected


@pytest.mark.parametrize("dialect", DIALECTS)
@pytest.mark.parametrize(
    "kind", ["plain", "desc_flag", "desc_string", "function", "column_asc", "column_desc", "column_flag", "alias"]
)
def test_builder_sites_emit_no_null_placement(dialect: str, kind: str) -> None:
    item = {
        "plain": "id",
        "desc_flag": "id",
        "desc_string": "id desc",
        "function": "lower(name) desc",
        "column_asc": Column("id").asc(),
        "column_desc": Column("id").desc(),
        "column_flag": Column("id"),
        "alias": Column("id").alias("desc"),
    }[kind]
    rendered = (
        sql.select("id").from_("t").order_by(item, desc=kind in {"desc_flag", "column_flag"}).to_sql(dialect=dialect)
    )
    assert "NULLS" not in rendered and "CASE" not in rendered
    assert rendered.count("DESC") == (0 if kind in {"plain", "column_asc"} else 1)


@pytest.mark.parametrize("nulls", ["first", "last", None])
def test_filter_round_trip_preserves_nulls(nulls: NullsPlacement | None) -> None:
    original = OrderByFilter("id", "desc", nulls=nulls)
    restored = pickle.loads(pickle.dumps(original))
    assert restored.nulls == nulls
    assert restored.get_cache_key() == original.get_cache_key()


def test_filter_rejects_invalid_nulls() -> None:
    with pytest.raises(ValueError, match="nulls must"):
        OrderByFilter("id", nulls="invalid")  # type: ignore[arg-type]


@pytest.mark.parametrize("dialect", DIALECTS)
def test_explicit_nulls_preserved_across_sites(dialect: str) -> None:
    expected = ordered(exp.column("id"), desc=True, nulls="first").sql(dialect=dialect)
    assert (
        SQL("SELECT * FROM t", statement_config=StatementConfig(dialect=dialect))
        .order_by("id DESC NULLS FIRST")
        .compile()[0]
        .endswith(expected)
    )
    assert (
        OrderByFilter("id", "desc", nulls="first")
        .append_to_statement(SQL("SELECT * FROM t", statement_config=StatementConfig(dialect=dialect)))
        .compile()[0]
        .endswith(expected)
    )
    assert (
        sql
        .select("id")
        .from_("t")
        .order_by(Column("id").desc(nulls="first"))
        .to_sql(dialect=dialect)
        .replace('"', "")
        .replace("`", "")
        .replace("[", "")
        .replace("]", "")
        .replace("t.", "")
        .endswith(expected)
    )


@pytest.mark.parametrize("dialect", DIALECTS)
@pytest.mark.parametrize("kind", ["count_string", "count_list", "count_expression", "row_string", "row_expression"])
def test_window_order_by_emits_no_null_placement(dialect: str, kind: str) -> None:
    expressions = {
        "count_string": sql.count_over(order_by="id"),
        "count_list": sql.count_over(order_by=["id"]),
        "count_expression": sql.count_over(order_by=exp.column("id")),
        "row_string": sql.row_number_.order_by("id").as_("position"),
        "row_expression": sql.row_number_.order_by(exp.column("id")).as_("position"),
    }
    rendered = sql.select(expressions[kind]).from_("t").to_sql(dialect=dialect)
    assert "ORDER BY" in rendered
    assert "NULLS" not in rendered and "CASE" not in rendered


def test_cross_dialect_and_cache_identity() -> None:
    implicit = sql.select("id").from_("t").order_by(Column("id").desc())
    explicit = sql.select("id").from_("t").order_by(Column("id").desc(nulls="last"))
    assert implicit._cache_key() != explicit._cache_key()
    for dialect in DIALECTS:
        rendered = implicit.to_sql(dialect=dialect)
        assert "NULLS" not in rendered and "CASE" not in rendered
    assert "NULLS LAST" in explicit.to_sql(dialect="postgres")


@pytest.mark.parametrize(
    ("source", "dialect", "expected"),
    [
        ("id NULLS /* placement */ LAST", "sqlite", "id NULLS LAST"),
        ("id NULLS -- placement\nLAST", "sqlite", "id NULLS LAST"),
        ("NULLIF(name, 'NULLS FIRST')", "postgres", "NULLIF(name, 'NULLS FIRST')"),
        ('"NULLS FIRST"', "postgres", '"NULLS FIRST"'),
        ("`NULLS FIRST`", "mysql", '"NULLS FIRST"'),
        ("[NULLS FIRST]", "tsql", '"NULLS FIRST"'),
    ],
)
def test_nulls_detection_uses_sql_tokens(source: str, dialect: str, expected: str) -> None:
    statement = SQL("SELECT * FROM t", statement_config=StatementConfig(dialect=dialect)).order_by(source)
    expression = statement.expression
    assert expression is not None
    order_item = expression.args["order"].expressions[0]
    rendered = order_item.sql(dialect="sqlite" if dialect in {"sqlite", "postgres"} else "postgres", comments=False)
    assert rendered == expected


def test_nested_null_placement_does_not_set_outer_ordering() -> None:
    source = "COALESCE((SELECT x FROM u ORDER BY x NULLS FIRST LIMIT 1), id)"
    expression = default_nulls(parse_one(source, into=exp.Ordered), source)
    assert expression.sql(dialect="postgres").endswith("LIMIT 1), id)")
    explicit_source = source + " NULLS FIRST"
    explicit = default_nulls(parse_one(explicit_source, into=exp.Ordered), explicit_source)
    assert explicit.sql(dialect="postgres").endswith("LIMIT 1), id) NULLS FIRST")
