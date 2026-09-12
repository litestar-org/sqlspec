"""Tests for filling SQL-file slots through ``get_sql(name, **slots)``."""

from pathlib import Path

import pytest
from sqlglot import exp

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.core import SQL, LimitOffsetFilter, ParameterDeclaration
from sqlspec.exceptions import SQLFileParseError, SQLSlotError
from sqlspec.loader import SlotDeclaration, SQLFileLoader

EFFORT_SQL = """\
-- fragment: decision_fact_ctes
decisions AS (
    SELECT d.id, d.workspace_id, d.strategy
    FROM decision d
    WHERE d.workspace_id = :workspace_id
),
classified AS (
    SELECT id, workspace_id, strategy AS journey
    FROM decisions
)

-- name: list_efforts
-- param: workspace_id str
-- slot: predicates = TRUE
-- slot: order_by = e.id
WITH
/* include: decision_fact_ctes */
SELECT e.id, e.journey
FROM classified e
WHERE /* slot: predicates */
ORDER BY /* slot: order_by */

-- name: count_efforts
-- param: workspace_id str
WITH
/* include: decision_fact_ctes */
SELECT count(*) FROM classified e WHERE /* slot: predicates */

-- name: plain_query
SELECT id FROM decision
"""


def _normalize(sql: str) -> str:
    return " ".join(sql.split())


@pytest.fixture
def loader(tmp_path: Path) -> SQLFileLoader:
    path = tmp_path / "effort.sql"
    path.write_text(EFFORT_SQL)
    sql_loader = SQLFileLoader()
    sql_loader.load_sql(path)
    return sql_loader


def test_defaults_fill(loader: SQLFileLoader) -> None:
    stmt = loader.get_sql("list_efforts")

    assert "WHERE TRUE ORDER BY e.id" in _normalize(stmt.sql)
    assert "/* slot:" not in stmt.sql
    assert "/* include:" not in stmt.sql
    assert stmt.named_parameters == {}


def test_str_expression_sql_values(loader: SQLFileLoader) -> None:
    by_str = loader.get_sql("list_efforts", order_by="e.journey, e.id DESC")
    by_expression = loader.get_sql("count_efforts", predicates=exp.column("journey").eq("modernize"))
    by_sql = loader.get_sql("count_efforts", predicates=SQL("e.journey = 'lift'"))

    assert "WHERE TRUE ORDER BY e.journey, e.id DESC" in _normalize(by_str.sql)
    assert _normalize(by_expression.sql).endswith("WHERE journey = 'modernize'")
    assert _normalize(by_sql.sql).endswith("WHERE e.journey = 'lift'")


def test_expression_value_uses_dialect(tmp_path: Path) -> None:
    path = tmp_path / "users.sql"
    path.write_text("-- name: q\n-- dialect: mysql\nSELECT id FROM users WHERE /* slot: predicates */\n")
    sql_loader = SQLFileLoader()
    sql_loader.load_sql(path)

    stmt = sql_loader.get_sql("q", predicates=exp.column("name").ilike("%a%"))

    assert "LOWER(name) LIKE LOWER('%a%')" in stmt.sql


def test_sql_value_merges_parameters(loader: SQLFileLoader) -> None:
    stmt = loader.get_sql("list_efforts", predicates=SQL("e.journey = :journey", journey="modernize"))

    assert "WHERE e.journey = :journey ORDER BY e.id" in _normalize(stmt.sql)
    assert stmt.named_parameters == {"journey": "modernize"}


def test_sql_value_with_positional_parameters_raises(loader: SQLFileLoader) -> None:
    with pytest.raises(SQLSlotError, match="positional"):
        loader.get_sql("count_efforts", predicates=SQL("e.journey = ?", "modernize"))


def test_unsupported_value_type_raises(loader: SQLFileLoader) -> None:
    with pytest.raises(TypeError, match=r"slot .predicates. value must be"):
        loader.get_sql("count_efforts", predicates=42)


def test_missing_slot_raises(loader: SQLFileLoader) -> None:
    with pytest.raises(SQLSlotError, match="count_efforts") as exc_info:
        loader.get_sql("count_efforts")

    assert "predicates" in str(exc_info.value)
    assert exc_info.value.statement == "count_efforts"


@pytest.mark.parametrize("name", ["list_efforts", "plain_query"])
def test_unknown_slot_raises(loader: SQLFileLoader, name: str) -> None:
    with pytest.raises(SQLSlotError, match="limit_rows"):
        loader.get_sql(name, limit_rows="10")


def test_parameter_collision_raises(tmp_path: Path, loader: SQLFileLoader) -> None:
    path = tmp_path / "two.sql"
    path.write_text("-- name: two_slots\nSELECT id FROM t WHERE /* slot: first */ AND /* slot: second */\n")
    loader.load_sql(path)

    with pytest.raises(SQLSlotError, match="'p'"):
        loader.get_sql("two_slots", first=SQL("a = :p", p=1), second=SQL("b = :p", p=2))
    with pytest.raises(SQLSlotError, match="workspace_id"):
        loader.get_sql("count_efforts", predicates=SQL("e.workspace_id = :workspace_id", workspace_id="other"))


def test_param_check_runs_on_final_text(tmp_path: Path, loader: SQLFileLoader) -> None:
    path = tmp_path / "checked.sql"
    path.write_text("-- name: needs_journey\n-- param: journey str\nSELECT id FROM t WHERE /* slot: predicates */\n")
    loader.load_sql(path)

    assert "workspace_id" in loader.get_sql("count_efforts", predicates="TRUE").sql
    filled = loader.get_sql("needs_journey", predicates=SQL("journey = :journey", journey="lift"))
    assert filled.named_parameters == {"journey": "lift"}
    with pytest.raises(SQLFileParseError, match="journey"):
        loader.get_sql("needs_journey", predicates="TRUE")


def test_cache_only_default_form(loader: SQLFileLoader) -> None:
    default_form = loader.get_sql("list_efforts")

    assert loader.get_sql("list_efforts") is default_form
    filled = loader.get_sql("list_efforts", order_by="e.id")
    assert filled is not default_form
    assert loader.get_sql("list_efforts", order_by="e.id") is not filled
    assert loader.get_sql("list_efforts") is default_form
    assert loader.get_sql("plain_query") is loader.get_sql("plain_query")


def test_sqlspec_get_sql_forwards_slots(tmp_path: Path) -> None:
    path = tmp_path / "effort.sql"
    path.write_text(EFFORT_SQL)
    spec = SQLSpec()
    spec.load_sql_files(path)

    stmt = spec.get_sql("list_efforts", order_by="e.journey")

    assert "ORDER BY e.journey" in _normalize(stmt.sql)
    with pytest.raises(SQLSlotError, match="count_efforts"):
        spec.get_sql("count_efforts")


def test_add_named_sql_resolves_includes_and_fills_slots() -> None:
    sql_loader = SQLFileLoader()
    sql_loader.add_fragment("scoped", "t.workspace_id = :workspace_id")
    sql_loader.add_named_sql(
        "scoped_rows",
        "SELECT id FROM t WHERE /* include: scoped */ AND /* slot: predicates */",
        parameters=[ParameterDeclaration("workspace_id", "str")],
    )

    assert sql_loader.get_query_text("scoped_rows") == (
        "SELECT id FROM t WHERE t.workspace_id = :workspace_id AND /* slot: predicates */"
    )
    assert sql_loader.get_query_slots("scoped_rows") == (SlotDeclaration("predicates"),)
    with pytest.raises(SQLSlotError, match="predicates"):
        sql_loader.get_sql("scoped_rows")
    filled = sql_loader.get_sql("scoped_rows", predicates="t.id > 1")
    assert _normalize(filled.sql) == "SELECT id FROM t WHERE t.workspace_id = :workspace_id AND t.id > 1"


def test_fill_ending_in_line_comment_keeps_following_sql(tmp_path: Path) -> None:
    path = tmp_path / "commented.sql"
    path.write_text(
        "-- name: newest_first\n"
        "-- slot: p = TRUE -- default\n"
        "SELECT id FROM item WHERE /* slot: p */ ORDER BY id DESC\n"
    )
    spec = SQLSpec()
    spec.load_sql_files(path)
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(config) as session:
        session.execute_script("CREATE TABLE item (id INTEGER PRIMARY KEY); INSERT INTO item VALUES (1), (2), (3);")
        default_rows = session.select(spec.get_sql("newest_first"))
        filled_rows = session.select(spec.get_sql("newest_first", p="id < 3 -- only early rows"))
    config.close_pool()

    assert [row["id"] for row in default_rows] == [3, 2, 1]
    assert [row["id"] for row in filled_rows] == [2, 1]


def test_sql_value_with_filters_raises(loader: SQLFileLoader) -> None:
    filtered = SQL("e.journey = :journey", LimitOffsetFilter(10, 0), journey="modernize")

    with pytest.raises(SQLSlotError, match="filters"):
        loader.get_sql("count_efforts", predicates=filtered)


def test_repeated_parameter_with_equal_value_is_not_a_collision(tmp_path: Path, loader: SQLFileLoader) -> None:
    path = tmp_path / "repeated.sql"
    path.write_text("-- name: repeated\nSELECT id FROM t WHERE /* slot: first */ OR /* slot: second */\n")
    loader.load_sql(path)
    shared = SQL("kind = :kind", kind="a")

    same_object = loader.get_sql("repeated", first=shared, second=shared)
    equal_values = loader.get_sql("repeated", first=shared, second=SQL("alt_kind = :kind", kind="a"))

    assert same_object.named_parameters == {"kind": "a"}
    assert _normalize(same_object.sql) == "SELECT id FROM t WHERE kind = :kind OR kind = :kind"
    assert equal_values.named_parameters == {"kind": "a"}
    with pytest.raises(SQLSlotError, match="'kind'"):
        loader.get_sql("repeated", first=shared, second=SQL("alt_kind = :kind", kind="b"))
