# pyright: reportPrivateUsage = false
"""Tests for SQL-file fragments, includes, and slot declarations."""

from pathlib import Path

import pytest

from sqlspec.exceptions import SQLFileParseError, SQLFragmentNotFoundError, SQLStatementNotFoundError
from sqlspec.loader import SlotDeclaration, SQLFileLoader

SHARED_FRAGMENT = """\
-- fragment: active_users
active AS (SELECT id FROM users WHERE active = TRUE)
"""

INCLUDING_STATEMENT = """\
-- name: count_active
WITH /* include: active_users */
SELECT count(*) FROM active
"""


def _write(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return path


def test_fragment_sections_split_from_statements(tmp_path: Path, effort_sql: str) -> None:
    loader = SQLFileLoader()
    loader.load_sql(_write(tmp_path / "effort.sql", effort_sql))

    assert loader.list_queries() == ["count_efforts", "list_efforts"]
    assert loader.list_fragments() == ["decision_fact_ctes"]
    assert loader.has_fragment("decision-fact-ctes")
    assert not loader.has_query("decision_fact_ctes")
    assert loader.get_fragment_text("decision_fact_ctes").startswith("decisions AS (")
    assert loader.get_fragment_text("decision_fact_ctes").endswith("FROM decisions\n)")


def test_fragment_only_file_loads(tmp_path: Path) -> None:
    loader = SQLFileLoader()
    path = _write(tmp_path / "shared.sql", SHARED_FRAGMENT)

    loader.load_sql(path)

    assert loader.list_queries() == []
    assert loader.list_fragments() == ["active_users"]
    assert loader.list_files() == [str(path)]


def test_duplicate_fragment_raises(tmp_path: Path) -> None:
    loader = SQLFileLoader()
    loader.load_sql(_write(tmp_path / "a.sql", SHARED_FRAGMENT))

    with pytest.raises(SQLFileParseError, match="active_users"):
        loader.load_sql(_write(tmp_path / "b.sql", SHARED_FRAGMENT))

    with pytest.raises(SQLFileParseError, match="Duplicate fragment name"):
        SQLFileLoader._parse_statements(SHARED_FRAGMENT + "\n" + SHARED_FRAGMENT, "dup.sql")


@pytest.mark.parametrize(
    "directive", ["-- param: user_id int", "-- dialect: postgres", "-- slot: predicates = TRUE"], ids=str
)
def test_directive_on_fragment_raises(directive: str) -> None:
    content = f"-- fragment: filtered\n{directive}\nusers AS (SELECT id FROM users)\n"

    with pytest.raises(SQLFileParseError, match=r"Fragment 'filtered' cannot declare"):
        SQLFileLoader._parse_statements(content, "section.sql")


def test_no_fragment_file_unchanged(tmp_path: Path) -> None:
    content = """\
-- name: get_user
-- dialect: postgres
-- param: user_id int
-- Fetch one user
SELECT id, name
FROM users
WHERE id = :user_id;

-- name: list_users
SELECT id FROM users /* keep */ ORDER BY id
"""
    loader = SQLFileLoader()
    loader.load_sql(_write(tmp_path / "users.sql", content))

    assert loader.get_query_text("get_user") == "SELECT id, name\nFROM users\nWHERE id = :user_id;"
    assert loader.get_query_text("list_users") == "SELECT id FROM users /* keep */ ORDER BY id"
    assert loader.get_query_slots("get_user") == ()
    assert loader.list_fragments() == []
    statements, fragments = SQLFileLoader._parse_statements(content, "users.sql")
    assert fragments == {}
    assert statements["get_user"].slots == ()
    assert statements["get_user"].has_includes is False


def test_slot_directive_parsed(tmp_path: Path, effort_sql: str) -> None:
    loader = SQLFileLoader()
    loader.load_sql(_write(tmp_path / "effort.sql", effort_sql))

    assert loader.get_query_slots("list_efforts") == (
        SlotDeclaration("predicates", "TRUE"),
        SlotDeclaration("order_by", "e.id"),
    )


def test_slot_default_keeps_trailing_comment() -> None:
    content = (
        "-- name: q\n-- slot: order_by = id DESC -- newest first\nSELECT id FROM t ORDER BY /* slot: order_by */\n"
    )

    statements, _ = SQLFileLoader._parse_statements(content, "q.sql")

    assert statements["q"].slots == (SlotDeclaration("order_by", "id DESC -- newest first"),)


def test_undeclared_marker_is_required(tmp_path: Path, effort_sql: str) -> None:
    loader = SQLFileLoader()
    loader.load_sql(_write(tmp_path / "effort.sql", effort_sql))

    assert loader.get_query_slots("count_efforts") == (SlotDeclaration("predicates", None),)


def test_required_slot_from_included_fragment(tmp_path: Path) -> None:
    content = """\
-- fragment: filtered
users AS (SELECT id FROM users WHERE /* slot: user_filter */)

-- name: q
-- slot: limit_rows = 10
WITH /* include: filtered */
SELECT id FROM users LIMIT /* slot: limit_rows */
"""
    loader = SQLFileLoader()
    loader.load_sql(_write(tmp_path / "q.sql", content))

    assert loader.get_query_slots("q") == (SlotDeclaration("limit_rows", "10"), SlotDeclaration("user_filter", None))
    assert "/* slot: user_filter */" in loader.get_fragment_text("filtered")


def test_declared_slot_without_marker_raises(tmp_path: Path) -> None:
    content = "-- name: q\n-- slot: order_by = id\nSELECT id FROM t\n"
    loader = SQLFileLoader()
    loader.load_sql(_write(tmp_path / "q.sql", content))

    with pytest.raises(SQLFileParseError, match="order_by"):
        loader.get_query_text("q")
    with pytest.raises(SQLFileParseError, match="order_by"):
        loader.get_query_slots("q")


def test_slot_directive_after_sql_raises() -> None:
    content = "-- name: q\nSELECT id FROM t\n-- slot: order_by = id\nORDER BY /* slot: order_by */\n"

    with pytest.raises(SQLFileParseError, match="slot"):
        SQLFileLoader._parse_statements(content, "q.sql")


@pytest.mark.parametrize(
    "directive",
    ["-- slot: 1bad = x", "-- slot: order_by = id\n-- slot: order_by = name"],
    ids=["malformed", "duplicate"],
)
def test_invalid_slot_directive_raises(directive: str) -> None:
    content = f"-- name: q\n{directive}\nSELECT id FROM t ORDER BY /* slot: order_by */\n"

    with pytest.raises(SQLFileParseError, match="slot"):
        SQLFileLoader._parse_statements(content, "q.sql")


@pytest.mark.parametrize("fragment_first", [True, False], ids=["fragment-first", "statement-first"])
def test_include_resolves_across_files(tmp_path: Path, fragment_first: bool) -> None:
    fragment_path = _write(tmp_path / "shared.sql", SHARED_FRAGMENT)
    statement_path = _write(tmp_path / "report.sql", INCLUDING_STATEMENT)
    loader = SQLFileLoader()

    order = (fragment_path, statement_path) if fragment_first else (statement_path, fragment_path)
    for path in order:
        loader.load_sql(path)

    assert (
        loader.get_query_text("count_active")
        == "WITH active AS (SELECT id FROM users WHERE active = TRUE)\nSELECT count(*) FROM active"
    )
    assert "active = TRUE" in loader.get_sql("count_active").sql


def test_include_param_check_runs_on_resolved_text(tmp_path: Path) -> None:
    content = """\
-- fragment: scoped
scoped AS (SELECT id FROM t WHERE workspace_id = :workspace_id)

-- name: q
-- param: workspace_id str
WITH /* include: scoped */
SELECT id FROM scoped

-- name: bad
-- param: account_id str
WITH /* include: scoped */
SELECT id FROM scoped
"""
    loader = SQLFileLoader()
    loader.load_sql(_write(tmp_path / "q.sql", content))

    assert "workspace_id" in loader.get_sql("q").sql
    with pytest.raises(SQLFileParseError, match="account_id"):
        loader.get_sql("bad")


def test_nested_include_resolves() -> None:
    loader = SQLFileLoader()
    loader.add_fragment("inner", "SELECT 1 AS x")
    loader.add_fragment("outer", "wrapped AS (/* include: inner */)")

    assert loader.get_fragment_text("outer") == "wrapped AS (SELECT 1 AS x)"


def test_include_cycle_raises() -> None:
    loader = SQLFileLoader()
    loader.add_fragment("self_ref", "a AS (/* include: self_ref */)")
    loader.add_fragment("first", "/* include: second */")
    loader.add_fragment("second", "/* include: first */")

    with pytest.raises(SQLFileParseError, match="self_ref -> self_ref"):
        loader.get_fragment_text("self_ref")
    with pytest.raises(SQLFileParseError) as exc_info:
        loader.get_fragment_text("first")
    assert "first -> second -> first" in str(exc_info.value)


def test_unknown_include_raises(tmp_path: Path) -> None:
    loader = SQLFileLoader()
    loader.load_sql(_write(tmp_path / "report.sql", INCLUDING_STATEMENT))

    with pytest.raises(SQLStatementNotFoundError, match=r"SQL fragment 'active_users' not found\. No SQL fragments"):
        loader.get_query_text("count_active")

    loader.add_fragment("other", "SELECT 1")
    with pytest.raises(SQLFragmentNotFoundError, match=r"SQL fragment 'missing' not found") as exc_info:
        loader.get_fragment_text("missing")
    assert "list_fragments()" in str(exc_info.value)
    assert "list_queries()" not in str(exc_info.value)


def test_namespaced_include_resolves(tmp_path: Path) -> None:
    _write(tmp_path / "reports" / "shared.sql", SHARED_FRAGMENT)
    _write(tmp_path / "reports" / "count.sql", INCLUDING_STATEMENT)
    _write(tmp_path / "common" / "ctes.sql", "-- fragment: numbers\nnumbers AS (SELECT 1 AS n)\n")
    _write(
        tmp_path / "other" / "explicit.sql",
        "-- name: explicit\nWITH /* include: common.numbers */\nSELECT n FROM numbers\n",
    )
    loader = SQLFileLoader()

    loader.load_sql(tmp_path)

    assert loader.list_fragments() == ["common.numbers", "reports.active_users"]
    assert "active = TRUE" in loader.get_query_text("reports.count_active")
    assert loader.get_query_text("other.explicit") == "WITH numbers AS (SELECT 1 AS n)\nSELECT n FROM numbers"


def test_nested_include_uses_containing_fragment_namespace(tmp_path: Path) -> None:
    _write(
        tmp_path / "lib" / "ctes.sql",
        "-- fragment: base\nbase AS (SELECT 1 AS n)\n\n-- fragment: wrapper\n/* include: base */, top AS (SELECT n FROM base)\n",
    )
    _write(tmp_path / "app" / "q.sql", "-- name: q\nWITH /* include: lib.wrapper */\nSELECT n FROM top\n")
    loader = SQLFileLoader()

    loader.load_sql(tmp_path)

    assert (
        loader.get_query_text("app.q") == "WITH base AS (SELECT 1 AS n), top AS (SELECT n FROM base)\nSELECT n FROM top"
    )


def test_fragment_reload_invalidates_resolved_statement(tmp_path: Path) -> None:
    fragment_path = _write(tmp_path / "shared.sql", SHARED_FRAGMENT)
    loader = SQLFileLoader()
    loader.load_sql(fragment_path, _write(tmp_path / "report.sql", INCLUDING_STATEMENT))
    first = loader.get_sql("count_active")
    assert "active = TRUE" in loader.get_query_text("count_active")

    fragment_path.write_text("-- fragment: active_users\nactive AS (SELECT id FROM users WHERE active = FALSE)\n")
    assert loader._reload_changed_files() == [str(fragment_path)]

    assert "active = FALSE" in loader.get_query_text("count_active")
    refreshed = loader.get_sql("count_active")
    assert refreshed is not first
    assert "FALSE" in refreshed.sql


def test_add_fragment_after_resolution_invalidates(tmp_path: Path) -> None:
    loader = SQLFileLoader()
    loader.add_named_sql("plain", "SELECT 1")
    loader.load_sql(_write(tmp_path / "report.sql", INCLUDING_STATEMENT))
    plain = loader.get_sql("plain")
    with pytest.raises(SQLStatementNotFoundError):
        loader.get_sql("count_active")

    loader.add_fragment("active_users", "active AS (SELECT id FROM users)")

    compiled = loader.get_sql("count_active")
    assert "active AS" in loader.get_query_text("count_active")
    assert loader.get_sql("count_active") is compiled
    assert loader.get_sql("plain") is plain

    loader.add_fragment("unrelated", "SELECT 2")
    assert loader.get_sql("count_active") is not compiled
    assert loader.get_sql("plain") is plain


def test_add_fragment_duplicate_raises() -> None:
    loader = SQLFileLoader()
    loader.add_fragment("shared", "SELECT 1")

    with pytest.raises(ValueError, match="shared"):
        loader.add_fragment("shared", "SELECT 2")


def test_clear_cache_drops_fragments(tmp_path: Path, effort_sql: str) -> None:
    loader = SQLFileLoader()
    loader.load_sql(_write(tmp_path / "effort.sql", effort_sql))

    loader.clear_cache()

    assert loader.list_fragments() == []
    assert not loader.has_fragment("decision_fact_ctes")


def test_marker_text_inside_literals_and_comments_is_ignored(tmp_path: Path) -> None:
    content = """\
-- fragment: real
real AS (SELECT 'it''s /* slot: in_fragment_literal */' AS note)

-- name: literal_markers
SELECT '/* slot: s */' AS a, '/* include: real */' AS b, "/* slot: t */" AS c, $$/* slot: u */$$ AS d
-- see /* include: real */ for details

-- name: literal_directive
SELECT '
-- slot: x
' AS a

-- name: comment_directive
SELECT 1 /*
-- slot: y
*/ AS a

-- name: mixed
WITH /* include: real */
SELECT 'it''s' AS a FROM real WHERE /* slot: predicate */
"""
    loader = SQLFileLoader()
    loader.load_sql(_write(tmp_path / "literals.sql", content))
    statements, _ = SQLFileLoader._parse_statements(content, "literals.sql")

    assert statements["literal_markers"].has_includes is False
    assert statements["literal_markers"].slots == ()
    assert loader.get_query_slots("literal_markers") == ()
    assert loader.get_query_text("literal_markers") == statements["literal_markers"].sql
    assert loader.get_query_text("literal_directive") == "SELECT '\n-- slot: x\n' AS a"
    assert loader.get_query_slots("comment_directive") == ()
    assert loader.get_query_slots("mixed") == (SlotDeclaration("predicate", None),)
    assert "/* slot: in_fragment_literal */" in loader.get_query_text("mixed")


@pytest.mark.parametrize("source_first", [True, False], ids=["source-loaded-first", "destination-loaded-first"])
def test_fragment_moved_between_files_reloads(tmp_path: Path, source_first: bool) -> None:
    source = _write(tmp_path / "source.sql", SHARED_FRAGMENT + "\n-- name: source_query\nSELECT 1\n")
    destination = _write(tmp_path / "destination.sql", INCLUDING_STATEMENT)
    loader = SQLFileLoader()
    loader.load_sql(*((source, destination) if source_first else (destination, source)))
    assert "active = TRUE" in loader.get_query_text("count_active")

    source.write_text("-- name: source_query\nSELECT 1\n")
    destination.write_text(
        "-- fragment: active_users\nactive AS (SELECT id FROM users WHERE active = FALSE)\n\n" + INCLUDING_STATEMENT
    )

    assert sorted(loader._reload_changed_files()) == sorted([str(source), str(destination)])
    assert loader.list_fragments() == ["active_users"]
    assert "active = FALSE" in loader.get_query_text("count_active")
    assert loader.get_file_for_query("count_active") == loader.get_file(destination)
