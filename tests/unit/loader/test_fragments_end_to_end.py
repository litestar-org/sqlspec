"""End-to-end execution of SQL files that use fragments, includes, and slots."""

from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest
from sqlglot import exp

from sqlspec import SQLSpec
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.core import SQL

SETUP_SQL = """\
CREATE TABLE decision (id INTEGER PRIMARY KEY, workspace_id VARCHAR NOT NULL, strategy VARCHAR NOT NULL);
INSERT INTO decision (id, workspace_id, strategy) VALUES
    (1, 'ws-1', 'lift'),
    (2, 'ws-1', 'refactor'),
    (3, 'ws-1', 'rebuild'),
    (4, 'ws-2', 'refactor');
"""


@pytest.fixture
def spec(tmp_path: Path, effort_sql: str) -> SQLSpec:
    sql_path = tmp_path / "effort.sql"
    sql_path.write_text(effort_sql)
    sql_spec = SQLSpec()
    sql_spec.load_sql_files(sql_path)
    return sql_spec


@pytest.fixture
def sqlite_session(spec: SQLSpec) -> Generator[Any, None, None]:
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))
    with spec.provide_session(config) as session:
        session.execute_script(SETUP_SQL)
        yield session
    config.close_pool()


@pytest.fixture
def duckdb_session(spec: SQLSpec) -> Generator[Any, None, None]:
    config = spec.add_config(DuckDBConfig(connection_config={"database": ":memory:"}))
    with spec.provide_session(config) as session:
        session.execute_script(SETUP_SQL)
        yield session
    config.close_pool()


def _assert_filled_queries_execute(spec: SQLSpec, session: Any) -> None:
    all_rows = session.select(spec.get_sql("list_efforts"), workspace_id="ws-1")
    assert [(row["id"], row["journey"]) for row in all_rows] == [(1, "homogeneous"), (2, "modernize"), (3, "modernize")]

    modernize = spec.get_sql("list_efforts", predicates=SQL("e.journey = :journey", journey="modernize"))
    assert [row["id"] for row in session.select(modernize, workspace_id="ws-1")] == [2, 3]
    assert [row["id"] for row in session.select(modernize, workspace_id="ws-2")] == [4]

    newest_first = spec.get_sql(
        "list_efforts", predicates=SQL("e.journey = :journey", journey="modernize"), order_by="e.id DESC"
    )
    assert [row["id"] for row in session.select(newest_first, {"workspace_id": "ws-1"})] == [3, 2]

    counted = spec.get_sql("count_efforts", predicates=exp.column("journey").eq("modernize"))
    assert session.select_value(counted, workspace_id="ws-1") == 2
    assert session.select_value(counted, workspace_id="ws-2") == 1


def test_sqlite_filled_query_executes(spec: SQLSpec, sqlite_session: Any) -> None:
    _assert_filled_queries_execute(spec, sqlite_session)


def test_duckdb_filled_query_executes(spec: SQLSpec, duckdb_session: Any) -> None:
    _assert_filled_queries_execute(spec, duckdb_session)


def test_fill_ending_in_line_comment_keeps_following_sql(tmp_path: Path, spec: SQLSpec, sqlite_session: Any) -> None:
    path = tmp_path / "commented.sql"
    path.write_text(
        "-- name: newest_first\n-- slot: p = TRUE -- default\nSELECT id FROM decision WHERE /* slot: p */ ORDER BY id DESC\n"
    )
    spec.load_sql_files(path)

    default_rows = sqlite_session.select(spec.get_sql("newest_first"))
    filled_rows = sqlite_session.select(spec.get_sql("newest_first", p="id < 3 -- only early rows"))

    assert [row["id"] for row in default_rows] == [4, 3, 2, 1]
    assert [row["id"] for row in filled_rows] == [2, 1]
