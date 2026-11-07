"""Execute table and alias text through real engines."""

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from sqlglot import exp

from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.builder import QueryBuilder, sql
from sqlspec.exceptions import SQLBuilderError, SQLSpecError

pytestmark = pytest.mark.xdist_group("builder_table_reference")

SECRET = "top-secret"
SETUP_STATEMENTS = (
    "CREATE TABLE t (id INTEGER, pw VARCHAR)",
    "CREATE TABLE s (id INTEGER, pw VARCHAR)",
    "CREATE TABLE secrets (id INTEGER, pw VARCHAR)",
    "CREATE TABLE z (id INTEGER)",
    "INSERT INTO t VALUES (1, 'public')",
    "INSERT INTO s VALUES (1, 'joined')",
    f"INSERT INTO secrets VALUES (1, '{SECRET}')",
    "INSERT INTO z VALUES (1)",
)
UNSAFE_TEXT = ("t x WHERE 1=0 UNION SELECT id, pw FROM secrets --", "t x; DROP TABLE z", "t; DROP TABLE z")


def _sqlite_config(path: Path) -> Any:
    return SqliteConfig(connection_config={"database": str(path / "reference.sqlite")})


def _duckdb_config(path: Path) -> Any:
    return DuckDBConfig(connection_config={"database": str(path / "reference.duckdb")})


@pytest.fixture(params=[_sqlite_config, _duckdb_config], ids=["sqlite", "duckdb"])
def session(request: pytest.FixtureRequest, tmp_path: Path) -> Any:
    config = request.param(tmp_path)
    try:
        with config.provide_session() as driver:
            for statement in SETUP_STATEMENTS:
                driver.execute(statement)
            driver.commit()
            yield driver
    finally:
        config.close_pool()


def _assert_nothing_changed(session: Any) -> None:
    assert session.execute("SELECT id FROM z").get_data() == [{"id": 1}]
    assert session.execute("SELECT pw FROM secrets").get_data() == [{"pw": SECRET}]


@pytest.mark.parametrize(
    "build",
    [
        lambda text: sql.select("*").from_(text),
        lambda text: sql.select("*").from_("t", alias=text),
        lambda text: sql.select("*").from_("t").join(text, "t.id = 1"),
        lambda text: sql.delete(text),
        lambda text: sql.update(text).set(pw="changed"),
    ],
)
@pytest.mark.parametrize("text", UNSAFE_TEXT)
def test_unsafe_text_is_rejected_before_execution(
    session: Any, build: Callable[[str], QueryBuilder], text: str
) -> None:
    with pytest.raises(SQLBuilderError):
        session.execute(build(text))
    _assert_nothing_changed(session)


@pytest.mark.parametrize("text", UNSAFE_TEXT)
def test_quoted_text_is_only_ever_a_table_name(session: Any, text: str) -> None:
    with pytest.raises(SQLSpecError):
        session.execute(sql.select("*").from_(f'"{text}"'))
    _assert_nothing_changed(session)


@pytest.mark.parametrize("alias", UNSAFE_TEXT)
def test_alias_on_expression_source_is_only_ever_an_alias(session: Any, alias: str) -> None:
    rows = session.execute(sql.select("id", "pw").from_(exp.to_table("t"), alias=alias)).get_data()
    assert rows == [{"id": 1, "pw": "public"}]
    _assert_nothing_changed(session)


@pytest.mark.parametrize(
    "build",
    [
        lambda: sql.select("u.pw").from_("t AS u"),
        lambda: sql.select("u.pw").from_("t u"),
        lambda: sql.select("u.pw").from_("t", alias="u"),
    ],
)
def test_aliased_tables_execute(session: Any, build: Callable[[], QueryBuilder]) -> None:
    assert session.execute(build()).get_data() == [{"pw": "public"}]


def test_aliased_join_executes(session: Any) -> None:
    query = sql.select("x.pw").from_("t AS u").join("s AS x", "u.id = x.id")
    assert session.execute(query).get_data() == [{"pw": "joined"}]


PAYLOADS = (
    "`.json_each((SELECT json_quote(pw) FROM secrets)) --`",
    '".json_each((SELECT json_quote(pw) FROM secrets)) --"',
    "[.json_each((SELECT json_quote(pw) FROM secrets)) --]",
    "`\njson_each((SELECT json_quote(pw) FROM secrets)) --`",
    "`.json_each((SELECT json_quote(pw) FROM secrets)) /*`",
    "`.json_each((SELECT json_quote(pw) FROM secrets)) #`",
    '#."\nread_csv((SELECT pw FROM secrets)) --"',
    '--."\njson_each((SELECT json_quote(pw) FROM secrets)) --"',
    "`x\n; DROP TABLE z --`",
    '"x\n; DROP TABLE z --"',
    "json_each((SELECT json_quote(pw) FROM secrets))",
    "unnest((SELECT list(pw) FROM secrets))",
)


def _execute_tolerating_rejection(session: Any, build: Callable[[], QueryBuilder]) -> Any:
    try:
        return session.execute(build()).get_data()
    except (SQLBuilderError, SQLSpecError):
        return []


@pytest.mark.parametrize(
    "build",
    [
        lambda text, dialect: sql.select("*", dialect=dialect).from_(text),
        lambda text, dialect: sql.select("*", dialect=dialect).from_("t").join(text, "t.id = 1"),
        lambda text, dialect: sql.update("t", dialect=dialect).set(pw="changed").from_(text),
    ],
    ids=["from", "join", "update_from"],
)
@pytest.mark.parametrize("use_session_dialect", [False, True], ids=["default_builder", "session_dialect"])
@pytest.mark.parametrize("text", PAYLOADS)
def test_payload_text_runs_nothing(
    session: Any, build: Callable[[str, Any], QueryBuilder], use_session_dialect: bool, text: str
) -> None:
    dialect = session.dialect if use_session_dialect else None
    rows = _execute_tolerating_rejection(session, lambda: build(text, dialect))
    assert SECRET not in str(rows)
    _assert_nothing_changed(session)
    assert session.execute("SELECT pw FROM t").get_data() == [{"pw": "public"}]


def test_parameterized_function_expression_binds_and_executes(session: Any) -> None:
    source = sql.raw("json_each(:doc)", doc="[1]") if session.dialect == "sqlite" else sql.raw("range(:n)", n=1)
    assert len(session.execute(sql.select("*").from_(source, alias="f")).get_data()) == 1
