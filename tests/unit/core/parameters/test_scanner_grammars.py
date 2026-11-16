"""Placeholder scanner grammar behavior: bracket identifiers, nested comments, unterminated literals."""

from typing import Any

import pytest

from sqlspec import SQL
from sqlspec.adapters.adbc.core import get_statement_config as adbc_statement_config
from sqlspec.adapters.asyncpg.core import default_statement_config as asyncpg_config
from sqlspec.adapters.psycopg.core import default_statement_config as psycopg_config
from sqlspec.core import ParameterStyle
from sqlspec.core.parameters import ParameterValidator

NESTED_COMMENT_SQL = "select /* :b ? /* nested :a */ still comment :b */ :a as a, :b as b"


def test_asyncpg_nested_comment_hides_placeholders() -> None:
    compiled_sql, params = SQL(NESTED_COMMENT_SQL, statement_config=asyncpg_config, a="A", b="B").compile()
    assert compiled_sql == "select /* :b ? /* nested :a */ still comment :b */ $1 as a, $2 as b"
    assert tuple(params) == ("A", "B")


def test_psycopg_nested_comment_hides_placeholders() -> None:
    compiled_sql, params = SQL("select /* /* x */ :b */ :a as a", statement_config=psycopg_config, a="A").compile()
    assert compiled_sql == "select /* /* x */ :b */ %s as a"
    assert tuple(params) == ("A",)


@pytest.mark.parametrize(
    ("sql", "expected_sql", "expected_params"),
    [
        ("SELECT 1 AS [x :b], :a AS a", "SELECT 1 AS [x :b], ? AS a", ("A",)),
        ("SELECT [we?ird] AS c, :a AS a, :b AS b", "SELECT [we?ird] AS c, ? AS a, ? AS b", ("A", "B")),
    ],
)
def test_adbc_sqlite_bracket_identifiers_hide_placeholders(
    sql: str, expected_sql: str, expected_params: "tuple[str, ...]"
) -> None:
    config = adbc_statement_config("sqlite")
    named: dict[str, Any] = {"a": "A", "b": "B"}
    if expected_params == ("A",):
        named = {"a": "A"}
    compiled_sql, params = SQL(sql, statement_config=config, **named).compile()
    assert compiled_sql == expected_sql
    assert tuple(params) == expected_params


def test_sqlite_bracket_identifier_executes() -> None:
    from sqlspec.adapters.sqlite import SqliteConfig

    config = SqliteConfig(connection_config={"database": ":memory:"})
    try:
        with config.provide_session() as session:
            rows = session.select("SELECT 1 AS [x ?], ? AS a", "A")
        assert rows == [{"x ?": 1, "a": "A"}]
    finally:
        config.close_pool()


def test_duckdb_nested_comment_executes() -> None:
    from sqlspec.adapters.duckdb import DuckDBConfig

    config = DuckDBConfig(connection_config={"database": ":memory:"})
    try:
        with config.provide_session() as session:
            rows = session.select(NESTED_COMMENT_SQL, a="A", b="B")
        assert rows == [{"a": "A", "b": "B"}]
    finally:
        config.close_pool()


def test_duckdb_list_slice_keeps_placeholders() -> None:
    from sqlspec.adapters.duckdb import DuckDBConfig

    config = DuckDBConfig(connection_config={"database": ":memory:"})
    try:
        with config.provide_session() as session:
            rows = session.select("select [1,2,3][:2] as x, :a as a", a="A")
        assert rows == [{"x": [1, 2], "a": "A"}]
    finally:
        config.close_pool()


@pytest.mark.parametrize("dialect", [None, "mysql", "bigquery", "spanner", "snowflake", "postgres", "sqlite"])
def test_unterminated_literal_hides_trailing_placeholders(dialect: "str | None") -> None:
    validator = ParameterValidator()
    assert validator.extract_parameters("SELECT 'abc ?", dialect=dialect) == []


@pytest.mark.parametrize(
    ("dialect", "expected"), [("postgres", [ParameterStyle.QMARK]), ("duckdb", [ParameterStyle.QMARK]), ("oracle", [])]
)
def test_nested_comments_resume_after_outer_comment(dialect: str, expected: "list[ParameterStyle]") -> None:
    validator = ParameterValidator()
    infos = validator.extract_parameters("select /* /* x */ it's */ ? as a", dialect=dialect)
    assert [info.style for info in infos] == expected
