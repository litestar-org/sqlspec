"""Tests for merging statement-bound parameters with execute-time parameters."""

from collections.abc import AsyncGenerator, Generator
from pathlib import Path
from types import MappingProxyType

import pytest

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.core import SQL
from sqlspec.exceptions import SQLSpecError


@pytest.fixture
def sqlite_session() -> Generator[SqliteDriver, None, None]:
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))
    with spec.provide_session(config) as session:
        yield session
    config.close_pool()


@pytest.fixture
async def aiosqlite_session(tmp_path: Path) -> AsyncGenerator[AiosqliteDriver, None]:
    spec = SQLSpec()
    config = spec.add_config(AiosqliteConfig(connection_config={"database": str(tmp_path / "merge.db")}))
    async with spec.provide_session(config) as session:
        yield session
    await config.close_pool()


def test_sync_kwargs_merge_with_bound_named_parameters(sqlite_session: SqliteDriver) -> None:
    statement = SQL("SELECT :a AS a, :b AS b", a=1)

    assert sqlite_session.select_one(statement, b=2) == {"a": 1, "b": 2}
    assert sqlite_session.select_one(statement, b=3) == {"a": 1, "b": 3}
    assert statement.named_parameters == {"a": 1}


def test_sync_execute_time_value_replaces_bound_value(sqlite_session: SqliteDriver) -> None:
    statement = SQL("SELECT :a AS a, :b AS b", a=1)

    assert sqlite_session.select_one(statement, a=5, b=2) == {"a": 5, "b": 2}
    assert sqlite_session.select_one(statement, {"a": 6, "b": 7}) == {"a": 6, "b": 7}


def test_sync_mapping_merges_with_bound_named_parameters(sqlite_session: SqliteDriver) -> None:
    statement = SQL("SELECT :a AS a, :b AS b", a=1)

    assert sqlite_session.select_one(statement, {"b": 2}) == {"a": 1, "b": 2}
    assert sqlite_session.select_one(statement, MappingProxyType({"b": 5})) == {"a": 1, "b": 5}


def test_sync_statement_without_extra_parameters_unchanged(sqlite_session: SqliteDriver) -> None:
    assert sqlite_session.select_one(SQL("SELECT :a AS a", a=1)) == {"a": 1}
    assert sqlite_session.select_one(SQL("SELECT ? AS a", 1)) == {"a": 1}
    assert sqlite_session.select_one(SQL("SELECT :a AS a"), a=4) == {"a": 4}


def test_sync_positional_bound_parameters_keep_appending(sqlite_session: SqliteDriver) -> None:
    assert sqlite_session.select_one(SQL("SELECT ? AS a, ? AS b", 1), 2) == {"a": 1, "b": 2}


def test_sync_named_bound_with_positional_execute_parameters_not_merged(sqlite_session: SqliteDriver) -> None:
    with pytest.raises(SQLSpecError, match="Parameter count mismatch"):
        sqlite_session.select_one(SQL("SELECT :a AS a, :b AS b", a=1), (2,))


def test_sync_repeated_merged_executions_return_matching_rows(sqlite_session: SqliteDriver) -> None:
    sqlite_session.execute_script(
        "CREATE TABLE item (id INTEGER PRIMARY KEY, kind TEXT, owner TEXT);"
        "INSERT INTO item VALUES (1, 'a', 'x'), (2, 'a', 'y'), (3, 'b', 'x');"
    )
    statement = SQL("SELECT id FROM item WHERE kind = :kind AND owner = :owner ORDER BY id", kind="a")

    for owner, expected in [("x", [1]), ("y", [2]), ("x", [1]), ("z", [])]:
        assert [row["id"] for row in sqlite_session.select(statement, owner=owner)] == expected
    assert [row["id"] for row in sqlite_session.select(statement, kind="b", owner="x")] == [3]


async def test_async_kwargs_and_mapping_merge_with_bound_named_parameters(aiosqlite_session: AiosqliteDriver) -> None:
    statement = SQL("SELECT :a AS a, :b AS b", a=1)

    assert await aiosqlite_session.select_one(statement, b=2) == {"a": 1, "b": 2}
    assert await aiosqlite_session.select_one(statement, {"b": 3}) == {"a": 1, "b": 3}
    assert await aiosqlite_session.select_one(statement, a=9, b=4) == {"a": 9, "b": 4}
    assert await aiosqlite_session.select_one(SQL("SELECT :a AS a", a=1)) == {"a": 1}
