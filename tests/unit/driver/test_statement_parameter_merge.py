"""Tests for merging statement-bound parameters with execute-time parameters."""

from collections.abc import AsyncGenerator, Generator
from pathlib import Path
from types import MappingProxyType
from typing import Any

import pytest

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.core import SQL


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


BOUND_A = ("SELECT :a AS a, :b AS b", (), {"a": 1})


@pytest.mark.parametrize(
    ("statement", "args", "kwargs", "expected"),
    [
        pytest.param(BOUND_A, (), {"b": 2}, {"a": 1, "b": 2}, id="kwargs-merge"),
        pytest.param(BOUND_A, (), {"a": 5, "b": 2}, {"a": 5, "b": 2}, id="kwargs-replace-bound"),
        pytest.param(BOUND_A, ({"b": 2},), {}, {"a": 1, "b": 2}, id="mapping-merge"),
        pytest.param(BOUND_A, ({"a": 6, "b": 7},), {}, {"a": 6, "b": 7}, id="mapping-replace-bound"),
        pytest.param(BOUND_A, (MappingProxyType({"b": 5}),), {}, {"a": 1, "b": 5}, id="non-dict-mapping-merge"),
        pytest.param(("SELECT :a AS a", (), {"a": 1}), (), {}, {"a": 1}, id="named-bound-no-extra"),
        pytest.param(("SELECT ? AS a", (1,), {}), (), {}, {"a": 1}, id="positional-bound-no-extra"),
        pytest.param(("SELECT :a AS a", (), {}), (), {"a": 4}, {"a": 4}, id="unbound-kwargs"),
        pytest.param(("SELECT ? AS a, ? AS b", (1,), {}), (2,), {}, {"a": 1, "b": 2}, id="positional-appends"),
        pytest.param(BOUND_A, ((2,),), {}, {"a": 1, "b": 2}, id="named-bound-positional-execute-merged"),
    ],
)
def test_sync_statement_parameter_merge(
    sqlite_session: SqliteDriver,
    statement: "tuple[str, tuple[Any, ...], dict[str, Any]]",
    args: "tuple[Any, ...]",
    kwargs: "dict[str, Any]",
    expected: "dict[str, Any] | type[Exception]",
) -> None:
    sql_text, bound_args, bound_kwargs = statement
    sql = SQL(sql_text, *bound_args, **bound_kwargs)

    if isinstance(expected, dict):
        assert sqlite_session.select_one(sql, *args, **kwargs) == expected
    else:
        with pytest.raises(expected, match="Parameter count mismatch"):
            sqlite_session.select_one(sql, *args, **kwargs)
    assert sql.named_parameters == bound_kwargs


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


@pytest.mark.parametrize("shape", ["filter", "direct-filter", "where", "kwargs", "named", "repeated", "extra"])
def test_mixed_values_preserve_tenant_on_every_call(sqlite_session: SqliteDriver, shape: str) -> None:
    from sqlspec.core import LimitOffsetFilter

    sqlite_session.execute("CREATE TABLE tenant_rows (id INTEGER, tenant INTEGER, v INTEGER)")
    sqlite_session.execute_many(
        "INSERT INTO tenant_rows VALUES (?, ?, ?)", [(1, 1, 10), (2, 1, 20), (3, 2, 30), (4, 1, 40)]
    )
    for _ in range(3):
        base = "SELECT id FROM tenant_rows WHERE tenant = ? ORDER BY id"
        args: tuple[Any, ...] = ()
        expected = [1, 2]
        if shape == "filter":
            statement = SQL(base, 1)
            args = (LimitOffsetFilter(2, 0),)
        elif shape == "direct-filter":
            statement = LimitOffsetFilter(2, 0).append_to_statement(SQL(base, 1))
        elif shape == "where":
            statement = SQL(base, 1).where_eq("v", 40)
            expected = [4]
        elif shape == "kwargs":
            statement = SQL("SELECT id FROM tenant_rows WHERE tenant = ? AND v = :x", 1, x=20)
            expected = [2]
        elif shape == "named":
            statement = SQL(base.replace("?", ":tenant"), 1)
            args = (LimitOffsetFilter(2, 0),)
        elif shape == "repeated":
            statement = SQL("SELECT id FROM tenant_rows WHERE tenant = :a AND v > :a", 1).where_eq("v", 40)
            expected = [4]
        else:
            statement = SQL(base).where_eq("v", 40)
            args = (1,)
            expected = [4]
        assert [row["id"] for row in sqlite_session.select(statement, *args)] == expected
