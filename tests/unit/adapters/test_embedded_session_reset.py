"""Opt-in session reset drops per-session state when an embedded connection is released."""

import importlib
import inspect
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.sqlite import SqliteConfig

KINDS = ["sqlite", "aiosqlite", "duckdb"]
SQLITE_KINDS = ["sqlite", "aiosqlite"]


def build_config(kind: str, tmp_path: Path, *, reset: bool, **features: Any) -> Any:
    database = str(tmp_path / f"{kind}.db")
    driver_features = {**features, **({"reset_session_on_release": True} if reset else {})}
    if kind == "sqlite":
        return SqliteConfig(connection_config={"database": database}, driver_features=driver_features)
    if kind == "aiosqlite":
        return AiosqliteConfig(
            connection_config={"database": database, "pool_size": 1}, driver_features=driver_features
        )
    return DuckDBConfig(connection_config={"database": database}, driver_features=driver_features)


async def resolve(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


async def rows(session: Any, sql: str) -> "list[Any]":
    result = await resolve(session.select(sql))
    return list(result)


@asynccontextmanager
async def session_scope(config: Any) -> "AsyncGenerator[Any, None]":
    context = config.provide_session()
    if hasattr(context, "__aenter__"):
        async with context as session:
            yield session
    else:
        with context as session:
            yield session


async def close_config(config: Any) -> None:
    await resolve(config.close_pool())


async def create_temp_table(kind: str, session: Any) -> None:
    await resolve(session.execute("CREATE TEMP TABLE scratch (x INTEGER)"))
    await resolve(session.execute("INSERT INTO scratch VALUES (1)"))
    if kind == "duckdb":
        await resolve(session.commit())


async def temp_table_names(kind: str, session: Any) -> "list[str]":
    if kind == "duckdb":
        found = await rows(session, "SELECT table_name FROM duckdb_tables() WHERE temporary")
        return [row["table_name"] for row in found]
    found = await rows(session, "SELECT name FROM temp.sqlite_master WHERE type = 'table'")
    return [row["name"] for row in found]


@pytest.mark.anyio
@pytest.mark.parametrize("kind", KINDS)
async def test_temp_table_dropped_on_release_when_enabled(kind: str, tmp_path: Path) -> None:
    config = build_config(kind, tmp_path, reset=True)
    try:
        async with session_scope(config) as session:
            await create_temp_table(kind, session)
            assert await temp_table_names(kind, session) == ["scratch"]
        async with session_scope(config) as session:
            assert await temp_table_names(kind, session) == []
    finally:
        await close_config(config)


@pytest.mark.anyio
@pytest.mark.parametrize("kind", KINDS)
async def test_temp_table_kept_when_reset_disabled(kind: str, tmp_path: Path) -> None:
    config = build_config(kind, tmp_path, reset=False)
    try:
        async with session_scope(config) as session:
            await create_temp_table(kind, session)
        async with session_scope(config) as session:
            assert await temp_table_names(kind, session) == ["scratch"]
    finally:
        await close_config(config)


@pytest.mark.anyio
@pytest.mark.parametrize("kind", SQLITE_KINDS)
async def test_pragma_baseline_restored(kind: str, tmp_path: Path) -> None:
    config = build_config(kind, tmp_path, reset=True)
    try:
        async with session_scope(config) as session:
            await resolve(session.execute("PRAGMA foreign_keys = ON"))
            await resolve(session.execute("PRAGMA query_only = ON"))
            await resolve(session.execute("PRAGMA trusted_schema = OFF"))
            await resolve(session.execute("PRAGMA recursive_triggers = ON"))
        async with session_scope(config) as session:
            for pragma, expected in (
                ("foreign_keys", 0),
                ("query_only", 0),
                ("trusted_schema", 1),
                ("recursive_triggers", 0),
            ):
                found = await rows(session, f"PRAGMA {pragma}")
                assert list(found[0].values()) == [expected], pragma
    finally:
        await close_config(config)


@pytest.mark.anyio
@pytest.mark.parametrize("kind", KINDS)
async def test_attached_database_detached_on_release(kind: str, tmp_path: Path) -> None:
    config = build_config(kind, tmp_path, reset=True)
    other = str(tmp_path / "other.db")
    try:
        async with session_scope(config) as session:
            await resolve(session.execute(f"ATTACH DATABASE '{other}' AS other"))
        async with session_scope(config) as session:
            if kind == "duckdb":
                found = await rows(session, "SELECT database_name FROM duckdb_databases() WHERE NOT internal")
                assert "other" not in [row["database_name"] for row in found]
            else:
                found = await rows(session, "PRAGMA database_list")
                assert sorted(row["name"] for row in found) == ["main", "temp"]
    finally:
        await close_config(config)


@pytest.mark.anyio
@pytest.mark.parametrize("kind", KINDS)
async def test_reset_failure_retires_connection(kind: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = build_config(kind, tmp_path, reset=True)
    module = {
        "sqlite": "sqlspec.adapters.sqlite.pool",
        "aiosqlite": "sqlspec.adapters.aiosqlite.pool",
        "duckdb": "sqlspec.adapters.duckdb.pool",
    }[kind]
    pool_module = importlib.import_module(module)
    original = pool_module._reset_session_state
    failures = {"remaining": 1}

    async def failing_async(*args: Any, **kwargs: Any) -> None:
        if failures["remaining"]:
            failures["remaining"] -= 1
            msg = "reset failed"
            raise RuntimeError(msg)
        await original(*args, **kwargs)

    def failing_sync(*args: Any, **kwargs: Any) -> None:
        if failures["remaining"]:
            failures["remaining"] -= 1
            msg = "reset failed"
            raise RuntimeError(msg)
        original(*args, **kwargs)

    failing = failing_async if inspect.iscoroutinefunction(original) else failing_sync
    monkeypatch.setattr(pool_module, "_reset_session_state", failing)
    try:
        async with session_scope(config) as session:
            await create_temp_table(kind, session)
        async with session_scope(config) as session:
            assert await temp_table_names(kind, session) == []
        assert failures["remaining"] == 0
    finally:
        await close_config(config)


def test_sqlite_reset_runs_for_connection_contexts(tmp_path: Path) -> None:
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "ctx.db")}, driver_features={"reset_session_on_release": True}
    )
    try:
        with config.provide_connection() as connection:
            connection.execute("CREATE TEMP TABLE scratch (x INTEGER)")
        with config.provide_connection() as connection:
            assert connection.execute("SELECT name FROM temp.sqlite_master").fetchall() == []
    finally:
        config.close_pool()
