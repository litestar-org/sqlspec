"""Integration tests for aiosqlite runtime setup behavior."""

import sqlite3
from pathlib import Path

import aiosqlite
import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.core import SQLResult
from sqlspec.exceptions import SQLSpecError

pytestmark = pytest.mark.xdist_group("sqlite")

TRACE_STATEMENTS: list[str] = []
PROGRESS_CALLS: list[int] = []


def _double(value: int) -> int:
    return value * 2


def _reverse_collation(left: str, right: str) -> int:
    if left < right:
        return 1
    if left > right:
        return -1
    return 0


class _SumAggregate:
    def __init__(self) -> None:
        self.total = 0

    def step(self, value: int | None) -> None:
        if value is not None:
            self.total += int(value)

    def finalize(self) -> int:
        return self.total


def _deny_secrets_read(
    action: int, arg1: str | None, arg2: str | None, db_name: str | None, trigger_name: str | None
) -> int:
    _ = arg2, db_name, trigger_name
    if action == sqlite3.SQLITE_READ and arg1 == "secrets":
        return sqlite3.SQLITE_DENY
    return sqlite3.SQLITE_OK


def _progress_handler() -> int:
    PROGRESS_CALLS.append(1)
    return 0


def _trace_callback(statement: str) -> None:
    TRACE_STATEMENTS.append(statement)


async def test_custom_function_visible_in_sql() -> None:
    config = AiosqliteConfig(
        driver_features={
            "custom_functions": [{"name": "double_value", "narg": 1, "func": _double, "deterministic": True}]
        }
    )

    try:
        async with config.provide_session() as session:
            result = await session.execute("SELECT double_value(21) AS value")
            assert isinstance(result, SQLResult)
            assert result.get_data()[0]["value"] == 42
    finally:
        await config.close_pool()


async def test_custom_aggregate_visible_in_sql(tmp_path: Path) -> None:
    db_path = tmp_path / "aggregate.db"
    config = AiosqliteConfig(
        connection_config={"database": db_path},
        driver_features={"custom_aggregates": [{"name": "sum_values", "narg": 1, "aggregate_class": _SumAggregate}]},
    )

    try:
        async with config.provide_session() as session:
            await session.execute_script("""
                CREATE TABLE numbers (value INTEGER);
                INSERT INTO numbers (value) VALUES (1);
                INSERT INTO numbers (value) VALUES (2);
                INSERT INTO numbers (value) VALUES (3);
                INSERT INTO numbers (value) VALUES (4);
            """)
            await session.commit()

            result = await session.execute("SELECT sum_values(value) AS total FROM numbers")
            assert isinstance(result, SQLResult)
            assert result.get_data()[0]["total"] == 10
    finally:
        await config.close_pool()


async def test_custom_collation_orders_results(tmp_path: Path) -> None:
    db_path = tmp_path / "collation.db"
    config = AiosqliteConfig(
        connection_config={"database": db_path},
        driver_features={"custom_collations": [{"name": "reverse_order", "func": _reverse_collation}]},
    )

    try:
        async with config.provide_session() as session:
            await session.execute_script("""
                CREATE TABLE names (name TEXT);
                INSERT INTO names (name) VALUES ('alice');
                INSERT INTO names (name) VALUES ('bob');
                INSERT INTO names (name) VALUES ('carol');
            """)
            await session.commit()

            result = await session.execute("SELECT name FROM names ORDER BY name COLLATE reverse_order")
            assert isinstance(result, SQLResult)
            assert [row["name"] for row in result.get_data()] == ["carol", "bob", "alice"]
    finally:
        await config.close_pool()


async def test_authorizer_blocks_table_read(tmp_path: Path) -> None:
    db_path = tmp_path / "authorizer.db"

    config = AiosqliteConfig(connection_config={"database": db_path})
    try:
        async with config.provide_session() as session:
            await session.execute_script("""
                CREATE TABLE secrets (id INTEGER PRIMARY KEY, secret TEXT);
                INSERT INTO secrets (secret) VALUES ('hidden');
            """)
            await session.commit()
    finally:
        await config.close_pool()

    config = AiosqliteConfig(
        connection_config={"database": db_path}, driver_features={"authorizer_callback": _deny_secrets_read}
    )
    try:
        async with config.provide_session() as session:
            with pytest.raises(SQLSpecError):
                await session.execute("SELECT * FROM secrets")
    finally:
        await config.close_pool()


async def test_progress_handler_fires() -> None:
    PROGRESS_CALLS.clear()
    config = AiosqliteConfig(driver_features={"progress_handler": _progress_handler, "progress_handler_interval": 10})

    try:
        async with config.provide_session() as session:
            result = await session.execute(
                """
                SELECT count(*) AS total
                FROM (
                    WITH RECURSIVE c(x) AS (
                        SELECT 1
                        UNION ALL
                        SELECT x + 1 FROM c WHERE x < 5000
                    )
                    SELECT x FROM c
                )
                """
            )
            assert isinstance(result, SQLResult)
            assert result.get_data()[0]["total"] == 5000
            assert PROGRESS_CALLS
    finally:
        await config.close_pool()


async def test_trace_callback_records_statements() -> None:
    TRACE_STATEMENTS.clear()
    config = AiosqliteConfig(driver_features={"trace_callback": _trace_callback})

    try:
        async with config.provide_session() as session:
            result = await session.execute("SELECT 1")
            assert isinstance(result, SQLResult)
    finally:
        await config.close_pool()

    assert any("SELECT 1" in statement for statement in TRACE_STATEMENTS)


async def test_row_factory_row_literal_applies() -> None:
    config = AiosqliteConfig(driver_features={"row_factory": "row"})

    try:
        async with config.provide_connection() as conn:
            cursor = await conn.execute("SELECT 1 AS v")
            try:
                row = await cursor.fetchone()
            finally:
                await cursor.close()
            assert row is not None
            assert row["v"] == 1

        async with config.provide_session() as session:
            result = await session.execute("SELECT 1 AS v")
            assert isinstance(result, SQLResult)
            assert result.get_data()[0]["v"] == 1
    finally:
        await config.close_pool()


async def test_text_factory_applies() -> None:
    config = AiosqliteConfig(driver_features={"text_factory": bytes})

    try:
        async with config.provide_connection() as conn:
            cursor = await conn.execute("SELECT 'hello'")
            try:
                row = await cursor.fetchone()
            finally:
                await cursor.close()
            assert row is not None
            assert row[0] == b"hello"
    finally:
        await config.close_pool()


async def test_user_pragmas_override_optimizations(tmp_path: Path) -> None:
    db_path = tmp_path / "pragma.db"
    config = AiosqliteConfig(
        connection_config={"database": db_path}, driver_features={"pragmas": {"synchronous": "FULL"}}
    )

    try:
        async with config.provide_connection() as conn:
            cursor = await conn.execute("PRAGMA synchronous")
            try:
                row = await cursor.fetchone()
            finally:
                await cursor.close()
            assert row is not None
            assert row[0] == 2
    finally:
        await config.close_pool()


async def test_extension_loading_attempts_paths(tmp_path: Path) -> None:
    config = AiosqliteConfig(driver_features={"extensions": [str(tmp_path / "missing_extension.so")]})

    with pytest.raises((sqlite3.OperationalError, aiosqlite.OperationalError)):
        async with config.provide_session():
            pass

    await config.close_pool()
