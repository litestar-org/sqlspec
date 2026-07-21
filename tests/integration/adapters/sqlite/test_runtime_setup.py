"""Shared SQLite and aiosqlite runtime setup behavior."""

import sqlite3
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import aiosqlite
import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import SQLSpecError

pytestmark = pytest.mark.xdist_group("sqlite")

RuntimeConfig = SqliteConfig | AiosqliteConfig
RuntimeConfigFactory = Callable[..., RuntimeConfig]

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


@pytest.fixture(params=(SqliteConfig, AiosqliteConfig), ids=("sqlite", "aiosqlite"))
def runtime_config_factory(request: pytest.FixtureRequest) -> RuntimeConfigFactory:
    """Parametrize one runtime behavior body over both SQLite adapters."""
    return cast("RuntimeConfigFactory", request.param)


async def _close_config(config: RuntimeConfig) -> None:
    if isinstance(config, AiosqliteConfig):
        await config.close_pool()
        config.connection_instance = None
        return
    config.close_pool()


async def _execute_script(config: RuntimeConfig, statement: str) -> None:
    if isinstance(config, AiosqliteConfig):
        async with config.provide_session() as session:
            await session.execute_script(statement)
            await session.commit()
        return
    with config.provide_session() as session:
        session.execute_script(statement)
        session.commit()


async def _select(config: RuntimeConfig, statement: str) -> list[dict[str, Any]]:
    if isinstance(config, AiosqliteConfig):
        async with config.provide_session() as session:
            return (await session.execute(statement)).get_data()
    with config.provide_session() as session:
        return session.execute(statement).get_data()


async def _select_value(config: RuntimeConfig, statement: str) -> Any:
    rows = await _select(config, statement)
    return next(iter(rows[0].values()))


async def _raw_fetchone(config: RuntimeConfig, statement: str) -> Any:
    if isinstance(config, AiosqliteConfig):
        async with config.provide_connection() as connection:
            cursor = await connection.execute(statement)
            try:
                return await cursor.fetchone()
            finally:
                await cursor.close()
    with config.provide_connection() as connection:
        return connection.execute(statement).fetchone()


async def test_custom_function_visible_in_sql(runtime_config_factory: RuntimeConfigFactory) -> None:
    """Custom SQLite functions are callable from both drivers."""
    config = runtime_config_factory(
        driver_features={
            "custom_functions": [{"name": "double_value", "narg": 1, "func": _double, "deterministic": True}]
        }
    )
    try:
        assert await _select_value(config, "SELECT double_value(21) AS value") == 42
    finally:
        await _close_config(config)


async def test_custom_aggregate_visible_in_sql(runtime_config_factory: RuntimeConfigFactory, tmp_path: Path) -> None:
    """Custom SQLite aggregates are callable from both drivers."""
    config = runtime_config_factory(
        connection_config={"database": tmp_path / "aggregate.db"},
        driver_features={"custom_aggregates": [{"name": "sum_values", "narg": 1, "aggregate_class": _SumAggregate}]},
    )
    try:
        await _execute_script(
            config,
            """
            CREATE TABLE numbers (value INTEGER);
            INSERT INTO numbers (value) VALUES (1);
            INSERT INTO numbers (value) VALUES (2);
            INSERT INTO numbers (value) VALUES (3);
            INSERT INTO numbers (value) VALUES (4);
            """,
        )
        assert await _select_value(config, "SELECT sum_values(value) AS total FROM numbers") == 10
    finally:
        await _close_config(config)


async def test_custom_collation_orders_results(runtime_config_factory: RuntimeConfigFactory, tmp_path: Path) -> None:
    """Custom collations change ordering for both drivers."""
    config = runtime_config_factory(
        connection_config={"database": tmp_path / "collation.db"},
        driver_features={"custom_collations": [{"name": "reverse_order", "func": _reverse_collation}]},
    )
    try:
        await _execute_script(
            config,
            """
            CREATE TABLE names (name TEXT);
            INSERT INTO names (name) VALUES ('alice');
            INSERT INTO names (name) VALUES ('bob');
            INSERT INTO names (name) VALUES ('carol');
            """,
        )
        rows = await _select(config, "SELECT name FROM names ORDER BY name COLLATE reverse_order")
        assert [row["name"] for row in rows] == ["carol", "bob", "alice"]
    finally:
        await _close_config(config)


async def test_authorizer_blocks_table_read(runtime_config_factory: RuntimeConfigFactory, tmp_path: Path) -> None:
    """Authorizer callbacks can deny reads for both drivers."""
    connection_config = {"database": tmp_path / "authorizer.db"}
    seed_config = runtime_config_factory(connection_config=connection_config)
    try:
        await _execute_script(
            seed_config,
            """
            CREATE TABLE secrets (id INTEGER PRIMARY KEY, secret TEXT);
            INSERT INTO secrets (secret) VALUES ('hidden');
            """,
        )
    finally:
        await _close_config(seed_config)

    config = runtime_config_factory(
        connection_config=connection_config, driver_features={"authorizer_callback": _deny_secrets_read}
    )
    try:
        with pytest.raises(SQLSpecError):
            await _select(config, "SELECT * FROM secrets")
    finally:
        await _close_config(config)


async def test_progress_handler_fires(runtime_config_factory: RuntimeConfigFactory) -> None:
    """Progress handlers are invoked by both drivers."""
    PROGRESS_CALLS.clear()
    config = runtime_config_factory(
        driver_features={"progress_handler": _progress_handler, "progress_handler_interval": 10}
    )
    try:
        result = await _select_value(
            config,
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
            """,
        )
        assert result == 5000
        assert PROGRESS_CALLS
    finally:
        await _close_config(config)


async def test_trace_callback_records_statements(runtime_config_factory: RuntimeConfigFactory) -> None:
    """Trace callbacks record statements from both drivers."""
    TRACE_STATEMENTS.clear()
    config = runtime_config_factory(driver_features={"trace_callback": _trace_callback})
    try:
        assert await _select_value(config, "SELECT 1 AS value") == 1
    finally:
        await _close_config(config)
    assert any("SELECT 1" in statement for statement in TRACE_STATEMENTS)


async def test_row_factory_row_literal_applies(runtime_config_factory: RuntimeConfigFactory) -> None:
    """The row_factory literal applies without breaking SQLSpec rows."""
    config = runtime_config_factory(driver_features={"row_factory": "row"})
    try:
        row = await _raw_fetchone(config, "SELECT 1 AS v")
        assert row is not None
        assert row["v"] == 1
        assert await _select_value(config, "SELECT 1 AS v") == 1
    finally:
        await _close_config(config)


async def test_text_factory_applies(runtime_config_factory: RuntimeConfigFactory) -> None:
    """The text_factory setting affects raw rows from both drivers."""
    config = runtime_config_factory(driver_features={"text_factory": bytes})
    try:
        row = await _raw_fetchone(config, "SELECT 'hello'")
        assert row is not None
        assert row[0] == b"hello"
    finally:
        await _close_config(config)


async def test_user_pragmas_override_optimizations(
    runtime_config_factory: RuntimeConfigFactory, tmp_path: Path
) -> None:
    """User PRAGMAs win over built-in optimizations for both drivers."""
    config = runtime_config_factory(
        connection_config={"database": tmp_path / "pragma.db"}, driver_features={"pragmas": {"synchronous": "FULL"}}
    )
    try:
        row = await _raw_fetchone(config, "PRAGMA synchronous")
        assert row is not None
        assert row[0] == 2
    finally:
        await _close_config(config)


async def test_extension_loading_attempts_paths(runtime_config_factory: RuntimeConfigFactory, tmp_path: Path) -> None:
    """Configured extension paths are attempted by both drivers."""
    config = runtime_config_factory(driver_features={"extensions": [str(tmp_path / "missing_extension.so")]})
    try:
        with pytest.raises((sqlite3.OperationalError, aiosqlite.OperationalError)):
            await _select(config, "SELECT 1")
    finally:
        await _close_config(config)
