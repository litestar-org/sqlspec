# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false
"""SQLite-family exception contract tests."""

import inspect
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from typing import Any

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import (
    CheckViolationError,
    ForeignKeyViolationError,
    NotNullViolationError,
    SQLParsingError,
    UniqueViolationError,
)

pytestmark = pytest.mark.xdist_group("sqlite")

SQLITE_ADAPTERS = [pytest.param("sqlite", id="sqlite"), pytest.param("aiosqlite", id="aiosqlite")]


@contextmanager
def _sqlite_session() -> Generator[Any, None, None]:
    config = SqliteConfig(connection_config={"database": ":memory:"})
    try:
        with config.provide_session() as driver:
            driver.execute("PRAGMA foreign_keys = ON")
            yield driver
    finally:
        config.close_pool()


@asynccontextmanager
async def _aiosqlite_session() -> AsyncGenerator[Any, None]:
    config = AiosqliteConfig()
    try:
        async with config.provide_session() as driver:
            await driver.execute("PRAGMA foreign_keys = ON")
            yield driver
    finally:
        await config.close_pool()


async def _execute(driver: Any, statement: str, parameters: tuple[Any, ...] | None = None) -> Any:
    result = driver.execute(statement) if parameters is None else driver.execute(statement, parameters)
    if inspect.isawaitable(result):
        return await result
    return result


async def _execute_script(driver: Any, script: str) -> Any:
    result = driver.execute_script(script)
    if inspect.isawaitable(result):
        return await result
    return result


async def _with_sqlite_family_driver(adapter: str, assertion: Any) -> None:
    if adapter == "sqlite":
        with _sqlite_session() as driver:
            await assertion(driver)
        return

    async with _aiosqlite_session() as driver:
        await assertion(driver)


@pytest.mark.parametrize("adapter", SQLITE_ADAPTERS)
async def test_sqlite_family_unique_violation(adapter: str) -> None:
    """SQLite-family adapters map unique constraint failures."""

    async def assertion(driver: Any) -> None:
        await _execute_script(
            driver,
            """
            DROP TABLE IF EXISTS test_unique_constraint;
            CREATE TABLE test_unique_constraint (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE NOT NULL
            );
            """,
        )

        await _execute(driver, "INSERT INTO test_unique_constraint (email) VALUES (?)", ("test@example.com",))

        with pytest.raises(UniqueViolationError) as exc_info:
            await _execute(driver, "INSERT INTO test_unique_constraint (email) VALUES (?)", ("test@example.com",))

        assert "unique" in str(exc_info.value).lower() or "2067" in str(exc_info.value)

        await _execute(driver, "DROP TABLE test_unique_constraint")

    await _with_sqlite_family_driver(adapter, assertion)


@pytest.mark.parametrize("adapter", SQLITE_ADAPTERS)
async def test_sqlite_family_foreign_key_violation(adapter: str) -> None:
    """SQLite-family adapters map foreign key failures."""

    async def assertion(driver: Any) -> None:
        await _execute_script(
            driver,
            """
            DROP TABLE IF EXISTS test_fk_child;
            DROP TABLE IF EXISTS test_fk_parent;
            CREATE TABLE test_fk_parent (
                id INTEGER PRIMARY KEY,
                name TEXT
            );
            CREATE TABLE test_fk_child (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER NOT NULL,
                FOREIGN KEY (parent_id) REFERENCES test_fk_parent(id)
            );
            """,
        )

        with pytest.raises(ForeignKeyViolationError) as exc_info:
            await _execute(driver, "INSERT INTO test_fk_child (parent_id) VALUES (?)", (999,))

        assert "foreign key" in str(exc_info.value).lower() or "787" in str(exc_info.value)

        await _execute_script(
            driver,
            """
            DROP TABLE IF EXISTS test_fk_child;
            DROP TABLE IF EXISTS test_fk_parent;
            """,
        )

    await _with_sqlite_family_driver(adapter, assertion)


@pytest.mark.parametrize("adapter", SQLITE_ADAPTERS)
async def test_sqlite_family_not_null_violation(adapter: str) -> None:
    """SQLite-family adapters map NOT NULL failures."""

    async def assertion(driver: Any) -> None:
        await _execute_script(
            driver,
            """
            DROP TABLE IF EXISTS test_not_null;
            CREATE TABLE test_not_null (
                id INTEGER PRIMARY KEY,
                required_field TEXT NOT NULL
            );
            """,
        )

        with pytest.raises(NotNullViolationError) as exc_info:
            await _execute(driver, "INSERT INTO test_not_null (id) VALUES (?)", (1,))

        assert "not null" in str(exc_info.value).lower() or "1811" in str(exc_info.value)

        await _execute(driver, "DROP TABLE test_not_null")

    await _with_sqlite_family_driver(adapter, assertion)


@pytest.mark.parametrize("adapter", SQLITE_ADAPTERS)
async def test_sqlite_family_check_violation(adapter: str) -> None:
    """SQLite-family adapters map CHECK constraint failures."""

    async def assertion(driver: Any) -> None:
        await _execute_script(
            driver,
            """
            DROP TABLE IF EXISTS test_check_constraint;
            CREATE TABLE test_check_constraint (
                id INTEGER PRIMARY KEY,
                age INTEGER CHECK (age >= 18)
            );
            """,
        )

        with pytest.raises(CheckViolationError) as exc_info:
            await _execute(driver, "INSERT INTO test_check_constraint (age) VALUES (?)", (15,))

        assert "check" in str(exc_info.value).lower() or "531" in str(exc_info.value)

        await _execute(driver, "DROP TABLE test_check_constraint")

    await _with_sqlite_family_driver(adapter, assertion)


@pytest.mark.parametrize("adapter", SQLITE_ADAPTERS)
async def test_sqlite_family_sql_parsing_error(adapter: str) -> None:
    """SQLite-family adapters map syntax failures."""

    async def assertion(driver: Any) -> None:
        with pytest.raises(SQLParsingError) as exc_info:
            await _execute(driver, "SELCT * FROM nonexistent_table")

        assert "syntax" in str(exc_info.value).lower()

    await _with_sqlite_family_driver(adapter, assertion)
