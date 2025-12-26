"""Integration tests for EXPLAIN plan support with aiosqlite adapter."""

from collections.abc import AsyncGenerator

import pytest

from sqlspec import SQLResult
from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.builder import Explain, sql
from sqlspec.core import SQL

pytestmark = [pytest.mark.xdist_group("sqlite"), pytest.mark.anyio]


@pytest.fixture
async def aiosqlite_explain_session(aiosqlite_config: AiosqliteConfig) -> AsyncGenerator[AiosqliteDriver, None]:
    """Create an aiosqlite session with test table."""
    async with aiosqlite_config.provide_session() as session:
        await session.execute_script("DROP TABLE IF EXISTS explain_test")
        await session.execute_script(
            """
            CREATE TABLE IF NOT EXISTS explain_test (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0
            )
            """
        )
        await session.commit()
        yield session

        try:
            await session.execute_script("DROP TABLE IF EXISTS explain_test")
        except Exception:
            pass


async def test_explain_query_plan_select(aiosqlite_explain_session: AiosqliteDriver) -> None:
    """Test EXPLAIN QUERY PLAN on SELECT statement."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="sqlite")
    result = await aiosqlite_explain_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_query_plan_with_where(aiosqlite_explain_session: AiosqliteDriver) -> None:
    """Test EXPLAIN QUERY PLAN with WHERE clause."""
    explain_stmt = Explain("SELECT * FROM explain_test WHERE id = 1", dialect="sqlite")
    result = await aiosqlite_explain_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_from_query_builder(aiosqlite_explain_session: AiosqliteDriver) -> None:
    """Test EXPLAIN from QueryBuilder via mixin."""
    query = sql.select("*").from_("explain_test").where("id > :id", id=0)
    explain_stmt = query.explain()
    result = await aiosqlite_explain_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_from_sql_factory(aiosqlite_explain_session: AiosqliteDriver) -> None:
    """Test sql.explain() factory method."""
    explain_stmt = sql.explain("SELECT * FROM explain_test", dialect="sqlite")
    result = await aiosqlite_explain_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_from_sql_object(aiosqlite_explain_session: AiosqliteDriver) -> None:
    """Test SQL.explain() method."""
    stmt = SQL("SELECT * FROM explain_test")
    explain_stmt = stmt.explain()
    result = await aiosqlite_explain_session.execute(explain_stmt)

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_insert(aiosqlite_explain_session: AiosqliteDriver) -> None:
    """Test EXPLAIN QUERY PLAN on INSERT statement."""
    explain_stmt = Explain("INSERT INTO explain_test (name, value) VALUES ('test', 1)", dialect="sqlite")
    result = await aiosqlite_explain_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_update(aiosqlite_explain_session: AiosqliteDriver) -> None:
    """Test EXPLAIN QUERY PLAN on UPDATE statement."""
    explain_stmt = Explain("UPDATE explain_test SET value = 100 WHERE id = 1", dialect="sqlite")
    result = await aiosqlite_explain_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_delete(aiosqlite_explain_session: AiosqliteDriver) -> None:
    """Test EXPLAIN QUERY PLAN on DELETE statement."""
    explain_stmt = Explain("DELETE FROM explain_test WHERE id = 1", dialect="sqlite")
    result = await aiosqlite_explain_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None
