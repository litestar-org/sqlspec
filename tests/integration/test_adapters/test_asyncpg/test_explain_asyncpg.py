"""Integration tests for EXPLAIN plan support with asyncpg adapter."""

from collections.abc import AsyncGenerator

import pytest

from sqlspec import SQLResult
from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
from sqlspec.builder import Explain, sql
from sqlspec.core import SQL

pytestmark = [pytest.mark.xdist_group("postgres"), pytest.mark.asyncio(loop_scope="function")]


@pytest.fixture
async def asyncpg_session(asyncpg_config: AsyncpgConfig) -> AsyncGenerator[AsyncpgDriver, None]:
    """Create an asyncpg session with test table."""
    async with asyncpg_config.provide_session() as session:
        await session.execute_script("DROP TABLE IF EXISTS explain_test")
        await session.execute_script(
            """
            CREATE TABLE IF NOT EXISTS explain_test (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0
            )
            """
        )
        yield session

        try:
            await session.execute_script("DROP TABLE IF EXISTS explain_test")
        except Exception:
            pass


async def test_explain_basic_select(asyncpg_session: AsyncpgDriver) -> None:
    """Test basic EXPLAIN on SELECT statement."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="postgres")
    result = await asyncpg_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_analyze(asyncpg_session: AsyncpgDriver) -> None:
    """Test EXPLAIN ANALYZE on SELECT statement."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="postgres").analyze()
    result = await asyncpg_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_with_format_json(asyncpg_session: AsyncpgDriver) -> None:
    """Test EXPLAIN with JSON format."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="postgres").format("json")
    result = await asyncpg_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_analyze_with_buffers(asyncpg_session: AsyncpgDriver) -> None:
    """Test EXPLAIN ANALYZE with BUFFERS option."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="postgres").analyze().buffers()
    result = await asyncpg_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_verbose(asyncpg_session: AsyncpgDriver) -> None:
    """Test EXPLAIN VERBOSE."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="postgres").verbose()
    result = await asyncpg_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_full_options(asyncpg_session: AsyncpgDriver) -> None:
    """Test EXPLAIN with multiple options."""
    explain_stmt = (
        Explain("SELECT * FROM explain_test", dialect="postgres").analyze().verbose().buffers().timing().format("json")
    )
    result = await asyncpg_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_from_query_builder(asyncpg_session: AsyncpgDriver) -> None:
    """Test EXPLAIN from QueryBuilder via mixin."""
    query = sql.select("*").from_("explain_test").where("id > :id", id=0)
    explain_stmt = query.explain(analyze=True)
    result = await asyncpg_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_from_sql_factory(asyncpg_session: AsyncpgDriver) -> None:
    """Test sql.explain() factory method."""
    explain_stmt = sql.explain("SELECT * FROM explain_test", analyze=True, dialect="postgres")
    result = await asyncpg_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_from_sql_object(asyncpg_session: AsyncpgDriver) -> None:
    """Test SQL.explain() method."""
    stmt = SQL("SELECT * FROM explain_test")
    explain_stmt = stmt.explain(analyze=True)
    result = await asyncpg_session.execute(explain_stmt)

    assert isinstance(result, SQLResult)
    assert result.data is not None
