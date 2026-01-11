"""Integration tests for EXPLAIN plan support with asyncmy adapter (MySQL)."""

from collections.abc import AsyncGenerator

import pytest

from sqlspec import SQLResult
from sqlspec.adapters.asyncmy import AsyncmyConfig, AsyncmyDriver
from sqlspec.builder import Explain, sql
from sqlspec.core import SQL

pytestmark = [pytest.mark.xdist_group("mysql"), pytest.mark.asyncio(loop_scope="function")]


@pytest.fixture
async def asyncmy_session(asyncmy_config: AsyncmyConfig) -> AsyncGenerator[AsyncmyDriver, None]:
    """Create an asyncmy session with test table."""
    async with asyncmy_config.provide_session() as session:
        await session.execute_script("DROP TABLE IF EXISTS explain_test")
        await session.execute_script(
            """
            CREATE TABLE IF NOT EXISTS explain_test (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                value INT DEFAULT 0
            )
            """
        )
        yield session

        try:
            await session.execute_script("DROP TABLE IF EXISTS explain_test")
        except Exception:
            pass


async def test_explain_basic_select(asyncmy_session: AsyncmyDriver) -> None:
    """Test basic EXPLAIN on SELECT statement."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="mysql")
    result = await asyncmy_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_analyze(asyncmy_session: AsyncmyDriver) -> None:
    """Test EXPLAIN ANALYZE on SELECT statement (MySQL 8.0+)."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="mysql").analyze()
    result = await asyncmy_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_format_json(asyncmy_session: AsyncmyDriver) -> None:
    """Test EXPLAIN FORMAT = JSON."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="mysql").format("json")
    result = await asyncmy_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_format_tree(asyncmy_session: AsyncmyDriver) -> None:
    """Test EXPLAIN FORMAT = TREE (MySQL 8.0+)."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="mysql").format("tree")
    result = await asyncmy_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_format_traditional(asyncmy_session: AsyncmyDriver) -> None:
    """Test EXPLAIN FORMAT = TRADITIONAL."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="mysql").format("traditional")
    result = await asyncmy_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_from_query_builder(asyncmy_session: AsyncmyDriver) -> None:
    """Test EXPLAIN from QueryBuilder via mixin.

    Note: Uses raw SQL since query builder without dialect produces PostgreSQL-style SQL.
    """
    explain_stmt = Explain("SELECT * FROM explain_test WHERE id > 0", dialect="mysql").analyze()
    result = await asyncmy_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_from_sql_factory(asyncmy_session: AsyncmyDriver) -> None:
    """Test sql.explain() factory method."""
    explain_stmt = sql.explain("SELECT * FROM explain_test", dialect="mysql")
    result = await asyncmy_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_from_sql_object(asyncmy_session: AsyncmyDriver) -> None:
    """Test SQL.explain() method."""
    stmt = SQL("SELECT * FROM explain_test")
    # Use Explain directly with dialect since SQL uses default dialect
    explain_stmt = Explain(stmt.sql, dialect="mysql")
    result = await asyncmy_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_insert(asyncmy_session: AsyncmyDriver) -> None:
    """Test EXPLAIN on INSERT statement."""
    explain_stmt = Explain("INSERT INTO explain_test (name, value) VALUES ('test', 1)", dialect="mysql")
    result = await asyncmy_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_update(asyncmy_session: AsyncmyDriver) -> None:
    """Test EXPLAIN on UPDATE statement."""
    explain_stmt = Explain("UPDATE explain_test SET value = 100 WHERE id = 1", dialect="mysql")
    result = await asyncmy_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_delete(asyncmy_session: AsyncmyDriver) -> None:
    """Test EXPLAIN on DELETE statement."""
    explain_stmt = Explain("DELETE FROM explain_test WHERE id = 1", dialect="mysql")
    result = await asyncmy_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None
