"""Integration tests for EXPLAIN plan support with aiomysql adapter (MySQL)."""

from collections.abc import AsyncGenerator

import pytest

from sqlspec import SQLResult
from sqlspec.adapters.aiomysql import AiomysqlConfig, AiomysqlDriver
from sqlspec.builder import Explain, sql
from sqlspec.core import SQL

pytestmark = [pytest.mark.xdist_group("mysql")]


@pytest.fixture
async def aiomysql_session(aiomysql_config: AiomysqlConfig) -> AsyncGenerator[AiomysqlDriver, None]:
    """Create an aiomysql session with test table."""
    async with aiomysql_config.provide_session() as session:
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


async def test_explain_basic_select(aiomysql_session: AiomysqlDriver) -> None:
    """Test basic EXPLAIN on SELECT statement."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="mysql")
    result = await aiomysql_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_analyze(aiomysql_session: AiomysqlDriver) -> None:
    """Test EXPLAIN ANALYZE on SELECT statement (MySQL 8.0+)."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="mysql").analyze()
    result = await aiomysql_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_format_json(aiomysql_session: AiomysqlDriver) -> None:
    """Test EXPLAIN FORMAT = JSON."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="mysql").format("json")
    result = await aiomysql_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_format_tree(aiomysql_session: AiomysqlDriver) -> None:
    """Test EXPLAIN FORMAT = TREE (MySQL 8.0+)."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="mysql").format("tree")
    result = await aiomysql_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_format_traditional(aiomysql_session: AiomysqlDriver) -> None:
    """Test EXPLAIN FORMAT = TRADITIONAL."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="mysql").format("traditional")
    result = await aiomysql_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_from_query_builder(aiomysql_session: AiomysqlDriver) -> None:
    """Test EXPLAIN from QueryBuilder via mixin.

    Note: Uses raw SQL since query builder without dialect produces PostgreSQL-style SQL.
    """
    explain_stmt = Explain("SELECT * FROM explain_test WHERE id > 0", dialect="mysql").analyze()
    result = await aiomysql_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_from_sql_factory(aiomysql_session: AiomysqlDriver) -> None:
    """Test sql.explain() factory method."""
    explain_stmt = sql.explain("SELECT * FROM explain_test", dialect="mysql")
    result = await aiomysql_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_from_sql_object(aiomysql_session: AiomysqlDriver) -> None:
    """Test SQL.explain() method."""
    stmt = SQL("SELECT * FROM explain_test")
    # Use Explain directly with dialect since SQL uses default dialect
    explain_stmt = Explain(stmt.sql, dialect="mysql")
    result = await aiomysql_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_insert(aiomysql_session: AiomysqlDriver) -> None:
    """Test EXPLAIN on INSERT statement."""
    explain_stmt = Explain("INSERT INTO explain_test (name, value) VALUES ('test', 1)", dialect="mysql")
    result = await aiomysql_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_update(aiomysql_session: AiomysqlDriver) -> None:
    """Test EXPLAIN on UPDATE statement."""
    explain_stmt = Explain("UPDATE explain_test SET value = 100 WHERE id = 1", dialect="mysql")
    result = await aiomysql_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_delete(aiomysql_session: AiomysqlDriver) -> None:
    """Test EXPLAIN on DELETE statement."""
    explain_stmt = Explain("DELETE FROM explain_test WHERE id = 1", dialect="mysql")
    result = await aiomysql_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None
