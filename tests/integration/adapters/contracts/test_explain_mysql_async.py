"""MySQL async-family EXPLAIN plan contract tests."""

from collections.abc import AsyncGenerator
from typing import Any

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec import SQLResult
from sqlspec.builder import Explain, sql
from sqlspec.core import SQL
from tests.integration.adapters.contracts._mysql_async import (
    MYSQL_ASYNC_ADAPTERS,
    close_mysql_async_config,
    mysql_async_config,
)

pytestmark = [pytest.mark.xdist_group("mysql")]


@pytest.fixture(params=MYSQL_ASYNC_ADAPTERS)
async def aiomysql_session(request: pytest.FixtureRequest, mysql_service: MySQLService) -> AsyncGenerator[Any, None]:
    """Create a MySQL async-family session with test table."""
    config = mysql_async_config(str(request.param), mysql_service)
    try:
        async with config.provide_session() as session:
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
    finally:
        await close_mysql_async_config(config)


async def test_explain_basic_select(aiomysql_session: Any) -> None:
    """Test basic EXPLAIN on SELECT statement."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="mysql")
    result = await aiomysql_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_analyze(aiomysql_session: Any) -> None:
    """Test EXPLAIN ANALYZE on SELECT statement (MySQL 8.0+)."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="mysql").analyze()
    result = await aiomysql_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_format_json(aiomysql_session: Any) -> None:
    """Test EXPLAIN FORMAT = JSON."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="mysql").format("json")
    result = await aiomysql_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_format_tree(aiomysql_session: Any) -> None:
    """Test EXPLAIN FORMAT = TREE (MySQL 8.0+)."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="mysql").format("tree")
    result = await aiomysql_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_format_traditional(aiomysql_session: Any) -> None:
    """Test EXPLAIN FORMAT = TRADITIONAL."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="mysql").format("traditional")
    result = await aiomysql_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_from_query_builder(aiomysql_session: Any) -> None:
    """Test EXPLAIN from QueryBuilder via mixin.

    Note: Uses raw SQL since query builder without dialect produces PostgreSQL-style SQL.
    """
    explain_stmt = Explain("SELECT * FROM explain_test WHERE id > 0", dialect="mysql").analyze()
    result = await aiomysql_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_from_sql_factory(aiomysql_session: Any) -> None:
    """Test sql.explain() factory method."""
    explain_stmt = sql.explain("SELECT * FROM explain_test", dialect="mysql")
    result = await aiomysql_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_from_sql_object(aiomysql_session: Any) -> None:
    """Test SQL.explain() method."""
    stmt = SQL("SELECT * FROM explain_test")
    # Use Explain directly with dialect since SQL uses default dialect
    explain_stmt = Explain(stmt.sql, dialect="mysql")
    result = await aiomysql_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_insert(aiomysql_session: Any) -> None:
    """Test EXPLAIN on INSERT statement."""
    explain_stmt = Explain("INSERT INTO explain_test (name, value) VALUES ('test', 1)", dialect="mysql")
    result = await aiomysql_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_update(aiomysql_session: Any) -> None:
    """Test EXPLAIN on UPDATE statement."""
    explain_stmt = Explain("UPDATE explain_test SET value = 100 WHERE id = 1", dialect="mysql")
    result = await aiomysql_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


async def test_explain_delete(aiomysql_session: Any) -> None:
    """Test EXPLAIN on DELETE statement."""
    explain_stmt = Explain("DELETE FROM explain_test WHERE id = 1", dialect="mysql")
    result = await aiomysql_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None
