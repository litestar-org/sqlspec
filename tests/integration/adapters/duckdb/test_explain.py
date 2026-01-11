"""Integration tests for EXPLAIN plan support with duckdb adapter."""

from collections.abc import Generator

import pytest

from sqlspec import SQLResult
from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBDriver
from sqlspec.builder import Explain, sql
from sqlspec.core import SQL

pytestmark = pytest.mark.xdist_group("duckdb")


@pytest.fixture
def duckdb_session(duckdb_basic_config: DuckDBConfig) -> Generator[DuckDBDriver, None, None]:
    """Create a duckdb session with test table."""
    with duckdb_basic_config.provide_session() as session:
        session.execute_script("DROP TABLE IF EXISTS explain_test")
        session.execute_script(
            """
            CREATE TABLE IF NOT EXISTS explain_test (
                id INTEGER PRIMARY KEY,
                name VARCHAR NOT NULL,
                value INTEGER DEFAULT 0
            )
            """
        )
        yield session

        try:
            session.execute_script("DROP TABLE IF EXISTS explain_test")
        except Exception:
            pass


def test_explain_basic_select(duckdb_session: DuckDBDriver) -> None:
    """Test basic EXPLAIN on SELECT statement."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="duckdb")
    result = duckdb_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_analyze(duckdb_session: DuckDBDriver) -> None:
    """Test EXPLAIN ANALYZE on SELECT statement."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="duckdb").analyze()
    result = duckdb_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_format_json(duckdb_session: DuckDBDriver) -> None:
    """Test EXPLAIN (FORMAT JSON)."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="duckdb").format("json")
    result = duckdb_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_with_where(duckdb_session: DuckDBDriver) -> None:
    """Test EXPLAIN with WHERE clause."""
    explain_stmt = Explain("SELECT * FROM explain_test WHERE id = 1", dialect="duckdb")
    result = duckdb_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_with_join(duckdb_session: DuckDBDriver) -> None:
    """Test EXPLAIN with JOIN."""
    duckdb_session.execute_script("DROP TABLE IF EXISTS explain_test2")
    duckdb_session.execute_script(
        """
        CREATE TABLE IF NOT EXISTS explain_test2 (
            id INTEGER PRIMARY KEY,
            test_id INTEGER,
            data VARCHAR
        )
        """
    )

    try:
        explain_stmt = Explain(
            "SELECT * FROM explain_test e JOIN explain_test2 e2 ON e.id = e2.test_id", dialect="duckdb"
        )
        result = duckdb_session.execute(explain_stmt.build())

        assert isinstance(result, SQLResult)
        assert result.data is not None
    finally:
        duckdb_session.execute_script("DROP TABLE IF EXISTS explain_test2")


def test_explain_from_query_builder(duckdb_session: DuckDBDriver) -> None:
    """Test EXPLAIN from QueryBuilder via mixin."""
    query = sql.select("*").from_("explain_test").where("id > :id", id=0)
    explain_stmt = query.explain()
    result = duckdb_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_from_sql_factory(duckdb_session: DuckDBDriver) -> None:
    """Test sql.explain() factory method."""
    explain_stmt = sql.explain("SELECT * FROM explain_test", dialect="duckdb")
    result = duckdb_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_from_sql_object(duckdb_session: DuckDBDriver) -> None:
    """Test SQL.explain() method."""
    stmt = SQL("SELECT * FROM explain_test")
    explain_stmt = stmt.explain()
    result = duckdb_session.execute(explain_stmt)

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_insert(duckdb_session: DuckDBDriver) -> None:
    """Test EXPLAIN on INSERT statement."""
    explain_stmt = Explain("INSERT INTO explain_test (id, name, value) VALUES (1, 'test', 1)", dialect="duckdb")
    result = duckdb_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_update(duckdb_session: DuckDBDriver) -> None:
    """Test EXPLAIN on UPDATE statement."""
    explain_stmt = Explain("UPDATE explain_test SET value = 100 WHERE id = 1", dialect="duckdb")
    result = duckdb_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_delete(duckdb_session: DuckDBDriver) -> None:
    """Test EXPLAIN on DELETE statement."""
    explain_stmt = Explain("DELETE FROM explain_test WHERE id = 1", dialect="duckdb")
    result = duckdb_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_aggregate(duckdb_session: DuckDBDriver) -> None:
    """Test EXPLAIN with aggregate functions."""
    explain_stmt = Explain("SELECT COUNT(*), SUM(value) FROM explain_test GROUP BY name", dialect="duckdb")
    result = duckdb_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None
