"""Integration tests for EXPLAIN plan support with sqlite adapter."""

from collections.abc import Generator

import pytest

from sqlspec import SQLResult
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.builder import Explain, sql
from sqlspec.core import SQL

pytestmark = pytest.mark.xdist_group("sqlite")


@pytest.fixture
def sqlite_explain_session() -> Generator[SqliteDriver, None, None]:
    """Create a sqlite session with test table."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    with config.provide_session() as session:
        session.execute_script("DROP TABLE IF EXISTS explain_test")
        session.execute_script(
            """
            CREATE TABLE IF NOT EXISTS explain_test (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0
            )
            """
        )
        session.commit()
        yield session

        try:
            session.execute_script("DROP TABLE IF EXISTS explain_test")
        except Exception:
            pass


def test_explain_query_plan_select(sqlite_explain_session: SqliteDriver) -> None:
    """Test EXPLAIN QUERY PLAN on SELECT statement."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="sqlite")
    result = sqlite_explain_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_query_plan_with_where(sqlite_explain_session: SqliteDriver) -> None:
    """Test EXPLAIN QUERY PLAN with WHERE clause."""
    explain_stmt = Explain("SELECT * FROM explain_test WHERE id = 1", dialect="sqlite")
    result = sqlite_explain_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_query_plan_with_join(sqlite_explain_session: SqliteDriver) -> None:
    """Test EXPLAIN QUERY PLAN with JOIN."""
    sqlite_explain_session.execute_script("DROP TABLE IF EXISTS explain_test2")
    sqlite_explain_session.execute_script(
        """
        CREATE TABLE IF NOT EXISTS explain_test2 (
            id INTEGER PRIMARY KEY,
            test_id INTEGER,
            data TEXT
        )
        """
    )
    sqlite_explain_session.commit()

    try:
        explain_stmt = Explain(
            "SELECT * FROM explain_test e JOIN explain_test2 e2 ON e.id = e2.test_id", dialect="sqlite"
        )
        result = sqlite_explain_session.execute(explain_stmt.build())

        assert isinstance(result, SQLResult)
        assert result.data is not None
    finally:
        sqlite_explain_session.execute_script("DROP TABLE IF EXISTS explain_test2")


def test_explain_from_query_builder(sqlite_explain_session: SqliteDriver) -> None:
    """Test EXPLAIN from QueryBuilder via mixin."""
    query = sql.select("*").from_("explain_test").where("id > :id", id=0)
    explain_stmt = query.explain()
    result = sqlite_explain_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_from_sql_factory(sqlite_explain_session: SqliteDriver) -> None:
    """Test sql.explain() factory method."""
    explain_stmt = sql.explain("SELECT * FROM explain_test", dialect="sqlite")
    result = sqlite_explain_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_from_sql_object(sqlite_explain_session: SqliteDriver) -> None:
    """Test SQL.explain() method."""
    stmt = SQL("SELECT * FROM explain_test")
    explain_stmt = stmt.explain()
    result = sqlite_explain_session.execute(explain_stmt)

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_insert(sqlite_explain_session: SqliteDriver) -> None:
    """Test EXPLAIN QUERY PLAN on INSERT statement."""
    explain_stmt = Explain("INSERT INTO explain_test (name, value) VALUES ('test', 1)", dialect="sqlite")
    result = sqlite_explain_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_update(sqlite_explain_session: SqliteDriver) -> None:
    """Test EXPLAIN QUERY PLAN on UPDATE statement."""
    explain_stmt = Explain("UPDATE explain_test SET value = 100 WHERE id = 1", dialect="sqlite")
    result = sqlite_explain_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_delete(sqlite_explain_session: SqliteDriver) -> None:
    """Test EXPLAIN QUERY PLAN on DELETE statement."""
    explain_stmt = Explain("DELETE FROM explain_test WHERE id = 1", dialect="sqlite")
    result = sqlite_explain_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_subquery(sqlite_explain_session: SqliteDriver) -> None:
    """Test EXPLAIN QUERY PLAN with subquery."""
    explain_stmt = Explain(
        "SELECT * FROM explain_test WHERE id IN (SELECT id FROM explain_test WHERE value > 0)", dialect="sqlite"
    )
    result = sqlite_explain_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None
