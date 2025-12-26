"""Integration tests for EXPLAIN plan support with psycopg adapter."""

from collections.abc import Generator

import pytest

from sqlspec import SQLResult
from sqlspec.adapters.psycopg import PsycopgSyncConfig, PsycopgSyncDriver
from sqlspec.builder import Explain, sql
from sqlspec.core import SQL, ExplainOptions

pytestmark = pytest.mark.xdist_group("postgres")


@pytest.fixture
def psycopg_session(psycopg_sync_config: PsycopgSyncConfig) -> Generator[PsycopgSyncDriver, None, None]:
    """Create a psycopg session with test table."""
    with psycopg_sync_config.provide_session() as session:
        session.execute_script("DROP TABLE IF EXISTS explain_test")
        session.execute_script(
            """
            CREATE TABLE IF NOT EXISTS explain_test (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0
            )
            """
        )
        session.commit()
        session.begin()
        yield session

        try:
            session.rollback()
        except Exception:
            pass

        try:
            session.execute_script("DROP TABLE IF EXISTS explain_test")
        except Exception:
            pass


def test_explain_basic_select(psycopg_session: PsycopgSyncDriver) -> None:
    """Test basic EXPLAIN on SELECT statement."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="postgres")
    result = psycopg_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_analyze(psycopg_session: PsycopgSyncDriver) -> None:
    """Test EXPLAIN ANALYZE on SELECT statement."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="postgres").analyze()
    result = psycopg_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_with_format_json(psycopg_session: PsycopgSyncDriver) -> None:
    """Test EXPLAIN with JSON format."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="postgres").format("json")
    result = psycopg_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_analyze_with_buffers(psycopg_session: PsycopgSyncDriver) -> None:
    """Test EXPLAIN ANALYZE with BUFFERS option."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="postgres").analyze().buffers()
    result = psycopg_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_analyze_with_timing(psycopg_session: PsycopgSyncDriver) -> None:
    """Test EXPLAIN ANALYZE with TIMING option."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="postgres").analyze().timing()
    result = psycopg_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_verbose(psycopg_session: PsycopgSyncDriver) -> None:
    """Test EXPLAIN VERBOSE."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="postgres").verbose()
    result = psycopg_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_full_options(psycopg_session: PsycopgSyncDriver) -> None:
    """Test EXPLAIN with multiple options."""
    explain_stmt = (
        Explain("SELECT * FROM explain_test", dialect="postgres").analyze().verbose().buffers().timing().format("json")
    )
    result = psycopg_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_from_query_builder(psycopg_session: PsycopgSyncDriver) -> None:
    """Test EXPLAIN from QueryBuilder via mixin."""
    query = sql.select("*").from_("explain_test").where("id > :id", id=0)
    explain_stmt = query.explain(analyze=True)
    result = psycopg_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_from_sql_factory(psycopg_session: PsycopgSyncDriver) -> None:
    """Test sql.explain() factory method."""
    explain_stmt = sql.explain("SELECT * FROM explain_test", analyze=True, dialect="postgres")
    result = psycopg_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_from_sql_object(psycopg_session: PsycopgSyncDriver) -> None:
    """Test SQL.explain() method."""
    stmt = SQL("SELECT * FROM explain_test")
    explain_stmt = stmt.explain(analyze=True)
    result = psycopg_session.execute(explain_stmt)

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_insert(psycopg_session: PsycopgSyncDriver) -> None:
    """Test EXPLAIN on INSERT statement."""
    explain_stmt = Explain("INSERT INTO explain_test (name, value) VALUES ('test', 1)", dialect="postgres").analyze()
    result = psycopg_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_update(psycopg_session: PsycopgSyncDriver) -> None:
    """Test EXPLAIN on UPDATE statement."""
    explain_stmt = Explain("UPDATE explain_test SET value = 100 WHERE id = 1", dialect="postgres").analyze()
    result = psycopg_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_delete(psycopg_session: PsycopgSyncDriver) -> None:
    """Test EXPLAIN on DELETE statement."""
    explain_stmt = Explain("DELETE FROM explain_test WHERE id = 1", dialect="postgres").analyze()
    result = psycopg_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_with_costs_disabled(psycopg_session: PsycopgSyncDriver) -> None:
    """Test EXPLAIN with COSTS FALSE."""
    options = ExplainOptions(costs=False)
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="postgres", options=options)
    result = psycopg_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_with_summary(psycopg_session: PsycopgSyncDriver) -> None:
    """Test EXPLAIN ANALYZE with SUMMARY option."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="postgres").analyze().summary()
    result = psycopg_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None
