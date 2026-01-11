"""Integration tests for EXPLAIN plan support with adbc adapter.

Note: ADBC uses COPY protocol which wraps queries in COPY (query) TO STDOUT,
making EXPLAIN statements incompatible. These tests are skipped.
"""

from collections.abc import Generator

import pytest

from sqlspec import SQLResult
from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.builder import Explain, sql
from sqlspec.core import SQL

pytestmark = [pytest.mark.xdist_group("postgres"), pytest.mark.skip(reason="ADBC COPY incompatible with EXPLAIN")]


@pytest.fixture
def adbc_session(adbc_postgres_config: AdbcConfig) -> Generator[AdbcDriver, None, None]:
    """Create an adbc session with test table.

    ADBC typically connects to PostgreSQL for these tests.
    """
    with adbc_postgres_config.provide_session() as session:
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
        yield session

        try:
            session.execute_script("DROP TABLE IF EXISTS explain_test")
        except Exception:
            pass


def test_explain_basic_select(adbc_session: AdbcDriver) -> None:
    """Test basic EXPLAIN on SELECT statement."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="postgres")
    result = adbc_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) > 0


def test_explain_analyze(adbc_session: AdbcDriver) -> None:
    """Test EXPLAIN ANALYZE on SELECT statement."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="postgres").analyze()
    result = adbc_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_with_format_json(adbc_session: AdbcDriver) -> None:
    """Test EXPLAIN with JSON format."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="postgres").format("json")
    result = adbc_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_verbose(adbc_session: AdbcDriver) -> None:
    """Test EXPLAIN VERBOSE."""
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="postgres").verbose()
    result = adbc_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_full_options(adbc_session: AdbcDriver) -> None:
    """Test EXPLAIN with multiple options."""
    explain_stmt = (
        Explain("SELECT * FROM explain_test", dialect="postgres").analyze().verbose().buffers().timing().format("json")
    )
    result = adbc_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_from_query_builder(adbc_session: AdbcDriver) -> None:
    """Test EXPLAIN from QueryBuilder via mixin."""
    query = sql.select("*").from_("explain_test").where("id > :id", id=0)
    explain_stmt = query.explain(analyze=True)
    result = adbc_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_from_sql_factory(adbc_session: AdbcDriver) -> None:
    """Test sql.explain() factory method."""
    explain_stmt = sql.explain("SELECT * FROM explain_test", analyze=True, dialect="postgres")
    result = adbc_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_from_sql_object(adbc_session: AdbcDriver) -> None:
    """Test SQL.explain() method."""
    stmt = SQL("SELECT * FROM explain_test")
    explain_stmt = stmt.explain(analyze=True)
    result = adbc_session.execute(explain_stmt)

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_insert(adbc_session: AdbcDriver) -> None:
    """Test EXPLAIN on INSERT statement."""
    explain_stmt = Explain("INSERT INTO explain_test (name, value) VALUES ('test', 1)", dialect="postgres").analyze()
    result = adbc_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_update(adbc_session: AdbcDriver) -> None:
    """Test EXPLAIN on UPDATE statement."""
    explain_stmt = Explain("UPDATE explain_test SET value = 100 WHERE id = 1", dialect="postgres").analyze()
    result = adbc_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_delete(adbc_session: AdbcDriver) -> None:
    """Test EXPLAIN on DELETE statement."""
    explain_stmt = Explain("DELETE FROM explain_test WHERE id = 1", dialect="postgres").analyze()
    result = adbc_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None
