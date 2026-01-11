"""Integration tests for EXPLAIN plan support with spanner adapter.

Note: Spanner uses query_mode=PLAN for execution plans, not SQL EXPLAIN syntax.
The emulator also has limited DDL support. These tests are skipped in CI.
"""

from collections.abc import Generator

import pytest

from sqlspec import SQLResult
from sqlspec.adapters.spanner import SpannerSyncConfig, SpannerSyncDriver
from sqlspec.builder import Explain, sql
from sqlspec.core import SQL

pytestmark = [pytest.mark.xdist_group("spanner"), pytest.mark.skip(reason="Spanner uses query_mode=PLAN")]


@pytest.fixture
def spanner_explain_session(spanner_config: SpannerSyncConfig) -> Generator[SpannerSyncDriver, None, None]:
    """Create a spanner session with test table."""
    with spanner_config.provide_session() as session:
        try:
            session.execute_script("DROP TABLE explain_test")
        except Exception:
            pass

        session.execute_script(
            """
            CREATE TABLE explain_test (
                id INT64 NOT NULL,
                name STRING(255) NOT NULL,
                value INT64
            ) PRIMARY KEY (id)
            """
        )
        yield session

        try:
            session.execute_script("DROP TABLE explain_test")
        except Exception:
            pass


def test_explain_basic_select(spanner_explain_session: SpannerSyncDriver) -> None:
    """Test basic EXPLAIN on SELECT statement.

    Spanner uses EXPLAIN syntax similar to generic SQL.
    """
    explain_stmt = Explain("SELECT * FROM explain_test", dialect="spanner")
    result = spanner_explain_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_with_where(spanner_explain_session: SpannerSyncDriver) -> None:
    """Test EXPLAIN with WHERE clause."""
    explain_stmt = Explain("SELECT * FROM explain_test WHERE id = 1", dialect="spanner")
    result = spanner_explain_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_from_query_builder(spanner_explain_session: SpannerSyncDriver) -> None:
    """Test EXPLAIN from QueryBuilder via mixin."""
    query = sql.select("*").from_("explain_test").where("id > :id", id=0)
    explain_stmt = query.explain()
    result = spanner_explain_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_from_sql_factory(spanner_explain_session: SpannerSyncDriver) -> None:
    """Test sql.explain() factory method."""
    explain_stmt = sql.explain("SELECT * FROM explain_test", dialect="spanner")
    result = spanner_explain_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_from_sql_object(spanner_explain_session: SpannerSyncDriver) -> None:
    """Test SQL.explain() method."""
    stmt = SQL("SELECT * FROM explain_test")
    explain_stmt = stmt.explain()
    result = spanner_explain_session.execute(explain_stmt)

    assert isinstance(result, SQLResult)
    assert result.data is not None
