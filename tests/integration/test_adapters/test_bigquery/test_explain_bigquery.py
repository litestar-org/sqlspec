"""Integration tests for EXPLAIN plan support with bigquery adapter.

Note: BigQuery emulator does not support EXPLAIN statements.
These tests are skipped in CI but can be run against real BigQuery.
"""

from collections.abc import Generator

import pytest

from sqlspec import SQLResult
from sqlspec.adapters.bigquery import BigQueryConfig, BigQueryDriver
from sqlspec.builder import Explain, sql
from sqlspec.core import SQL

pytestmark = [pytest.mark.xdist_group("bigquery"), pytest.mark.skip(reason="BigQuery emulator EXPLAIN unsupported")]


@pytest.fixture
def bigquery_explain_session(
    bigquery_config: BigQueryConfig, table_schema_prefix: str
) -> Generator[BigQueryDriver, None, None]:
    """Create a bigquery session with test table."""
    with bigquery_config.provide_session() as session:
        try:
            session.execute_script(f"DROP TABLE IF EXISTS {table_schema_prefix}.explain_test")
        except Exception:
            pass

        session.execute_script(
            f"""
            CREATE TABLE IF NOT EXISTS {table_schema_prefix}.explain_test (
                id INT64,
                name STRING NOT NULL,
                value INT64
            )
            """
        )
        yield session

        try:
            session.execute_script(f"DROP TABLE IF EXISTS {table_schema_prefix}.explain_test")
        except Exception:
            pass


def test_explain_basic_select(bigquery_explain_session: BigQueryDriver, table_schema_prefix: str) -> None:
    """Test basic EXPLAIN on SELECT statement."""
    explain_stmt = Explain(f"SELECT * FROM {table_schema_prefix}.explain_test", dialect="bigquery")
    result = bigquery_explain_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_with_where(bigquery_explain_session: BigQueryDriver, table_schema_prefix: str) -> None:
    """Test EXPLAIN with WHERE clause."""
    explain_stmt = Explain(f"SELECT * FROM {table_schema_prefix}.explain_test WHERE id = 1", dialect="bigquery")
    result = bigquery_explain_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_from_query_builder(bigquery_explain_session: BigQueryDriver, table_schema_prefix: str) -> None:
    """Test EXPLAIN from QueryBuilder via mixin."""
    query = sql.select("*").from_(f"{table_schema_prefix}.explain_test").where("id > :id", id=0)
    explain_stmt = query.explain()
    result = bigquery_explain_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_from_sql_factory(bigquery_explain_session: BigQueryDriver, table_schema_prefix: str) -> None:
    """Test sql.explain() factory method."""
    explain_stmt = sql.explain(f"SELECT * FROM {table_schema_prefix}.explain_test", dialect="bigquery")
    result = bigquery_explain_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_from_sql_object(bigquery_explain_session: BigQueryDriver, table_schema_prefix: str) -> None:
    """Test SQL.explain() method."""
    stmt = SQL(f"SELECT * FROM {table_schema_prefix}.explain_test")
    explain_stmt = stmt.explain()
    result = bigquery_explain_session.execute(explain_stmt)

    assert isinstance(result, SQLResult)
    assert result.data is not None


def test_explain_aggregate(bigquery_explain_session: BigQueryDriver, table_schema_prefix: str) -> None:
    """Test EXPLAIN with aggregate functions."""
    explain_stmt = Explain(
        f"SELECT COUNT(*), SUM(value) FROM {table_schema_prefix}.explain_test GROUP BY name", dialect="bigquery"
    )
    result = bigquery_explain_session.execute(explain_stmt.build())

    assert isinstance(result, SQLResult)
    assert result.data is not None
