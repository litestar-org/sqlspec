# pyright: ignore
"""Test ADBC connection with various database backends."""

from __future__ import annotations

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.adbc import AdbcConfig

# Import the decorator
from tests.integration.test_adapters.test_adbc.conftest import xfail_if_driver_missing


@pytest.mark.xdist_group("postgres")
@xfail_if_driver_missing
def test_connection(postgres_service: PostgresService) -> None:
    """Test ADBC connection to PostgreSQL."""
    # Test direct connection
    config = AdbcConfig(
        uri=f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
        driver_name="adbc_driver_postgresql.dbapi.connect",
    )

    with config.create_connection() as conn:
        assert conn is not None
        # Test basic query
        with conn.cursor() as cur:
            cur.execute("SELECT 1")  # pyright: ignore
            result = cur.fetchone()  # pyright: ignore
            assert result == (1,)


@pytest.mark.xdist_group("adbc_duckdb")
@xfail_if_driver_missing
def test_duckdb_connection() -> None:
    """Test ADBC connection to DuckDB."""
    config = AdbcConfig(driver_name="adbc_driver_duckdb.dbapi.connect")

    with config.create_connection() as conn:
        assert conn is not None
        # Test basic query
        with conn.cursor() as cur:
            cur.execute("SELECT 1")  # pyright: ignore
            result = cur.fetchone()  # pyright: ignore
            assert result == (1,)


@pytest.mark.xdist_group("adbc_sqlite")
@xfail_if_driver_missing
def test_sqlite_connection() -> None:
    """Test ADBC connection to SQLite."""
    config = AdbcConfig(uri=":memory:", driver_name="adbc_driver_sqlite.dbapi.connect")

    with config.create_connection() as conn:
        assert conn is not None
        # Test basic query
        with conn.cursor() as cur:
            cur.execute("SELECT 1")  # pyright: ignore
            result = cur.fetchone()  # pyright: ignore
            assert result == (1,)


@pytest.mark.skipif(
    "not config.getoption('--run-bigquery-tests', default=False)",
    reason="BigQuery ADBC tests require --run-bigquery-tests flag and valid GCP credentials",
)
@pytest.mark.xdist_group("adbc_bigquery")
@xfail_if_driver_missing
def test_bigquery_connection() -> None:
    """Test ADBC connection to BigQuery (requires valid GCP setup)."""
    config = AdbcConfig(
        driver_name="adbc_driver_bigquery.dbapi.connect",
        project_id="test-project",  # Would need to be real
        dataset_id="test_dataset",  # Would need to be real
    )

    # This will likely xfail due to missing credentials
    with config.create_connection() as conn:
        assert conn is not None
        # Test basic query
        with conn.cursor() as cur:
            cur.execute("SELECT 1 as test_value")  # pyright: ignore
            result = cur.fetchone()  # pyright: ignore
            assert result == (1,)
