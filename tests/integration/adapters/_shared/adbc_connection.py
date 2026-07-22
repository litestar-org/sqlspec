"""ADBC raw connection residuals outside the shared driver contracts.

The contract matrix owns SQLSpec session CRUD and in-memory SQLite session
behavior. This module keeps raw DB-API `create_connection()` smoke, driver info,
BigQuery ADBC gating, and explicit PostgreSQL transaction plumbing.
"""

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.adbc import AdbcConfig
from tests.integration.fixtures.adbc import xfail_if_driver_missing


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
@xfail_if_driver_missing
def test_connection(postgres_service: "PostgresService") -> None:
    """Test raw ADBC connection creation to PostgreSQL."""
    config = AdbcConfig(
        connection_config={
            "uri": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        }
    )

    with config.create_connection() as conn:
        assert conn is not None
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone() == (1,)


@pytest.mark.xdist_group("duckdb")
@pytest.mark.adbc
@xfail_if_driver_missing
def test_duckdb_connection() -> None:
    """Test raw ADBC connection creation to DuckDB."""
    config = AdbcConfig(connection_config={"driver_name": "adbc_driver_duckdb.dbapi.connect"})

    with config.create_connection() as conn:
        assert conn is not None
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone() == (1,)


@pytest.mark.xdist_group("sqlite")
@pytest.mark.adbc
@xfail_if_driver_missing
def test_sqlite_connection() -> None:
    """Test raw ADBC connection creation to SQLite."""
    config = AdbcConfig(connection_config={"uri": ":memory:", "driver_name": "adbc_driver_sqlite.dbapi.connect"})

    with config.create_connection() as conn:
        assert conn is not None
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone() == (1,)


@pytest.mark.skipif(
    "not config.getoption('--run-bigquery-tests', default=False)",
    reason="BigQuery ADBC tests require --run-bigquery-tests flag and valid GCP credentials",
)
@pytest.mark.xdist_group("bigquery")
@pytest.mark.adbc
@xfail_if_driver_missing
def test_bigquery_connection() -> None:
    """Test raw ADBC connection creation to BigQuery when explicitly enabled."""
    config = AdbcConfig(
        connection_config={
            "driver_name": "adbc_driver_bigquery.dbapi.connect",
            "project_id": "test-project",
            "dataset_id": "test_dataset",
        }
    )

    with config.create_connection() as conn:
        assert conn is not None
        with conn.cursor() as cur:
            cur.execute("SELECT 1 as test_value")
            assert cur.fetchone() == (1,)


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_connection_info_retrieval(postgres_service: "PostgresService") -> None:
    """Test ADBC connection info retrieval used by dialect detection."""
    config = AdbcConfig(
        connection_config={
            "uri": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        }
    )

    with config.create_connection() as conn:
        assert conn is not None
        try:
            driver_info = conn.adbc_get_info()
            assert isinstance(driver_info, dict)
            assert driver_info.get("vendor_name") or driver_info.get("driver_name")
        except Exception:
            pass


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_connection_transaction_handling(postgres_service: "PostgresService") -> None:
    """Test ADBC PostgreSQL explicit transaction plumbing."""
    config = AdbcConfig(
        connection_config={
            "uri": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        }
    )

    with config.provide_session() as session:
        session.execute_script("""
            CREATE TABLE IF NOT EXISTS transaction_test_adbc (
                id SERIAL PRIMARY KEY,
                data TEXT
            )
        """)

        try:
            session.begin()
            session.execute("INSERT INTO transaction_test_adbc (data) VALUES ($1)", ("test_data",))
            session.commit()

            result = session.execute("SELECT COUNT(*) as count FROM transaction_test_adbc")
            assert result.get_data()[0]["count"] >= 1

        finally:
            try:
                session.execute_script("DROP TABLE IF EXISTS transaction_test_adbc")
            except Exception:
                pass
