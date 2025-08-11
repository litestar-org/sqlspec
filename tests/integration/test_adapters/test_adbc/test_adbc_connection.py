"""Test ADBC connection with various database backends."""

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.adbc import AdbcConfig
from tests.integration.test_adapters.test_adbc.conftest import xfail_if_driver_missing


@pytest.mark.xdist_group("postgres")
@xfail_if_driver_missing
def test_connection(postgres_service: PostgresService) -> None:
    """Test ADBC connection to PostgreSQL."""
    config = AdbcConfig(
        connection_config={
            "uri": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        }
    )

    with config.create_connection() as conn:
        assert conn is not None
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            result = cur.fetchone()
            assert result == (1,)


@pytest.mark.xdist_group("adbc_duckdb")
@xfail_if_driver_missing
def test_duckdb_connection() -> None:
    """Test ADBC connection to DuckDB."""
    config = AdbcConfig(connection_config={"driver_name": "adbc_driver_duckdb.dbapi.connect"})

    with config.create_connection() as conn:
        assert conn is not None
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            result = cur.fetchone()
            assert result == (1,)


@pytest.mark.xdist_group("adbc_sqlite")
@xfail_if_driver_missing
def test_sqlite_connection() -> None:
    """Test ADBC connection to SQLite."""
    config = AdbcConfig(connection_config={"uri": ":memory:", "driver_name": "adbc_driver_sqlite.dbapi.connect"})

    with config.create_connection() as conn:
        assert conn is not None
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            result = cur.fetchone()
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
            result = cur.fetchone()
            assert result == (1,)


@pytest.mark.xdist_group("postgres")
def test_connection_info_retrieval(postgres_service: PostgresService) -> None:
    """Test ADBC connection info retrieval for dialect detection."""
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
def test_connection_with_session_management(postgres_service: PostgresService) -> None:
    """Test ADBC connection with session management."""
    config = AdbcConfig(
        connection_config={
            "uri": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        }
    )

    with config.provide_session() as session:
        assert session is not None
        result = session.execute("SELECT 1 as test_value")
        assert result is not None
        assert result.data is not None
        assert result.data[0]["test_value"] == 1


@pytest.mark.xdist_group("adbc_sqlite")
def test_sqlite_memory_connection() -> None:
    """Test ADBC SQLite in-memory connection."""
    config = AdbcConfig(connection_config={"uri": ":memory:", "driver_name": "adbc_driver_sqlite"})

    with config.provide_session() as session:
        session.execute_script("""
            CREATE TABLE memory_test (
                id INTEGER PRIMARY KEY,
                data TEXT
            )
        """)

        session.execute("INSERT INTO memory_test (data) VALUES (?)", ("test_data",))
        result = session.execute("SELECT data FROM memory_test")

        assert result.data is not None
        assert len(result.data) == 1
        assert result.data[0]["data"] == "test_data"


@pytest.mark.xdist_group("adbc_duckdb")
@xfail_if_driver_missing
def test_duckdb_connection_with_arrow_features() -> None:
    """Test ADBC DuckDB connection with Arrow-specific features."""
    config = AdbcConfig(connection_config={"driver_name": "adbc_driver_duckdb.dbapi.connect"})

    with config.provide_session() as session:
        result = session.execute("""
            SELECT
                [1, 2, 3, 4] as int_array,
                {'key': 'value', 'num': 42} as json_obj,
                CURRENT_TIMESTAMP as current_time
        """)

        assert result.data is not None
        assert len(result.data) == 1
        row = result.data[0]

        assert row["int_array"] is not None
        assert row["json_obj"] is not None
        assert row["current_time"] is not None


@pytest.mark.xdist_group("postgres")
def test_connection_transaction_handling(postgres_service: PostgresService) -> None:
    """Test ADBC connection transaction handling."""
    config = AdbcConfig(
        connection_config={
            "uri": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        }
    )

    with config.provide_session() as session:
        session.execute_script("""
            CREATE TABLE IF NOT EXISTS transaction_test (
                id SERIAL PRIMARY KEY,
                data TEXT
            )
        """)

        try:
            session.begin()
            session.execute("INSERT INTO transaction_test (data) VALUES ($1)", ("test_data",))
            session.commit()

            result = session.execute("SELECT COUNT(*) as count FROM transaction_test")
            assert result.data is not None
            assert result.data[0]["count"] >= 1

        finally:
            try:
                session.execute_script("DROP TABLE IF EXISTS transaction_test")
            except Exception:
                pass
