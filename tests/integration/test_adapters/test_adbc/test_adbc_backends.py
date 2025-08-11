"""Test ADBC multi-backend support and backend-specific features."""

import math
from collections.abc import Generator

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.core.result import SQLResult
from tests.integration.test_adapters.test_adbc.conftest import xfail_if_driver_missing


@pytest.fixture
def postgresql_session(postgres_service: PostgresService) -> Generator[AdbcDriver, None, None]:
    """PostgreSQL ADBC session fixture."""
    config = AdbcConfig(
        connection_config={
            "uri": f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
            "driver_name": "adbc_driver_postgresql",
        }
    )

    with config.provide_session() as session:
        yield session


@pytest.fixture
def sqlite_session() -> Generator[AdbcDriver, None, None]:
    """SQLite ADBC session fixture."""
    config = AdbcConfig(connection_config={"uri": ":memory:", "driver_name": "adbc_driver_sqlite"})

    with config.provide_session() as session:
        yield session


@pytest.fixture
def duckdb_session() -> Generator[AdbcDriver, None, None]:
    """DuckDB ADBC session fixture."""
    config = AdbcConfig(connection_config={"driver_name": "adbc_driver_duckdb.dbapi.connect"})

    with config.provide_session() as session:
        yield session


@pytest.mark.xdist_group("postgres")
def test_postgresql_specific_features(postgresql_session: AdbcDriver) -> None:
    """Test PostgreSQL-specific features with ADBC."""
    postgresql_session.execute_script("""
        CREATE TABLE IF NOT EXISTS pg_test (
            id SERIAL PRIMARY KEY,
            jsonb_col JSONB,
            array_col INTEGER[],
            uuid_col UUID DEFAULT gen_random_uuid(),
            tsvector_col TSVECTOR,
            inet_col INET
        )
    """)

    postgresql_session.execute(
        """
        INSERT INTO pg_test (jsonb_col, array_col, inet_col, tsvector_col)
        VALUES ($1::jsonb, $2, $3::inet, to_tsvector($4))
    """,
        (
            {"name": "John", "age": 30, "tags": ["developer", "python"]},
            [1, 2, 3, 4, 5],
            "192.168.1.1",
            "PostgreSQL full text search",
        ),
    )

    result = postgresql_session.execute("SELECT * FROM pg_test")
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1

    row = result.data[0]
    assert row["jsonb_col"] is not None
    assert row["array_col"] == [1, 2, 3, 4, 5]
    assert row["uuid_col"] is not None
    assert row["tsvector_col"] is not None
    assert row["inet_col"] is not None

    json_query = postgresql_session.execute("""
        SELECT
            jsonb_col ->> 'name' as name,
            jsonb_col ->> 'age' as age,
            array_length(array_col, 1) as array_len
        FROM pg_test
    """)

    assert json_query.data is not None
    assert json_query.data[0]["name"] == "John"
    assert json_query.data[0]["age"] == "30"
    assert json_query.data[0]["array_len"] == 5

    postgresql_session.execute_script("DROP TABLE IF EXISTS pg_test")


@pytest.mark.xdist_group("adbc_sqlite")
def test_sqlite_specific_features(sqlite_session: AdbcDriver) -> None:
    """Test SQLite-specific features with ADBC."""
    sqlite_session.execute_script("""
        CREATE TABLE test_sqlite (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            data BLOB,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            value REAL
        )
    """)

    test_blob = b"SQLite binary data test"
    sqlite_session.execute_many(
        """
        INSERT INTO test_sqlite (name, data, value) VALUES (?, ?, ?)
    """,
        [("test1", test_blob, math.pi), ("test2", None, math.e), ("test3", b"another blob", 1.41421)],
    )

    result = sqlite_session.execute("""
        SELECT
            *,
            length(data) as blob_length,
            typeof(value) as value_type
        FROM test_sqlite
        ORDER BY id
    """)

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 3

    first_row = result.data[0]
    assert first_row["name"] == "test1"
    assert first_row["data"] == test_blob
    assert first_row["blob_length"] == len(test_blob)
    assert first_row["value_type"] == "real"

    second_row = result.data[1]
    assert second_row["data"] is None
    assert second_row["blob_length"] is None

    func_result = sqlite_session.execute("""
        SELECT
            COUNT(*) as total,
            AVG(value) as avg_value,
            GROUP_CONCAT(name) as all_names,
            sqlite_version() as version
        FROM test_sqlite
    """)

    assert func_result.data is not None
    assert func_result.data[0]["total"] == 3
    assert func_result.data[0]["avg_value"] is not None
    assert "test1" in func_result.data[0]["all_names"]
    assert func_result.data[0]["version"] is not None


@pytest.mark.xdist_group("adbc_duckdb")
@xfail_if_driver_missing
def test_duckdb_specific_features(duckdb_session: AdbcDriver) -> None:
    """Test DuckDB-specific features with ADBC."""
    duckdb_session.execute_script("""
        CREATE TABLE duckdb_test (
            id INTEGER PRIMARY KEY,
            name TEXT,
            numbers INTEGER[],
            nested_data STRUCT(name VARCHAR, values INTEGER[]),
            map_data MAP(VARCHAR, INTEGER),
            timestamp_col TIMESTAMP,
            json_col JSON
        )
    """)

    duckdb_session.execute("""
        INSERT INTO duckdb_test VALUES (
            1,
            'DuckDB Test',
            [1, 2, 3, 4, 5],
            {'name': 'nested', 'values': [10, 20, 30]},
            MAP(['key1', 'key2'], [100, 200]),
            '2024-01-15 10:30:00',
            '{"type": "test", "version": 1}'
        )
    """)

    result = duckdb_session.execute("SELECT * FROM duckdb_test")
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1

    row = result.data[0]
    assert row["name"] == "DuckDB Test"
    assert row["numbers"] == [1, 2, 3, 4, 5]
    assert row["nested_data"] is not None
    assert row["map_data"] is not None
    assert row["timestamp_col"] is not None
    assert row["json_col"] is not None

    analytical_result = duckdb_session.execute("""
        SELECT
            name,
            numbers,
            array_length(numbers) as array_len,
            list_sum(numbers) as numbers_sum,
            json_extract_string(json_col, '$.type') as json_type
        FROM duckdb_test
    """)

    assert analytical_result.data is not None
    assert analytical_result.data[0]["array_len"] == 5
    assert analytical_result.data[0]["numbers_sum"] == 15  # 1+2+3+4+5
    assert analytical_result.data[0]["json_type"] == "test"


@pytest.mark.xdist_group("postgres")
def test_postgresql_dialect_detection(postgresql_session: AdbcDriver) -> None:
    """Test PostgreSQL dialect detection in ADBC driver."""
    assert hasattr(postgresql_session, "dialect")
    assert postgresql_session.dialect in ["postgres", "postgresql"]

    result = postgresql_session.execute("SELECT $1 as param_value", ("postgresql_test",))
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.data[0]["param_value"] == "postgresql_test"


@pytest.mark.xdist_group("adbc_sqlite")
def test_sqlite_dialect_detection(sqlite_session: AdbcDriver) -> None:
    """Test SQLite dialect detection in ADBC driver."""
    assert hasattr(sqlite_session, "dialect")
    assert sqlite_session.dialect == "sqlite"

    result = sqlite_session.execute("SELECT ? as param_value", ("test_sqlite",))
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.data[0]["param_value"] == "test_sqlite"


@pytest.mark.xdist_group("adbc_duckdb")
@xfail_if_driver_missing
def test_duckdb_dialect_detection(duckdb_session: AdbcDriver) -> None:
    """Test DuckDB dialect detection in ADBC driver."""
    # The driver should have detected DuckDB dialect
    assert hasattr(duckdb_session, "dialect")
    assert duckdb_session.dialect == "duckdb"

    # Test DuckDB-specific parameter style (qmark)
    result = duckdb_session.execute("SELECT ? as param_value", ("duckdb_test",))
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.data[0]["param_value"] == "duckdb_test"
