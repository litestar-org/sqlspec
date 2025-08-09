"""Test ADBC multi-backend support and backend-specific features using CORE_ROUND_3 architecture."""

from collections.abc import Generator
from typing import Any

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.core.result import SQLResult

# Import the decorator
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
    """Test PostgreSQL-specific features with ADBC using CORE_ROUND_3."""
    # Create test table with PostgreSQL-specific types
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

    # Insert data with PostgreSQL-specific types
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

    # Query and verify data
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

    # Test PostgreSQL-specific query features
    json_query = postgresql_session.execute("""
        SELECT
            jsonb_col ->> 'name' as name,
            jsonb_col ->> 'age' as age,
            array_length(array_col, 1) as array_len
        FROM pg_test
    """)

    assert json_query.data is not None
    assert json_query.data[0]["name"] == "John"
    assert json_query.data[0]["age"] == "30"  # JSON values are strings
    assert json_query.data[0]["array_len"] == 5

    # Clean up
    postgresql_session.execute_script("DROP TABLE IF EXISTS pg_test")


@pytest.mark.xdist_group("adbc_sqlite")
def test_sqlite_specific_features(sqlite_session: AdbcDriver) -> None:
    """Test SQLite-specific features with ADBC using CORE_ROUND_3."""
    # Create test table with SQLite features
    sqlite_session.execute_script("""
        CREATE TABLE sqlite_test (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            data BLOB,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            value REAL
        )
    """)

    # Insert test data including binary data
    test_blob = b"SQLite binary data test"
    sqlite_session.execute_many(
        """
        INSERT INTO sqlite_test (name, data, value) VALUES (?, ?, ?)
    """,
        [("test1", test_blob, 3.14159), ("test2", None, 2.71828), ("test3", b"another blob", 1.41421)],
    )

    # Test SQLite-specific queries
    result = sqlite_session.execute("""
        SELECT
            *,
            length(data) as blob_length,
            typeof(value) as value_type
        FROM sqlite_test
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

    # Test NULL blob handling
    second_row = result.data[1]
    assert second_row["data"] is None
    assert second_row["blob_length"] is None

    # Test SQLite functions
    func_result = sqlite_session.execute("""
        SELECT
            COUNT(*) as total,
            AVG(value) as avg_value,
            GROUP_CONCAT(name) as all_names,
            sqlite_version() as version
        FROM sqlite_test
    """)

    assert func_result.data is not None
    assert func_result.data[0]["total"] == 3
    assert func_result.data[0]["avg_value"] is not None
    assert "test1" in func_result.data[0]["all_names"]
    assert func_result.data[0]["version"] is not None


@pytest.mark.xdist_group("adbc_duckdb")
@xfail_if_driver_missing
def test_duckdb_specific_features(duckdb_session: AdbcDriver) -> None:
    """Test DuckDB-specific features with ADBC using CORE_ROUND_3."""
    # Create test table with DuckDB advanced types
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

    # Insert data using DuckDB syntax
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

    # Query and verify DuckDB-specific data types
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

    # Test DuckDB analytical functions
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


def test_backend_consistency_across_adapters(postgresql_session: AdbcDriver, sqlite_session: AdbcDriver) -> None:
    """Test consistency of basic operations across different ADBC backends using CORE_ROUND_3."""

    # Create similar tables in both backends
    postgresql_session.execute_script("""
        CREATE TABLE IF NOT EXISTS consistency_test (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER,
            is_active BOOLEAN DEFAULT true
        )
    """)

    sqlite_session.execute_script("""
        CREATE TABLE consistency_test (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            value INTEGER,
            is_active BOOLEAN DEFAULT 1
        )
    """)

    # Insert same test data (using appropriate parameter styles)
    test_data_pg = [("postgres_item1", 100, True), ("postgres_item2", 200, False)]
    test_data_sqlite = [("sqlite_item1", 100, True), ("sqlite_item2", 200, False)]

    postgresql_session.execute_many(
        """
        INSERT INTO consistency_test (name, value, is_active) VALUES ($1, $2, $3)
    """,
        test_data_pg,
    )

    sqlite_session.execute_many(
        """
        INSERT INTO consistency_test (name, value, is_active) VALUES (?, ?, ?)
    """,
        test_data_sqlite,
    )

    # Test similar queries on both backends
    pg_result = postgresql_session.execute("""
        SELECT COUNT(*) as count, SUM(value) as total, AVG(value) as average
        FROM consistency_test
    """)

    sqlite_result = sqlite_session.execute("""
        SELECT COUNT(*) as count, SUM(value) as total, AVG(value) as average
        FROM consistency_test
    """)

    # Both should have similar result structure
    assert isinstance(pg_result, SQLResult)
    assert isinstance(sqlite_result, SQLResult)

    pg_data = pg_result.data[0]
    sqlite_data = sqlite_result.data[0]

    # Both should have same counts and totals
    assert pg_data["count"] == sqlite_data["count"] == 2
    assert pg_data["total"] == sqlite_data["total"] == 300
    assert pg_data["average"] == sqlite_data["average"] == 150.0

    # Clean up
    postgresql_session.execute_script("DROP TABLE IF EXISTS consistency_test")
    # SQLite cleanup automatic with in-memory database


@pytest.mark.parametrize(
    "backend_name,session_fixture", [("PostgreSQL", "postgresql_session"), ("SQLite", "sqlite_session")]
)
def test_parameter_style_consistency(backend_name: str, session_fixture: str, request: Any) -> None:
    """Test parameter style consistency across ADBC backends using CORE_ROUND_3."""
    session = request.getfixturevalue(session_fixture)

    # Create test table
    if backend_name == "PostgreSQL":
        session.execute_script("""
            CREATE TABLE IF NOT EXISTS param_test (
                id SERIAL PRIMARY KEY,
                name TEXT,
                value INTEGER
            )
        """)
        insert_sql = "INSERT INTO param_test (name, value) VALUES ($1, $2)"
        select_sql = "SELECT * FROM param_test WHERE name = $1"
        params = ("test_param", 42)
        select_params = ("test_param",)
    else:  # SQLite
        session.execute_script("""
            CREATE TABLE param_test (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                value INTEGER
            )
        """)
        insert_sql = "INSERT INTO param_test (name, value) VALUES (?, ?)"
        select_sql = "SELECT * FROM param_test WHERE name = ?"
        params = ("test_param", 42)
        select_params = ("test_param",)

    # Test parameter binding
    insert_result = session.execute(insert_sql, params)
    assert isinstance(insert_result, SQLResult)

    # Test parameter retrieval
    select_result = session.execute(select_sql, select_params)
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "test_param"
    assert select_result.data[0]["value"] == 42

    # Clean up PostgreSQL table
    if backend_name == "PostgreSQL":
        session.execute_script("DROP TABLE IF EXISTS param_test")


@pytest.mark.xdist_group("postgres")
def test_postgresql_dialect_detection(postgresql_session: AdbcDriver) -> None:
    """Test PostgreSQL dialect detection in ADBC driver using CORE_ROUND_3."""
    # The driver should have detected PostgreSQL dialect
    assert hasattr(postgresql_session, "dialect")
    assert postgresql_session.dialect in ["postgres", "postgresql"]

    # Test PostgreSQL-specific parameter style (numeric)
    result = postgresql_session.execute("SELECT $1 as param_value", ("postgresql_test",))
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.data[0]["param_value"] == "postgresql_test"


@pytest.mark.xdist_group("adbc_sqlite")
def test_sqlite_dialect_detection(sqlite_session: AdbcDriver) -> None:
    """Test SQLite dialect detection in ADBC driver using CORE_ROUND_3."""
    # The driver should have detected SQLite dialect
    assert hasattr(sqlite_session, "dialect")
    assert sqlite_session.dialect == "sqlite"

    # Test SQLite-specific parameter style (qmark)
    result = sqlite_session.execute("SELECT ? as param_value", ("sqlite_test",))
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.data[0]["param_value"] == "sqlite_test"


@pytest.mark.xdist_group("adbc_duckdb")
@xfail_if_driver_missing
def test_duckdb_dialect_detection(duckdb_session: AdbcDriver) -> None:
    """Test DuckDB dialect detection in ADBC driver using CORE_ROUND_3."""
    # The driver should have detected DuckDB dialect
    assert hasattr(duckdb_session, "dialect")
    assert duckdb_session.dialect == "duckdb"

    # Test DuckDB-specific parameter style (qmark)
    result = duckdb_session.execute("SELECT ? as param_value", ("duckdb_test",))
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.data[0]["param_value"] == "duckdb_test"


def test_cross_backend_data_type_handling(postgresql_session: AdbcDriver, sqlite_session: AdbcDriver) -> None:
    """Test data type handling consistency across ADBC backends using CORE_ROUND_3."""

    # Test common data types in both backends
    common_types_data = [
        ("string_test", "Hello World"),
        ("integer_test", 42),
        ("float_test", 3.14159),
        ("boolean_test", True),
        ("null_test", None),
    ]

    # PostgreSQL
    postgresql_session.execute_script("""
        CREATE TABLE IF NOT EXISTS type_test (
            id SERIAL PRIMARY KEY,
            test_name TEXT,
            test_value TEXT
        )
    """)

    # SQLite
    sqlite_session.execute_script("""
        CREATE TABLE type_test (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            test_name TEXT,
            test_value TEXT
        )
    """)

    # Insert data using string representation for consistency
    for name, value in common_types_data:
        str_value = str(value) if value is not None else None

        postgresql_session.execute(
            """
            INSERT INTO type_test (test_name, test_value) VALUES ($1, $2)
        """,
            (name, str_value),
        )

        sqlite_session.execute(
            """
            INSERT INTO type_test (test_name, test_value) VALUES (?, ?)
        """,
            (name, str_value),
        )

    # Query and compare results
    pg_result = postgresql_session.execute("SELECT * FROM type_test ORDER BY test_name")
    sqlite_result = sqlite_session.execute("SELECT * FROM type_test ORDER BY test_name")

    assert isinstance(pg_result, SQLResult)
    assert isinstance(sqlite_result, SQLResult)

    assert pg_result.get_count() == sqlite_result.get_count()

    # Verify data consistency (excluding auto-generated IDs)
    for pg_row, sqlite_row in zip(pg_result.data, sqlite_result.data):
        assert pg_row["test_name"] == sqlite_row["test_name"]
        assert pg_row["test_value"] == sqlite_row["test_value"]

    # Clean up
    postgresql_session.execute_script("DROP TABLE IF EXISTS type_test")
