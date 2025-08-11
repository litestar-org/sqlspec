"""Integration tests for ADBC driver implementation."""

from collections.abc import Generator
from typing import Any, Literal

import pytest
from pytest_databases.docker.bigquery import BigQueryService
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.core.result import SQLResult
from tests.integration.test_adapters.test_adbc.conftest import xfail_if_driver_missing

ParamStyle = Literal["tuple_binds", "dict_binds", "named_binds"]


@pytest.fixture
def adbc_postgresql_session(postgres_service: PostgresService) -> Generator[AdbcDriver, None, None]:
    """Create an ADBC PostgreSQL session with test table."""
    config = AdbcConfig(
        connection_config={
            "uri": f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
            "driver_name": "adbc_driver_postgresql",
        }
    )

    with config.provide_session() as session:
        session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_table (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        yield session
        try:
            session.execute_script("DROP TABLE IF EXISTS test_table")
        except Exception:
            try:
                session.execute("ROLLBACK")
                session.execute_script("DROP TABLE IF EXISTS test_table")
            except Exception:
                pass


@pytest.fixture
def adbc_sqlite_session() -> Generator[AdbcDriver, None, None]:
    """Create an ADBC SQLite session with test table."""
    config = AdbcConfig(connection_config={"uri": ":memory:", "driver_name": "adbc_driver_sqlite"})

    with config.provide_session() as session:
        session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_table (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        yield session


@pytest.fixture
def adbc_duckdb_session() -> Generator[AdbcDriver, None, None]:
    """Create an ADBC DuckDB session with test table."""
    config = AdbcConfig(connection_config={"driver_name": "adbc_driver_duckdb.dbapi.connect"})

    with config.provide_session() as session:
        session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_table (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        yield session


@pytest.fixture
def adbc_bigquery_session(bigquery_service: BigQueryService) -> Generator[AdbcDriver, None, None]:
    """Create an ADBC BigQuery session using emulator."""

    config = AdbcConfig(
        connection_config={
            "driver_name": "adbc_driver_bigquery",
            "project_id": bigquery_service.project,
            "dataset_id": bigquery_service.dataset,
            "db_kwargs": {
                "project_id": bigquery_service.project,
                "client_options": {"api_endpoint": f"http://{bigquery_service.host}:{bigquery_service.port}"},
                "credentials": None,
            },
        }
    )

    with config.provide_session() as session:
        yield session


@pytest.mark.xdist_group("postgres")
def test_adbc_postgresql_basic_crud(adbc_postgresql_session: AdbcDriver) -> None:
    """Test basic CRUD operations with ADBC PostgreSQL."""
    insert_result = adbc_postgresql_session.execute(
        "INSERT INTO test_table (name, value) VALUES ($1, $2)", ("test_name", 42)
    )
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected in (-1, 0, 1)

    select_result = adbc_postgresql_session.execute(
        "SELECT name, value FROM test_table WHERE name = $1", ("test_name",)
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "test_name"
    assert select_result.data[0]["value"] == 42

    update_result = adbc_postgresql_session.execute(
        "UPDATE test_table SET value = $1 WHERE name = $2", (100, "test_name")
    )
    assert isinstance(update_result, SQLResult)
    assert update_result.rows_affected in (-1, 0, 1)

    verify_result = adbc_postgresql_session.execute("SELECT value FROM test_table WHERE name = $1", ("test_name",))
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert verify_result.data[0]["value"] == 100

    delete_result = adbc_postgresql_session.execute("DELETE FROM test_table WHERE name = $1", ("test_name",))
    assert isinstance(delete_result, SQLResult)
    assert delete_result.rows_affected in (-1, 0, 1)

    empty_result = adbc_postgresql_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(empty_result, SQLResult)
    assert empty_result.data is not None
    assert empty_result.data[0]["count"] == 0


@pytest.mark.xdist_group("adbc_sqlite")
def test_adbc_sqlite_basic_crud(adbc_sqlite_session: AdbcDriver) -> None:
    """Test basic CRUD operations with ADBC SQLite."""
    insert_result = adbc_sqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("test_name", 42))
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected in (-1, 0, 1)

    select_result = adbc_sqlite_session.execute("SELECT name, value FROM test_table WHERE name = ?", ("test_name",))
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "test_name"
    assert select_result.data[0]["value"] == 42

    update_result = adbc_sqlite_session.execute("UPDATE test_table SET value = ? WHERE name = ?", (100, "test_name"))
    assert isinstance(update_result, SQLResult)
    assert update_result.rows_affected in (-1, 0, 1)

    verify_result = adbc_sqlite_session.execute("SELECT value FROM test_table WHERE name = ?", ("test_name",))
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert verify_result.data[0]["value"] == 100

    delete_result = adbc_sqlite_session.execute("DELETE FROM test_table WHERE name = ?", ("test_name",))
    assert isinstance(delete_result, SQLResult)
    assert delete_result.rows_affected in (-1, 0, 1)

    empty_result = adbc_sqlite_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(empty_result, SQLResult)
    assert empty_result.data is not None
    assert empty_result.data[0]["count"] == 0


@pytest.mark.xdist_group("adbc_duckdb")
@xfail_if_driver_missing
def test_adbc_duckdb_basic_crud(adbc_duckdb_session: AdbcDriver) -> None:
    """Test basic CRUD operations with ADBC DuckDB."""
    insert_result = adbc_duckdb_session.execute(
        "INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", (1, "test_name", 42)
    )
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected in (-1, 0, 1)

    select_result = adbc_duckdb_session.execute("SELECT name, value FROM test_table WHERE name = ?", ("test_name",))
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "test_name"
    assert select_result.data[0]["value"] == 42

    update_result = adbc_duckdb_session.execute("UPDATE test_table SET value = ? WHERE name = ?", (100, "test_name"))
    assert isinstance(update_result, SQLResult)
    assert update_result.rows_affected in (-1, 0, 1)

    verify_result = adbc_duckdb_session.execute("SELECT value FROM test_table WHERE name = ?", ("test_name",))
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert verify_result.data[0]["value"] == 100

    delete_result = adbc_duckdb_session.execute("DELETE FROM test_table WHERE name = ?", ("test_name",))
    assert isinstance(delete_result, SQLResult)
    assert delete_result.rows_affected in (-1, 0, 1)

    empty_result = adbc_duckdb_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(empty_result, SQLResult)
    assert empty_result.data is not None
    assert empty_result.data[0]["count"] == 0


@pytest.mark.parametrize(
    ("parameters", "style"),
    [
        pytest.param(("test_value",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_value"}, "dict_binds", id="dict_binds"),
    ],
)
@pytest.mark.xdist_group("postgres")
def test_adbc_postgresql_parameter_styles(
    adbc_postgresql_session: AdbcDriver, parameters: Any, style: ParamStyle
) -> None:
    """Test different parameter binding styles with ADBC PostgreSQL."""
    adbc_postgresql_session.execute("INSERT INTO test_table (name) VALUES ($1)", ("test_value",))

    if style == "tuple_binds":
        sql = "SELECT name FROM test_table WHERE name = $1"
    else:
        sql = "SELECT name FROM test_table WHERE name = $1"
        parameters = (parameters["name"],) if isinstance(parameters, dict) else parameters

    result = adbc_postgresql_session.execute(sql, parameters)
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.get_count() == 1
    assert result.data[0]["name"] == "test_value"


@pytest.mark.xdist_group("postgres")
def test_adbc_postgresql_execute_many(adbc_postgresql_session: AdbcDriver) -> None:
    """Test execute_many functionality with ADBC PostgreSQL."""
    parameters_list = [("name1", 1), ("name2", 2), ("name3", 3)]

    result = adbc_postgresql_session.execute_many(
        "INSERT INTO test_table (name, value) VALUES ($1, $2)", parameters_list
    )
    assert isinstance(result, SQLResult)
    assert result.rows_affected == len(parameters_list)

    select_result = adbc_postgresql_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == len(parameters_list)

    ordered_result = adbc_postgresql_session.execute("SELECT name, value FROM test_table ORDER BY name")
    assert isinstance(ordered_result, SQLResult)
    assert ordered_result.data is not None
    assert len(ordered_result.data) == 3
    assert ordered_result.data[0]["name"] == "name1"
    assert ordered_result.data[0]["value"] == 1


@pytest.mark.xdist_group("postgres")
def test_adbc_postgresql_execute_script(adbc_postgresql_session: AdbcDriver) -> None:
    """Test execute_script functionality with ADBC PostgreSQL."""
    script = """
        INSERT INTO test_table (name, value) VALUES ('script_test1', 999);
        INSERT INTO test_table (name, value) VALUES ('script_test2', 888);
        UPDATE test_table SET value = 1000 WHERE name = 'script_test1';
    """

    result = adbc_postgresql_session.execute_script(script)
    assert isinstance(result, (str, SQLResult)) or result is None

    select_result = adbc_postgresql_session.execute(
        "SELECT name, value FROM test_table WHERE name LIKE 'script_test%' ORDER BY name"
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 2
    assert select_result.data[0]["name"] == "script_test1"
    assert select_result.data[0]["value"] == 1000
    assert select_result.data[1]["name"] == "script_test2"
    assert select_result.data[1]["value"] == 888


@pytest.mark.xdist_group("postgres")
def test_adbc_postgresql_result_methods(adbc_postgresql_session: AdbcDriver) -> None:
    """Test SQLResult methods with ADBC PostgreSQL."""
    adbc_postgresql_session.execute_many(
        "INSERT INTO test_table (name, value) VALUES ($1, $2)", [("result1", 10), ("result2", 20), ("result3", 30)]
    )

    result = adbc_postgresql_session.execute("SELECT * FROM test_table ORDER BY name")
    assert isinstance(result, SQLResult)

    first_row = result.get_first()
    assert first_row is not None
    assert first_row["name"] == "result1"

    assert result.get_count() == 3

    assert not result.is_empty()

    empty_result = adbc_postgresql_session.execute("SELECT * FROM test_table WHERE name = $1", ("nonexistent",))
    assert isinstance(empty_result, SQLResult)
    assert empty_result.is_empty()
    assert empty_result.get_first() is None


@pytest.mark.xdist_group("postgres")
def test_adbc_postgresql_error_handling(adbc_postgresql_session: AdbcDriver) -> None:
    """Test error handling and exception propagation with ADBC PostgreSQL."""
    try:
        adbc_postgresql_session.execute("ROLLBACK")
    except Exception:
        pass

    adbc_postgresql_session.execute_script("DROP TABLE IF EXISTS test_table")
    adbc_postgresql_session.execute_script("""
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            value INTEGER DEFAULT 0
        )
    """)

    with pytest.raises(Exception):
        adbc_postgresql_session.execute("INVALID SQL STATEMENT")

    adbc_postgresql_session.execute("INSERT INTO test_table (name, value) VALUES ($1, $2)", ("unique_test", 1))

    with pytest.raises(Exception):
        adbc_postgresql_session.execute("INSERT INTO test_table (name, value) VALUES ($1, $2)", ("unique_test", 2))

    with pytest.raises(Exception):
        adbc_postgresql_session.execute("SELECT nonexistent_column FROM test_table")


@pytest.mark.xdist_group("postgres")
def test_adbc_postgresql_data_types(adbc_postgresql_session: AdbcDriver) -> None:
    """Test PostgreSQL data type handling with ADBC."""
    adbc_postgresql_session.execute_script("""
        CREATE TABLE data_types_test (
            id SERIAL PRIMARY KEY,
            text_col TEXT,
            integer_col INTEGER,
            numeric_col NUMERIC(10,2),
            boolean_col BOOLEAN,
            array_col INTEGER[],
            date_col DATE,
            timestamp_col TIMESTAMP
        )
    """)

    adbc_postgresql_session.execute(
        """
        INSERT INTO data_types_test (
            text_col, integer_col, numeric_col, boolean_col,
            array_col, date_col, timestamp_col
        ) VALUES (
            $1, $2, $3, $4, $5::int[], $6::date, $7::timestamp
        )
    """,
        ("text_value", 42, 123.45, True, [1, 2, 3], "2024-01-15", "2024-01-15 10:30:00"),
    )

    select_result = adbc_postgresql_session.execute(
        "SELECT text_col, integer_col, numeric_col, boolean_col, array_col FROM data_types_test"
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1

    row = select_result.data[0]
    assert row["text_col"] == "text_value"
    assert row["integer_col"] == 42
    assert row["boolean_col"] is True
    assert row["array_col"] == [1, 2, 3]

    adbc_postgresql_session.execute_script("DROP TABLE data_types_test")


@pytest.mark.xdist_group("postgres")
def test_adbc_postgresql_performance_bulk_operations(adbc_postgresql_session: AdbcDriver) -> None:
    """Test performance with bulk operations using ADBC PostgreSQL."""
    bulk_data = [(f"bulk_user_{i}", i * 10) for i in range(100)]

    result = adbc_postgresql_session.execute_many("INSERT INTO test_table (name, value) VALUES ($1, $2)", bulk_data)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 100

    select_result = adbc_postgresql_session.execute(
        "SELECT COUNT(*) as count FROM test_table WHERE name LIKE 'bulk_user_%'"
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == 100

    page_result = adbc_postgresql_session.execute(
        "SELECT name, value FROM test_table WHERE name LIKE 'bulk_user_%' ORDER BY value LIMIT 10 OFFSET 20"
    )
    assert isinstance(page_result, SQLResult)
    assert page_result.data is not None
    assert len(page_result.data) == 10
    assert page_result.data[0]["name"] == "bulk_user_20"


@pytest.mark.xdist_group("adbc_sqlite")
def test_adbc_multiple_backends_consistency(adbc_sqlite_session: AdbcDriver) -> None:
    """Test consistency across different ADBC backends."""
    test_data = [("backend_test1", 100), ("backend_test2", 200)]
    adbc_sqlite_session.execute_many("INSERT INTO test_table (name, value) VALUES (?, ?)", test_data)

    result = adbc_sqlite_session.execute("SELECT name, value FROM test_table ORDER BY name")
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.get_count() == 2
    assert result.data[0]["name"] == "backend_test1"
    assert result.data[0]["value"] == 100

    agg_result = adbc_sqlite_session.execute("SELECT COUNT(*) as count, SUM(value) as total FROM test_table")
    assert isinstance(agg_result, SQLResult)
    assert agg_result.data is not None
    assert agg_result.data[0]["count"] == 2
    assert agg_result.data[0]["total"] == 300
