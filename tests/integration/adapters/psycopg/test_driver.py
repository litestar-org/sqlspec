"""Integration tests for psycopg driver implementation."""

from collections.abc import Generator
from typing import Any, Literal

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec import SQLResult, StatementStack, sql
from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig, PsycopgSyncDriver
from tests.conftest import requires_interpreted

ParamStyle = Literal["tuple_binds", "dict_binds", "named_binds"]

pytestmark = pytest.mark.xdist_group("postgres")


@pytest.fixture
def psycopg_session(psycopg_sync_config: "PsycopgSyncConfig") -> "Generator[PsycopgSyncDriver, None, None]":
    """Create a psycopg session with test table."""

    with psycopg_sync_config.provide_session() as session:
        session.execute_script("DROP TABLE IF EXISTS test_table")
        session.execute_script(
            """
                CREATE TABLE IF NOT EXISTS test_table (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    value INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
            session.execute_script("DROP TABLE IF EXISTS test_table")
        except Exception:
            try:
                session.connection.rollback()
            except Exception:
                pass

        try:
            session.execute_script("DROP TABLE IF EXISTS test_table")
        except Exception:
            pass


async def test_psycopg_async_connection(psycopg_async_config: "PsycopgAsyncConfig") -> None:
    """Test async connection components."""
    async with await psycopg_async_config.create_connection() as conn:
        assert conn is not None
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1 AS id")
            result = await cur.fetchone()
            assert result == {"id": 1}

    async with psycopg_async_config.provide_connection() as conn:
        assert conn is not None
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1 AS value")
            result = await cur.fetchone()
            assert result == {"value": 1}

    await psycopg_async_config.close_pool()


def test_psycopg_sync_connection(postgres_service: "PostgresService") -> None:
    """Test sync connection components."""
    conninfo = (
        f"host={postgres_service.host} port={postgres_service.port} user={postgres_service.user} "
        f"password={postgres_service.password} dbname={postgres_service.database}"
    )
    sync_config = PsycopgSyncConfig(connection_config={"conninfo": conninfo})
    try:
        with sync_config.create_connection() as conn:
            assert conn is not None
            with conn.cursor() as cur:
                cur.execute("SELECT 1 as id")
                result = cur.fetchone()
                assert result == {"id": 1}
    finally:
        sync_config.close_pool()

    another_config = PsycopgSyncConfig(
        connection_config={
            "conninfo": f"postgres://{postgres_service.user}:{postgres_service.password}@"
            f"{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
            "min_size": 1,
            "max_size": 5,
        }
    )
    try:
        with another_config.provide_connection() as conn:
            assert conn is not None
            with conn.cursor() as cur:
                cur.execute("SELECT 1 AS id")
                result = cur.fetchone()
                assert result == {"id": 1}
    finally:
        another_config.close_pool()


def test_psycopg_basic_crud(psycopg_session: "PsycopgSyncDriver") -> None:
    """Test basic CRUD operations."""

    insert_result = psycopg_session.execute("INSERT INTO test_table (name, value) VALUES (%s, %s)", "test_name", 42)
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    select_result = psycopg_session.execute("SELECT name, value FROM test_table WHERE name = %s", "test_name")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "test_name"
    assert select_result.data[0]["value"] == 42

    update_result = psycopg_session.execute("UPDATE test_table SET value = %s WHERE name = %s", 100, "test_name")
    assert isinstance(update_result, SQLResult)
    assert update_result.rows_affected == 1

    verify_result = psycopg_session.execute("SELECT value FROM test_table WHERE name = %s", "test_name")
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert verify_result.data[0]["value"] == 100

    delete_result = psycopg_session.execute("DELETE FROM test_table WHERE name = %s", "test_name")
    assert isinstance(delete_result, SQLResult)
    assert delete_result.rows_affected == 1

    empty_result = psycopg_session.execute("SELECT COUNT(*) as count FROM test_table")
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
def test_psycopg_parameter_styles(psycopg_session: "PsycopgSyncDriver", parameters: Any, style: ParamStyle) -> None:
    """Test different parameter binding styles."""

    psycopg_session.execute("INSERT INTO test_table (name) VALUES (%s)", "test_value")

    if style == "tuple_binds":
        sql = "SELECT name FROM test_table WHERE name = %s"

        result = psycopg_session.execute(sql, *parameters)
    else:
        sql = "SELECT name FROM test_table WHERE name = %(name)s"

        result = psycopg_session.execute(sql, **parameters)

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result) == 1
    assert result.data[0]["name"] == "test_value"


def test_psycopg_execute_many(psycopg_session: "PsycopgSyncDriver") -> None:
    """Test execute_many functionality."""
    parameters_list = [("name1", 1), ("name2", 2), ("name3", 3)]

    result = psycopg_session.execute_many("INSERT INTO test_table (name, value) VALUES (%s, %s)", parameters_list)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == len(parameters_list)

    select_result = psycopg_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == len(parameters_list)

    ordered_result = psycopg_session.execute("SELECT name, value FROM test_table ORDER BY name")
    assert isinstance(ordered_result, SQLResult)
    assert ordered_result.data is not None
    assert len(ordered_result.data) == 3
    assert ordered_result.data[0]["name"] == "name1"
    assert ordered_result.data[0]["value"] == 1


def test_psycopg_execute_script(psycopg_session: "PsycopgSyncDriver") -> None:
    """Test execute_script functionality."""
    script = """
        INSERT INTO test_table (name, value) VALUES ('script_test1', 999);
        INSERT INTO test_table (name, value) VALUES ('script_test2', 888);
        UPDATE test_table SET value = 1000 WHERE name = 'script_test1';
    """

    result = psycopg_session.execute_script(script)

    assert isinstance(result, SQLResult)
    assert result.operation_type == "SCRIPT"

    select_result = psycopg_session.execute(
        "SELECT name, value FROM test_table WHERE name LIKE 'script_test%' ORDER BY name"
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 2
    assert select_result.data[0]["name"] == "script_test1"
    assert select_result.data[0]["value"] == 1000
    assert select_result.data[1]["name"] == "script_test2"
    assert select_result.data[1]["value"] == 888


def test_psycopg_result_methods(psycopg_session: "PsycopgSyncDriver") -> None:
    """Test SelectResult and ExecuteResult methods."""

    psycopg_session.execute_many(
        "INSERT INTO test_table (name, value) VALUES (%s, %s)", [("result1", 10), ("result2", 20), ("result3", 30)]
    )

    result = psycopg_session.execute("SELECT * FROM test_table ORDER BY name")
    assert isinstance(result, SQLResult)

    first_row = result.get_first()
    assert first_row is not None
    assert first_row["name"] == "result1"

    assert result.get_count() == 3

    assert not result.is_empty()

    empty_result = psycopg_session.execute("SELECT * FROM test_table WHERE name = %s", "nonexistent")
    assert isinstance(empty_result, SQLResult)
    assert empty_result.is_empty()
    assert empty_result.get_first() is None


def test_psycopg_error_handling(psycopg_session: "PsycopgSyncDriver") -> None:
    """Test error handling and exception propagation."""

    with pytest.raises(Exception):
        psycopg_session.execute("INVALID SQL STATEMENT")

    psycopg_session.rollback()
    psycopg_session.begin()

    psycopg_session.execute("INSERT INTO test_table (name, value) VALUES (%s, %s)", "unique_test", 1)

    with pytest.raises(Exception):
        psycopg_session.execute("SELECT nonexistent_column FROM test_table")


def test_psycopg_statement_stack_pipeline(psycopg_session: "PsycopgSyncDriver") -> None:
    """StatementStack should leverage psycopg pipeline mode when available."""

    psycopg_session.execute("TRUNCATE TABLE test_table RESTART IDENTITY")

    stack = (
        StatementStack()
        .push_execute("INSERT INTO test_table (id, name, value) VALUES (%s, %s, %s)", (1, "sync-stack-one", 5))
        .push_execute("INSERT INTO test_table (id, name, value) VALUES (%s, %s, %s)", (2, "sync-stack-two", 15))
        .push_execute("SELECT COUNT(*) AS total FROM test_table WHERE name LIKE %s", ("sync-stack-%",))
    )

    results = psycopg_session.execute_stack(stack)

    assert len(results) == 3
    total_result = psycopg_session.execute(
        "SELECT COUNT(*) AS total FROM test_table WHERE name LIKE %s", "sync-stack-%"
    )
    assert total_result.data is not None
    assert total_result.data[0]["total"] == 2


@requires_interpreted
def test_psycopg_statement_stack_continue_on_error(psycopg_session: "PsycopgSyncDriver") -> None:
    """Pipeline execution should continue when instructed to handle errors."""

    psycopg_session.execute("TRUNCATE TABLE test_table RESTART IDENTITY")
    psycopg_session.commit()

    stack = (
        StatementStack()
        .push_execute("INSERT INTO test_table (id, name, value) VALUES (%s, %s, %s)", (1, "sync-initial", 10))
        .push_execute(  # duplicate PK triggers error
            "INSERT INTO test_table (id, name, value) VALUES (%s, %s, %s)", (1, "sync-duplicate", 20)
        )
        .push_execute("INSERT INTO test_table (id, name, value) VALUES (%s, %s, %s)", (2, "sync-success-final", 30))
    )

    results = psycopg_session.execute_stack(stack, continue_on_error=True)

    assert len(results) == 3
    assert results[1].error is not None
    assert results[0].error is None
    assert results[2].error is None

    verify = psycopg_session.execute("SELECT COUNT(*) AS total FROM test_table")
    assert verify.data is not None
    assert verify.data[0]["total"] == 2


def test_psycopg_data_types(psycopg_session: "PsycopgSyncDriver") -> None:
    """Test PostgreSQL data type handling with psycopg."""

    psycopg_session.execute_script("""
        CREATE TABLE psycopg_data_types_test (
            id SERIAL PRIMARY KEY,
            text_col TEXT,
            integer_col INTEGER,
            numeric_col NUMERIC(10,2),
            boolean_col BOOLEAN,
            json_col JSONB,
            array_col INTEGER[],
            date_col DATE,
            timestamp_col TIMESTAMP,
            uuid_col UUID
        )
    """)

    psycopg_session.execute(
        """
        INSERT INTO psycopg_data_types_test (
            text_col, integer_col, numeric_col, boolean_col, json_col,
            array_col, date_col, timestamp_col, uuid_col
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """,
        "text_value",
        42,
        123.45,
        True,
        '{"key": "value"}',
        [1, 2, 3],
        "2024-01-15",
        "2024-01-15 10:30:00",
        "550e8400-e29b-41d4-a716-446655440000",
    )

    select_result = psycopg_session.execute(
        "SELECT text_col, integer_col, numeric_col, boolean_col, json_col, array_col FROM psycopg_data_types_test"
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1

    row = select_result.data[0]
    assert row["text_col"] == "text_value"
    assert row["integer_col"] == 42
    assert row["boolean_col"] is True
    assert row["array_col"] == [1, 2, 3]

    psycopg_session.execute_script("DROP TABLE psycopg_data_types_test")


def test_psycopg_transactions(psycopg_session: "PsycopgSyncDriver") -> None:
    """Test transaction behavior."""

    psycopg_session.execute("TRUNCATE TABLE test_table RESTART IDENTITY")
    psycopg_session.commit()

    psycopg_session.execute("INSERT INTO test_table (name, value) VALUES (%s, %s)", "transaction_test", 100)

    result = psycopg_session.execute("SELECT COUNT(*) as count FROM test_table WHERE name = %s", ("transaction_test"))
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.data[0]["count"] == 1


def test_psycopg_complex_queries(psycopg_session: "PsycopgSyncDriver") -> None:
    """Test complex SQL queries."""

    psycopg_session.execute("TRUNCATE TABLE test_table RESTART IDENTITY")
    psycopg_session.commit()

    test_data = [("Alice", 25), ("Bob", 30), ("Charlie", 35), ("Diana", 28)]

    psycopg_session.execute_many("INSERT INTO test_table (name, value) VALUES (%s, %s)", test_data)

    join_result = psycopg_session.execute("""
        SELECT t1.name as name1, t2.name as name2, t1.value as value1, t2.value as value2
        FROM test_table t1
        CROSS JOIN test_table t2
        WHERE t1.value < t2.value
        ORDER BY t1.name, t2.name
        LIMIT 3
    """)
    assert isinstance(join_result, SQLResult)
    assert join_result.data is not None
    assert len(join_result.data) == 3

    agg_result = psycopg_session.execute("""
        SELECT
            COUNT(*) as total_count,
            AVG(value) as avg_value,
            MIN(value) as min_value,
            MAX(value) as max_value
        FROM test_table
    """)
    assert isinstance(agg_result, SQLResult)
    assert agg_result.data is not None
    assert agg_result.data[0]["total_count"] == 4
    assert agg_result.data[0]["avg_value"] == 29.5
    assert agg_result.data[0]["min_value"] == 25
    assert agg_result.data[0]["max_value"] == 35

    subquery_result = psycopg_session.execute("""
        SELECT name, value
        FROM test_table
        WHERE value > (SELECT AVG(value) FROM test_table)
        ORDER BY value
    """)
    assert isinstance(subquery_result, SQLResult)
    assert subquery_result.data is not None
    assert len(subquery_result.data) == 2
    assert subquery_result.data[0]["name"] == "Bob"
    assert subquery_result.data[1]["name"] == "Charlie"


def test_psycopg_schema_operations(psycopg_session: "PsycopgSyncDriver") -> None:
    """Test schema operations (DDL)."""

    psycopg_session.execute_script("""
        CREATE TABLE schema_test (
            id SERIAL PRIMARY KEY,
            description TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    insert_result = psycopg_session.execute("INSERT INTO schema_test (description) VALUES (%s)", "test description")
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    info_result = psycopg_session.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'schema_test'
        ORDER BY ordinal_position
    """)
    assert isinstance(info_result, SQLResult)
    assert info_result.data is not None
    assert len(info_result.data) == 3

    psycopg_session.execute_script("DROP TABLE schema_test")


def test_psycopg_column_names_and_metadata(psycopg_session: "PsycopgSyncDriver") -> None:
    """Test column names and result metadata."""

    psycopg_session.execute("INSERT INTO test_table (name, value) VALUES (%s, %s)", "metadata_test", 123)

    result = psycopg_session.execute(
        "SELECT id, name, value, created_at FROM test_table WHERE name = %s", ("metadata_test",)
    )
    assert isinstance(result, SQLResult)
    assert result.column_names == ["id", "name", "value", "created_at"]
    assert result.data is not None
    assert len(result) == 1

    row = result.data[0]
    assert row["name"] == "metadata_test"
    assert row["value"] == 123
    assert row["id"] is not None
    assert row["created_at"] is not None


def test_psycopg_performance_bulk_operations(psycopg_session: "PsycopgSyncDriver") -> None:
    """Test performance with bulk operations."""

    bulk_data = [(f"bulk_user_{i}", i * 10) for i in range(100)]

    result = psycopg_session.execute_many("INSERT INTO test_table (name, value) VALUES (%s, %s)", bulk_data)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 100

    select_result = psycopg_session.execute("SELECT COUNT(*) as count FROM test_table WHERE name LIKE 'bulk_user_%'")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == 100

    page_result = psycopg_session.execute(
        "SELECT name, value FROM test_table WHERE name LIKE 'bulk_user_%' ORDER BY value LIMIT 10 OFFSET 20"
    )
    assert isinstance(page_result, SQLResult)
    assert page_result.data is not None
    assert len(page_result.data) == 10
    assert page_result.data[0]["name"] == "bulk_user_20"


def test_psycopg_postgresql_specific_features(psycopg_session: "PsycopgSyncDriver") -> None:
    """Test PostgreSQL-specific features with psycopg."""

    returning_result = psycopg_session.execute(
        "INSERT INTO test_table (name, value) VALUES (%s, %s) RETURNING id, name", "returning_test", 999
    )
    assert isinstance(returning_result, SQLResult)
    assert returning_result.data is not None
    assert len(returning_result.data) == 1
    assert returning_result.data[0]["name"] == "returning_test"

    psycopg_session.execute_many(
        "INSERT INTO test_table (name, value) VALUES (%s, %s)", [("window1", 10), ("window2", 20), ("window3", 30)]
    )

    window_result = psycopg_session.execute("""
        SELECT
            name,
            value,
            ROW_NUMBER() OVER (ORDER BY value) as row_num,
            LAG(value) OVER (ORDER BY value) as prev_value
        FROM test_table
        WHERE name LIKE 'window%'
        ORDER BY value
    """)
    assert isinstance(window_result, SQLResult)
    assert window_result.data is not None
    assert len(window_result.data) == 3
    assert window_result.data[0]["row_num"] == 1
    assert window_result.data[0]["prev_value"] is None


def test_psycopg_json_operations(psycopg_session: "PsycopgSyncDriver") -> None:
    """Test PostgreSQL JSON operations with psycopg."""

    psycopg_session.execute_script("""
        CREATE TABLE IF NOT EXISTS json_test (
            id SERIAL PRIMARY KEY,
            data JSONB
        );
        DELETE FROM json_test;
    """)

    json_data = {"name": "test", "age": 30, "tags": ["postgres", "json"]}
    psycopg_session.execute("INSERT INTO json_test (data) VALUES (%s)", (json_data,))

    json_result = psycopg_session.execute("SELECT data->>'name' as name, data->>'age' as age FROM json_test")
    assert isinstance(json_result, SQLResult)
    assert json_result.data is not None
    assert json_result.data[0]["name"] == "test"
    assert json_result.data[0]["age"] == "30"

    psycopg_session.execute_script("DROP TABLE json_test")


def test_psycopg_copy_operations_positional(psycopg_session: "PsycopgSyncDriver") -> None:
    """Test PostgreSQL COPY operations with psycopg using positional parameters."""

    psycopg_session.execute_script("""
        DROP TABLE IF EXISTS copy_test_pos;
        CREATE TABLE copy_test_pos (
            id INTEGER,
            name TEXT,
            value INTEGER
        )
    """)

    copy_data = "1\ttest1\t100\n2\ttest2\t200\n"
    result = psycopg_session.execute("COPY copy_test_pos FROM STDIN WITH (FORMAT text)", copy_data)
    assert isinstance(result, SQLResult)
    assert result.rows_affected >= 0

    verify_result = psycopg_session.execute("SELECT * FROM copy_test_pos ORDER BY id")
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert len(verify_result.data) == 2
    assert verify_result.data[0]["name"] == "test1"
    assert verify_result.data[1]["value"] == 200

    psycopg_session.execute_script("DROP TABLE copy_test_pos")


def test_psycopg_copy_operations_keyword(psycopg_session: "PsycopgSyncDriver") -> None:
    """Test PostgreSQL COPY operations with psycopg using keyword parameters."""

    psycopg_session.execute_script("""
        DROP TABLE IF EXISTS copy_test_kw;
        CREATE TABLE copy_test_kw (
            id INTEGER,
            name TEXT,
            value INTEGER
        )
    """)

    copy_data = "3\ttest3\t300\n4\ttest4\t400\n"
    result = psycopg_session.execute("COPY copy_test_kw FROM STDIN WITH (FORMAT text)", copy_data)
    assert isinstance(result, SQLResult)
    assert result.rows_affected >= 0

    verify_result = psycopg_session.execute("SELECT * FROM copy_test_kw ORDER BY id")
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert len(verify_result.data) == 2
    assert verify_result.data[0]["name"] == "test3"
    assert verify_result.data[1]["value"] == 400

    psycopg_session.execute_script("DROP TABLE copy_test_kw")


def test_psycopg_copy_csv_format(psycopg_session: "PsycopgSyncDriver") -> None:
    """Test PostgreSQL COPY operations with CSV format."""

    psycopg_session.execute_script("""
        DROP TABLE IF EXISTS copy_csv_sync;
        CREATE TABLE copy_csv_sync (
            id INTEGER,
            name TEXT,
            value INTEGER
        )
    """)

    csv_data = "5,test5,500\n6,test6,600\n7,test7,700\n"
    result_pos = psycopg_session.execute("COPY copy_csv_sync FROM STDIN WITH (FORMAT csv)", csv_data)
    assert isinstance(result_pos, SQLResult)
    assert result_pos.rows_affected == 3

    verify_result = psycopg_session.execute("SELECT * FROM copy_csv_sync ORDER BY id")
    assert isinstance(verify_result, SQLResult)
    assert len(verify_result.data) == 3
    assert verify_result.data[0]["name"] == "test5"
    assert verify_result.data[2]["value"] == 700

    psycopg_session.execute_script("TRUNCATE TABLE copy_csv_sync")

    csv_data2 = "8,test8,800\n9,test9,900\n"
    result_kw = psycopg_session.execute("COPY copy_csv_sync FROM STDIN WITH (FORMAT csv)", csv_data2)
    assert isinstance(result_kw, SQLResult)
    assert result_kw.rows_affected == 2

    verify_result2 = psycopg_session.execute("SELECT * FROM copy_csv_sync ORDER BY id")
    assert isinstance(verify_result2, SQLResult)
    assert len(verify_result2.data) == 2
    assert verify_result2.data[0]["name"] == "test8"
    assert verify_result2.data[1]["value"] == 900

    psycopg_session.execute_script("DROP TABLE copy_csv_sync")


@pytest.mark.integration
def test_psycopg_sync_pgvector_integration(psycopg_session: "PsycopgSyncDriver") -> None:
    """Test that psycopg sync driver initializes pgvector support automatically via pool configure."""

    result = psycopg_session.execute("SELECT 1 as test_value")
    assert result.data is not None
    assert result.data[0]["test_value"] == 1


def test_psycopg_sync_for_update_locking(psycopg_session: "PsycopgSyncDriver") -> None:
    """Test FOR UPDATE row locking with psycopg (sync)."""

    # Setup test table
    psycopg_session.execute_script("DROP TABLE IF EXISTS test_table")
    psycopg_session.execute_script("""
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50),
            value INTEGER
        )
    """)

    # Insert test data
    psycopg_session.execute("INSERT INTO test_table (name, value) VALUES (%s, %s)", ("psycopg_lock", 100))

    try:
        psycopg_session.begin()

        # Test basic FOR UPDATE
        result = psycopg_session.select_one(
            sql.select("id", "name", "value").from_("test_table").where_eq("name", "psycopg_lock").for_update()
        )
        assert result is not None
        assert result["name"] == "psycopg_lock"
        assert result["value"] == 100

        psycopg_session.commit()
    except Exception:
        psycopg_session.rollback()
        raise
    finally:
        psycopg_session.execute_script("DROP TABLE IF EXISTS test_table")


def test_psycopg_sync_for_update_skip_locked(psycopg_session: "PsycopgSyncDriver") -> None:
    """Test FOR UPDATE SKIP LOCKED with psycopg (sync)."""

    # Setup test table
    psycopg_session.execute_script("DROP TABLE IF EXISTS test_table")
    psycopg_session.execute_script("""
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50),
            value INTEGER
        )
    """)

    # Insert test data
    psycopg_session.execute("INSERT INTO test_table (name, value) VALUES (%s, %s)", ("psycopg_skip", 200))

    try:
        psycopg_session.begin()

        # Test FOR UPDATE SKIP LOCKED
        result = psycopg_session.select_one(
            sql.select("*").from_("test_table").where_eq("name", "psycopg_skip").for_update(skip_locked=True)
        )
        assert result is not None
        assert result["name"] == "psycopg_skip"

        psycopg_session.commit()
    except Exception:
        psycopg_session.rollback()
        raise
    finally:
        psycopg_session.execute_script("DROP TABLE IF EXISTS test_table")


def test_psycopg_sync_for_share_locking(psycopg_session: "PsycopgSyncDriver") -> None:
    """Test FOR SHARE row locking with psycopg (sync)."""

    # Setup test table
    psycopg_session.execute_script("DROP TABLE IF EXISTS test_table")
    psycopg_session.execute_script("""
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50),
            value INTEGER
        )
    """)

    # Insert test data
    psycopg_session.execute("INSERT INTO test_table (name, value) VALUES (%s, %s)", ("psycopg_share", 300))

    try:
        psycopg_session.begin()

        # Test FOR SHARE
        result = psycopg_session.select_one(
            sql.select("id", "name", "value").from_("test_table").where_eq("name", "psycopg_share").for_share()
        )
        assert result is not None
        assert result["name"] == "psycopg_share"
        assert result["value"] == 300

        psycopg_session.commit()
    except Exception:
        psycopg_session.rollback()
        raise
    finally:
        psycopg_session.execute_script("DROP TABLE IF EXISTS test_table")
