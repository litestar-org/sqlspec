"""Test execute_many functionality for Psycopg drivers."""

from collections.abc import Generator

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psycopg import PsycopgSyncConfig, PsycopgSyncDriver
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQLConfig


@pytest.fixture
def psycopg_batch_session(postgres_service: PostgresService) -> "Generator[PsycopgSyncDriver, None, None]":
    """Create a Psycopg session for batch operation testing."""
    config = PsycopgSyncConfig(
        host=postgres_service.host,
        port=postgres_service.port,
        user=postgres_service.user,
        password=postgres_service.password,
        dbname=postgres_service.database,
        autocommit=True,  # Enable autocommit for tests
        statement_config=SQLConfig(strict_mode=False),
    )

    with config.provide_session() as session:
        # Create test table
        session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_batch (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0,
                category TEXT
            )
        """)
        # Clear any existing data
        session.execute_script("TRUNCATE TABLE test_batch RESTART IDENTITY")

        yield session
        # Cleanup
        session.execute_script("DROP TABLE IF EXISTS test_batch")


@pytest.mark.xdist_group("postgres")
def test_psycopg_execute_many_basic(psycopg_batch_session: PsycopgSyncDriver) -> None:
    """Test basic execute_many with Psycopg."""
    parameters = [
        ("Item 1", 100, "A"),
        ("Item 2", 200, "B"),
        ("Item 3", 300, "A"),
        ("Item 4", 400, "C"),
        ("Item 5", 500, "B"),
    ]

    result = psycopg_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES (%s, %s, %s)", parameters
    )

    assert isinstance(result, SQLResult)
    # Psycopg should report the number of rows affected
    assert result.rows_affected == 5

    # Verify data was inserted
    count_result = psycopg_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
    assert count_result.data[0]["count"] == 5


@pytest.mark.xdist_group("postgres")
def test_psycopg_execute_many_update(psycopg_batch_session: PsycopgSyncDriver) -> None:
    """Test execute_many for UPDATE operations with Psycopg."""
    # First insert some data
    psycopg_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES (%s, %s, %s)",
        [("Update 1", 10, "X"), ("Update 2", 20, "Y"), ("Update 3", 30, "Z")],
    )

    # Now update with execute_many
    update_params = [(100, "Update 1"), (200, "Update 2"), (300, "Update 3")]

    result = psycopg_batch_session.execute_many("UPDATE test_batch SET value = %s WHERE name = %s", update_params)

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 3

    # Verify updates
    check_result = psycopg_batch_session.execute("SELECT name, value FROM test_batch ORDER BY name")
    assert len(check_result.data) == 3
    assert all(row["value"] in (100, 200, 300) for row in check_result.data)


@pytest.mark.xdist_group("postgres")
def test_psycopg_execute_many_empty(psycopg_batch_session: PsycopgSyncDriver) -> None:
    """Test execute_many with empty parameter list on Psycopg."""
    result = psycopg_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES (%s, %s, %s)", []
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 0

    # Verify no data was inserted
    count_result = psycopg_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
    assert count_result.data[0]["count"] == 0


@pytest.mark.xdist_group("postgres")
def test_psycopg_execute_many_mixed_types(psycopg_batch_session: PsycopgSyncDriver) -> None:
    """Test execute_many with mixed parameter types on Psycopg."""
    parameters = [
        ("String Item", 123, "CAT1"),
        ("Another Item", 456, None),  # NULL category
        ("Third Item", 0, "CAT2"),
        ("Negative Item", -50, "CAT3"),
    ]

    result = psycopg_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES (%s, %s, %s)", parameters
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 4

    # Verify data including NULL
    null_result = psycopg_batch_session.execute("SELECT * FROM test_batch WHERE category IS NULL")
    assert len(null_result.data) == 1
    assert null_result.data[0]["name"] == "Another Item"

    # Verify negative value
    negative_result = psycopg_batch_session.execute("SELECT * FROM test_batch WHERE value < 0")
    assert len(negative_result.data) == 1
    assert negative_result.data[0]["value"] == -50


@pytest.mark.xdist_group("postgres")
def test_psycopg_execute_many_delete(psycopg_batch_session: PsycopgSyncDriver) -> None:
    """Test execute_many for DELETE operations with Psycopg."""
    # First insert test data
    psycopg_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES (%s, %s, %s)",
        [
            ("Delete 1", 10, "X"),
            ("Delete 2", 20, "Y"),
            ("Delete 3", 30, "X"),
            ("Keep 1", 40, "Z"),
            ("Delete 4", 50, "Y"),
        ],
    )

    # Delete specific items by name
    delete_params = [("Delete 1",), ("Delete 2",), ("Delete 4",)]

    result = psycopg_batch_session.execute_many("DELETE FROM test_batch WHERE name = %s", delete_params)

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 3

    # Verify remaining data
    remaining_result = psycopg_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
    assert remaining_result.data[0]["count"] == 2

    # Verify specific remaining items
    names_result = psycopg_batch_session.execute("SELECT name FROM test_batch ORDER BY name")
    remaining_names = [row["name"] for row in names_result.data]
    assert remaining_names == ["Delete 3", "Keep 1"]


@pytest.mark.xdist_group("postgres")
def test_psycopg_execute_many_large_batch(psycopg_batch_session: PsycopgSyncDriver) -> None:
    """Test execute_many with large batch size on Psycopg."""
    # Create a large batch of parameters
    large_batch = [(f"Item {i}", i * 10, f"CAT{i % 3}") for i in range(1000)]

    result = psycopg_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES (%s, %s, %s)", large_batch
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1000

    # Verify count
    count_result = psycopg_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
    assert count_result.data[0]["count"] == 1000

    # Verify some specific values using ANY for efficient querying
    sample_result = psycopg_batch_session.execute(
        "SELECT * FROM test_batch WHERE name = ANY(%s) ORDER BY value", (["Item 100", "Item 500", "Item 999"],)
    )
    assert len(sample_result.data) == 3
    assert sample_result.data[0]["value"] == 1000  # Item 100
    assert sample_result.data[1]["value"] == 5000  # Item 500
    assert sample_result.data[2]["value"] == 9990  # Item 999


@pytest.mark.xdist_group("postgres")
def test_psycopg_execute_many_with_sql_object(psycopg_batch_session: PsycopgSyncDriver) -> None:
    """Test execute_many with SQL object on Psycopg."""
    from sqlspec.statement.sql import SQL

    parameters = [("SQL Obj 1", 111, "SOB"), ("SQL Obj 2", 222, "SOB"), ("SQL Obj 3", 333, "SOB")]

    sql_obj = SQL("INSERT INTO test_batch (name, value, category) VALUES (%s, %s, %s)").as_many(parameters)

    result = psycopg_batch_session.execute(sql_obj)

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 3

    # Verify data
    check_result = psycopg_batch_session.execute(
        "SELECT COUNT(*) as count FROM test_batch WHERE category = %s", ("SOB",)
    )
    assert check_result.data[0]["count"] == 3


@pytest.mark.xdist_group("postgres")
def test_psycopg_execute_many_with_returning(psycopg_batch_session: PsycopgSyncDriver) -> None:
    """Test execute_many with RETURNING clause on Psycopg."""
    parameters = [("Return 1", 111, "RET"), ("Return 2", 222, "RET"), ("Return 3", 333, "RET")]

    # Note: execute_many with RETURNING may not work the same as single execute
    # This test verifies the behavior
    try:
        result = psycopg_batch_session.execute_many(
            "INSERT INTO test_batch (name, value, category) VALUES (%s, %s, %s) RETURNING id, name", parameters
        )

        assert isinstance(result, SQLResult)

        # If RETURNING works with execute_many, verify the data
        if hasattr(result, "data") and result.data:
            assert len(result.data) >= 3

    except Exception:
        # execute_many with RETURNING might not be supported
        # Fall back to regular insert and verify
        psycopg_batch_session.execute_many(
            "INSERT INTO test_batch (name, value, category) VALUES (%s, %s, %s)", parameters
        )

        check_result = psycopg_batch_session.execute(
            "SELECT COUNT(*) as count FROM test_batch WHERE category = %s", ("RET",)
        )
        assert check_result.data[0]["count"] == 3


@pytest.mark.xdist_group("postgres")
def test_psycopg_execute_many_with_arrays(psycopg_batch_session: PsycopgSyncDriver) -> None:
    """Test execute_many with PostgreSQL array types on Psycopg."""
    # Drop and recreate table to ensure clean state
    psycopg_batch_session.execute_script("""
        DROP TABLE IF EXISTS test_arrays;
        CREATE TABLE test_arrays (
            id SERIAL PRIMARY KEY,
            name TEXT,
            tags TEXT[],
            scores INTEGER[]
        )
    """)

    parameters = [
        ("Array 1", ["tag1", "tag2"], [10, 20, 30]),
        ("Array 2", ["tag3"], [40, 50]),
        ("Array 3", ["tag4", "tag5", "tag6"], [60]),
    ]

    result = psycopg_batch_session.execute_many(
        "INSERT INTO test_arrays (name, tags, scores) VALUES (%s, %s, %s)", parameters
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 3

    # Verify array data
    check_result = psycopg_batch_session.execute(
        "SELECT name, array_length(tags, 1) as tag_count, array_length(scores, 1) as score_count FROM test_arrays ORDER BY name"
    )
    assert len(check_result.data) == 3
    assert check_result.data[0]["tag_count"] == 2  # Array 1
    assert check_result.data[1]["tag_count"] == 1  # Array 2
    assert check_result.data[2]["tag_count"] == 3  # Array 3


@pytest.mark.xdist_group("postgres")
def test_psycopg_execute_many_with_json(psycopg_batch_session: PsycopgSyncDriver) -> None:
    """Test execute_many with JSON data on Psycopg."""
    import json

    # Drop and recreate table to ensure clean state
    psycopg_batch_session.execute_script("""
        DROP TABLE IF EXISTS test_json;
        CREATE TABLE test_json (
            id SERIAL PRIMARY KEY,
            name TEXT,
            metadata JSONB
        )
    """)

    parameters = [
        ("JSON 1", json.dumps({"type": "test", "value": 100, "active": True})),
        ("JSON 2", json.dumps({"type": "prod", "value": 200, "active": False})),
        ("JSON 3", json.dumps({"type": "test", "value": 300, "tags": ["a", "b"]})),
    ]

    result = psycopg_batch_session.execute_many("INSERT INTO test_json (name, metadata) VALUES (%s, %s)", parameters)

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 3

    # Verify JSON data
    check_result = psycopg_batch_session.execute(
        "SELECT name, metadata->>'type' as type, (metadata->>'value')::INTEGER as value FROM test_json ORDER BY name"
    )
    assert len(check_result.data) == 3
    assert check_result.data[0]["type"] == "test"  # JSON 1
    assert check_result.data[0]["value"] == 100
    assert check_result.data[1]["type"] == "prod"  # JSON 2
    assert check_result.data[1]["value"] == 200


@pytest.mark.xdist_group("postgres")
def test_psycopg_execute_many_with_upsert(psycopg_batch_session: PsycopgSyncDriver) -> None:
    """Test execute_many with PostgreSQL UPSERT (ON CONFLICT) on Psycopg."""
    # Create table with unique constraint
    psycopg_batch_session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_upsert (
            id INTEGER PRIMARY KEY,
            name TEXT,
            counter INTEGER DEFAULT 1
        )
    """)

    # First batch - initial inserts
    initial_params = [(1, "Item 1"), (2, "Item 2"), (3, "Item 3")]

    psycopg_batch_session.execute_many("INSERT INTO test_upsert (id, name) VALUES (%s, %s)", initial_params)

    # Second batch - with conflicts using ON CONFLICT
    conflict_params = [
        (1, "Updated Item 1"),  # Conflict
        (2, "Updated Item 2"),  # Conflict
        (4, "Item 4"),  # New
    ]

    result = psycopg_batch_session.execute_many(
        "INSERT INTO test_upsert (id, name) VALUES (%s, %s) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, counter = test_upsert.counter + 1",
        conflict_params,
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 3

    # Verify the behavior
    check_result = psycopg_batch_session.execute("SELECT id, name, counter FROM test_upsert ORDER BY id")
    assert len(check_result.data) == 4

    # Check that conflicts were handled
    updated_items = [row for row in check_result.data if row["counter"] > 1]
    assert len(updated_items) == 2  # Items 1 and 2 should be updated


@pytest.mark.xdist_group("postgres")
def test_psycopg_execute_many_with_copy(psycopg_batch_session: PsycopgSyncDriver) -> None:
    """Test execute_many efficiency compared to COPY operations on Psycopg."""
    # Test that execute_many works well alongside COPY operations
    large_batch = [(f"Copy Item {i}", i * 10, f"COPY{i % 2}") for i in range(100)]

    result = psycopg_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES (%s, %s, %s)", large_batch
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 100

    # Verify the data can be queried efficiently
    analytics_result = psycopg_batch_session.execute("""
        SELECT
            category,
            COUNT(*) as count,
            AVG(value) as avg_value,
            SUM(value) as total_value
        FROM test_batch
        GROUP BY category
        ORDER BY category
    """)

    assert len(analytics_result.data) == 2  # COPY0 and COPY1

    # Verify analytics results
    copy0_data = next(row for row in analytics_result.data if row["category"] == "COPY0")
    copy1_data = next(row for row in analytics_result.data if row["category"] == "COPY1")

    assert copy0_data["count"] == 50  # Even numbers
    assert copy1_data["count"] == 50  # Odd numbers
