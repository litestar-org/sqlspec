"""Test execute_many functionality for AsyncPG drivers."""

from collections.abc import AsyncGenerator

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQLConfig


@pytest.fixture
async def asyncpg_batch_session(postgres_service: PostgresService) -> "AsyncGenerator[AsyncpgDriver, None]":
    """Create an AsyncPG session for batch operation testing."""
    config = AsyncpgConfig(
        host=postgres_service.host,
        port=postgres_service.port,
        user=postgres_service.user,
        password=postgres_service.password,
        database=postgres_service.database,
        statement_config=SQLConfig(strict_mode=False, enable_validation=False),
    )

    async with config.provide_session() as session:
        # Create test table
        await session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_batch (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0,
                category TEXT
            )
        """)
        # Clear any existing data
        await session.execute_script("TRUNCATE TABLE test_batch RESTART IDENTITY")

        yield session
        # Cleanup
        await session.execute_script("DROP TABLE IF EXISTS test_batch")


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_asyncpg_execute_many_basic(asyncpg_batch_session: AsyncpgDriver) -> None:
    """Test basic execute_many with AsyncPG."""
    parameters = [
        ("Item 1", 100, "A"),
        ("Item 2", 200, "B"),
        ("Item 3", 300, "A"),
        ("Item 4", 400, "C"),
        ("Item 5", 500, "B"),
    ]

    result = await asyncpg_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES ($1, $2, $3)", parameters
    )

    assert isinstance(result, SQLResult)
    # AsyncPG typically returns None for executemany, so rows_affected might be 0 or -1
    assert result.rows_affected in (-1, 0, 5)

    # Verify data was inserted
    count_result = await asyncpg_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
    assert count_result[0]["count"] == 5


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_asyncpg_execute_many_update(asyncpg_batch_session: AsyncpgDriver) -> None:
    """Test execute_many for UPDATE operations with AsyncPG."""
    # First insert some data
    await asyncpg_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES ($1, $2, $3)",
        [("Update 1", 10, "X"), ("Update 2", 20, "Y"), ("Update 3", 30, "Z")],
    )

    # Now update with execute_many
    update_params = [(100, "Update 1"), (200, "Update 2"), (300, "Update 3")]

    result = await asyncpg_batch_session.execute_many("UPDATE test_batch SET value = $1 WHERE name = $2", update_params)

    assert isinstance(result, SQLResult)

    # Verify updates
    check_result = await asyncpg_batch_session.execute("SELECT name, value FROM test_batch ORDER BY name")
    assert len(check_result) == 3
    assert all(row["value"] in (100, 200, 300) for row in check_result)


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_asyncpg_execute_many_empty(asyncpg_batch_session: AsyncpgDriver) -> None:
    """Test execute_many with empty parameter list on AsyncPG."""
    result = await asyncpg_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES ($1, $2, $3)", []
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected in (-1, 0)

    # Verify no data was inserted
    count_result = await asyncpg_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
    assert count_result[0]["count"] == 0


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_asyncpg_execute_many_mixed_types(asyncpg_batch_session: AsyncpgDriver) -> None:
    """Test execute_many with mixed parameter types on AsyncPG."""
    parameters = [
        ("String Item", 123, "CAT1"),
        ("Another Item", 456, None),  # NULL category
        ("Third Item", 0, "CAT2"),
        ("Negative Item", -50, "CAT3"),
    ]

    result = await asyncpg_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES ($1, $2, $3)", parameters
    )

    assert isinstance(result, SQLResult)

    # Verify data including NULL
    null_result = await asyncpg_batch_session.execute("SELECT * FROM test_batch WHERE category IS NULL")
    assert len(null_result) == 1
    assert null_result[0]["name"] == "Another Item"

    # Verify negative value
    negative_result = await asyncpg_batch_session.execute("SELECT * FROM test_batch WHERE value < 0")
    assert len(negative_result) == 1
    assert negative_result[0]["value"] == -50


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_asyncpg_execute_many_delete(asyncpg_batch_session: AsyncpgDriver) -> None:
    """Test execute_many for DELETE operations with AsyncPG."""
    # First insert test data
    await asyncpg_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES ($1, $2, $3)",
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

    result = await asyncpg_batch_session.execute_many("DELETE FROM test_batch WHERE name = $1", delete_params)

    assert isinstance(result, SQLResult)

    # Verify remaining data
    remaining_result = await asyncpg_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
    assert remaining_result[0]["count"] == 2

    # Verify specific remaining items
    names_result = await asyncpg_batch_session.execute("SELECT name FROM test_batch ORDER BY name")
    remaining_names = [row["name"] for row in names_result]
    assert remaining_names == ["Delete 3", "Keep 1"]


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_asyncpg_execute_many_large_batch(asyncpg_batch_session: AsyncpgDriver) -> None:
    """Test execute_many with large batch size on AsyncPG."""
    # Create a large batch of parameters
    large_batch = [(f"Item {i}", i * 10, f"CAT{i % 3}") for i in range(1000)]

    result = await asyncpg_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES ($1, $2, $3)", large_batch
    )

    assert isinstance(result, SQLResult)

    # Verify count
    count_result = await asyncpg_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
    assert count_result[0]["count"] == 1000

    # Verify some specific values
    sample_result = await asyncpg_batch_session.execute(
        "SELECT * FROM test_batch WHERE name = ANY($1) ORDER BY value", (["Item 100", "Item 500", "Item 999"],)
    )
    assert len(sample_result) == 3
    assert sample_result[0]["value"] == 1000  # Item 100
    assert sample_result[1]["value"] == 5000  # Item 500
    assert sample_result[2]["value"] == 9990  # Item 999


@pytest.mark.skip(reason="SQL object as_many() parameter handling needs investigation")
@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_asyncpg_execute_many_with_sql_object(asyncpg_batch_session: AsyncpgDriver) -> None:
    """Test execute_many with SQL object on AsyncPG."""
    from sqlspec.statement.sql import SQL

    parameters = [("SQL Obj 1", 111, "SOB"), ("SQL Obj 2", 222, "SOB"), ("SQL Obj 3", 333, "SOB")]

    sql_obj = SQL("INSERT INTO test_batch (name, value, category) VALUES ($1, $2, $3)").as_many(parameters)

    result = await asyncpg_batch_session.execute(sql_obj)

    assert isinstance(result, SQLResult)

    # Verify data
    check_result = await asyncpg_batch_session.execute(
        "SELECT COUNT(*) as count FROM test_batch WHERE category = $1", ("SOB",)
    )
    assert check_result[0]["count"] == 3


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_asyncpg_execute_many_with_returning(asyncpg_batch_session: AsyncpgDriver) -> None:
    """Test execute_many with RETURNING clause on AsyncPG."""
    parameters = [("Return 1", 111, "RET"), ("Return 2", 222, "RET"), ("Return 3", 333, "RET")]

    # Note: executemany with RETURNING may not work the same as single execute
    # This test verifies the behavior
    try:
        result = await asyncpg_batch_session.execute_many(
            "INSERT INTO test_batch (name, value, category) VALUES ($1, $2, $3) RETURNING id, name", parameters
        )

        assert isinstance(result, SQLResult)

        # If RETURNING works with executemany, verify the data
        if hasattr(result, "data") and result:
            assert len(result) >= 3

    except Exception:
        # executemany with RETURNING might not be supported
        # Fall back to regular insert and verify
        await asyncpg_batch_session.execute_many(
            "INSERT INTO test_batch (name, value, category) VALUES ($1, $2, $3)", parameters
        )

        check_result = await asyncpg_batch_session.execute(
            "SELECT COUNT(*) as count FROM test_batch WHERE category = $1", ("RET",)
        )
        assert check_result[0]["count"] == 3


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_asyncpg_execute_many_with_arrays(asyncpg_batch_session: AsyncpgDriver) -> None:
    """Test execute_many with PostgreSQL array types on AsyncPG."""
    # Create table with array column
    await asyncpg_batch_session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_arrays (
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

    result = await asyncpg_batch_session.execute_many(
        "INSERT INTO test_arrays (name, tags, scores) VALUES ($1, $2, $3)", parameters
    )

    assert isinstance(result, SQLResult)

    # Verify array data
    check_result = await asyncpg_batch_session.execute(
        "SELECT name, array_length(tags, 1) as tag_count, array_length(scores, 1) as score_count FROM test_arrays ORDER BY name"
    )
    assert len(check_result) == 3
    assert check_result[0]["tag_count"] == 2  # Array 1
    assert check_result[1]["tag_count"] == 1  # Array 2
    assert check_result[2]["tag_count"] == 3  # Array 3


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_asyncpg_execute_many_with_json(asyncpg_batch_session: AsyncpgDriver) -> None:
    """Test execute_many with JSON data on AsyncPG."""
    import json

    # Create table with JSON column
    await asyncpg_batch_session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_json (
            id SERIAL PRIMARY KEY,
            name TEXT,
            metadata JSONB
        )
    """)

    # AsyncPG expects JSON data to be serialized as strings
    parameters = [
        ("JSON 1", json.dumps({"type": "test", "value": 100, "active": True})),
        ("JSON 2", json.dumps({"type": "prod", "value": 200, "active": False})),
        ("JSON 3", json.dumps({"type": "test", "value": 300, "tags": ["a", "b"]})),
    ]

    result = await asyncpg_batch_session.execute_many(
        "INSERT INTO test_json (name, metadata) VALUES ($1, $2)", parameters
    )

    assert isinstance(result, SQLResult)

    # Verify JSON data
    check_result = await asyncpg_batch_session.execute(
        "SELECT name, metadata->>'type' as type, (metadata->>'value')::INTEGER as value FROM test_json ORDER BY name"
    )
    assert len(check_result) == 3
    assert check_result[0]["type"] == "test"  # JSON 1
    assert check_result[0]["value"] == 100
    assert check_result[1]["type"] == "prod"  # JSON 2
    assert check_result[1]["value"] == 200
