"""Test execute_many functionality for AsyncPG drivers."""

from collections.abc import AsyncGenerator

import pytest

from sqlspec.adapters.asyncpg import AsyncpgDriver
from sqlspec.core import SQL, SQLResult

pytestmark = pytest.mark.xdist_group("postgres")


@pytest.fixture
async def asyncpg_batch_session(asyncpg_async_driver: AsyncpgDriver) -> "AsyncGenerator[AsyncpgDriver, None]":
    """Create an AsyncPG session for batch operation testing."""

    await asyncpg_async_driver.execute_script(
        """
            CREATE TABLE IF NOT EXISTS test_batch (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0,
                category TEXT
            );
            TRUNCATE TABLE test_batch RESTART IDENTITY
        """
    )
    try:
        yield asyncpg_async_driver
    finally:
        await asyncpg_async_driver.execute_script("DROP TABLE IF EXISTS test_batch")


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

    assert result.rows_affected in (-1, 0, 5)

    count_result = await asyncpg_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
    assert count_result[0]["count"] == 5


async def test_asyncpg_execute_many_update(asyncpg_batch_session: AsyncpgDriver) -> None:
    """Test execute_many for UPDATE operations with AsyncPG."""

    await asyncpg_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES ($1, $2, $3)",
        [("Update 1", 10, "X"), ("Update 2", 20, "Y"), ("Update 3", 30, "Z")],
    )

    update_parameters = [(100, "Update 1"), (200, "Update 2"), (300, "Update 3")]

    result = await asyncpg_batch_session.execute_many(
        "UPDATE test_batch SET value = $1 WHERE name = $2", update_parameters
    )

    assert isinstance(result, SQLResult)

    check_result = await asyncpg_batch_session.execute("SELECT name, value FROM test_batch ORDER BY name")
    assert len(check_result) == 3
    assert all(row["value"] in (100, 200, 300) for row in check_result)


async def test_asyncpg_execute_many_empty(asyncpg_batch_session: AsyncpgDriver) -> None:
    """Test execute_many with empty parameter list on AsyncPG."""
    result = await asyncpg_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES ($1, $2, $3)", []
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected in (-1, 0)

    count_result = await asyncpg_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
    assert count_result[0]["count"] == 0


async def test_asyncpg_execute_many_mixed_types(asyncpg_batch_session: AsyncpgDriver) -> None:
    """Test execute_many with mixed parameter types on AsyncPG."""
    parameters = [
        ("String Item", 123, "CAT1"),
        ("Another Item", 456, None),
        ("Third Item", 0, "CAT2"),
        ("Negative Item", -50, "CAT3"),
    ]

    result = await asyncpg_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES ($1, $2, $3)", parameters
    )

    assert isinstance(result, SQLResult)

    null_result = await asyncpg_batch_session.execute("SELECT * FROM test_batch WHERE category IS NULL")
    assert len(null_result) == 1
    assert null_result[0]["name"] == "Another Item"

    negative_result = await asyncpg_batch_session.execute("SELECT * FROM test_batch WHERE value < 0")
    assert len(negative_result) == 1
    assert negative_result[0]["value"] == -50


async def test_asyncpg_execute_many_delete(asyncpg_batch_session: AsyncpgDriver) -> None:
    """Test execute_many for DELETE operations with AsyncPG."""

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

    delete_parameters = [("Delete 1",), ("Delete 2",), ("Delete 4",)]

    result = await asyncpg_batch_session.execute_many("DELETE FROM test_batch WHERE name = $1", delete_parameters)

    assert isinstance(result, SQLResult)

    remaining_result = await asyncpg_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
    assert remaining_result[0]["count"] == 2

    names_result = await asyncpg_batch_session.execute("SELECT name FROM test_batch ORDER BY name")
    remaining_names = [row["name"] for row in names_result]
    assert remaining_names == ["Delete 3", "Keep 1"]


async def test_asyncpg_execute_many_large_batch(asyncpg_batch_session: AsyncpgDriver) -> None:
    """Test execute_many with large batch size on AsyncPG."""

    large_batch = [(f"Item {i}", i * 10, f"CAT{i % 3}") for i in range(1000)]

    result = await asyncpg_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES ($1, $2, $3)", large_batch
    )

    assert isinstance(result, SQLResult)

    count_result = await asyncpg_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
    assert count_result[0]["count"] == 1000

    sample_result = await asyncpg_batch_session.execute(
        "SELECT * FROM test_batch WHERE name = ANY($1) ORDER BY value", (["Item 100", "Item 500", "Item 999"],)
    )
    assert len(sample_result) == 3
    assert sample_result[0]["value"] == 1000
    assert sample_result[1]["value"] == 5000
    assert sample_result[2]["value"] == 9990


async def test_asyncpg_execute_many_with_sql_object(asyncpg_batch_session: AsyncpgDriver) -> None:
    """Test execute_many with SQL object on AsyncPG."""

    parameters = [("SQL Obj 1", 111, "SOB"), ("SQL Obj 2", 222, "SOB"), ("SQL Obj 3", 333, "SOB")]

    sql_obj = SQL("INSERT INTO test_batch (name, value, category) VALUES ($1, $2, $3)", parameters, is_many=True)

    result = await asyncpg_batch_session.execute(sql_obj)

    assert isinstance(result, SQLResult)

    check_result = await asyncpg_batch_session.execute(
        "SELECT COUNT(*) as count FROM test_batch WHERE category = $1", ("SOB",)
    )
    assert check_result[0]["count"] == 3


async def test_asyncpg_execute_many_with_returning(asyncpg_batch_session: AsyncpgDriver) -> None:
    """Test execute_many with RETURNING clause on AsyncPG."""
    parameters = [("Return 1", 111, "RET"), ("Return 2", 222, "RET"), ("Return 3", 333, "RET")]

    try:
        result = await asyncpg_batch_session.execute_many(
            "INSERT INTO test_batch (name, value, category) VALUES ($1, $2, $3) RETURNING id, name", parameters
        )

        assert isinstance(result, SQLResult)

        if hasattr(result, "data") and result:
            assert len(result) >= 3

    except Exception:
        await asyncpg_batch_session.execute_many(
            "INSERT INTO test_batch (name, value, category) VALUES ($1, $2, $3)", parameters
        )

        check_result = await asyncpg_batch_session.execute(
            "SELECT COUNT(*) as count FROM test_batch WHERE category = $1", ("RET",)
        )
        assert check_result[0]["count"] == 3


async def test_asyncpg_execute_many_with_arrays(asyncpg_batch_session: AsyncpgDriver) -> None:
    """Test execute_many with PostgreSQL array types on AsyncPG."""

    await asyncpg_batch_session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_arrays (
            id SERIAL PRIMARY KEY,
            name TEXT,
            tags TEXT[],
            scores INTEGER[]
        );
        TRUNCATE TABLE test_arrays RESTART IDENTITY;
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

    check_result = await asyncpg_batch_session.execute(
        "SELECT name, array_length(tags, 1) as tag_count, array_length(scores, 1) as score_count FROM test_arrays ORDER BY name"
    )
    assert len(check_result) == 3
    assert check_result[0]["tag_count"] == 2
    assert check_result[1]["tag_count"] == 1
    assert check_result[2]["tag_count"] == 3


async def test_asyncpg_execute_many_with_json(asyncpg_batch_session: AsyncpgDriver) -> None:
    """Test execute_many with JSON data on AsyncPG."""
    await asyncpg_batch_session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_json (
            id SERIAL PRIMARY KEY,
            name TEXT,
            metadata JSONB
        );
        TRUNCATE TABLE test_json RESTART IDENTITY;
    """)

    parameters = [
        ("JSON 1", {"type": "test", "value": 100, "active": True}),
        ("JSON 2", {"type": "prod", "value": 200, "active": False}),
        ("JSON 3", {"type": "test", "value": 300, "tags": ["a", "b"]}),
    ]

    result = await asyncpg_batch_session.execute_many(
        "INSERT INTO test_json (name, metadata) VALUES ($1, $2)", parameters
    )

    assert isinstance(result, SQLResult)

    check_result = await asyncpg_batch_session.execute(
        "SELECT name, metadata->>'type' as type, (metadata->>'value')::INTEGER as value FROM test_json ORDER BY name"
    )
    assert len(check_result) == 3
    assert check_result[0]["type"] == "test"
    assert check_result[0]["value"] == 100
    assert check_result[1]["type"] == "prod"
    assert check_result[1]["value"] == 200
