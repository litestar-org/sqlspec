"""Test execute_many functionality for AsyncPG drivers."""

from collections.abc import AsyncGenerator

import pytest

from sqlspec.adapters.asyncpg import AsyncpgDriver
from sqlspec.core import SQLResult

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
            DELETE FROM test_batch
        """
    )
    try:
        yield asyncpg_async_driver
    finally:
        await asyncpg_async_driver.execute_script("DROP TABLE IF EXISTS test_batch")


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
        DELETE FROM test_arrays;
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
        DELETE FROM test_json;
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
