"""Integration tests for AsyncPG session store."""

import asyncio
import math

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.asyncpg.config import AsyncpgConfig
from sqlspec.extensions.litestar import SQLSpecSessionStore

pytestmark = [pytest.mark.asyncpg, pytest.mark.postgres, pytest.mark.integration, pytest.mark.xdist_group("postgres")]


@pytest.fixture
async def asyncpg_config(postgres_service: PostgresService) -> AsyncpgConfig:
    """Create AsyncPG configuration for testing."""
    return AsyncpgConfig(
        pool_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "database": postgres_service.database,
            "min_size": 2,
            "max_size": 10,
        }
    )


@pytest.fixture
async def store(asyncpg_config: AsyncpgConfig) -> SQLSpecSessionStore:
    """Create a session store instance."""
    # Create the table manually since we're not using migrations here
    async with asyncpg_config.provide_session() as driver:
        await driver.execute_script("""CREATE TABLE IF NOT EXISTS test_store_asyncpg (
            key TEXT PRIMARY KEY,
            value JSONB NOT NULL,
            expires TIMESTAMP WITH TIME ZONE NOT NULL,
            created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
        )""")
        await driver.execute_script(
            "CREATE INDEX IF NOT EXISTS idx_test_store_asyncpg_expires ON test_store_asyncpg(expires)"
        )

    return SQLSpecSessionStore(
        config=asyncpg_config,
        table_name="test_store_asyncpg",
        session_id_column="key",
        data_column="value",
        expires_at_column="expires",
        created_at_column="created",
    )


async def test_asyncpg_store_table_creation(store: SQLSpecSessionStore, asyncpg_config: AsyncpgConfig) -> None:
    """Test that store table is created automatically with proper structure."""
    async with asyncpg_config.provide_session() as driver:
        # Verify table exists
        result = await driver.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = 'test_store_asyncpg'
        """)
        assert len(result.data) == 1
        assert result.data[0]["table_name"] == "test_store_asyncpg"

        # Verify table structure
        result = await driver.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public'
            AND table_name = 'test_store_asyncpg'
            ORDER BY ordinal_position
        """)
        columns = {row["column_name"]: row["data_type"] for row in result.data}
        assert "key" in columns
        assert "value" in columns
        assert "expires" in columns
        assert "created" in columns

        # Verify index on key column
        result = await driver.execute("""
            SELECT indexname
            FROM pg_indexes
            WHERE tablename = 'test_store_asyncpg'
            AND indexdef LIKE '%UNIQUE%'
        """)
        assert len(result.data) > 0  # Should have unique index on key


async def test_asyncpg_store_crud_operations(store: SQLSpecSessionStore) -> None:
    """Test complete CRUD operations on the AsyncPG store."""
    key = "asyncpg-test-key"
    value = {
        "user_id": 999,
        "data": ["item1", "item2", "item3"],
        "nested": {"key": "value", "number": 123.45},
        "postgres_specific": {"json": True, "array": [1, 2, 3]},
    }

    # Create
    await store.set(key, value, expires_in=3600)

    # Read
    retrieved = await store.get(key)
    assert retrieved == value
    assert retrieved["postgres_specific"]["json"] is True

    # Update with new structure
    updated_value = {
        "user_id": 1000,
        "new_field": "new_value",
        "postgres_types": {"boolean": True, "null": None, "float": math.pi},
    }
    await store.set(key, updated_value, expires_in=3600)

    retrieved = await store.get(key)
    assert retrieved == updated_value
    assert retrieved["postgres_types"]["null"] is None

    # Delete
    await store.delete(key)
    result = await store.get(key)
    assert result is None


async def test_asyncpg_store_expiration(store: SQLSpecSessionStore) -> None:
    """Test that expired entries are not returned from AsyncPG."""
    key = "asyncpg-expiring-key"
    value = {"test": "postgres_data", "expires": True}

    # Set with 1 second expiration
    await store.set(key, value, expires_in=1)

    # Should exist immediately
    result = await store.get(key)
    assert result == value

    # Wait for expiration
    await asyncio.sleep(2)

    # Should be expired
    result = await store.get(key)
    assert result is None


async def test_asyncpg_store_bulk_operations(store: SQLSpecSessionStore) -> None:
    """Test bulk operations on the AsyncPG store."""
    # Create multiple entries efficiently
    entries = {}
    tasks = []
    for i in range(50):  # More entries to test PostgreSQL performance
        key = f"asyncpg-bulk-{i}"
        value = {"index": i, "data": f"value-{i}", "metadata": {"created_by": "test", "batch": i // 10}}
        entries[key] = value
        tasks.append(store.set(key, value, expires_in=3600))

    # Execute all inserts concurrently
    await asyncio.gather(*tasks)

    # Verify all entries exist
    verify_tasks = [store.get(key) for key in entries]
    results = await asyncio.gather(*verify_tasks)

    for (key, expected_value), result in zip(entries.items(), results):
        assert result == expected_value

    # Delete all entries concurrently
    delete_tasks = [store.delete(key) for key in entries]
    await asyncio.gather(*delete_tasks)

    # Verify all are deleted
    verify_tasks = [store.get(key) for key in entries]
    results = await asyncio.gather(*verify_tasks)
    assert all(result is None for result in results)


async def test_asyncpg_store_large_data(store: SQLSpecSessionStore) -> None:
    """Test storing large data structures in AsyncPG."""
    # Create a large data structure that tests PostgreSQL's JSONB capabilities
    large_data = {
        "users": [
            {
                "id": i,
                "name": f"user_{i}",
                "email": f"user{i}@example.com",
                "profile": {
                    "bio": f"Bio text for user {i} " + "x" * 100,
                    "tags": [f"tag_{j}" for j in range(10)],
                    "settings": {f"setting_{j}": j for j in range(20)},
                },
            }
            for i in range(200)  # More users to test PostgreSQL capacity
        ],
        "analytics": {
            "metrics": {f"metric_{i}": {"value": i * 1.5, "timestamp": f"2024-01-{i:02d}"} for i in range(1, 32)},
            "events": [{"type": f"event_{i}", "data": "x" * 500} for i in range(100)],
        },
    }

    key = "asyncpg-large-data"
    await store.set(key, large_data, expires_in=3600)

    # Retrieve and verify
    retrieved = await store.get(key)
    assert retrieved == large_data
    assert len(retrieved["users"]) == 200
    assert len(retrieved["analytics"]["metrics"]) == 31
    assert len(retrieved["analytics"]["events"]) == 100


async def test_asyncpg_store_concurrent_access(store: SQLSpecSessionStore) -> None:
    """Test concurrent access to the AsyncPG store."""

    async def update_value(key: str, value: int) -> None:
        """Update a value in the store."""
        await store.set(key, {"value": value, "thread": asyncio.current_task().get_name()}, expires_in=3600)  # pyright: ignore

    # Create many concurrent updates to test PostgreSQL's concurrency handling
    key = "asyncpg-concurrent-key"
    tasks = [update_value(key, i) for i in range(100)]  # More concurrent updates
    await asyncio.gather(*tasks)

    # The last update should win
    result = await store.get(key)
    assert result is not None
    assert "value" in result
    assert 0 <= result["value"] <= 99
    assert "thread" in result


async def test_asyncpg_store_get_all(store: SQLSpecSessionStore) -> None:
    """Test retrieving all entries from the AsyncPG store."""
    # Create multiple entries with different expiration times
    test_entries = {
        "asyncpg-all-1": ({"data": 1, "type": "persistent"}, 3600),
        "asyncpg-all-2": ({"data": 2, "type": "persistent"}, 3600),
        "asyncpg-all-3": ({"data": 3, "type": "temporary"}, 1),
        "asyncpg-all-4": ({"data": 4, "type": "persistent"}, 3600),
    }

    for key, (value, expires_in) in test_entries.items():
        await store.set(key, value, expires_in=expires_in)

    # Get all entries
    all_entries = {key: value async for key, value in store.get_all() if key.startswith("asyncpg-all-")}

    # Should have all four initially
    assert len(all_entries) >= 3  # At least the non-expiring ones
    assert all_entries.get("asyncpg-all-1") == {"data": 1, "type": "persistent"}
    assert all_entries.get("asyncpg-all-2") == {"data": 2, "type": "persistent"}

    # Wait for one to expire
    await asyncio.sleep(2)

    # Get all again
    all_entries = {}
    async for key, value in store.get_all():
        if key.startswith("asyncpg-all-"):
            all_entries[key] = value

    # Should only have non-expired entries
    assert "asyncpg-all-1" in all_entries
    assert "asyncpg-all-2" in all_entries
    assert "asyncpg-all-3" not in all_entries  # Should be expired
    assert "asyncpg-all-4" in all_entries


async def test_asyncpg_store_delete_expired(store: SQLSpecSessionStore) -> None:
    """Test deletion of expired entries in AsyncPG."""
    # Create entries with different expiration times
    short_lived = ["asyncpg-short-1", "asyncpg-short-2", "asyncpg-short-3"]
    long_lived = ["asyncpg-long-1", "asyncpg-long-2"]

    for key in short_lived:
        await store.set(key, {"data": key, "ttl": "short"}, expires_in=1)

    for key in long_lived:
        await store.set(key, {"data": key, "ttl": "long"}, expires_in=3600)

    # Wait for short-lived entries to expire
    await asyncio.sleep(2)

    # Delete expired entries
    await store.delete_expired()

    # Check which entries remain
    for key in short_lived:
        assert await store.get(key) is None

    for key in long_lived:
        result = await store.get(key)
        assert result is not None
        assert result["ttl"] == "long"


async def test_asyncpg_store_special_characters(store: SQLSpecSessionStore) -> None:
    """Test handling of special characters in keys and values with AsyncPG."""
    # Test special characters in keys (PostgreSQL specific)
    special_keys = [
        "key-with-dash",
        "key_with_underscore",
        "key.with.dots",
        "key:with:colons",
        "key/with/slashes",
        "key@with@at",
        "key#with#hash",
        "key$with$dollar",
        "key%with%percent",
        "key&with&ampersand",
        "key'with'quote",  # Single quote
        'key"with"doublequote',  # Double quote
    ]

    for key in special_keys:
        value = {"key": key, "postgres": True}
        await store.set(key, value, expires_in=3600)
        retrieved = await store.get(key)
        assert retrieved == value

    # Test PostgreSQL-specific data types and special characters in values
    special_value = {
        "unicode": "PostgreSQL: 🐘 База данных データベース",
        "emoji": "🚀🎉😊🐘🔥💻",
        "quotes": "He said \"hello\" and 'goodbye' and `backticks`",
        "newlines": "line1\nline2\r\nline3",
        "tabs": "col1\tcol2\tcol3",
        "special": "!@#$%^&*()[]{}|\\<>?,./",
        "postgres_arrays": [1, 2, 3, [4, 5, [6, 7]]],
        "postgres_json": {"nested": {"deep": {"value": 42}}},
        "null_handling": {"null": None, "not_null": "value"},
        "escape_chars": "\\n\\t\\r\\b\\f",
        "sql_injection_attempt": "'; DROP TABLE test; --",  # Should be safely handled
    }

    await store.set("asyncpg-special-value", special_value, expires_in=3600)
    retrieved = await store.get("asyncpg-special-value")
    assert retrieved == special_value
    assert retrieved["null_handling"]["null"] is None
    assert retrieved["postgres_arrays"][3] == [4, 5, [6, 7]]


async def test_asyncpg_store_transaction_isolation(store: SQLSpecSessionStore, asyncpg_config: AsyncpgConfig) -> None:
    """Test transaction isolation in AsyncPG store operations."""
    key = "asyncpg-transaction-test"

    # Set initial value
    await store.set(key, {"counter": 0}, expires_in=3600)

    async def increment_counter() -> None:
        """Increment counter in a transaction-like manner."""
        current = await store.get(key)
        if current:
            current["counter"] += 1
            await store.set(key, current, expires_in=3600)

    # Run multiple concurrent increments
    tasks = [increment_counter() for _ in range(20)]
    await asyncio.gather(*tasks)

    # Due to the non-transactional nature, the final count might not be 20
    # but it should be set to some value
    result = await store.get(key)
    assert result is not None
    assert "counter" in result
    assert result["counter"] > 0  # At least one increment should have succeeded
