"""Integration tests for AsyncMy (MySQL) session store."""

import asyncio

import pytest

from sqlspec.adapters.asyncmy.config import AsyncmyConfig
from sqlspec.extensions.litestar import SQLSpecSessionStore

pytestmark = [pytest.mark.asyncmy, pytest.mark.mysql, pytest.mark.integration]


@pytest.fixture
async def asyncmy_config() -> AsyncmyConfig:
    """Create AsyncMy configuration for testing."""
    return AsyncmyConfig(
        pool_config={
            "host": "localhost",
            "port": 3306,
            "user": "root",
            "password": "password",
            "database": "test",
            "minsize": 2,
            "maxsize": 10,
        }
    )


@pytest.fixture
async def store(asyncmy_config: AsyncmyConfig) -> SQLSpecSessionStore:
    """Create a session store instance."""
    store = SQLSpecSessionStore(
        config=asyncmy_config,
        table_name="test_store_mysql",
        session_id_column="session_key",
        data_column="session_data",
        expires_at_column="expires_at",
        created_at_column="created_at",
    )
    # Ensure table exists
    async with asyncmy_config.provide_session() as driver:
        await store._ensure_table_exists(driver)
    return store


async def test_mysql_store_table_creation(store: SQLSpecSessionStore, asyncmy_config: AsyncmyConfig) -> None:
    """Test that store table is created automatically with proper structure."""
    async with asyncmy_config.provide_session() as driver:
        # Verify table exists
        result = await driver.execute("""
            SELECT TABLE_NAME 
            FROM information_schema.TABLES 
            WHERE TABLE_SCHEMA = 'test' 
            AND TABLE_NAME = 'test_store_mysql'
        """)
        assert len(result.data) == 1
        assert result.data[0]["TABLE_NAME"] == "test_store_mysql"

        # Verify table structure
        result = await driver.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_SET_NAME 
            FROM information_schema.COLUMNS 
            WHERE TABLE_SCHEMA = 'test' 
            AND TABLE_NAME = 'test_store_mysql'
            ORDER BY ORDINAL_POSITION
        """)
        columns = {row["COLUMN_NAME"]: row["DATA_TYPE"] for row in result.data}
        assert "session_key" in columns
        assert "session_data" in columns
        assert "expires_at" in columns
        assert "created_at" in columns

        # Verify UTF8MB4 charset for text columns
        for row in result.data:
            if row["DATA_TYPE"] in ("varchar", "text", "longtext"):
                assert row["CHARACTER_SET_NAME"] == "utf8mb4"


async def test_mysql_store_crud_operations(store: SQLSpecSessionStore) -> None:
    """Test complete CRUD operations on the MySQL store."""
    key = "mysql-test-key"
    value = {
        "user_id": 777,
        "cart": ["item1", "item2", "item3"],
        "preferences": {"lang": "en", "currency": "USD"},
        "mysql_specific": {"json_field": True, "decimal": 123.45},
    }

    # Create
    await store.set(key, value, expires_in=3600)

    # Read
    retrieved = await store.get(key)
    assert retrieved == value
    assert retrieved["mysql_specific"]["decimal"] == 123.45

    # Update
    updated_value = {"user_id": 888, "new_field": "mysql_update", "datetime": "2024-01-01 12:00:00"}
    await store.set(key, updated_value, expires_in=3600)

    retrieved = await store.get(key)
    assert retrieved == updated_value
    assert retrieved["datetime"] == "2024-01-01 12:00:00"

    # Delete
    await store.delete(key)
    result = await store.get(key)
    assert result is None


async def test_mysql_store_expiration(store: SQLSpecSessionStore) -> None:
    """Test that expired entries are not returned from MySQL."""
    key = "mysql-expiring-key"
    value = {"test": "mysql_data", "engine": "InnoDB"}

    # Set with 1 second expiration
    await store.set(key, value, expires_in=1)

    # Should exist immediately
    result = await store.get(key)
    assert result == value

    # Wait for expiration
    await asyncio.sleep(2)

    # Should be expired
    result = await store.get(key, default={"expired": True})
    assert result == {"expired": True}


async def test_mysql_store_bulk_operations(store: SQLSpecSessionStore) -> None:
    """Test bulk operations on the MySQL store."""
    # Create multiple entries
    entries = {}
    tasks = []
    for i in range(30):  # Test MySQL's concurrent handling
        key = f"mysql-bulk-{i}"
        value = {"index": i, "data": f"value-{i}", "metadata": {"created": "2024-01-01", "category": f"cat-{i % 5}"}}
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


async def test_mysql_store_large_data(store: SQLSpecSessionStore) -> None:
    """Test storing large data structures in MySQL."""
    # Create a large data structure that tests MySQL's JSON and TEXT capabilities
    large_data = {
        "users": [
            {
                "id": i,
                "name": f"user_{i}",
                "email": f"user{i}@example.com",
                "profile": {
                    "bio": f"Bio text for user {i} " + "x" * 200,  # Large text
                    "tags": [f"tag_{j}" for j in range(20)],
                    "settings": {f"setting_{j}": {"value": j, "enabled": j % 2 == 0} for j in range(30)},
                },
            }
            for i in range(100)  # Test MySQL's capacity
        ],
        "logs": [{"timestamp": f"2024-01-{i:02d}", "message": "Log entry " * 50} for i in range(1, 32)],
    }

    key = "mysql-large-data"
    await store.set(key, large_data, expires_in=3600)

    # Retrieve and verify
    retrieved = await store.get(key)
    assert retrieved == large_data
    assert len(retrieved["users"]) == 100
    assert len(retrieved["logs"]) == 31


async def test_mysql_store_concurrent_access(store: SQLSpecSessionStore) -> None:
    """Test concurrent access to the MySQL store with transactions."""

    async def update_value(key: str, value: int) -> None:
        """Update a value in the store."""
        await store.set(
            key, {"value": value, "thread_id": value, "timestamp": f"2024-01-01T{value:02d}:00:00"}, expires_in=3600
        )

    # Create many concurrent updates to test MySQL's locking
    key = "mysql-concurrent-key"
    tasks = [update_value(key, i) for i in range(50)]
    await asyncio.gather(*tasks)

    # The last update should win
    result = await store.get(key)
    assert result is not None
    assert "value" in result
    assert 0 <= result["value"] <= 49


async def test_mysql_store_get_all(store: SQLSpecSessionStore) -> None:
    """Test retrieving all entries from the MySQL store."""
    # Create multiple entries
    test_entries = {
        "mysql-all-1": ({"data": 1, "status": "active"}, 3600),
        "mysql-all-2": ({"data": 2, "status": "active"}, 3600),
        "mysql-all-3": ({"data": 3, "status": "pending"}, 1),
        "mysql-all-4": ({"data": 4, "status": "active"}, 3600),
    }

    for key, (value, expires_in) in test_entries.items():
        await store.set(key, value, expires_in=expires_in)

    # Get all entries
    all_entries = {}
    async for key, value in store.get_all():
        if key.startswith("mysql-all-"):
            all_entries[key] = value

    # Should have all four initially
    assert len(all_entries) >= 3
    assert all_entries.get("mysql-all-1") == {"data": 1, "status": "active"}
    assert all_entries.get("mysql-all-2") == {"data": 2, "status": "active"}

    # Wait for one to expire
    await asyncio.sleep(2)

    # Get all again
    all_entries = {}
    async for key, value in store.get_all():
        if key.startswith("mysql-all-"):
            all_entries[key] = value

    # Should only have non-expired entries
    assert "mysql-all-1" in all_entries
    assert "mysql-all-2" in all_entries
    assert "mysql-all-3" not in all_entries
    assert "mysql-all-4" in all_entries


async def test_mysql_store_delete_expired(store: SQLSpecSessionStore) -> None:
    """Test deletion of expired entries in MySQL."""
    # Create entries with different TTLs
    short_lived = ["mysql-short-1", "mysql-short-2", "mysql-short-3"]
    long_lived = ["mysql-long-1", "mysql-long-2"]

    for key in short_lived:
        await store.set(key, {"ttl": "short", "key": key}, expires_in=1)

    for key in long_lived:
        await store.set(key, {"ttl": "long", "key": key}, expires_in=3600)

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


async def test_mysql_store_utf8mb4_characters(store: SQLSpecSessionStore) -> None:
    """Test handling of UTF8MB4 characters and emojis in MySQL."""
    # Test UTF8MB4 characters in keys
    special_keys = ["key-with-emoji-🚀", "key-with-chinese-你好", "key-with-arabic-مرحبا", "key-with-special-♠♣♥♦"]

    for key in special_keys:
        value = {"key": key, "mysql": True}
        await store.set(key, value, expires_in=3600)
        retrieved = await store.get(key)
        assert retrieved == value

    # Test MySQL-specific data with UTF8MB4
    special_value = {
        "unicode": "MySQL: 🐬 база данных 数据库 ডাটাবেস",
        "emoji_collection": "🚀🎉😊🐬🔥💻🌟🎨🎭🎪",
        "mysql_quotes": "He said \"hello\" and 'goodbye' and `backticks`",
        "special_chars": "!@#$%^&*()[]{}|\\<>?,./±§©®™",
        "json_data": {"nested": {"emoji": "🐬", "text": "MySQL supports JSON"}},
        "null_values": [None, "not_null", None],
        "escape_sequences": "\\n\\t\\r\\b\\f\\'\\\"\\\\",
        "sql_safe": "'; DROP TABLE test; --",  # Should be safely handled
        "utf8mb4_only": "𝐇𝐞𝐥𝐥𝐨 𝕎𝕠𝕣𝕝𝕕 🏴󠁧󠁢󠁳󠁣󠁴󠁿",  # 4-byte UTF-8 characters
    }

    await store.set("mysql-utf8mb4-value", special_value, expires_in=3600)
    retrieved = await store.get("mysql-utf8mb4-value")
    assert retrieved == special_value
    assert retrieved["null_values"][0] is None
    assert retrieved["utf8mb4_only"] == "𝐇𝐞𝐥𝐥𝐨 𝕎𝕠𝕣𝕝𝕕 🏴󠁧󠁢󠁳󠁣󠁴󠁿"
