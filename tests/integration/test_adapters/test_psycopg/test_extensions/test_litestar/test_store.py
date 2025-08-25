"""Integration tests for Psycopg session store."""

import asyncio
import json
import math
import tempfile
import time
from pathlib import Path
from typing import Any

import pytest

from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.extensions.litestar import SQLSpecSessionStore
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands
from sqlspec.utils.sync_tools import async_, run_

pytestmark = [pytest.mark.psycopg, pytest.mark.postgres, pytest.mark.integration, pytest.mark.xdist_group("postgres")]


@pytest.fixture
def psycopg_sync_config(postgres_service, request: pytest.FixtureRequest) -> PsycopgSyncConfig:
    """Create Psycopg sync configuration for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create unique names for test isolation
        worker_id = getattr(request.config, "workerinput", {}).get("workerid", "master")
        table_suffix = f"{worker_id}_{abs(hash(request.node.nodeid)) % 100000}"
        migration_table = f"sqlspec_migrations_psycopg_sync_{table_suffix}"
        session_table = f"litestar_session_psycopg_sync_{table_suffix}"

        # Create a migration to create the session table
        migration_content = f'''"""Create test session table."""

def up():
    """Create the litestar_session table."""
    return [
        """
        CREATE TABLE IF NOT EXISTS {session_table} (
            session_id VARCHAR(255) PRIMARY KEY,
            data JSONB NOT NULL,
            expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_{session_table}_expires_at
        ON {session_table}(expires_at)
        """,
    ]

def down():
    """Drop the litestar_session table."""
    return [
        "DROP INDEX IF EXISTS idx_{session_table}_expires_at",
        "DROP TABLE IF EXISTS {session_table}",
    ]
'''
        migration_file = migration_dir / "0001_create_session_table.py"
        migration_file.write_text(migration_content)

        config = PsycopgSyncConfig(
            pool_config={
                "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
            },
            migration_config={"script_location": str(migration_dir), "version_table_name": migration_table},
        )
        # Run migrations to create the table
        commands = SyncMigrationCommands(config)
        commands.init(str(migration_dir), package=False)
        commands.upgrade()
        config._session_table_name = session_table  # Store for cleanup
        yield config

        # Cleanup: drop test tables and close pool
        try:
            with config.provide_session() as driver:
                driver.execute(f"DROP TABLE IF EXISTS {session_table}")
                driver.execute(f"DROP TABLE IF EXISTS {migration_table}")
        except Exception:
            pass  # Ignore cleanup errors

        if config.pool_instance:
            config.close_pool()


@pytest.fixture
async def psycopg_async_config(postgres_service, request: pytest.FixtureRequest) -> PsycopgAsyncConfig:
    """Create Psycopg async configuration for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create unique names for test isolation
        worker_id = getattr(request.config, "workerinput", {}).get("workerid", "master")
        table_suffix = f"{worker_id}_{abs(hash(request.node.nodeid)) % 100000}"
        migration_table = f"sqlspec_migrations_psycopg_async_{table_suffix}"
        session_table = f"litestar_session_psycopg_async_{table_suffix}"

        # Create a migration to create the session table
        migration_content = f'''"""Create test session table."""

def up():
    """Create the litestar_session table."""
    return [
        """
        CREATE TABLE IF NOT EXISTS {session_table} (
            session_id VARCHAR(255) PRIMARY KEY,
            data JSONB NOT NULL,
            expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_{session_table}_expires_at
        ON {session_table}(expires_at)
        """,
    ]

def down():
    """Drop the litestar_session table."""
    return [
        "DROP INDEX IF EXISTS idx_{session_table}_expires_at",
        "DROP TABLE IF EXISTS {session_table}",
    ]
'''
        migration_file = migration_dir / "0001_create_session_table.py"
        migration_file.write_text(migration_content)

        config = PsycopgAsyncConfig(
            pool_config={
                "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
            },
            migration_config={"script_location": str(migration_dir), "version_table_name": migration_table},
        )
        # Run migrations to create the table
        commands = AsyncMigrationCommands(config)
        await commands.init(str(migration_dir), package=False)
        await commands.upgrade()
        config._session_table_name = session_table  # Store for cleanup
        yield config

        # Cleanup: drop test tables and close pool
        try:
            async with config.provide_session() as driver:
                await driver.execute(f"DROP TABLE IF EXISTS {session_table}")
                await driver.execute(f"DROP TABLE IF EXISTS {migration_table}")
        except Exception:
            pass  # Ignore cleanup errors

        await config.close_pool()


@pytest.fixture
def sync_store(psycopg_sync_config: PsycopgSyncConfig) -> SQLSpecSessionStore:
    """Create a sync session store instance."""
    return SQLSpecSessionStore(
        config=psycopg_sync_config,
        table_name=getattr(psycopg_sync_config, "_session_table_name", "litestar_session"),
        session_id_column="session_id",
        data_column="data",
        expires_at_column="expires_at",
        created_at_column="created_at",
    )


@pytest.fixture
async def async_store(psycopg_async_config: PsycopgAsyncConfig) -> SQLSpecSessionStore:
    """Create an async session store instance."""
    return SQLSpecSessionStore(
        config=psycopg_async_config,
        table_name=getattr(psycopg_async_config, "_session_table_name", "litestar_session"),
        session_id_column="session_id",
        data_column="data",
        expires_at_column="expires_at",
        created_at_column="created_at",
    )


def test_psycopg_sync_store_table_creation(sync_store: SQLSpecSessionStore, psycopg_sync_config: PsycopgSyncConfig) -> None:
    """Test that store table is created automatically with sync driver."""
    with psycopg_sync_config.provide_session() as driver:
        # Verify table exists
        table_name = getattr(psycopg_sync_config, "_session_table_name", "litestar_session")
        result = driver.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = %s", (table_name,)
        )
        assert len(result.data) == 1
        assert result.data[0]["table_name"] == table_name

        # Verify table structure with PostgreSQL specific features
        result = driver.execute(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s", (table_name,)
        )
        columns = {row["column_name"]: row["data_type"] for row in result.data}
        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns
        assert "created_at" in columns

        # PostgreSQL specific: verify JSONB type
        assert columns["data"] == "jsonb"
        assert "timestamp" in columns["expires_at"].lower()


async def test_psycopg_async_store_table_creation(
    async_store: SQLSpecSessionStore, psycopg_async_config: PsycopgAsyncConfig
) -> None:
    """Test that store table is created automatically with async driver."""
    async with psycopg_async_config.provide_session() as driver:
        # Verify table exists
        table_name = getattr(psycopg_async_config, "_session_table_name", "litestar_session")
        result = await driver.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = %s", (table_name,)
        )
        assert len(result.data) == 1
        assert result.data[0]["table_name"] == table_name

        # Verify table structure with PostgreSQL specific features
        result = await driver.execute(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s", (table_name,)
        )
        columns = {row["column_name"]: row["data_type"] for row in result.data}
        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns
        assert "created_at" in columns

        # PostgreSQL specific: verify JSONB type
        assert columns["data"] == "jsonb"
        assert "timestamp" in columns["expires_at"].lower()


def test_psycopg_sync_store_crud_operations(sync_store: SQLSpecSessionStore) -> None:
    """Test complete CRUD operations on the sync store."""
    key = "test-key-psycopg-sync"
    value = {
        "user_id": 123,
        "data": ["item1", "item2", "postgres_sync"],
        "nested": {"key": "value", "postgres": True},
        "metadata": {"driver": "psycopg", "mode": "sync", "jsonb": True},
    }

    # Create
    run_(sync_store.set)(key, value, expires_in=3600)

    # Read
    retrieved = run_(sync_store.get)(key)
    assert retrieved == value

    # Update
    updated_value = {
        "user_id": 456,
        "new_field": "new_value",
        "postgres_features": ["JSONB", "ACID", "MVCC"],
        "metadata": {"driver": "psycopg", "mode": "sync", "updated": True},
    }
    run_(sync_store.set)(key, updated_value, expires_in=3600)

    retrieved = run_(sync_store.get)(key)
    assert retrieved == updated_value

    # Delete
    run_(sync_store.delete)(key)
    result = run_(sync_store.get)(key)
    assert result is None


async def test_psycopg_async_store_crud_operations(async_store: SQLSpecSessionStore) -> None:
    """Test complete CRUD operations on the async store."""
    key = "test-key-psycopg-async"
    value = {
        "user_id": 789,
        "data": ["item1", "item2", "postgres_async"],
        "nested": {"key": "value", "postgres": True},
        "metadata": {"driver": "psycopg", "mode": "async", "jsonb": True, "pool": True},
    }

    # Create
    await async_store.set(key, value, expires_in=3600)

    # Read
    retrieved = await async_store.get(key)
    assert retrieved == value

    # Update
    updated_value = {
        "user_id": 987,
        "new_field": "new_async_value",
        "postgres_features": ["JSONB", "ACID", "MVCC", "ASYNC"],
        "metadata": {"driver": "psycopg", "mode": "async", "updated": True, "pool": True},
    }
    await async_store.set(key, updated_value, expires_in=3600)

    retrieved = await async_store.get(key)
    assert retrieved == updated_value

    # Delete
    await async_store.delete(key)
    result = await async_store.get(key)
    assert result is None


def test_psycopg_sync_store_expiration(sync_store: SQLSpecSessionStore, psycopg_sync_config: PsycopgSyncConfig) -> None:
    """Test that expired entries are not returned with sync driver."""
    key = "expiring-key-psycopg-sync"
    value = {"test": "data", "driver": "psycopg_sync", "postgres": True}

    # Set with 1 second expiration
    run_(sync_store.set)(key, value, expires_in=1)

    # Should exist immediately
    result = run_(sync_store.get)(key)
    assert result == value

    # Check what's actually in the database
    table_name = getattr(psycopg_sync_config, "_session_table_name", "litestar_session")
    with psycopg_sync_config.provide_session() as driver:
        check_result = driver.execute(f"SELECT * FROM {table_name} WHERE session_id = %s", (key,))
        assert len(check_result.data) > 0

    # Wait for expiration (add buffer for timing issues)
    time.sleep(3)

    # Should be expired
    result = run_(sync_store.get)(key)
    assert result is None


async def test_psycopg_async_store_expiration(
    async_store: SQLSpecSessionStore, psycopg_async_config: PsycopgAsyncConfig
) -> None:
    """Test that expired entries are not returned with async driver."""
    key = "expiring-key-psycopg-async"
    value = {"test": "data", "driver": "psycopg_async", "postgres": True}

    # Set with 1 second expiration
    await async_store.set(key, value, expires_in=1)

    # Should exist immediately
    result = await async_store.get(key)
    assert result == value

    # Check what's actually in the database
    table_name = getattr(psycopg_async_config, "_session_table_name", "litestar_session")
    async with psycopg_async_config.provide_session() as driver:
        check_result = await driver.execute(f"SELECT * FROM {table_name} WHERE session_id = %s", (key,))
        assert len(check_result.data) > 0

    # Wait for expiration (add buffer for timing issues)
    await asyncio.sleep(3)

    # Should be expired
    result = await async_store.get(key)
    assert result is None


def test_psycopg_sync_store_default_values(sync_store: SQLSpecSessionStore) -> None:
    """Test default value handling with sync driver."""
    # Non-existent key should return None
    result = run_(sync_store.get)("non-existent-psycopg-sync")
    assert result is None

    # Test with our own default handling
    result = run_(sync_store.get)("non-existent-psycopg-sync")
    if result is None:
        result = {"default": True, "driver": "psycopg_sync"}
    assert result == {"default": True, "driver": "psycopg_sync"}


async def test_psycopg_async_store_default_values(async_store: SQLSpecSessionStore) -> None:
    """Test default value handling with async driver."""
    # Non-existent key should return None
    result = await async_store.get("non-existent-psycopg-async")
    assert result is None

    # Test with our own default handling
    result = await async_store.get("non-existent-psycopg-async")
    if result is None:
        result = {"default": True, "driver": "psycopg_async"}
    assert result == {"default": True, "driver": "psycopg_async"}


async def test_psycopg_sync_store_bulk_operations(sync_store: SQLSpecSessionStore) -> None:
    """Test bulk operations on the Psycopg sync store."""

    @async_
    async def run_bulk_test():
        # Create multiple entries efficiently
        entries = {}
        tasks = []
        for i in range(25):  # PostgreSQL can handle this efficiently
            key = f"psycopg-sync-bulk-{i}"
            value = {
                "index": i,
                "data": f"value-{i}",
                "metadata": {"created_by": "test", "batch": i // 5, "postgres": True},
                "postgres_info": {"driver": "psycopg", "mode": "sync", "jsonb": True},
            }
            entries[key] = value
            tasks.append(sync_store.set(key, value, expires_in=3600))

        # Execute all inserts concurrently
        await asyncio.gather(*tasks)

        # Verify all entries exist
        verify_tasks = [sync_store.get(key) for key in entries]
        results = await asyncio.gather(*verify_tasks)

        for (key, expected_value), result in zip(entries.items(), results):
            assert result == expected_value

        # Delete all entries concurrently
        delete_tasks = [sync_store.delete(key) for key in entries]
        await asyncio.gather(*delete_tasks)

        # Verify all are deleted
        verify_tasks = [sync_store.get(key) for key in entries]
        results = await asyncio.gather(*verify_tasks)
        assert all(result is None for result in results)

    await run_bulk_test()


async def test_psycopg_async_store_bulk_operations(async_store: SQLSpecSessionStore) -> None:
    """Test bulk operations on the Psycopg async store."""
    # Create multiple entries efficiently
    entries = {}
    tasks = []
    for i in range(30):  # PostgreSQL async can handle this well
        key = f"psycopg-async-bulk-{i}"
        value = {
            "index": i,
            "data": f"value-{i}",
            "metadata": {"created_by": "test", "batch": i // 6, "postgres": True},
            "postgres_info": {"driver": "psycopg", "mode": "async", "jsonb": True, "pool": True},
        }
        entries[key] = value
        tasks.append(async_store.set(key, value, expires_in=3600))

    # Execute all inserts concurrently
    await asyncio.gather(*tasks)

    # Verify all entries exist
    verify_tasks = [async_store.get(key) for key in entries]
    results = await asyncio.gather(*verify_tasks)

    for (key, expected_value), result in zip(entries.items(), results):
        assert result == expected_value

    # Delete all entries concurrently
    delete_tasks = [async_store.delete(key) for key in entries]
    await asyncio.gather(*delete_tasks)

    # Verify all are deleted
    verify_tasks = [async_store.get(key) for key in entries]
    results = await asyncio.gather(*verify_tasks)
    assert all(result is None for result in results)


def test_psycopg_sync_store_large_data(sync_store: SQLSpecSessionStore) -> None:
    """Test storing large data structures in Psycopg sync store."""
    # Create a large data structure that tests PostgreSQL's JSONB capabilities
    large_data = {
        "users": [
            {
                "id": i,
                "name": f"user_{i}",
                "email": f"user{i}@postgres.com",
                "profile": {
                    "bio": f"Bio text for user {i} with PostgreSQL " + "x" * 100,
                    "tags": [f"tag_{j}" for j in range(10)],
                    "settings": {f"setting_{j}": j for j in range(20)},
                    "postgres_metadata": {"jsonb": True, "driver": "psycopg", "mode": "sync"},
                },
            }
            for i in range(100)  # Test PostgreSQL capacity
        ],
        "analytics": {
            "metrics": {
                f"metric_{i}": {"value": i * 1.5, "timestamp": f"2024-01-{i:02d}"} for i in range(1, 32)
            },
            "events": [{"type": f"event_{i}", "data": "x" * 300, "postgres": True} for i in range(50)],
            "postgres_info": {"jsonb_support": True, "gin_indexes": True, "btree_indexes": True},
        },
        "postgres_metadata": {
            "driver": "psycopg",
            "version": "3.x",
            "mode": "sync",
            "features": ["JSONB", "ACID", "MVCC", "WAL"],
        },
    }

    key = "psycopg-sync-large-data"
    run_(sync_store.set)(key, large_data, expires_in=3600)

    # Retrieve and verify
    retrieved = run_(sync_store.get)(key)
    assert retrieved == large_data
    assert len(retrieved["users"]) == 100
    assert len(retrieved["analytics"]["metrics"]) == 31
    assert len(retrieved["analytics"]["events"]) == 50
    assert retrieved["postgres_metadata"]["driver"] == "psycopg"


async def test_psycopg_async_store_large_data(async_store: SQLSpecSessionStore) -> None:
    """Test storing large data structures in Psycopg async store."""
    # Create a large data structure that tests PostgreSQL's JSONB capabilities
    large_data = {
        "users": [
            {
                "id": i,
                "name": f"async_user_{i}",
                "email": f"user{i}@postgres-async.com",
                "profile": {
                    "bio": f"Bio text for async user {i} with PostgreSQL " + "x" * 120,
                    "tags": [f"async_tag_{j}" for j in range(12)],
                    "settings": {f"async_setting_{j}": j for j in range(25)},
                    "postgres_metadata": {"jsonb": True, "driver": "psycopg", "mode": "async", "pool": True},
                },
            }
            for i in range(120)  # Test PostgreSQL async capacity
        ],
        "analytics": {
            "metrics": {
                f"async_metric_{i}": {"value": i * 2.5, "timestamp": f"2024-01-{i:02d}"} for i in range(1, 32)
            },
            "events": [{"type": f"async_event_{i}", "data": "y" * 350, "postgres": True} for i in range(60)],
            "postgres_info": {"jsonb_support": True, "gin_indexes": True, "concurrent": True},
        },
        "postgres_metadata": {
            "driver": "psycopg",
            "version": "3.x",
            "mode": "async",
            "features": ["JSONB", "ACID", "MVCC", "WAL", "CONNECTION_POOLING"],
        },
    }

    key = "psycopg-async-large-data"
    await async_store.set(key, large_data, expires_in=3600)

    # Retrieve and verify
    retrieved = await async_store.get(key)
    assert retrieved == large_data
    assert len(retrieved["users"]) == 120
    assert len(retrieved["analytics"]["metrics"]) == 31
    assert len(retrieved["analytics"]["events"]) == 60
    assert retrieved["postgres_metadata"]["driver"] == "psycopg"
    assert "CONNECTION_POOLING" in retrieved["postgres_metadata"]["features"]


async def test_psycopg_sync_store_concurrent_access(sync_store: SQLSpecSessionStore) -> None:
    """Test concurrent access to the Psycopg sync store."""

    async def update_value(key: str, value: int) -> None:
        """Update a value in the store."""
        await sync_store.set(
            key,
            {"value": value, "operation": f"update_{value}", "postgres": "sync", "jsonb": True},
            expires_in=3600,
        )

    @async_
    async def run_concurrent_test():
        # Create many concurrent updates to test PostgreSQL's concurrency handling
        key = "psycopg-sync-concurrent-key"
        tasks = [update_value(key, i) for i in range(50)]
        await asyncio.gather(*tasks)

        # The last update should win (PostgreSQL handles this well)
        result = await sync_store.get(key)
        assert result is not None
        assert "value" in result
        assert 0 <= result["value"] <= 49
        assert "operation" in result
        assert result["postgres"] == "sync"
        assert result["jsonb"] is True

    await run_concurrent_test()


async def test_psycopg_async_store_concurrent_access(async_store: SQLSpecSessionStore) -> None:
    """Test concurrent access to the Psycopg async store."""

    async def update_value(key: str, value: int) -> None:
        """Update a value in the store."""
        await async_store.set(
            key,
            {"value": value, "operation": f"update_{value}", "postgres": "async", "jsonb": True, "pool": True},
            expires_in=3600,
        )

    # Create many concurrent updates to test PostgreSQL async's concurrency handling
    key = "psycopg-async-concurrent-key"
    tasks = [update_value(key, i) for i in range(60)]
    await asyncio.gather(*tasks)

    # The last update should win (PostgreSQL handles this well)
    result = await async_store.get(key)
    assert result is not None
    assert "value" in result
    assert 0 <= result["value"] <= 59
    assert "operation" in result
    assert result["postgres"] == "async"
    assert result["jsonb"] is True
    assert result["pool"] is True


def test_psycopg_sync_store_get_all(sync_store: SQLSpecSessionStore) -> None:
    """Test retrieving all entries from the sync store."""

    # Create multiple entries with different expiration times
    run_(sync_store.set)("sync_key1", {"data": 1, "postgres": "sync"}, expires_in=3600)
    run_(sync_store.set)("sync_key2", {"data": 2, "postgres": "sync"}, expires_in=3600)
    run_(sync_store.set)("sync_key3", {"data": 3, "postgres": "sync"}, expires_in=1)  # Will expire soon

    # Get all entries - need to consume async generator
    async def collect_all() -> dict[str, Any]:
        return {key: value async for key, value in sync_store.get_all()}

    all_entries = asyncio.run(collect_all())

    # Should have all three initially
    assert len(all_entries) >= 2  # At least the non-expiring ones
    if "sync_key1" in all_entries:
        assert all_entries["sync_key1"] == {"data": 1, "postgres": "sync"}
    if "sync_key2" in all_entries:
        assert all_entries["sync_key2"] == {"data": 2, "postgres": "sync"}

    # Wait for one to expire
    time.sleep(3)

    # Get all again
    all_entries = asyncio.run(collect_all())

    # Should only have non-expired entries
    assert "sync_key1" in all_entries
    assert "sync_key2" in all_entries
    assert "sync_key3" not in all_entries  # Should be expired


async def test_psycopg_async_store_get_all(async_store: SQLSpecSessionStore) -> None:
    """Test retrieving all entries from the async store."""

    # Create multiple entries with different expiration times
    await async_store.set("async_key1", {"data": 1, "postgres": "async"}, expires_in=3600)
    await async_store.set("async_key2", {"data": 2, "postgres": "async"}, expires_in=3600)
    await async_store.set("async_key3", {"data": 3, "postgres": "async"}, expires_in=1)  # Will expire soon

    # Get all entries - consume async generator
    async def collect_all() -> dict[str, Any]:
        return {key: value async for key, value in async_store.get_all()}

    all_entries = await collect_all()

    # Should have all three initially
    assert len(all_entries) >= 2  # At least the non-expiring ones
    if "async_key1" in all_entries:
        assert all_entries["async_key1"] == {"data": 1, "postgres": "async"}
    if "async_key2" in all_entries:
        assert all_entries["async_key2"] == {"data": 2, "postgres": "async"}

    # Wait for one to expire
    await asyncio.sleep(3)

    # Get all again
    all_entries = await collect_all()

    # Should only have non-expired entries
    assert "async_key1" in all_entries
    assert "async_key2" in all_entries
    assert "async_key3" not in all_entries  # Should be expired


def test_psycopg_sync_store_delete_expired(sync_store: SQLSpecSessionStore) -> None:
    """Test deletion of expired entries with sync driver."""
    # Create entries with different expiration times
    run_(sync_store.set)("sync_short1", {"data": 1, "postgres": "sync"}, expires_in=1)
    run_(sync_store.set)("sync_short2", {"data": 2, "postgres": "sync"}, expires_in=1)
    run_(sync_store.set)("sync_long1", {"data": 3, "postgres": "sync"}, expires_in=3600)
    run_(sync_store.set)("sync_long2", {"data": 4, "postgres": "sync"}, expires_in=3600)

    # Wait for short-lived entries to expire (add buffer)
    time.sleep(3)

    # Delete expired entries
    run_(sync_store.delete_expired)()

    # Check which entries remain
    assert run_(sync_store.get)("sync_short1") is None
    assert run_(sync_store.get)("sync_short2") is None
    assert run_(sync_store.get)("sync_long1") == {"data": 3, "postgres": "sync"}
    assert run_(sync_store.get)("sync_long2") == {"data": 4, "postgres": "sync"}


async def test_psycopg_async_store_delete_expired(async_store: SQLSpecSessionStore) -> None:
    """Test deletion of expired entries with async driver."""
    # Create entries with different expiration times
    await async_store.set("async_short1", {"data": 1, "postgres": "async"}, expires_in=1)
    await async_store.set("async_short2", {"data": 2, "postgres": "async"}, expires_in=1)
    await async_store.set("async_long1", {"data": 3, "postgres": "async"}, expires_in=3600)
    await async_store.set("async_long2", {"data": 4, "postgres": "async"}, expires_in=3600)

    # Wait for short-lived entries to expire (add buffer)
    await asyncio.sleep(3)

    # Delete expired entries
    await async_store.delete_expired()

    # Check which entries remain
    assert await async_store.get("async_short1") is None
    assert await async_store.get("async_short2") is None
    assert await async_store.get("async_long1") == {"data": 3, "postgres": "async"}
    assert await async_store.get("async_long2") == {"data": 4, "postgres": "async"}


def test_psycopg_sync_store_special_characters(sync_store: SQLSpecSessionStore) -> None:
    """Test handling of special characters in keys and values with Psycopg sync."""
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
        "key::postgres::namespace",  # PostgreSQL namespace style
    ]

    for key in special_keys:
        value = {"key": key, "postgres": "sync", "driver": "psycopg", "jsonb": True}
        run_(sync_store.set)(key, value, expires_in=3600)
        retrieved = run_(sync_store.get)(key)
        assert retrieved == value

    # Test PostgreSQL-specific data types and special characters in values
    special_value = {
        "unicode": "PostgreSQL: 🐘 База данных データベース ฐานข้อมูล",
        "emoji": "🚀🎉😊💾🔥💻🐘📊",
        "quotes": "He said \"hello\" and 'goodbye' and `backticks` and PostgreSQL",
        "newlines": "line1\nline2\r\nline3\npostgres",
        "tabs": "col1\tcol2\tcol3\tpostgres",
        "special": "!@#$%^&*()[]{}|\\<>?,./;':\"",
        "postgres_arrays": [1, 2, 3, [4, 5, [6, 7]], {"jsonb": True}],
        "postgres_json": {"nested": {"deep": {"value": 42, "postgres": True}}},
        "null_handling": {"null": None, "not_null": "value", "postgres": "sync"},
        "escape_chars": "\\n\\t\\r\\b\\f",
        "sql_injection_attempt": "'; DROP TABLE test; --",  # Should be safely handled
        "boolean_types": {"true": True, "false": False, "postgres": True},
        "numeric_types": {"int": 123, "float": 123.456, "pi": math.pi},
        "postgres_specific": {
            "jsonb_ops": True,
            "gin_index": True,
            "btree_index": True,
            "uuid": "550e8400-e29b-41d4-a716-446655440000",
        },
    }

    run_(sync_store.set)("psycopg-sync-special-value", special_value, expires_in=3600)
    retrieved = run_(sync_store.get)("psycopg-sync-special-value")
    assert retrieved == special_value
    assert retrieved["null_handling"]["null"] is None
    assert retrieved["postgres_arrays"][3] == [4, 5, [6, 7]]
    assert retrieved["boolean_types"]["true"] is True
    assert retrieved["numeric_types"]["pi"] == math.pi
    assert retrieved["postgres_specific"]["jsonb_ops"] is True


async def test_psycopg_async_store_special_characters(async_store: SQLSpecSessionStore) -> None:
    """Test handling of special characters in keys and values with Psycopg async."""
    # Test special characters in keys (PostgreSQL specific)
    special_keys = [
        "async-key-with-dash",
        "async_key_with_underscore",
        "async.key.with.dots",
        "async:key:with:colons",
        "async/key/with/slashes",
        "async@key@with@at",
        "async#key#with#hash",
        "async$key$with$dollar",
        "async%key%with%percent",
        "async&key&with&ampersand",
        "async'key'with'quote",  # Single quote
        'async"key"with"doublequote',  # Double quote
        "async::postgres::namespace",  # PostgreSQL namespace style
    ]

    for key in special_keys:
        value = {"key": key, "postgres": "async", "driver": "psycopg", "jsonb": True, "pool": True}
        await async_store.set(key, value, expires_in=3600)
        retrieved = await async_store.get(key)
        assert retrieved == value

    # Test PostgreSQL-specific data types and special characters in values
    special_value = {
        "unicode": "PostgreSQL Async: 🐘 База данных データベース ฐานข้อมูล",
        "emoji": "🚀🎉😊💾🔥💻🐘📊⚡",
        "quotes": "He said \"hello\" and 'goodbye' and `backticks` and PostgreSQL async",
        "newlines": "line1\nline2\r\nline3\nasync_postgres",
        "tabs": "col1\tcol2\tcol3\tasync_postgres",
        "special": "!@#$%^&*()[]{}|\\<>?,./;':\"~`",
        "postgres_arrays": [1, 2, 3, [4, 5, [6, 7]], {"jsonb": True, "async": True}],
        "postgres_json": {"nested": {"deep": {"value": 42, "postgres": "async"}}},
        "null_handling": {"null": None, "not_null": "value", "postgres": "async"},
        "escape_chars": "\\n\\t\\r\\b\\f",
        "sql_injection_attempt": "'; DROP TABLE test; --",  # Should be safely handled
        "boolean_types": {"true": True, "false": False, "postgres": "async"},
        "numeric_types": {"int": 456, "float": 456.789, "pi": math.pi},
        "postgres_specific": {
            "jsonb_ops": True,
            "gin_index": True,
            "btree_index": True,
            "async_pool": True,
            "uuid": "550e8400-e29b-41d4-a716-446655440001",
        },
    }

    await async_store.set("psycopg-async-special-value", special_value, expires_in=3600)
    retrieved = await async_store.get("psycopg-async-special-value")
    assert retrieved == special_value
    assert retrieved["null_handling"]["null"] is None
    assert retrieved["postgres_arrays"][3] == [4, 5, [6, 7]]
    assert retrieved["boolean_types"]["true"] is True
    assert retrieved["numeric_types"]["pi"] == math.pi
    assert retrieved["postgres_specific"]["async_pool"] is True


def test_psycopg_sync_store_exists_and_expires_in(sync_store: SQLSpecSessionStore) -> None:
    """Test exists and expires_in functionality with sync driver."""
    key = "psycopg-sync-exists-test"
    value = {"test": "data", "postgres": "sync"}

    # Test non-existent key
    assert run_(sync_store.exists)(key) is False
    assert run_(sync_store.expires_in)(key) == 0

    # Set key
    run_(sync_store.set)(key, value, expires_in=3600)

    # Test existence
    assert run_(sync_store.exists)(key) is True
    expires_in = run_(sync_store.expires_in)(key)
    assert 3590 <= expires_in <= 3600  # Should be close to 3600

    # Delete and test again
    run_(sync_store.delete)(key)
    assert run_(sync_store.exists)(key) is False
    assert run_(sync_store.expires_in)(key) == 0


async def test_psycopg_async_store_exists_and_expires_in(async_store: SQLSpecSessionStore) -> None:
    """Test exists and expires_in functionality with async driver."""
    key = "psycopg-async-exists-test"
    value = {"test": "data", "postgres": "async"}

    # Test non-existent key
    assert await async_store.exists(key) is False
    assert await async_store.expires_in(key) == 0

    # Set key
    await async_store.set(key, value, expires_in=3600)

    # Test existence
    assert await async_store.exists(key) is True
    expires_in = await async_store.expires_in(key)
    assert 3590 <= expires_in <= 3600  # Should be close to 3600

    # Delete and test again
    await async_store.delete(key)
    assert await async_store.exists(key) is False
    assert await async_store.expires_in(key) == 0


async def test_psycopg_sync_store_postgresql_features(
    sync_store: SQLSpecSessionStore, psycopg_sync_config: PsycopgSyncConfig
) -> None:
    """Test PostgreSQL-specific features with sync driver."""

    @async_
    async def test_jsonb_operations():
        # Test JSONB-specific operations
        key = "psycopg-sync-jsonb-test"
        complex_data = {
            "user": {
                "id": 123,
                "profile": {
                    "name": "John Postgres",
                    "settings": {"theme": "dark", "notifications": True},
                    "tags": ["admin", "user", "postgres"],
                },
            },
            "metadata": {"created": "2024-01-01", "jsonb": True, "driver": "psycopg_sync"},
        }

        # Store complex data
        await sync_store.set(key, complex_data, expires_in=3600)

        # Test direct JSONB queries to verify data is stored as JSONB
        table_name = getattr(psycopg_sync_config, "_session_table_name", "litestar_session")
        with psycopg_sync_config.provide_session() as driver:
            # Query JSONB field directly using PostgreSQL JSONB operators
            result = driver.execute(
                f"SELECT data->>'user' as user_data FROM {table_name} WHERE session_id = %s", (key,)
            )
            assert len(result.data) == 1

            user_data = json.loads(result.data[0]["user_data"])
            assert user_data["id"] == 123
            assert user_data["profile"]["name"] == "John Postgres"
            assert "admin" in user_data["profile"]["tags"]

            # Test JSONB contains operator
            result = driver.execute(
                f"SELECT session_id FROM {table_name} WHERE data @> %s",
                ('{"metadata": {"jsonb": true}}',),
            )
            assert len(result.data) == 1
            assert result.data[0]["session_id"] == key

    await test_jsonb_operations()


async def test_psycopg_async_store_postgresql_features(
    async_store: SQLSpecSessionStore, psycopg_async_config: PsycopgAsyncConfig
) -> None:
    """Test PostgreSQL-specific features with async driver."""
    # Test JSONB-specific operations
    key = "psycopg-async-jsonb-test"
    complex_data = {
        "user": {
            "id": 456,
            "profile": {
                "name": "Jane PostgresAsync",
                "settings": {"theme": "light", "notifications": False},
                "tags": ["editor", "reviewer", "postgres_async"],
            },
        },
        "metadata": {"created": "2024-01-01", "jsonb": True, "driver": "psycopg_async", "pool": True},
    }

    # Store complex data
    await async_store.set(key, complex_data, expires_in=3600)

    # Test direct JSONB queries to verify data is stored as JSONB
    table_name = getattr(psycopg_async_config, "_session_table_name", "litestar_session")
    async with psycopg_async_config.provide_session() as driver:
        # Query JSONB field directly using PostgreSQL JSONB operators
        result = await driver.execute(
            f"SELECT data->>'user' as user_data FROM {table_name} WHERE session_id = %s", (key,)
        )
        assert len(result.data) == 1

        user_data = json.loads(result.data[0]["user_data"])
        assert user_data["id"] == 456
        assert user_data["profile"]["name"] == "Jane PostgresAsync"
        assert "postgres_async" in user_data["profile"]["tags"]

        # Test JSONB contains operator
        result = await driver.execute(
            f"SELECT session_id FROM {table_name} WHERE data @> %s",
            ('{"metadata": {"jsonb": true}}',),
        )
        assert len(result.data) == 1
        assert result.data[0]["session_id"] == key

        # Test async-specific JSONB query
        result = await driver.execute(
            f"SELECT session_id FROM {table_name} WHERE data @> %s",
            ('{"metadata": {"pool": true}}',),
        )
        assert len(result.data) == 1
        assert result.data[0]["session_id"] == key


async def test_psycopg_store_transaction_behavior(
    async_store: SQLSpecSessionStore, psycopg_async_config: PsycopgAsyncConfig
) -> None:
    """Test transaction-like behavior in PostgreSQL store operations."""
    key = "psycopg-transaction-test"

    # Set initial value
    await async_store.set(key, {"counter": 0, "postgres": "transaction_test"}, expires_in=3600)

    async def increment_counter() -> None:
        """Increment counter in a transaction-like manner."""
        current = await async_store.get(key)
        if current:
            current["counter"] += 1
            current["postgres"] = "transaction_updated"
            await async_store.set(key, current, expires_in=3600)

    # Run multiple increments concurrently (PostgreSQL will handle this)
    tasks = [increment_counter() for _ in range(10)]
    await asyncio.gather(*tasks)

    # Final count should be 10 (PostgreSQL handles concurrent updates well)
    result = await async_store.get(key)
    assert result is not None
    assert "counter" in result
    assert result["counter"] == 10
    assert result["postgres"] == "transaction_updated"
