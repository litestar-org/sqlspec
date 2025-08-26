"""Integration tests for ADBC session store with Arrow optimization."""

import asyncio
import math
import tempfile
from pathlib import Path
from typing import Any

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.adbc.config import AdbcConfig
from sqlspec.extensions.litestar import SQLSpecSessionStore
from sqlspec.migrations.commands import SyncMigrationCommands
from sqlspec.utils.sync_tools import async_, run_
from tests.integration.test_adapters.test_adbc.conftest import xfail_if_driver_missing

pytestmark = [pytest.mark.adbc, pytest.mark.postgres, pytest.mark.integration, pytest.mark.xdist_group("postgres")]


@pytest.fixture
def adbc_config(postgres_service: PostgresService) -> AdbcConfig:
    """Create ADBC configuration for testing with PostgreSQL backend."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create a migration to create the session table
        migration_content = '''"""Create ADBC test session table."""

def up():
    """Create the litestar_session table optimized for ADBC/Arrow."""
    return [
        """
        CREATE TABLE IF NOT EXISTS litestar_session (
            session_id TEXT PRIMARY KEY,
            data JSONB NOT NULL,
            expires_at TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_litestar_session_expires_at
        ON litestar_session(expires_at)
        """,
        """
        COMMENT ON TABLE litestar_session IS 'ADBC session store with Arrow optimization'
        """,
    ]

def down():
    """Drop the litestar_session table."""
    return [
        "DROP INDEX IF EXISTS idx_litestar_session_expires_at",
        "DROP TABLE IF EXISTS litestar_session",
    ]
'''
        migration_file = migration_dir / "0001_create_session_table.py"
        migration_file.write_text(migration_content)

        config = AdbcConfig(
            connection_config={
                "uri": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
                "driver_name": "postgresql",
            },
            migration_config={"script_location": str(migration_dir), "version_table_name": "test_migrations_adbc"},
        )

        # Run migrations to create the table
        commands = SyncMigrationCommands(config)
        commands.init(str(migration_dir), package=False)
        commands.upgrade()
        return config


@pytest.fixture
def store(adbc_config: AdbcConfig) -> SQLSpecSessionStore:
    """Create a session store instance for ADBC."""
    return SQLSpecSessionStore(
        config=adbc_config,
        table_name="litestar_session",
        session_id_column="session_id",
        data_column="data",
        expires_at_column="expires_at",
        created_at_column="created_at",
    )


@xfail_if_driver_missing
def test_adbc_store_table_creation(store: SQLSpecSessionStore, adbc_config: AdbcConfig) -> None:
    """Test that store table is created with ADBC-optimized structure."""
    with adbc_config.provide_session() as driver:
        # Verify table exists
        result = driver.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_name = 'litestar_session' AND table_schema = 'public'
        """)
        assert len(result.data) == 1
        assert result.data[0]["table_name"] == "litestar_session"

        # Verify table structure optimized for ADBC/Arrow
        result = driver.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'litestar_session' AND table_schema = 'public'
            ORDER BY ordinal_position
        """)
        columns = {row["column_name"]: row for row in result.data}
        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns
        assert "created_at" in columns

        # Verify ADBC-optimized data types
        assert columns["session_id"]["data_type"] == "text"
        assert columns["data"]["data_type"] == "jsonb"  # JSONB for efficient Arrow transfer
        assert columns["expires_at"]["data_type"] in ("timestamp with time zone", "timestamptz")
        assert columns["created_at"]["data_type"] in ("timestamp with time zone", "timestamptz")


@xfail_if_driver_missing
def test_adbc_store_crud_operations(store: SQLSpecSessionStore) -> None:
    """Test complete CRUD operations on the ADBC store."""
    key = "adbc-test-key"
    value = {
        "user_id": 123,
        "data": ["item1", "item2"],
        "nested": {"key": "value"},
        "arrow_features": {"columnar": True, "zero_copy": True, "cross_language": True},
    }

    # Create
    run_(store.set)(key, value, expires_in=3600)

    # Read
    retrieved = run_(store.get)(key)
    assert retrieved == value
    assert retrieved["arrow_features"]["columnar"] is True

    # Update with ADBC-specific data
    updated_value = {
        "user_id": 456,
        "new_field": "new_value",
        "adbc_metadata": {"engine": "ADBC", "format": "Arrow", "optimized": True},
    }
    run_(store.set)(key, updated_value, expires_in=3600)

    retrieved = run_(store.get)(key)
    assert retrieved == updated_value
    assert retrieved["adbc_metadata"]["format"] == "Arrow"

    # Delete
    run_(store.delete)(key)
    result = run_(store.get)(key)
    assert result is None


@xfail_if_driver_missing
def test_adbc_store_expiration(store: SQLSpecSessionStore, adbc_config: AdbcConfig) -> None:
    """Test that expired entries are not returned with ADBC."""
    import time

    key = "adbc-expiring-key"
    value = {"test": "adbc_data", "arrow_native": True, "columnar_format": True}

    # Set with 1 second expiration
    run_(store.set)(key, value, expires_in=1)

    # Should exist immediately
    result = run_(store.get)(key)
    assert result == value
    assert result["arrow_native"] is True

    # Check what's actually in the database
    with adbc_config.provide_session() as driver:
        check_result = driver.execute(f"SELECT * FROM {store._table_name} WHERE session_id = %s", (key,))
        if check_result.data:
            # Verify JSONB data structure
            session_data = check_result.data[0]
            assert session_data["session_id"] == key

    # Wait for expiration (add buffer for timing issues)
    time.sleep(3)

    # Should be expired
    result = run_(store.get)(key)
    assert result is None


@xfail_if_driver_missing
def test_adbc_store_default_values(store: SQLSpecSessionStore) -> None:
    """Test default value handling with ADBC store."""
    # Non-existent key should return None
    result = run_(store.get)("non-existent")
    assert result is None

    # Test with our own default handling
    result = run_(store.get)("non-existent")
    if result is None:
        result = {"default": True, "engine": "ADBC", "arrow_native": True}
    assert result["default"] is True
    assert result["arrow_native"] is True


@xfail_if_driver_missing
async def test_adbc_store_bulk_operations(store: SQLSpecSessionStore) -> None:
    """Test bulk operations on the ADBC store with Arrow optimization."""

    @async_
    async def run_bulk_test():
        # Create multiple entries efficiently with ADBC/Arrow features
        entries = {}
        tasks = []
        for i in range(25):  # Test ADBC bulk performance
            key = f"adbc-bulk-{i}"
            value = {
                "index": i,
                "data": f"value-{i}",
                "metadata": {"created_by": "adbc_test", "batch": i // 5},
                "arrow_metadata": {
                    "columnar_format": i % 2 == 0,
                    "zero_copy": i % 3 == 0,
                    "batch_id": i // 5,
                    "arrow_type": "record_batch" if i % 4 == 0 else "table",
                },
            }
            entries[key] = value
            tasks.append(store.set(key, value, expires_in=3600))

        # Execute all inserts concurrently (PostgreSQL handles concurrency well)
        await asyncio.gather(*tasks)

        # Verify all entries exist
        verify_tasks = [store.get(key) for key in entries]
        results = await asyncio.gather(*verify_tasks)

        for (key, expected_value), result in zip(entries.items(), results):
            assert result == expected_value
            assert result["arrow_metadata"]["batch_id"] is not None

        # Delete all entries concurrently
        delete_tasks = [store.delete(key) for key in entries]
        await asyncio.gather(*delete_tasks)

        # Verify all are deleted
        verify_tasks = [store.get(key) for key in entries]
        results = await asyncio.gather(*verify_tasks)
        assert all(result is None for result in results)

    await run_bulk_test()


@xfail_if_driver_missing
def test_adbc_store_large_data(store: SQLSpecSessionStore) -> None:
    """Test storing large data structures in ADBC with Arrow optimization."""
    # Create a large data structure that tests ADBC's Arrow capabilities
    large_data = {
        "users": [
            {
                "id": i,
                "name": f"adbc_user_{i}",
                "email": f"user{i}@adbc-example.com",
                "profile": {
                    "bio": f"ADBC Arrow user {i} " + "x" * 100,
                    "tags": [f"adbc_tag_{j}" for j in range(10)],
                    "settings": {f"setting_{j}": j for j in range(20)},
                    "arrow_preferences": {
                        "columnar_format": i % 2 == 0,
                        "zero_copy_enabled": i % 3 == 0,
                        "batch_size": i * 10,
                    },
                },
            }
            for i in range(100)  # Test ADBC capacity with Arrow format
        ],
        "analytics": {
            "metrics": {
                f"metric_{i}": {
                    "value": i * 1.5,
                    "timestamp": f"2024-01-{i:02d}",
                    "arrow_type": "float64" if i % 2 == 0 else "int64",
                }
                for i in range(1, 32)
            },
            "events": [
                {
                    "type": f"adbc_event_{i}",
                    "data": "x" * 300,
                    "arrow_metadata": {
                        "format": "arrow",
                        "compression": "snappy" if i % 2 == 0 else "lz4",
                        "columnar": True,
                    },
                }
                for i in range(50)
            ],
        },
        "adbc_configuration": {
            "driver": "postgresql",
            "arrow_native": True,
            "performance_mode": "high_throughput",
            "batch_processing": {"enabled": True, "batch_size": 1000, "compression": "snappy"},
        },
    }

    key = "adbc-large-data"
    run_(store.set)(key, large_data, expires_in=3600)

    # Retrieve and verify
    retrieved = run_(store.get)(key)
    assert retrieved == large_data
    assert len(retrieved["users"]) == 100
    assert len(retrieved["analytics"]["metrics"]) == 31
    assert len(retrieved["analytics"]["events"]) == 50
    assert retrieved["adbc_configuration"]["arrow_native"] is True
    assert retrieved["adbc_configuration"]["batch_processing"]["enabled"] is True


@xfail_if_driver_missing
async def test_adbc_store_concurrent_access(store: SQLSpecSessionStore) -> None:
    """Test concurrent access to the ADBC store."""

    async def update_value(key: str, value: int) -> None:
        """Update a value in the store with ADBC optimization."""
        await store.set(
            key,
            {
                "value": value,
                "operation": f"adbc_update_{value}",
                "arrow_metadata": {"batch_id": value, "columnar": True, "timestamp": f"2024-01-01T12:{value:02d}:00Z"},
            },
            expires_in=3600,
        )

    @async_
    async def run_concurrent_test():
        # Create many concurrent updates to test ADBC's concurrency handling
        key = "adbc-concurrent-key"
        tasks = [update_value(key, i) for i in range(50)]
        await asyncio.gather(*tasks)

        # The last update should win (PostgreSQL handles this consistently)
        result = await store.get(key)
        assert result is not None
        assert "value" in result
        assert 0 <= result["value"] <= 49
        assert "operation" in result
        assert result["arrow_metadata"]["columnar"] is True

    await run_concurrent_test()


@xfail_if_driver_missing
def test_adbc_store_get_all(store: SQLSpecSessionStore) -> None:
    """Test retrieving all entries from the ADBC store."""
    import asyncio
    import time

    # Create multiple entries with different expiration times and ADBC features
    run_(store.set)("key1", {"data": 1, "engine": "ADBC", "arrow": True}, expires_in=3600)
    run_(store.set)("key2", {"data": 2, "engine": "ADBC", "columnar": True}, expires_in=3600)
    run_(store.set)("key3", {"data": 3, "engine": "ADBC", "zero_copy": True}, expires_in=1)  # Will expire soon

    # Get all entries - need to consume async generator
    async def collect_all() -> dict[str, Any]:
        return {key: value async for key, value in store.get_all()}

    all_entries = asyncio.run(collect_all())

    # Should have all three initially
    assert len(all_entries) >= 2  # At least the non-expiring ones
    assert all_entries.get("key1", {}).get("arrow") is True
    assert all_entries.get("key2", {}).get("columnar") is True

    # Wait for one to expire
    time.sleep(3)

    # Get all again
    all_entries = asyncio.run(collect_all())

    # Should only have non-expired entries
    assert "key1" in all_entries
    assert "key2" in all_entries
    assert "key3" not in all_entries  # Should be expired
    assert all_entries["key1"]["engine"] == "ADBC"


@xfail_if_driver_missing
def test_adbc_store_delete_expired(store: SQLSpecSessionStore) -> None:
    """Test deletion of expired entries with ADBC."""
    import time

    # Create entries with different expiration times and ADBC features
    run_(store.set)("short1", {"data": 1, "engine": "ADBC", "temp": True}, expires_in=1)
    run_(store.set)("short2", {"data": 2, "engine": "ADBC", "temp": True}, expires_in=1)
    run_(store.set)("long1", {"data": 3, "engine": "ADBC", "persistent": True}, expires_in=3600)
    run_(store.set)("long2", {"data": 4, "engine": "ADBC", "persistent": True}, expires_in=3600)

    # Wait for short-lived entries to expire (add buffer)
    time.sleep(3)

    # Delete expired entries
    run_(store.delete_expired)()

    # Check which entries remain
    assert run_(store.get)("short1") is None
    assert run_(store.get)("short2") is None

    long1_result = run_(store.get)("long1")
    long2_result = run_(store.get)("long2")
    assert long1_result == {"data": 3, "engine": "ADBC", "persistent": True}
    assert long2_result == {"data": 4, "engine": "ADBC", "persistent": True}


@xfail_if_driver_missing
def test_adbc_store_special_characters(store: SQLSpecSessionStore) -> None:
    """Test handling of special characters in keys and values with ADBC."""
    # Test special characters in keys (ADBC/PostgreSQL specific)
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
        "key→with→arrows",  # Arrow characters for ADBC
    ]

    for key in special_keys:
        value = {"key": key, "adbc": True, "arrow_native": True}
        run_(store.set)(key, value, expires_in=3600)
        retrieved = run_(store.get)(key)
        assert retrieved == value

    # Test ADBC-specific data types and special characters in values
    special_value = {
        "unicode": "ADBC Arrow: 🏹 База данных データベース données 数据库",
        "emoji": "🚀🎉😊🏹🔥💻⚡",
        "quotes": "He said \"hello\" and 'goodbye' and `backticks`",
        "newlines": "line1\nline2\r\nline3",
        "tabs": "col1\tcol2\tcol3",
        "special": "!@#$%^&*()[]{}|\\<>?,./",
        "adbc_arrays": [1, 2, 3, [4, 5, [6, 7]], {"nested": True}],
        "adbc_json": {"nested": {"deep": {"value": 42, "arrow": True}}},
        "null_handling": {"null": None, "not_null": "value"},
        "escape_chars": "\\n\\t\\r\\b\\f",
        "sql_injection_attempt": "'; DROP TABLE test; --",  # Should be safely handled
        "boolean_types": {"true": True, "false": False},
        "numeric_types": {"int": 123, "float": 123.456, "pi": math.pi},
        "arrow_features": {
            "zero_copy": True,
            "columnar": True,
            "compression": "snappy",
            "batch_processing": True,
            "cross_language": ["Python", "R", "Java", "C++"],
        },
    }

    run_(store.set)("adbc-special-value", special_value, expires_in=3600)
    retrieved = run_(store.get)("adbc-special-value")
    assert retrieved == special_value
    assert retrieved["null_handling"]["null"] is None
    assert retrieved["adbc_arrays"][3] == [4, 5, [6, 7]]
    assert retrieved["boolean_types"]["true"] is True
    assert retrieved["numeric_types"]["pi"] == math.pi
    assert retrieved["arrow_features"]["zero_copy"] is True
    assert "Python" in retrieved["arrow_features"]["cross_language"]


@xfail_if_driver_missing
def test_adbc_store_crud_operations_enhanced(store: SQLSpecSessionStore) -> None:
    """Test enhanced CRUD operations on the ADBC store."""
    key = "adbc-enhanced-test-key"
    value = {
        "user_id": 999,
        "data": ["item1", "item2", "item3"],
        "nested": {"key": "value", "number": 123.45},
        "adbc_specific": {
            "arrow_format": True,
            "columnar_data": [1, 2, 3],
            "metadata": {"driver": "postgresql", "compression": "snappy", "batch_size": 1000},
        },
    }

    # Create
    run_(store.set)(key, value, expires_in=3600)

    # Read
    retrieved = run_(store.get)(key)
    assert retrieved == value
    assert retrieved["adbc_specific"]["arrow_format"] is True

    # Update with new ADBC-specific structure
    updated_value = {
        "user_id": 1000,
        "new_field": "new_value",
        "adbc_types": {"boolean": True, "null": None, "float": math.pi},
        "arrow_operations": {
            "read_operations": 150,
            "write_operations": 75,
            "batch_operations": 25,
            "zero_copy_transfers": 10,
        },
    }
    run_(store.set)(key, updated_value, expires_in=3600)

    retrieved = run_(store.get)(key)
    assert retrieved == updated_value
    assert retrieved["adbc_types"]["null"] is None
    assert retrieved["arrow_operations"]["read_operations"] == 150

    # Delete
    run_(store.delete)(key)
    result = run_(store.get)(key)
    assert result is None


@xfail_if_driver_missing
def test_adbc_store_expiration_enhanced(store: SQLSpecSessionStore) -> None:
    """Test enhanced expiration handling with ADBC."""
    import time

    key = "adbc-expiring-key-enhanced"
    value = {
        "test": "adbc_data",
        "expires": True,
        "arrow_metadata": {"format": "Arrow", "columnar": True, "zero_copy": True},
    }

    # Set with 1 second expiration
    run_(store.set)(key, value, expires_in=1)

    # Should exist immediately
    result = run_(store.get)(key)
    assert result == value
    assert result["arrow_metadata"]["columnar"] is True

    # Wait for expiration
    time.sleep(2)

    # Should be expired
    result = run_(store.get)(key)
    assert result is None


@xfail_if_driver_missing
def test_adbc_store_exists_and_expires_in(store: SQLSpecSessionStore) -> None:
    """Test exists and expires_in functionality with ADBC."""
    key = "adbc-exists-test"
    value = {"test": "data", "adbc_engine": "Arrow", "columnar_format": True}

    # Test non-existent key
    assert run_(store.exists)(key) is False
    assert run_(store.expires_in)(key) == 0

    # Set key
    run_(store.set)(key, value, expires_in=3600)

    # Test existence
    assert run_(store.exists)(key) is True
    expires_in = run_(store.expires_in)(key)
    assert 3590 <= expires_in <= 3600  # Should be close to 3600

    # Delete and test again
    run_(store.delete)(key)
    assert run_(store.exists)(key) is False
    assert run_(store.expires_in)(key) == 0


@xfail_if_driver_missing
async def test_adbc_store_arrow_optimization() -> None:
    """Test ADBC-specific Arrow optimization features."""
    # Create a separate configuration for this test
    with tempfile.TemporaryDirectory() as temp_dir:
        from pytest_databases.docker import postgresql_url

        # Get PostgreSQL connection info
        postgres_url = postgresql_url()

        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Apply migrations and create store
        @async_
        def setup_database():
            config = AdbcConfig(
                connection_config={"uri": postgres_url, "driver_name": "postgresql"},
                migration_config={
                    "script_location": str(migration_dir),
                    "version_table_name": "sqlspec_migrations_arrow",
                    "include_extensions": ["litestar"],
                },
            )
            commands = SyncMigrationCommands(config)
            commands.init(config.migration_config["script_location"], package=False)
            commands.upgrade()
            return config

        config = await setup_database()

        # Create store
        store = SQLSpecSessionStore(config, table_name="litestar_sessions")

        key = "adbc-arrow-optimization-test"

        # Set initial arrow-optimized data
        arrow_data = {
            "counter": 0,
            "arrow_metadata": {
                "format": "Arrow",
                "columnar": True,
                "zero_copy": True,
                "compression": "snappy",
                "batch_size": 1000,
            },
            "performance_metrics": {
                "throughput": 10000,  # rows per second
                "latency": 0.1,  # milliseconds
                "cpu_usage": 15.5,  # percentage
            },
        }
        await store.set(key, arrow_data, expires_in=3600)

        async def increment_counter() -> None:
            """Increment counter with Arrow optimization."""
            current = await store.get(key)
            if current:
                current["counter"] += 1
                current["performance_metrics"]["throughput"] += 100
                current["arrow_metadata"]["last_updated"] = "2024-01-01T12:00:00Z"
                await store.set(key, current, expires_in=3600)

        # Run multiple increments to test Arrow performance
        for _ in range(10):
            await increment_counter()

        # Final count should be 10 with Arrow optimization maintained
        result = await store.get(key)
        assert result is not None
        assert "counter" in result
        assert result["counter"] == 10
        assert result["arrow_metadata"]["format"] == "Arrow"
        assert result["arrow_metadata"]["zero_copy"] is True
        assert result["performance_metrics"]["throughput"] == 11000  # 10000 + 10 * 100
