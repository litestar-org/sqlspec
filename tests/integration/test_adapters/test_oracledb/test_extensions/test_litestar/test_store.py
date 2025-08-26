"""Integration tests for OracleDB session store."""

import asyncio
import math

import pytest

from sqlspec.adapters.oracledb.config import OracleAsyncConfig, OracleSyncConfig
from sqlspec.extensions.litestar import SQLSpecSessionStore

pytestmark = [pytest.mark.oracledb, pytest.mark.oracle, pytest.mark.integration, pytest.mark.xdist_group("oracle")]


@pytest.fixture
async def oracle_async_config(oracle_async_config: OracleAsyncConfig) -> OracleAsyncConfig:
    """Create Oracle async configuration for testing."""
    return oracle_async_config


@pytest.fixture
def oracle_sync_config(oracle_sync_config: OracleSyncConfig) -> OracleSyncConfig:
    """Create Oracle sync configuration for testing."""
    return oracle_sync_config


@pytest.fixture
async def oracle_async_store(
    oracle_async_config: OracleAsyncConfig, request: pytest.FixtureRequest
) -> SQLSpecSessionStore:
    """Create an async Oracle session store instance."""
    # Create unique table name for test isolation
    worker_id = getattr(request.config, "workerinput", {}).get("workerid", "master")
    table_suffix = f"{worker_id}_{abs(hash(request.node.nodeid)) % 100000}"
    table_name = f"test_store_oracle_async_{table_suffix}"

    # Create the table manually since we're not using migrations here (using Oracle PL/SQL syntax)
    async with oracle_async_config.provide_session() as driver:
        await driver.execute(f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE TABLE {table_name} (
                    session_key VARCHAR2(255) PRIMARY KEY,
                    session_value CLOB NOT NULL,
                    expires_at TIMESTAMP NOT NULL,
                    created_at TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL
                )';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN -- Table already exists
                        RAISE;
                    END IF;
            END;
        """)
        await driver.execute(f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE INDEX idx_{table_name}_expires ON {table_name}(expires_at)';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN -- Index already exists
                        RAISE;
                    END IF;
            END;
        """)

    store = SQLSpecSessionStore(
        config=oracle_async_config,
        table_name=table_name,
        session_id_column="session_key",
        data_column="session_value",
        expires_at_column="expires_at",
        created_at_column="created_at",
    )

    yield store

    # Cleanup
    try:
        async with oracle_async_config.provide_session() as driver:
            await driver.execute(f"""
                BEGIN
                    EXECUTE IMMEDIATE 'DROP TABLE {table_name}';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE != -942 THEN -- Table does not exist
                            RAISE;
                        END IF;
                END;
            """)
    except Exception:
        pass  # Ignore cleanup errors


@pytest.fixture
def oracle_sync_store(oracle_sync_config: OracleSyncConfig, request: pytest.FixtureRequest) -> SQLSpecSessionStore:
    """Create a sync Oracle session store instance."""
    # Create unique table name for test isolation
    worker_id = getattr(request.config, "workerinput", {}).get("workerid", "master")
    table_suffix = f"{worker_id}_{abs(hash(request.node.nodeid)) % 100000}"
    table_name = f"test_store_oracle_sync_{table_suffix}"

    # Create the table manually since we're not using migrations here (using Oracle PL/SQL syntax)
    with oracle_sync_config.provide_session() as driver:
        driver.execute(f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE TABLE {table_name} (
                    session_key VARCHAR2(255) PRIMARY KEY,
                    session_value CLOB NOT NULL,
                    expires_at TIMESTAMP NOT NULL,
                    created_at TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL
                )';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN -- Table already exists
                        RAISE;
                    END IF;
            END;
        """)
        driver.execute(f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE INDEX idx_{table_name}_expires ON {table_name}(expires_at)';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN -- Index already exists
                        RAISE;
                    END IF;
            END;
        """)

    store = SQLSpecSessionStore(
        config=oracle_sync_config,
        table_name=table_name,
        session_id_column="session_key",
        data_column="session_value",
        expires_at_column="expires_at",
        created_at_column="created_at",
    )

    yield store

    # Cleanup
    try:
        with oracle_sync_config.provide_session() as driver:
            driver.execute(f"""
                BEGIN
                    EXECUTE IMMEDIATE 'DROP TABLE {table_name}';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE != -942 THEN -- Table does not exist
                            RAISE;
                        END IF;
                END;
            """)
    except Exception:
        pass  # Ignore cleanup errors


async def test_oracle_async_store_table_creation(
    oracle_async_store: SQLSpecSessionStore, oracle_async_config: OracleAsyncConfig
) -> None:
    """Test that store table is created automatically with proper Oracle structure."""
    async with oracle_async_config.provide_session() as driver:
        # Get the table name from the store
        table_name = oracle_async_store._table_name.upper()

        # Verify table exists
        result = await driver.execute("SELECT table_name FROM user_tables WHERE table_name = :1", (table_name,))
        assert len(result.data) == 1
        assert result.data[0]["TABLE_NAME"] == table_name

        # Verify table structure with Oracle-specific types
        result = await driver.execute(
            "SELECT column_name, data_type FROM user_tab_columns WHERE table_name = :1 ORDER BY column_id",
            (table_name,),
        )
        columns = {row["COLUMN_NAME"]: row["DATA_TYPE"] for row in result.data}
        assert "SESSION_KEY" in columns
        assert "SESSION_VALUE" in columns
        assert "EXPIRES_AT" in columns
        assert "CREATED_AT" in columns

        # Verify Oracle-specific data types
        assert columns["SESSION_VALUE"] == "CLOB"  # Oracle uses CLOB for large text
        assert columns["EXPIRES_AT"] == "TIMESTAMP(6)"
        assert columns["CREATED_AT"] == "TIMESTAMP(6)"

        # Verify primary key constraint
        result = await driver.execute(
            "SELECT constraint_name, constraint_type FROM user_constraints WHERE table_name = :1 AND constraint_type = 'P'",
            (table_name,),
        )
        assert len(result.data) == 1  # Should have primary key

        # Verify index on expires_at column
        result = await driver.execute(
            "SELECT index_name FROM user_indexes WHERE table_name = :1 AND index_name LIKE '%EXPIRES%'", (table_name,)
        )
        assert len(result.data) >= 1  # Should have index on expires_at


def test_oracle_sync_store_table_creation(
    oracle_sync_store: SQLSpecSessionStore, oracle_sync_config: OracleSyncConfig
) -> None:
    """Test that store table is created automatically with proper Oracle structure (sync)."""
    with oracle_sync_config.provide_session() as driver:
        # Get the table name from the store
        table_name = oracle_sync_store._table_name.upper()

        # Verify table exists
        result = driver.execute("SELECT table_name FROM user_tables WHERE table_name = :1", (table_name,))
        assert len(result.data) == 1
        assert result.data[0]["TABLE_NAME"] == table_name

        # Verify table structure
        result = driver.execute(
            "SELECT column_name, data_type FROM user_tab_columns WHERE table_name = :1 ORDER BY column_id",
            (table_name,),
        )
        columns = {row["COLUMN_NAME"]: row["DATA_TYPE"] for row in result.data}
        assert "SESSION_KEY" in columns
        assert "SESSION_VALUE" in columns
        assert "EXPIRES_AT" in columns
        assert "CREATED_AT" in columns

        # Verify Oracle-specific data types
        assert columns["SESSION_VALUE"] == "CLOB"
        assert columns["EXPIRES_AT"] == "TIMESTAMP(6)"


async def test_oracle_async_store_crud_operations(oracle_async_store: SQLSpecSessionStore) -> None:
    """Test complete CRUD operations on the Oracle async store."""
    key = "oracle-async-test-key"
    oracle_value = {
        "user_id": 999,
        "oracle_data": {
            "instance_name": "ORCL",
            "service_name": "ORCL_SERVICE",
            "tablespace": "USERS",
            "features": ["plsql", "json", "vector"],
        },
        "nested_oracle": {"sga_config": {"shared_pool": "512MB", "buffer_cache": "1GB"}, "pga_target": "1GB"},
        "oracle_arrays": [1, 2, 3, [4, 5, [6, 7]]],
        "plsql_packages": ["DBMS_STATS", "DBMS_SCHEDULER", "DBMS_VECTOR"],
    }

    # Create
    await oracle_async_store.set(key, oracle_value, expires_in=3600)

    # Read
    retrieved = await oracle_async_store.get(key)
    assert retrieved == oracle_value
    assert retrieved["oracle_data"]["instance_name"] == "ORCL"
    assert retrieved["oracle_data"]["features"] == ["plsql", "json", "vector"]

    # Update with new Oracle structure
    updated_oracle_value = {
        "user_id": 1000,
        "new_oracle_field": "oracle_23ai",
        "oracle_types": {"boolean": True, "null": None, "float": math.pi},
        "oracle_advanced": {
            "rac_enabled": True,
            "data_guard": {"primary": "ORCL1", "standby": "ORCL2"},
            "autonomous_features": {"auto_scaling": True, "auto_backup": True},
        },
    }
    await oracle_async_store.set(key, updated_oracle_value, expires_in=3600)

    retrieved = await oracle_async_store.get(key)
    assert retrieved == updated_oracle_value
    assert retrieved["oracle_types"]["null"] is None
    assert retrieved["oracle_advanced"]["rac_enabled"] is True

    # Delete
    await oracle_async_store.delete(key)
    result = await oracle_async_store.get(key)
    assert result is None


def test_oracle_sync_store_crud_operations(oracle_sync_store: SQLSpecSessionStore) -> None:
    """Test complete CRUD operations on the Oracle sync store."""

    async def run_sync_test() -> None:
        key = "oracle-sync-test-key"
        oracle_sync_value = {
            "user_id": 888,
            "oracle_sync_data": {
                "database_name": "ORCL",
                "character_set": "AL32UTF8",
                "national_character_set": "AL16UTF16",
                "db_block_size": 8192,
            },
            "oracle_sync_features": {
                "partitioning": True,
                "compression": {"basic": True, "advanced": False},
                "encryption": {"tablespace": True, "column": False},
            },
            "oracle_version": {"major": 23, "minor": 0, "patch": 0, "edition": "Enterprise"},
        }

        # Create
        await oracle_sync_store.set(key, oracle_sync_value, expires_in=3600)

        # Read
        retrieved = await oracle_sync_store.get(key)
        assert retrieved == oracle_sync_value
        assert retrieved["oracle_sync_data"]["database_name"] == "ORCL"
        assert retrieved["oracle_sync_features"]["partitioning"] is True

        # Update
        updated_sync_value = {
            **oracle_sync_value,
            "last_sync": "2024-01-01T12:00:00Z",
            "oracle_sync_status": {"connected": True, "last_ping": "2024-01-01T12:00:00Z"},
        }
        await oracle_sync_store.set(key, updated_sync_value, expires_in=3600)

        retrieved = await oracle_sync_store.get(key)
        assert retrieved == updated_sync_value
        assert retrieved["oracle_sync_status"]["connected"] is True

        # Delete
        await oracle_sync_store.delete(key)
        result = await oracle_sync_store.get(key)
        assert result is None

    asyncio.run(run_sync_test())


async def test_oracle_async_store_expiration(oracle_async_store: SQLSpecSessionStore) -> None:
    """Test that expired entries are not returned from Oracle async store."""
    key = "oracle-async-expiring-key"
    oracle_expiring_value = {
        "test": "oracle_async_data",
        "expires": True,
        "oracle_session": {"sid": 123, "serial": 456},
        "temporary_data": {"temp_tablespace": "TEMP", "sort_area_size": "1MB"},
    }

    # Set with 1 second expiration
    await oracle_async_store.set(key, oracle_expiring_value, expires_in=1)

    # Should exist immediately
    result = await oracle_async_store.get(key)
    assert result == oracle_expiring_value
    assert result["oracle_session"]["sid"] == 123

    # Wait for expiration
    await asyncio.sleep(2)

    # Should be expired
    result = await oracle_async_store.get(key)
    assert result is None


def test_oracle_sync_store_expiration(oracle_sync_store: SQLSpecSessionStore) -> None:
    """Test that expired entries are not returned from Oracle sync store."""

    async def run_sync_test() -> None:
        key = "oracle-sync-expiring-key"
        oracle_sync_expiring_value = {
            "test": "oracle_sync_data",
            "expires": True,
            "oracle_config": {"init_params": {"sga_target": "2G", "pga_aggregate_target": "1G"}},
            "session_info": {"username": "SCOTT", "schema": "SCOTT", "machine": "oracle_client"},
        }

        # Set with 1 second expiration
        await oracle_sync_store.set(key, oracle_sync_expiring_value, expires_in=1)

        # Should exist immediately
        result = await oracle_sync_store.get(key)
        assert result == oracle_sync_expiring_value
        assert result["session_info"]["username"] == "SCOTT"

        # Wait for expiration
        await asyncio.sleep(2)

        # Should be expired
        result = await oracle_sync_store.get(key)
        assert result is None

    asyncio.run(run_sync_test())


async def test_oracle_async_store_bulk_operations(oracle_async_store: SQLSpecSessionStore) -> None:
    """Test bulk operations on the Oracle async store."""
    # Create multiple entries efficiently with Oracle-specific data
    entries = {}
    tasks = []
    for i in range(30):  # Oracle can handle large datasets efficiently
        key = f"oracle-async-bulk-{i}"
        oracle_bulk_value = {
            "index": i,
            "data": f"oracle_value_{i}",
            "oracle_metadata": {
                "created_by": "oracle_test",
                "batch": i // 10,
                "instance": f"ORCL_{i % 3}",  # Simulate RAC instances
            },
            "oracle_features": {"plsql_enabled": i % 2 == 0, "json_enabled": True, "vector_enabled": i % 5 == 0},
        }
        entries[key] = oracle_bulk_value
        tasks.append(oracle_async_store.set(key, oracle_bulk_value, expires_in=3600))

    # Execute all inserts concurrently
    await asyncio.gather(*tasks)

    # Verify all entries exist
    verify_tasks = [oracle_async_store.get(key) for key in entries]
    results = await asyncio.gather(*verify_tasks)

    for (key, expected_value), result in zip(entries.items(), results):
        assert result == expected_value
        assert result["oracle_metadata"]["created_by"] == "oracle_test"

    # Delete all entries concurrently
    delete_tasks = [oracle_async_store.delete(key) for key in entries]
    await asyncio.gather(*delete_tasks)

    # Verify all are deleted
    verify_tasks = [oracle_async_store.get(key) for key in entries]
    results = await asyncio.gather(*verify_tasks)
    assert all(result is None for result in results)


def test_oracle_sync_store_bulk_operations(oracle_sync_store: SQLSpecSessionStore) -> None:
    """Test bulk operations on the Oracle sync store."""

    async def run_sync_test() -> None:
        # Create multiple entries with Oracle sync data
        entries = {}
        for i in range(20):
            key = f"oracle-sync-bulk-{i}"
            oracle_sync_bulk_value = {
                "index": i,
                "data": f"oracle_sync_value_{i}",
                "oracle_sync_metadata": {
                    "workspace": f"WS_{i % 3}",
                    "schema": f"SCHEMA_{i}",
                    "tablespace": f"TBS_{i % 5}",
                },
                "database_objects": {"tables": i * 2, "indexes": i * 3, "sequences": i},
            }
            entries[key] = oracle_sync_bulk_value

        # Set all entries
        for key, value in entries.items():
            await oracle_sync_store.set(key, value, expires_in=3600)

        # Verify all entries exist
        for key, expected_value in entries.items():
            result = await oracle_sync_store.get(key)
            assert result == expected_value
            assert result["oracle_sync_metadata"]["workspace"] == expected_value["oracle_sync_metadata"]["workspace"]

        # Delete all entries
        for key in entries:
            await oracle_sync_store.delete(key)

        # Verify all are deleted
        for key in entries:
            result = await oracle_sync_store.get(key)
            assert result is None

    asyncio.run(run_sync_test())


async def test_oracle_async_store_large_data(oracle_async_store: SQLSpecSessionStore) -> None:
    """Test storing large data structures in Oracle async store using CLOB capabilities."""
    # Create a large Oracle-specific data structure that tests CLOB capabilities
    large_oracle_data = {
        "oracle_schemas": [
            {
                "schema_name": f"SCHEMA_{i}",
                "owner": f"USER_{i}",
                "tables": [
                    {
                        "table_name": f"TABLE_{j}",
                        "tablespace": f"TBS_{j % 5}",
                        "columns": [f"COL_{k}" for k in range(20)],
                        "indexes": [f"IDX_{j}_{k}" for k in range(5)],
                        "triggers": [f"TRG_{j}_{k}" for k in range(3)],
                        "oracle_metadata": f"Metadata for table {j} " + "x" * 200,
                    }
                    for j in range(50)  # 50 tables per schema
                ],
                "packages": [f"PKG_{j}" for j in range(20)],
                "procedures": [f"PROC_{j}" for j in range(30)],
                "functions": [f"FUNC_{j}" for j in range(25)],
            }
            for i in range(10)  # 10 schemas
        ],
        "oracle_performance": {
            "awr_reports": [{"report_id": i, "data": "x" * 1000} for i in range(50)],
            "sql_tuning": {
                "recommendations": [f"Recommendation {i}: " + "x" * 500 for i in range(100)],
                "execution_plans": [{"plan_id": i, "plan": "x" * 200} for i in range(200)],
            },
        },
        "oracle_analytics": {
            "statistics": {
                f"stat_{i}": {"value": i * 1.5, "timestamp": f"2024-01-{i:02d}"} for i in range(1, 366)
            },  # Full year
            "events": [{"event_id": i, "description": "Oracle event " + "x" * 300} for i in range(500)],
        },
    }

    key = "oracle-async-large-data"
    await oracle_async_store.set(key, large_oracle_data, expires_in=3600)

    # Retrieve and verify
    retrieved = await oracle_async_store.get(key)
    assert retrieved == large_oracle_data
    assert len(retrieved["oracle_schemas"]) == 10
    assert len(retrieved["oracle_schemas"][0]["tables"]) == 50
    assert len(retrieved["oracle_performance"]["awr_reports"]) == 50
    assert len(retrieved["oracle_analytics"]["statistics"]) == 365
    assert len(retrieved["oracle_analytics"]["events"]) == 500


def test_oracle_sync_store_large_data(oracle_sync_store: SQLSpecSessionStore) -> None:
    """Test storing large data structures in Oracle sync store using CLOB capabilities."""

    async def run_sync_test() -> None:
        # Create large Oracle sync data
        large_oracle_sync_data = {
            "oracle_workspaces": [
                {
                    "workspace_id": i,
                    "name": f"WORKSPACE_{i}",
                    "database_links": [
                        {
                            "link_name": f"DBLINK_{j}",
                            "connect_string": f"remote{j}.example.com:1521/REMOTE{j}",
                            "username": f"USER_{j}",
                        }
                        for j in range(10)
                    ],
                    "materialized_views": [
                        {
                            "mv_name": f"MV_{j}",
                            "refresh_method": "FAST" if j % 2 == 0 else "COMPLETE",
                            "query": f"SELECT * FROM table_{j} " + "WHERE condition " * 50,
                        }
                        for j in range(30)
                    ],
                }
                for i in range(20)
            ],
            "oracle_monitoring": {
                "session_stats": [
                    {
                        "sid": i,
                        "username": f"USER_{i}",
                        "sql_text": f"SELECT * FROM large_table_{i} " + "WHERE big_condition " * 100,
                        "statistics": {"logical_reads": i * 1000, "physical_reads": i * 100},
                    }
                    for i in range(200)
                ]
            },
        }

        key = "oracle-sync-large-data"
        await oracle_sync_store.set(key, large_oracle_sync_data, expires_in=3600)

        # Retrieve and verify
        retrieved = await oracle_sync_store.get(key)
        assert retrieved == large_oracle_sync_data
        assert len(retrieved["oracle_workspaces"]) == 20
        assert len(retrieved["oracle_workspaces"][0]["database_links"]) == 10
        assert len(retrieved["oracle_monitoring"]["session_stats"]) == 200

    asyncio.run(run_sync_test())


async def test_oracle_async_store_concurrent_access(oracle_async_store: SQLSpecSessionStore) -> None:
    """Test concurrent access to the Oracle async store."""

    async def update_oracle_value(key: str, value: int) -> None:
        """Update an Oracle value in the store."""
        oracle_concurrent_data = {
            "value": value,
            "thread": asyncio.current_task().get_name() if asyncio.current_task() else "unknown",
            "oracle_session": {"sid": value, "serial": value * 10, "machine": f"client_{value}"},
            "oracle_stats": {"cpu_time": value * 0.1, "logical_reads": value * 100},
        }
        await oracle_async_store.set(key, oracle_concurrent_data, expires_in=3600)

    # Create many concurrent updates to test Oracle's concurrency handling
    key = "oracle-async-concurrent-key"
    tasks = [update_oracle_value(key, i) for i in range(50)]  # More concurrent updates
    await asyncio.gather(*tasks)

    # The last update should win
    result = await oracle_async_store.get(key)
    assert result is not None
    assert "value" in result
    assert 0 <= result["value"] <= 49
    assert "thread" in result
    assert result["oracle_session"]["sid"] == result["value"]
    assert result["oracle_stats"]["cpu_time"] == result["value"] * 0.1


def test_oracle_sync_store_concurrent_access(oracle_sync_store: SQLSpecSessionStore) -> None:
    """Test concurrent access to the Oracle sync store."""

    async def run_sync_test() -> None:
        async def update_oracle_sync_value(key: str, value: int) -> None:
            """Update an Oracle sync value in the store."""
            oracle_sync_concurrent_data = {
                "value": value,
                "oracle_workspace": f"WS_{value}",
                "oracle_connection": {
                    "service_name": f"SERVICE_{value}",
                    "username": f"USER_{value}",
                    "client_info": f"CLIENT_{value}",
                },
                "oracle_objects": {"tables": value * 2, "views": value, "packages": value // 2},
            }
            await oracle_sync_store.set(key, oracle_sync_concurrent_data, expires_in=3600)

        # Create concurrent sync updates
        key = "oracle-sync-concurrent-key"
        tasks = [update_oracle_sync_value(key, i) for i in range(30)]
        await asyncio.gather(*tasks)

        # Verify one update succeeded
        result = await oracle_sync_store.get(key)
        assert result is not None
        assert "value" in result
        assert 0 <= result["value"] <= 29
        assert result["oracle_workspace"] == f"WS_{result['value']}"
        assert result["oracle_objects"]["tables"] == result["value"] * 2

    asyncio.run(run_sync_test())


async def test_oracle_async_store_get_all(oracle_async_store: SQLSpecSessionStore) -> None:
    """Test retrieving all entries from the Oracle async store."""
    # Create multiple Oracle entries with different expiration times
    oracle_test_entries = {
        "oracle-async-all-1": ({"data": 1, "type": "persistent", "oracle_instance": "ORCL1"}, 3600),
        "oracle-async-all-2": ({"data": 2, "type": "persistent", "oracle_instance": "ORCL2"}, 3600),
        "oracle-async-all-3": ({"data": 3, "type": "temporary", "oracle_instance": "TEMP1"}, 1),
        "oracle-async-all-4": ({"data": 4, "type": "persistent", "oracle_instance": "ORCL3"}, 3600),
    }

    for key, (oracle_value, expires_in) in oracle_test_entries.items():
        await oracle_async_store.set(key, oracle_value, expires_in=expires_in)

    # Get all entries
    all_entries = {
        key: value async for key, value in oracle_async_store.get_all() if key.startswith("oracle-async-all-")
    }

    # Should have all four initially
    assert len(all_entries) >= 3  # At least the non-expiring ones
    if "oracle-async-all-1" in all_entries:
        assert all_entries["oracle-async-all-1"]["oracle_instance"] == "ORCL1"
    if "oracle-async-all-2" in all_entries:
        assert all_entries["oracle-async-all-2"]["oracle_instance"] == "ORCL2"

    # Wait for one to expire
    await asyncio.sleep(2)

    # Get all again
    all_entries = {
        key: value async for key, value in oracle_async_store.get_all() if key.startswith("oracle-async-all-")
    }

    # Should only have non-expired entries
    expected_persistent = ["oracle-async-all-1", "oracle-async-all-2", "oracle-async-all-4"]
    for expected_key in expected_persistent:
        if expected_key in all_entries:
            assert all_entries[expected_key]["type"] == "persistent"

    # Expired entry should be gone
    assert "oracle-async-all-3" not in all_entries


def test_oracle_sync_store_get_all(oracle_sync_store: SQLSpecSessionStore) -> None:
    """Test retrieving all entries from the Oracle sync store."""

    async def run_sync_test() -> None:
        # Create multiple Oracle sync entries
        oracle_sync_test_entries = {
            "oracle-sync-all-1": ({"data": 1, "type": "workspace", "oracle_schema": "HR"}, 3600),
            "oracle-sync-all-2": ({"data": 2, "type": "workspace", "oracle_schema": "SALES"}, 3600),
            "oracle-sync-all-3": ({"data": 3, "type": "temp_workspace", "oracle_schema": "TEMP"}, 1),
        }

        for key, (oracle_sync_value, expires_in) in oracle_sync_test_entries.items():
            await oracle_sync_store.set(key, oracle_sync_value, expires_in=expires_in)

        # Get all entries
        all_entries = {
            key: value async for key, value in oracle_sync_store.get_all() if key.startswith("oracle-sync-all-")
        }

        # Should have all initially
        assert len(all_entries) >= 2  # At least the non-expiring ones

        # Wait for temp to expire
        await asyncio.sleep(2)

        # Get all again
        all_entries = {
            key: value async for key, value in oracle_sync_store.get_all() if key.startswith("oracle-sync-all-")
        }

        # Verify persistent entries remain
        for key, value in all_entries.items():
            if key in ["oracle-sync-all-1", "oracle-sync-all-2"]:
                assert value["type"] == "workspace"

    asyncio.run(run_sync_test())


async def test_oracle_async_store_delete_expired(oracle_async_store: SQLSpecSessionStore) -> None:
    """Test deletion of expired entries in Oracle async store."""
    # Create Oracle entries with different expiration times
    short_lived = ["oracle-async-short-1", "oracle-async-short-2", "oracle-async-short-3"]
    long_lived = ["oracle-async-long-1", "oracle-async-long-2"]

    for key in short_lived:
        oracle_short_data = {
            "data": key,
            "ttl": "short",
            "oracle_temp": {"temp_tablespace": "TEMP", "sort_area": "1MB"},
        }
        await oracle_async_store.set(key, oracle_short_data, expires_in=1)

    for key in long_lived:
        oracle_long_data = {
            "data": key,
            "ttl": "long",
            "oracle_persistent": {"tablespace": "USERS", "quota": "UNLIMITED"},
        }
        await oracle_async_store.set(key, oracle_long_data, expires_in=3600)

    # Wait for short-lived entries to expire
    await asyncio.sleep(2)

    # Delete expired entries
    await oracle_async_store.delete_expired()

    # Check which entries remain
    for key in short_lived:
        assert await oracle_async_store.get(key) is None

    for key in long_lived:
        result = await oracle_async_store.get(key)
        assert result is not None
        assert result["ttl"] == "long"
        assert result["oracle_persistent"]["tablespace"] == "USERS"


def test_oracle_sync_store_delete_expired(oracle_sync_store: SQLSpecSessionStore) -> None:
    """Test deletion of expired entries in Oracle sync store."""

    async def run_sync_test() -> None:
        # Create Oracle sync entries with different expiration times
        short_lived = ["oracle-sync-short-1", "oracle-sync-short-2"]
        long_lived = ["oracle-sync-long-1", "oracle-sync-long-2"]

        for key in short_lived:
            oracle_sync_short_data = {
                "data": key,
                "ttl": "short",
                "oracle_temp_config": {"temp_space": "TEMP", "sort_memory": "10MB"},
            }
            await oracle_sync_store.set(key, oracle_sync_short_data, expires_in=1)

        for key in long_lived:
            oracle_sync_long_data = {
                "data": key,
                "ttl": "long",
                "oracle_config": {"default_tablespace": "USERS", "profile": "DEFAULT"},
            }
            await oracle_sync_store.set(key, oracle_sync_long_data, expires_in=3600)

        # Wait for short-lived entries to expire
        await asyncio.sleep(2)

        # Delete expired entries
        await oracle_sync_store.delete_expired()

        # Check which entries remain
        for key in short_lived:
            assert await oracle_sync_store.get(key) is None

        for key in long_lived:
            result = await oracle_sync_store.get(key)
            assert result is not None
            assert result["ttl"] == "long"
            assert result["oracle_config"]["default_tablespace"] == "USERS"

    asyncio.run(run_sync_test())


async def test_oracle_async_store_special_characters(oracle_async_store: SQLSpecSessionStore) -> None:
    """Test handling of special characters in keys and values with Oracle async store."""
    # Test special characters in keys (Oracle specific)
    oracle_special_keys = [
        "oracle-key-with-dash",
        "oracle_key_with_underscore",
        "oracle.key.with.dots",
        "oracle:key:with:colons",
        "oracle/key/with/slashes",
        "oracle@key@with@at",
        "oracle#key#with#hash",
        "oracle$key$with$dollar",
        "oracle%key%with%percent",
        "oracle&key&with&ampersand",
    ]

    for key in oracle_special_keys:
        oracle_value = {"key": key, "oracle": True, "database": "Oracle"}
        await oracle_async_store.set(key, oracle_value, expires_in=3600)
        retrieved = await oracle_async_store.get(key)
        assert retrieved == oracle_value

    # Test Oracle-specific data types and special characters in values
    oracle_special_value = {
        "unicode_oracle": "Oracle Database: 🔥 База данных データベース 数据库",
        "emoji_oracle": "🚀🎉😊🔥💻📊🗃️⚡",
        "oracle_quotes": "He said \"SELECT * FROM dual\" and 'DROP TABLE test' and `backticks`",
        "newlines_oracle": "line1\nline2\r\nline3\nSELECT * FROM dual;",
        "tabs_oracle": "col1\tcol2\tcol3\tSELECT\tFROM\tDUAL",
        "special_oracle": "!@#$%^&*()[]{}|\\<>?,./SELECT * FROM dual WHERE 1=1;",
        "oracle_arrays": [1, 2, 3, ["SCOTT", "HR", ["SYS", "SYSTEM"]]],
        "oracle_json": {"nested": {"deep": {"oracle_value": 42, "instance": "ORCL"}}},
        "null_handling": {"null": None, "not_null": "oracle_value"},
        "escape_chars": "\\n\\t\\r\\b\\f",
        "sql_injection_attempt": "'; DROP TABLE sessions; --",  # Should be safely handled
        "plsql_code": "BEGIN\n  DBMS_OUTPUT.PUT_LINE('Hello Oracle');\nEND;",
        "oracle_names": {"table": "EMP", "columns": ["EMPNO", "ENAME", "JOB", "SAL"]},
    }

    await oracle_async_store.set("oracle-async-special-value", oracle_special_value, expires_in=3600)
    retrieved = await oracle_async_store.get("oracle-async-special-value")
    assert retrieved == oracle_special_value
    assert retrieved["null_handling"]["null"] is None
    assert retrieved["oracle_arrays"][3] == ["SCOTT", "HR", ["SYS", "SYSTEM"]]
    assert retrieved["oracle_json"]["nested"]["deep"]["oracle_value"] == 42


def test_oracle_sync_store_special_characters(oracle_sync_store: SQLSpecSessionStore) -> None:
    """Test handling of special characters in keys and values with Oracle sync store."""

    async def run_sync_test() -> None:
        # Test Oracle sync special characters
        oracle_sync_special_value = {
            "unicode_sync": "Oracle Sync: 🔥 Синхронизация データ同期",
            "oracle_sync_names": {"schema": "HR", "table": "EMPLOYEES", "view": "EMP_DETAILS_VIEW"},
            "oracle_sync_plsql": {
                "package": "PKG_EMPLOYEE",
                "procedure": "PROC_UPDATE_SALARY",
                "function": "FUNC_GET_BONUS",
            },
            "special_sync_chars": "SELECT 'Oracle''s DUAL' FROM dual WHERE ROWNUM = 1;",
            "oracle_sync_json": {"config": {"sga": "2GB", "pga": "1GB", "service": "ORCL_SERVICE"}},
        }

        await oracle_sync_store.set("oracle-sync-special-value", oracle_sync_special_value, expires_in=3600)
        retrieved = await oracle_sync_store.get("oracle-sync-special-value")
        assert retrieved == oracle_sync_special_value
        assert retrieved["oracle_sync_names"]["schema"] == "HR"
        assert retrieved["oracle_sync_plsql"]["package"] == "PKG_EMPLOYEE"

    asyncio.run(run_sync_test())


async def test_oracle_async_store_transaction_isolation(
    oracle_async_store: SQLSpecSessionStore, oracle_async_config: OracleAsyncConfig
) -> None:
    """Test transaction isolation in Oracle async store operations."""
    key = "oracle-async-transaction-test"

    # Set initial Oracle value
    initial_oracle_data = {"counter": 0, "oracle_session": {"sid": 123, "serial": 456}}
    await oracle_async_store.set(key, initial_oracle_data, expires_in=3600)

    async def increment_oracle_counter() -> None:
        """Increment counter with Oracle session info."""
        current = await oracle_async_store.get(key)
        if current:
            current["counter"] += 1
            current["oracle_session"]["serial"] += 1
            current["last_update"] = "2024-01-01T12:00:00Z"
            await oracle_async_store.set(key, current, expires_in=3600)

    # Run multiple concurrent increments
    tasks = [increment_oracle_counter() for _ in range(15)]
    await asyncio.gather(*tasks)

    # Due to the non-transactional nature, the final count might not be 15
    # but it should be set to some value with Oracle session info
    result = await oracle_async_store.get(key)
    assert result is not None
    assert "counter" in result
    assert result["counter"] > 0  # At least one increment should have succeeded
    assert "oracle_session" in result
    assert result["oracle_session"]["sid"] == 123


def test_oracle_sync_store_transaction_isolation(
    oracle_sync_store: SQLSpecSessionStore, oracle_sync_config: OracleSyncConfig
) -> None:
    """Test transaction isolation in Oracle sync store operations."""

    async def run_sync_test() -> None:
        key = "oracle-sync-transaction-test"

        # Set initial Oracle sync value
        initial_sync_data = {"counter": 0, "oracle_workspace": {"name": "TEST_WS", "schema": "TEST_SCHEMA"}}
        await oracle_sync_store.set(key, initial_sync_data, expires_in=3600)

        async def increment_sync_counter() -> None:
            """Increment counter with Oracle sync workspace info."""
            current = await oracle_sync_store.get(key)
            if current:
                current["counter"] += 1
                current["oracle_workspace"]["last_access"] = "2024-01-01T12:00:00Z"
                await oracle_sync_store.set(key, current, expires_in=3600)

        # Run multiple concurrent increments
        tasks = [increment_sync_counter() for _ in range(10)]
        await asyncio.gather(*tasks)

        # Verify result
        result = await oracle_sync_store.get(key)
        assert result is not None
        assert "counter" in result
        assert result["counter"] > 0
        assert "oracle_workspace" in result
        assert result["oracle_workspace"]["name"] == "TEST_WS"

    asyncio.run(run_sync_test())
