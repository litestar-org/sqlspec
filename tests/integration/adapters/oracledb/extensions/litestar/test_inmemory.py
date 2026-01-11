"""Oracle IN MEMORY table feature tests for Litestar session store.

Tests verify:
- Tables created with INMEMORY PRIORITY HIGH clause when in_memory=True
- Tables created without INMEMORY clause when in_memory=False (default)
- INMEMORY status verifiable via Oracle data dictionary
- Works with both async and sync stores
"""

import pytest

from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleSyncConfig
from sqlspec.adapters.oracledb.litestar.store import OracleAsyncStore, OracleSyncStore

pytestmark = [pytest.mark.xdist_group("oracle"), pytest.mark.oracledb, pytest.mark.integration]


@pytest.mark.oracledb
async def test_inmemory_enabled_creates_session_table_with_inmemory_async(
    oracle_async_config: OracleAsyncConfig,
) -> None:
    """Test that in_memory=True creates session table with INMEMORY PRIORITY HIGH clause."""
    config = OracleAsyncConfig(
        connection_config=oracle_async_config.connection_config, extension_config={"litestar": {"in_memory": True}}
    )

    store = OracleAsyncStore(config)
    await store.create_table()

    try:
        async with config.provide_connection() as conn:
            cursor = conn.cursor()
            await cursor.execute(
                """
                SELECT inmemory, inmemory_priority, inmemory_distribute
                FROM user_tables
                WHERE table_name = 'LITESTAR_SESSION'
                """
            )
            row = await cursor.fetchone()

        assert row is not None, "Session table should exist"
        inmemory_status, inmemory_priority = row[0], row[1]
        assert inmemory_status == "ENABLED", f"Session table should have INMEMORY enabled, got: {inmemory_status}"
        assert inmemory_priority == "HIGH", (
            f"Session table should have INMEMORY PRIORITY HIGH, got: {inmemory_priority}"
        )

    finally:
        async with config.provide_connection() as conn:
            cursor = conn.cursor()
            for stmt in store._get_drop_table_sql():  # pyright: ignore[reportPrivateUsage]
                try:
                    await cursor.execute(stmt)
                except Exception:
                    pass
            await conn.commit()


@pytest.mark.oracledb
async def test_inmemory_disabled_creates_table_without_inmemory_async(oracle_async_config: OracleAsyncConfig) -> None:
    """Test that in_memory=False (default) creates table without INMEMORY clause."""
    config = OracleAsyncConfig(
        connection_config=oracle_async_config.connection_config, extension_config={"litestar": {"in_memory": False}}
    )

    store = OracleAsyncStore(config)
    await store.create_table()

    try:
        async with config.provide_connection() as conn:
            cursor = conn.cursor()
            await cursor.execute(
                """
                SELECT inmemory
                FROM user_tables
                WHERE table_name = 'LITESTAR_SESSION'
                """
            )
            row = await cursor.fetchone()

        assert row is not None, "Session table should exist"
        inmemory_status = row[0]
        assert inmemory_status == "DISABLED", f"Session table should have INMEMORY disabled, got: {inmemory_status}"

    finally:
        async with config.provide_connection() as conn:
            cursor = conn.cursor()
            for stmt in store._get_drop_table_sql():  # pyright: ignore[reportPrivateUsage]
                try:
                    await cursor.execute(stmt)
                except Exception:
                    pass
            await conn.commit()


@pytest.mark.oracledb
async def test_inmemory_default_disabled_async(oracle_async_config: OracleAsyncConfig) -> None:
    """Test that in_memory defaults to False when not specified."""
    config = OracleAsyncConfig(
        connection_config=oracle_async_config.connection_config, extension_config={"litestar": {}}
    )

    store = OracleAsyncStore(config)
    await store.create_table()

    try:
        async with config.provide_connection() as conn:
            cursor = conn.cursor()
            await cursor.execute(
                """
                SELECT inmemory
                FROM user_tables
                WHERE table_name = 'LITESTAR_SESSION'
                """
            )
            row = await cursor.fetchone()

        assert row is not None
        inmemory_status = row[0]
        assert inmemory_status == "DISABLED", "Default should be INMEMORY disabled"

    finally:
        async with config.provide_connection() as conn:
            cursor = conn.cursor()
            for stmt in store._get_drop_table_sql():  # pyright: ignore[reportPrivateUsage]
                try:
                    await cursor.execute(stmt)
                except Exception:
                    pass
            await conn.commit()


@pytest.mark.oracledb
async def test_inmemory_table_functional_async(oracle_async_config: OracleAsyncConfig) -> None:
    """Test that INMEMORY table works correctly for session operations."""
    config = OracleAsyncConfig(
        connection_config=oracle_async_config.connection_config, extension_config={"litestar": {"in_memory": True}}
    )

    store = OracleAsyncStore(config)
    await store.create_table()

    try:
        session_id = "inmemory-test-session"
        session_data = b"test-session-data"

        await store.set(session_id, session_data)

        retrieved = await store.get(session_id)
        assert retrieved is not None
        assert retrieved == session_data

        exists = await store.exists(session_id)
        assert exists is True

        await store.delete(session_id)

        exists_after = await store.exists(session_id)
        assert exists_after is False

    finally:
        async with config.provide_connection() as conn:
            cursor = conn.cursor()
            for stmt in store._get_drop_table_sql():  # pyright: ignore[reportPrivateUsage]
                try:
                    await cursor.execute(stmt)
                except Exception:
                    pass
            await conn.commit()


@pytest.mark.oracledb
def test_inmemory_enabled_sync(oracle_sync_config: OracleSyncConfig) -> None:
    """Test that in_memory=True works with sync store."""
    config = OracleSyncConfig(
        connection_config=oracle_sync_config.connection_config, extension_config={"litestar": {"in_memory": True}}
    )

    store = OracleSyncStore(config)
    import asyncio

    asyncio.run(store.create_table())

    try:
        with config.provide_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT inmemory, inmemory_priority
                FROM user_tables
                WHERE table_name = 'LITESTAR_SESSION'
                """
            )
            row = cursor.fetchone()

        assert row is not None, "Session table should exist"
        inmemory_status, inmemory_priority = row[0], row[1]
        assert inmemory_status == "ENABLED", f"Table should have INMEMORY enabled, got: {inmemory_status}"
        assert inmemory_priority == "HIGH", f"Table should have INMEMORY PRIORITY HIGH, got: {inmemory_priority}"

    finally:
        with config.provide_connection() as conn:
            cursor = conn.cursor()
            for stmt in store._get_drop_table_sql():  # pyright: ignore[reportPrivateUsage]
                try:
                    cursor.execute(stmt)
                except Exception:
                    pass
            conn.commit()


@pytest.mark.oracledb
def test_inmemory_disabled_sync(oracle_sync_config: OracleSyncConfig) -> None:
    """Test that in_memory=False works with sync store."""
    config = OracleSyncConfig(
        connection_config=oracle_sync_config.connection_config, extension_config={"litestar": {"in_memory": False}}
    )

    store = OracleSyncStore(config)
    import asyncio

    asyncio.run(store.create_table())

    try:
        with config.provide_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT inmemory
                FROM user_tables
                WHERE table_name = 'LITESTAR_SESSION'
                """
            )
            row = cursor.fetchone()

        assert row is not None
        inmemory_status = row[0]
        assert inmemory_status == "DISABLED", f"Table should have INMEMORY disabled, got: {inmemory_status}"

    finally:
        with config.provide_connection() as conn:
            cursor = conn.cursor()
            for stmt in store._get_drop_table_sql():  # pyright: ignore[reportPrivateUsage]
                try:
                    cursor.execute(stmt)
                except Exception:
                    pass
            conn.commit()


@pytest.mark.oracledb
def test_inmemory_table_functional_sync(oracle_sync_config: OracleSyncConfig) -> None:
    """Test that INMEMORY table works correctly in sync mode."""
    config = OracleSyncConfig(
        connection_config=oracle_sync_config.connection_config, extension_config={"litestar": {"in_memory": True}}
    )

    store = OracleSyncStore(config)
    import asyncio

    asyncio.run(store.create_table())

    try:
        session_id = "inmemory-sync-session"
        session_data = b"sync-session-data"

        asyncio.run(store.set(session_id, session_data))

        retrieved = asyncio.run(store.get(session_id))
        assert retrieved is not None
        assert retrieved == session_data

    finally:
        with config.provide_connection() as conn:
            cursor = conn.cursor()
            for stmt in store._get_drop_table_sql():  # pyright: ignore[reportPrivateUsage]
                try:
                    cursor.execute(stmt)
                except Exception:
                    pass
            conn.commit()
