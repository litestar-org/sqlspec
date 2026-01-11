"""Oracle IN MEMORY table feature tests for ADK extension.

Tests verify:
- Tables created with INMEMORY clause when in_memory=True
- Tables created without INMEMORY clause when in_memory=False (default)
- INMEMORY status verifiable via Oracle data dictionary
- Works with both async and sync stores
- Compatible with owner_id_column feature
"""

import pytest

from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleSyncConfig
from sqlspec.adapters.oracledb.adk import OracleAsyncADKStore, OracleSyncADKStore

pytestmark = [pytest.mark.xdist_group("oracle"), pytest.mark.oracledb, pytest.mark.integration]


@pytest.mark.oracledb
async def test_inmemory_enabled_creates_sessions_table_with_inmemory_async(
    oracle_async_config: OracleAsyncConfig,
) -> None:
    """Test that in_memory=True creates sessions table with INMEMORY clause."""
    config = OracleAsyncConfig(
        connection_config=oracle_async_config.connection_config, extension_config={"adk": {"in_memory": True}}
    )

    store = OracleAsyncADKStore(config)
    await store.create_tables()

    try:
        async with config.provide_connection() as conn:
            cursor = conn.cursor()
            await cursor.execute(
                """
                SELECT inmemory, inmemory_priority, inmemory_distribute
                FROM user_tables
                WHERE table_name = 'ADK_SESSIONS'
                """
            )
            row = await cursor.fetchone()

        assert row is not None, "Sessions table should exist"
        inmemory_status, inmemory_priority = row[0], row[1]
        assert inmemory_status == "ENABLED", f"Sessions table should have INMEMORY enabled, got: {inmemory_status}"
        assert inmemory_priority == "HIGH", (
            f"Sessions table should have INMEMORY PRIORITY HIGH, got: {inmemory_priority}"
        )

    finally:
        async with config.provide_connection() as conn:
            cursor = conn.cursor()
            for stmt in store._get_drop_tables_sql():  # pyright: ignore[reportPrivateUsage]
                try:
                    await cursor.execute(stmt)
                except Exception:
                    pass
            await conn.commit()


@pytest.mark.oracledb
async def test_inmemory_enabled_creates_events_table_with_inmemory_async(
    oracle_async_config: OracleAsyncConfig,
) -> None:
    """Test that in_memory=True creates events table with INMEMORY clause."""
    config = OracleAsyncConfig(
        connection_config=oracle_async_config.connection_config, extension_config={"adk": {"in_memory": True}}
    )

    store = OracleAsyncADKStore(config)
    await store.create_tables()

    try:
        async with config.provide_connection() as conn:
            cursor = conn.cursor()
            await cursor.execute(
                """
                SELECT inmemory, inmemory_priority, inmemory_distribute
                FROM user_tables
                WHERE table_name = 'ADK_EVENTS'
                """
            )
            row = await cursor.fetchone()

        assert row is not None, "Events table should exist"
        inmemory_status, inmemory_priority = row[0], row[1]
        assert inmemory_status == "ENABLED", f"Events table should have INMEMORY enabled, got: {inmemory_status}"
        assert inmemory_priority == "HIGH", f"Events table should have INMEMORY PRIORITY HIGH, got: {inmemory_priority}"

    finally:
        async with config.provide_connection() as conn:
            cursor = conn.cursor()
            for stmt in store._get_drop_tables_sql():  # pyright: ignore[reportPrivateUsage]
                try:
                    await cursor.execute(stmt)
                except Exception:
                    pass
            await conn.commit()


@pytest.mark.oracledb
async def test_inmemory_disabled_creates_tables_without_inmemory_async(oracle_async_config: OracleAsyncConfig) -> None:
    """Test that in_memory=False (default) creates tables without INMEMORY clause."""
    config = OracleAsyncConfig(
        connection_config=oracle_async_config.connection_config, extension_config={"adk": {"in_memory": False}}
    )

    store = OracleAsyncADKStore(config)
    await store.create_tables()

    try:
        async with config.provide_connection() as conn:
            cursor = conn.cursor()
            await cursor.execute(
                """
                SELECT inmemory, inmemory_priority, inmemory_distribute
                FROM user_tables
                WHERE table_name IN ('ADK_SESSIONS', 'ADK_EVENTS')
                ORDER BY table_name
                """
            )
            rows = await cursor.fetchall()

        assert len(rows) == 2, "Both tables should exist"

        for row in rows:
            inmemory_status = row[0]
            assert inmemory_status == "DISABLED", f"Table should have INMEMORY disabled, got: {inmemory_status}"

    finally:
        async with config.provide_connection() as conn:
            cursor = conn.cursor()
            for stmt in store._get_drop_tables_sql():  # pyright: ignore[reportPrivateUsage]
                try:
                    await cursor.execute(stmt)
                except Exception:
                    pass
            await conn.commit()


@pytest.mark.oracledb
async def test_inmemory_default_disabled_async(oracle_async_config: OracleAsyncConfig) -> None:
    """Test that in_memory defaults to False when not specified."""
    config = OracleAsyncConfig(connection_config=oracle_async_config.connection_config, extension_config={"adk": {}})

    store = OracleAsyncADKStore(config)
    await store.create_tables()

    try:
        async with config.provide_connection() as conn:
            cursor = conn.cursor()
            await cursor.execute(
                """
                SELECT inmemory
                FROM user_tables
                WHERE table_name = 'ADK_SESSIONS'
                """
            )
            row = await cursor.fetchone()

        assert row is not None
        inmemory_status = row[0]
        assert inmemory_status == "DISABLED", "Default should be INMEMORY disabled"

    finally:
        async with config.provide_connection() as conn:
            cursor = conn.cursor()
            for stmt in store._get_drop_tables_sql():  # pyright: ignore[reportPrivateUsage]
                try:
                    await cursor.execute(stmt)
                except Exception:
                    pass
            await conn.commit()


@pytest.mark.oracledb
async def test_inmemory_with_owner_id_column_async(oracle_async_config: OracleAsyncConfig) -> None:
    """Test that in_memory works together with owner_id_column feature."""
    async with oracle_async_config.provide_connection() as conn:
        cursor = conn.cursor()
        await cursor.execute(
            """
            BEGIN
                EXECUTE IMMEDIATE 'CREATE TABLE test_owners (
                    id NUMBER(10) PRIMARY KEY,
                    name VARCHAR2(128) NOT NULL
                )';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN
                        RAISE;
                    END IF;
            END;
            """
        )
        await cursor.execute("INSERT INTO test_owners (id, name) VALUES (1, 'Owner 1')")
        await conn.commit()

    try:
        config = OracleAsyncConfig(
            connection_config=oracle_async_config.connection_config,
            extension_config={
                "adk": {"in_memory": True, "owner_id_column": "owner_id NUMBER(10) NOT NULL REFERENCES test_owners(id)"}
            },
        )

        store = OracleAsyncADKStore(config)
        await store.create_tables()

        async with config.provide_connection() as conn:
            cursor = conn.cursor()
            await cursor.execute(
                """
                SELECT inmemory, column_name
                FROM user_tables t
                LEFT JOIN user_tab_columns c ON t.table_name = c.table_name
                WHERE t.table_name = 'ADK_SESSIONS' AND (c.column_name = 'OWNER_ID' OR c.column_name IS NULL)
                """
            )
            rows = await cursor.fetchall()

        inmemory_enabled = any(row[0] == "ENABLED" for row in rows)
        owner_id_exists = any(row[1] == "OWNER_ID" for row in rows)

        assert inmemory_enabled, "Sessions table should have INMEMORY enabled"
        assert owner_id_exists, "Sessions table should have owner_id column"

        session_id = "test-session-with-fk"
        session = await store.create_session(session_id, "test-app", "user-123", {"data": "test"}, owner_id=1)
        assert session["id"] == session_id

        async with config.provide_connection() as conn:
            cursor = conn.cursor()
            for stmt in store._get_drop_tables_sql():  # pyright: ignore[reportPrivateUsage]
                try:
                    await cursor.execute(stmt)
                except Exception:
                    pass
            await conn.commit()

    finally:
        async with oracle_async_config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                await cursor.execute(
                    """
                    BEGIN
                        EXECUTE IMMEDIATE 'DROP TABLE test_owners';
                    EXCEPTION
                        WHEN OTHERS THEN
                            IF SQLCODE != -942 THEN
                                RAISE;
                            END IF;
                    END;
                    """
                )
                await conn.commit()
            except Exception:
                pass


@pytest.mark.oracledb
async def test_inmemory_tables_functional_async(oracle_async_config: OracleAsyncConfig) -> None:
    """Test that INMEMORY tables work correctly for session operations."""
    config = OracleAsyncConfig(
        connection_config=oracle_async_config.connection_config, extension_config={"adk": {"in_memory": True}}
    )

    store = OracleAsyncADKStore(config)
    await store.create_tables()

    try:
        session_id = "inmemory-test-session"
        app_name = "test-app"
        user_id = "user-123"
        state = {"data": "test", "count": 42}

        session = await store.create_session(session_id, app_name, user_id, state)
        assert session["id"] == session_id
        assert session["state"] == state

        retrieved = await store.get_session(session_id)
        assert retrieved is not None
        assert retrieved["state"] == state

        updated_state = {"data": "updated", "count": 100}
        await store.update_session_state(session_id, updated_state)

        retrieved_updated = await store.get_session(session_id)
        assert retrieved_updated is not None
        assert retrieved_updated["state"] == updated_state

    finally:
        async with config.provide_connection() as conn:
            cursor = conn.cursor()
            for stmt in store._get_drop_tables_sql():  # pyright: ignore[reportPrivateUsage]
                try:
                    await cursor.execute(stmt)
                except Exception:
                    pass
            await conn.commit()


@pytest.mark.oracledb
def test_inmemory_enabled_sync(oracle_sync_config: OracleSyncConfig) -> None:
    """Test that in_memory=True works with sync store."""
    config = OracleSyncConfig(
        connection_config=oracle_sync_config.connection_config, extension_config={"adk": {"in_memory": True}}
    )

    store = OracleSyncADKStore(config)
    store.create_tables()

    try:
        with config.provide_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT inmemory, inmemory_priority
                FROM user_tables
                WHERE table_name IN ('ADK_SESSIONS', 'ADK_EVENTS')
                ORDER BY table_name
                """
            )
            rows = cursor.fetchall()

        assert len(rows) == 2, "Both tables should exist"

        for row in rows:
            inmemory_status, inmemory_priority = row[0], row[1]
            assert inmemory_status == "ENABLED", f"Table should have INMEMORY enabled, got: {inmemory_status}"
            assert inmemory_priority == "HIGH", f"Table should have INMEMORY PRIORITY HIGH, got: {inmemory_priority}"

    finally:
        with config.provide_connection() as conn:
            cursor = conn.cursor()
            for stmt in store._get_drop_tables_sql():  # pyright: ignore[reportPrivateUsage]
                try:
                    cursor.execute(stmt)
                except Exception:
                    pass
            conn.commit()


@pytest.mark.oracledb
def test_inmemory_disabled_sync(oracle_sync_config: OracleSyncConfig) -> None:
    """Test that in_memory=False works with sync store."""
    config = OracleSyncConfig(
        connection_config=oracle_sync_config.connection_config, extension_config={"adk": {"in_memory": False}}
    )

    store = OracleSyncADKStore(config)
    store.create_tables()

    try:
        with config.provide_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT inmemory, inmemory_priority
                FROM user_tables
                WHERE table_name IN ('ADK_SESSIONS', 'ADK_EVENTS')
                """
            )
            rows = cursor.fetchall()

        assert len(rows) == 2

        for row in rows:
            inmemory_status = row[0]
            assert inmemory_status == "DISABLED", f"Table should have INMEMORY disabled, got: {inmemory_status}"

    finally:
        with config.provide_connection() as conn:
            cursor = conn.cursor()
            for stmt in store._get_drop_tables_sql():  # pyright: ignore[reportPrivateUsage]
                try:
                    cursor.execute(stmt)
                except Exception:
                    pass
            conn.commit()


@pytest.mark.oracledb
def test_inmemory_tables_functional_sync(oracle_sync_config: OracleSyncConfig) -> None:
    """Test that INMEMORY tables work correctly in sync mode."""
    config = OracleSyncConfig(
        connection_config=oracle_sync_config.connection_config, extension_config={"adk": {"in_memory": True}}
    )

    store = OracleSyncADKStore(config)
    store.create_tables()

    try:
        session_id = "inmemory-sync-session"
        app_name = "test-app"
        user_id = "user-456"
        state = {"sync": True, "value": 99}

        session = store.create_session(session_id, app_name, user_id, state)
        assert session["id"] == session_id
        assert session["state"] == state

        retrieved = store.get_session(session_id)
        assert retrieved is not None
        assert retrieved["state"] == state

    finally:
        with config.provide_connection() as conn:
            cursor = conn.cursor()
            for stmt in store._get_drop_tables_sql():  # pyright: ignore[reportPrivateUsage]
                try:
                    cursor.execute(stmt)
                except Exception:
                    pass
            conn.commit()
