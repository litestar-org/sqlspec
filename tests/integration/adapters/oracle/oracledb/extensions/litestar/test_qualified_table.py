"""Integration tests for schema-qualified Oracle Litestar session tables."""

import asyncio
import contextlib

import pytest

from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleSyncConfig
from sqlspec.adapters.oracledb.litestar.store import OracleAsyncStore, OracleSyncStore

pytestmark = [pytest.mark.xdist_group("oracle"), pytest.mark.oracledb, pytest.mark.integration]


@pytest.mark.oracledb
async def test_oracle_qualified_session_table_round_trip_async(oracle_async_config: OracleAsyncConfig) -> None:
    """Qualified <SCHEMA>.sess table round-trips set, get, expires_in, and delete on Oracle."""
    async with oracle_async_config.provide_connection() as conn:
        with conn.cursor() as cursor:
            await cursor.execute("SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL")
            schema_row = await cursor.fetchone()

    assert schema_row is not None
    schema_name = str(schema_row[0])
    qualified_table = f"{schema_name}.sess"

    config = OracleAsyncConfig(
        connection_config=oracle_async_config.connection_config,
        extension_config={"litestar": {"session_table": qualified_table}},
    )
    store = OracleAsyncStore(config)

    try:
        await store.create_table()

        async with config.provide_connection() as conn:
            with conn.cursor() as cursor:
                await cursor.execute("SELECT table_name FROM user_tables WHERE table_name = 'SESS'")
                table_row = await cursor.fetchone()
        assert table_row is not None
        assert table_row[0] == "SESS"

        await store.set("qual-async-1", b"oracle-qualified-payload", expires_in=120)
        assert await store.get("qual-async-1") == b"oracle-qualified-payload"

        remaining = await store.expires_in("qual-async-1")
        assert remaining is not None
        assert 0 < remaining <= 120

        await store.delete("qual-async-1")
        assert await store.get("qual-async-1") is None

        async with config.provide_connection() as conn:
            with conn.cursor() as cursor:
                await cursor.execute(f"SELECT COUNT(*) FROM {qualified_table}")
                count_row = await cursor.fetchone()
        assert count_row is not None
        assert int(count_row[0]) == 0
    finally:
        async with config.provide_connection() as conn:
            with conn.cursor() as cursor:
                for stmt in store._drop_table_sql():
                    with contextlib.suppress(Exception):
                        await cursor.execute(stmt)
            await conn.commit()
        if config.connection_instance is not None:
            await config.close_pool()


@pytest.mark.oracledb
def test_oracle_qualified_session_table_round_trip_sync(oracle_sync_config: OracleSyncConfig) -> None:
    """Qualified <SCHEMA>.sess_sync table round-trips set, get, expires_in, and delete via OracleSyncStore."""
    with oracle_sync_config.provide_connection() as conn, conn.cursor() as cursor:
        cursor.execute("SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL")
        schema_row = cursor.fetchone()

    assert schema_row is not None
    schema_name = str(schema_row[0])
    qualified_table = f"{schema_name}.sess_sync"

    config = OracleSyncConfig(
        connection_config=oracle_sync_config.connection_config,
        extension_config={"litestar": {"session_table": qualified_table}},
    )
    store = OracleSyncStore(config)

    try:
        asyncio.run(store.create_table())

        with config.provide_connection() as conn, conn.cursor() as cursor:
            cursor.execute("SELECT table_name FROM user_tables WHERE table_name = 'SESS_SYNC'")
            table_row = cursor.fetchone()
        assert table_row is not None
        assert table_row[0] == "SESS_SYNC"

        asyncio.run(store.set("qual-sync-1", b"oracle-sync-payload", expires_in=120))
        assert asyncio.run(store.get("qual-sync-1")) == b"oracle-sync-payload"

        remaining = asyncio.run(store.expires_in("qual-sync-1"))
        assert remaining is not None
        assert 0 < remaining <= 120

        asyncio.run(store.delete("qual-sync-1"))
        assert asyncio.run(store.get("qual-sync-1")) is None

        with config.provide_connection() as conn, conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {qualified_table}")
            count_row = cursor.fetchone()
        assert count_row is not None
        assert int(count_row[0]) == 0
    finally:
        with config.provide_connection() as conn:
            with conn.cursor() as cursor:
                for stmt in store._drop_table_sql():
                    with contextlib.suppress(Exception):
                        cursor.execute(stmt)
            conn.commit()
        if config.connection_instance is not None:
            config.close_pool()
