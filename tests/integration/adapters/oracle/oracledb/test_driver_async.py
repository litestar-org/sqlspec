"""Test OracleDB async driver implementation."""

import pytest
from pytest_databases.docker.oracle import OracleService

from sqlspec import sql
from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleAsyncDriver, OraclePoolParams
from sqlspec.exceptions import SQLSpecError

pytestmark = [pytest.mark.xdist_group("oracle")]


async def test_async_connection(oracle_23ai_service: "OracleService") -> None:
    """Test async connection components for OracleDB."""
    base_config = OraclePoolParams(
        host=oracle_23ai_service.host,
        port=oracle_23ai_service.port,
        service_name=oracle_23ai_service.service_name,
        user=oracle_23ai_service.user,
        password=oracle_23ai_service.password,
    )
    async_config = OracleAsyncConfig(connection_config=base_config)
    pool = await async_config.create_pool()
    assert pool is not None
    try:
        async with pool.acquire() as conn:
            assert conn is not None
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1 FROM dual")
                result = await cur.fetchone()
                assert result == (1,)
    finally:
        await pool.close()

    pooled_config = OraclePoolParams(**base_config)
    pooled_config["min"] = 1
    pooled_config["max"] = 5
    another_config = OracleAsyncConfig(connection_config=pooled_config)
    pool = await another_config.create_pool()
    assert pool is not None
    try:
        async with pool.acquire() as conn:
            assert conn is not None
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1 FROM dual")
                result = await cur.fetchone()
                assert result == (1,)
    finally:
        await pool.close()


async def test_oracle_for_update_nowait(oracle_async_session: "OracleAsyncDriver") -> None:
    """Test FOR UPDATE NOWAIT with Oracle."""

    # Setup test table
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table_oracledb_async'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
    await oracle_async_session.execute_script("""
        CREATE TABLE test_table_oracledb_async (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(50),
            value NUMBER
        )
    """)

    # Insert test data
    await oracle_async_session.execute(
        "INSERT INTO test_table_oracledb_async (id, name, value) VALUES (1, :1, :2)", ("oracle_nowait", 200)
    )

    try:
        await oracle_async_session.begin()

        # Test FOR UPDATE NOWAIT
        result = await oracle_async_session.select_one(
            sql.select("*").from_("test_table_oracledb_async").where_eq("name", "oracle_nowait").for_update(nowait=True)
        )
        assert result is not None
        assert result["name"] == "oracle_nowait"

        await oracle_async_session.commit()
    except Exception:
        await oracle_async_session.rollback()
        raise
    finally:
        await oracle_async_session.execute_script(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table_oracledb_async'; EXCEPTION WHEN OTHERS THEN NULL; END;"
        )


async def test_oracle_for_share_locking_unsupported(oracle_async_session: "OracleAsyncDriver") -> None:
    """Test that FOR SHARE is not supported in Oracle and raises expected error."""

    # Setup test table
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table_oracledb_async'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
    await oracle_async_session.execute_script("""
        CREATE TABLE test_table_oracledb_async (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(50),
            value NUMBER
        )
    """)

    # Insert test data
    await oracle_async_session.execute(
        "INSERT INTO test_table_oracledb_async (id, name, value) VALUES (1, :1, :2)", ("oracle_share", 300)
    )

    try:
        await oracle_async_session.begin()

        # Test FOR SHARE - Oracle doesn't support this syntax, should raise ORA-02000
        # Note: Oracle only supports FOR UPDATE for row-level locking
        with pytest.raises(SQLSpecError, match=r"ORA-02000.*missing COMPRESS or UPDATE keyword"):
            await oracle_async_session.select_one(
                sql
                .select("id", "name", "value")
                .from_("test_table_oracledb_async")
                .where_eq("name", "oracle_share")
                .for_share()
            )

        await oracle_async_session.rollback()
    except Exception:
        await oracle_async_session.rollback()
        raise
    finally:
        await oracle_async_session.execute_script(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table_oracledb_async'; EXCEPTION WHEN OTHERS THEN NULL; END;"
        )
