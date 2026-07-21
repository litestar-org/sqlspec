"""Test OracleDB sync driver implementation."""

import pytest
from pytest_databases.docker.oracle import OracleService

from sqlspec import sql
from sqlspec.adapters.oracledb import OraclePoolParams, OracleSyncConfig, OracleSyncDriver
from sqlspec.exceptions import SQLSpecError

pytestmark = pytest.mark.xdist_group("oracle")


def test_sync_connection(oracle_23ai_service: "OracleService") -> None:
    """Test sync connection components for OracleDB."""
    base_config = OraclePoolParams(
        host=oracle_23ai_service.host,
        port=oracle_23ai_service.port,
        service_name=oracle_23ai_service.service_name,
        user=oracle_23ai_service.user,
        password=oracle_23ai_service.password,
    )
    sync_config = OracleSyncConfig(connection_config=base_config)
    pool = sync_config.create_pool()
    assert pool is not None
    try:
        with pool.acquire() as conn:
            assert conn is not None
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM dual")
                result = cur.fetchone()
                assert result == (1,)
    finally:
        pool.close()

    pooled_config = OraclePoolParams(**base_config)
    pooled_config["min"] = 1
    pooled_config["max"] = 5
    another_config = OracleSyncConfig(connection_config=pooled_config)
    pool = another_config.create_pool()
    assert pool is not None
    try:
        with pool.acquire() as conn:
            assert conn is not None
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM dual")
                result = cur.fetchone()
                assert result == (1,)
    finally:
        pool.close()


def test_oracle_sync_for_update_nowait(oracle_sync_session: "OracleSyncDriver") -> None:
    """Test FOR UPDATE NOWAIT with Oracle (sync)."""

    # Setup test table
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table_oracledb_sync'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
    oracle_sync_session.execute_script("""
        CREATE TABLE test_table_oracledb_sync (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(50),
            value NUMBER
        )
    """)

    # Insert test data
    oracle_sync_session.execute(
        "INSERT INTO test_table_oracledb_sync (id, name, value) VALUES (1, :1, :2)", ("oracle_sync_nowait", 200)
    )

    try:
        oracle_sync_session.begin()

        # Test FOR UPDATE NOWAIT
        result = oracle_sync_session.select_one(
            sql
            .select("*")
            .from_("test_table_oracledb_sync")
            .where_eq("name", "oracle_sync_nowait")
            .for_update(nowait=True)
        )
        assert result is not None
        assert result["name"] == "oracle_sync_nowait"

        oracle_sync_session.commit()
    except Exception:
        oracle_sync_session.rollback()
        raise
    finally:
        oracle_sync_session.execute_script(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table_oracledb_sync'; EXCEPTION WHEN OTHERS THEN NULL; END;"
        )


def test_oracle_sync_for_share_locking_unsupported(oracle_sync_session: "OracleSyncDriver") -> None:
    """Test that FOR SHARE is not supported in Oracle and raises expected error (sync)."""

    # Setup test table
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table_oracledb_sync'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
    oracle_sync_session.execute_script("""
        CREATE TABLE test_table_oracledb_sync (
            id NUMBER PRIMARY KEY,
            name VARCHAR2(50),
            value NUMBER
        )
    """)

    # Insert test data
    oracle_sync_session.execute(
        "INSERT INTO test_table_oracledb_sync (id, name, value) VALUES (1, :1, :2)", ("oracle_sync_share", 300)
    )

    try:
        oracle_sync_session.begin()

        # Test FOR SHARE - Oracle doesn't support this syntax, should raise ORA-02000
        # Note: Oracle only supports FOR UPDATE for row-level locking
        with pytest.raises(SQLSpecError, match=r"ORA-02000.*missing COMPRESS or UPDATE keyword"):
            oracle_sync_session.select_one(
                sql
                .select("id", "name", "value")
                .from_("test_table_oracledb_sync")
                .where_eq("name", "oracle_sync_share")
                .for_share()
            )

        oracle_sync_session.rollback()
    except Exception:
        oracle_sync_session.rollback()
        raise
    finally:
        oracle_sync_session.execute_script(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table_oracledb_sync'; EXCEPTION WHEN OTHERS THEN NULL; END;"
        )
