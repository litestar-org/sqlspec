"""Test OracleDB connection mechanisms with CORE_ROUND_3 architecture."""

import pytest
from pytest_databases.docker.oracle import OracleService

from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleSyncConfig


@pytest.mark.xdist_group("oracle")
async def test_async_connection(oracle_23ai_service: OracleService) -> None:
    """Test async connection components for OracleDB."""
    async_config = OracleAsyncConfig(
        pool_config={
            "host": oracle_23ai_service.host,
            "port": oracle_23ai_service.port,
            "service_name": oracle_23ai_service.service_name,
            "user": oracle_23ai_service.user,
            "password": oracle_23ai_service.password,
        }
    )

    # Test direct connection
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

    # Test pool with connection parameters
    another_config = OracleAsyncConfig(
        pool_config={
            "host": oracle_23ai_service.host,
            "port": oracle_23ai_service.port,
            "service_name": oracle_23ai_service.service_name,
            "user": oracle_23ai_service.user,
            "password": oracle_23ai_service.password,
            "min": 1,
            "max": 5,
        }
    )
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


@pytest.mark.xdist_group("oracle")
def test_sync_connection(oracle_23ai_service: OracleService) -> None:
    """Test sync connection components for OracleDB."""
    sync_config = OracleSyncConfig(
        pool_config={
            "host": oracle_23ai_service.host,
            "port": oracle_23ai_service.port,
            "service_name": oracle_23ai_service.service_name,
            "user": oracle_23ai_service.user,
            "password": oracle_23ai_service.password,
        }
    )

    # Test direct connection
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

    # Test pool with connection parameters
    another_config = OracleSyncConfig(
        pool_config={
            "host": oracle_23ai_service.host,
            "port": oracle_23ai_service.port,
            "service_name": oracle_23ai_service.service_name,
            "user": oracle_23ai_service.user,
            "password": oracle_23ai_service.password,
            "min": 1,
            "max": 5,
        }
    )
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
