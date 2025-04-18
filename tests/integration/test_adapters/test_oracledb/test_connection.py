"""Test OracleDB connection mechanisms."""

from __future__ import annotations

import pytest
from pytest_databases.docker.oracle import OracleService

from sqlspec.adapters.oracledb import OracleAsync, OracleAsyncPool, OracleSync, OracleSyncPool

pytestmark = pytest.mark.asyncio(loop_scope="session")


async def test_async_connection(oracle_23ai_service: OracleService) -> None:
    """Test async connection components for OracleDB."""
    async_config = OracleAsync(
        pool_config=OracleAsyncPool(
            host=oracle_23ai_service.host,
            port=oracle_23ai_service.port,
            service_name=oracle_23ai_service.service_name,
            user=oracle_23ai_service.user,
            password=oracle_23ai_service.password,
        )
    )

    # Test direct connection (if applicable, depends on adapter design)
    # Assuming create_pool is the primary way for oracledb async
    pool = await async_config.create_pool()
    assert pool is not None
    try:
        async with pool.acquire() as conn:  # Use acquire() for async pool
            assert conn is not None
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1 FROM dual")  # Oracle uses FROM dual
                result = await cur.fetchone()
                assert result == (1,)
    finally:
        await pool.close()

    # Test pool re-creation and connection acquisition
    pool_config = OracleAsyncPool(
        host=oracle_23ai_service.host,
        port=oracle_23ai_service.port,
        service_name=oracle_23ai_service.service_name,
        user=oracle_23ai_service.user,
        password=oracle_23ai_service.password,
    )
    another_config = OracleAsync(pool_config=pool_config)
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


def test_sync_connection(oracle_23ai_service: OracleService) -> None:
    """Test sync connection components for OracleDB."""
    sync_config = OracleSync(
        pool_config=OracleSyncPool(
            host=oracle_23ai_service.host,
            port=oracle_23ai_service.port,
            service_name=oracle_23ai_service.service_name,
            user=oracle_23ai_service.user,
            password=oracle_23ai_service.password,
        )
    )

    # Test direct connection (if applicable, depends on adapter design)
    # Assuming create_pool is the primary way for oracledb sync
    pool = sync_config.create_pool()
    assert pool is not None
    try:
        with pool.acquire() as conn:  # Use acquire() for sync pool
            assert conn is not None
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM dual")  # Oracle uses FROM dual
                result = cur.fetchone()
                assert result == (1,)
    finally:
        pool.close()

    # Test pool re-creation and connection acquisition
    pool_config = OracleSyncPool(
        host=oracle_23ai_service.host,
        port=oracle_23ai_service.port,
        service_name=oracle_23ai_service.service_name,
        user=oracle_23ai_service.user,
        password=oracle_23ai_service.password,
    )
    another_config = OracleSync(pool_config=pool_config)
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
