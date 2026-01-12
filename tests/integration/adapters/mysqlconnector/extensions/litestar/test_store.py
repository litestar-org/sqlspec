"""Integration tests for MysqlConnector session store."""

import asyncio
from collections.abc import AsyncGenerator
from datetime import timedelta

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.mysqlconnector.config import MysqlConnectorAsyncConfig, MysqlConnectorSyncConfig
from sqlspec.adapters.mysqlconnector.litestar.store import MysqlConnectorAsyncStore, MysqlConnectorSyncStore

pytestmark = [pytest.mark.xdist_group("mysql"), pytest.mark.mysql_connector, pytest.mark.integration]


@pytest.fixture
async def mysqlconnector_async_store(mysql_service: MySQLService) -> "AsyncGenerator[MysqlConnectorAsyncStore, None]":
    """Create MysqlConnector async store with test database."""
    config = MysqlConnectorAsyncConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "use_pure": True,
        },
        extension_config={"litestar": {"session_table": "test_mysqlconnector_sessions"}},
    )
    store = MysqlConnectorAsyncStore(config)
    try:
        await store.create_table()
        yield store
        try:
            await store.delete_all()
        except Exception:
            pass
    finally:
        config.connection_instance = None


@pytest.fixture
async def mysqlconnector_sync_store(mysql_service: MySQLService) -> "AsyncGenerator[MysqlConnectorSyncStore, None]":
    """Create MysqlConnector sync store with test database."""
    config = MysqlConnectorSyncConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "use_pure": True,
        },
        extension_config={"litestar": {"session_table": "test_mysqlconnector_sync_sessions"}},
    )
    store = MysqlConnectorSyncStore(config)
    try:
        await store.create_table()
        yield store
        try:
            await store.delete_all()
        except Exception:
            pass
    finally:
        if config.connection_instance:
            config.close_pool()


async def test_mysqlconnector_async_store_set_get(mysqlconnector_async_store: MysqlConnectorAsyncStore) -> None:
    """Test async store set/get operations."""
    await mysqlconnector_async_store.set("session1", "value1")
    result = await mysqlconnector_async_store.get("session1")
    assert result == b"value1"


async def test_mysqlconnector_async_store_expiry(mysqlconnector_async_store: MysqlConnectorAsyncStore) -> None:
    """Test async store expiry handling."""
    await mysqlconnector_async_store.set("session_exp", "value", expires_in=timedelta(seconds=1))
    exists = await mysqlconnector_async_store.exists("session_exp")
    assert exists is True
    await asyncio.sleep(1.1)
    exists_after = await mysqlconnector_async_store.exists("session_exp")
    assert exists_after is False


async def test_mysqlconnector_async_store_delete(mysqlconnector_async_store: MysqlConnectorAsyncStore) -> None:
    """Test async store delete operations."""
    await mysqlconnector_async_store.set("session_del", "value")
    await mysqlconnector_async_store.delete("session_del")
    result = await mysqlconnector_async_store.get("session_del")
    assert result is None


async def test_mysqlconnector_sync_store_set_get(mysqlconnector_sync_store: MysqlConnectorSyncStore) -> None:
    """Test sync store set/get operations."""
    await mysqlconnector_sync_store.set("session1", "value1")
    result = await mysqlconnector_sync_store.get("session1")
    assert result == b"value1"


async def test_mysqlconnector_sync_store_delete(mysqlconnector_sync_store: MysqlConnectorSyncStore) -> None:
    """Test sync store delete operations."""
    await mysqlconnector_sync_store.set("session_del", "value")
    await mysqlconnector_sync_store.delete("session_del")
    result = await mysqlconnector_sync_store.get("session_del")
    assert result is None
