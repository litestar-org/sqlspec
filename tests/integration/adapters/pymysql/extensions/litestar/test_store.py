"""Integration tests for PyMySQL session store."""

import asyncio
from collections.abc import AsyncGenerator
from datetime import timedelta

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.pymysql.config import PyMysqlConfig
from sqlspec.adapters.pymysql.litestar.store import PyMysqlStore

pytestmark = [pytest.mark.xdist_group("mysql"), pytest.mark.mysql, pytest.mark.pymysql, pytest.mark.integration]


@pytest.fixture
async def pymysql_store(mysql_service: MySQLService) -> "AsyncGenerator[PyMysqlStore, None]":
    """Create PyMySQL store with test database."""
    config = PyMysqlConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
        },
        extension_config={"litestar": {"session_table": "test_pymysql_sessions"}},
    )
    store = PyMysqlStore(config)
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


async def test_pymysql_store_set_get(pymysql_store: PyMysqlStore) -> None:
    """Test store set/get operations."""
    await pymysql_store.set("session1", "value1")
    result = await pymysql_store.get("session1")
    assert result == b"value1"


async def test_pymysql_store_expiry(pymysql_store: PyMysqlStore) -> None:
    """Test store expiry handling."""
    await pymysql_store.set("session_exp", "value", expires_in=timedelta(seconds=1))
    exists = await pymysql_store.exists("session_exp")
    assert exists is True
    await asyncio.sleep(1.1)
    await pymysql_store.delete_expired()
    exists_after = await pymysql_store.exists("session_exp")
    assert exists_after is False


async def test_pymysql_store_delete(pymysql_store: PyMysqlStore) -> None:
    """Test store delete operations."""
    await pymysql_store.set("session_del", "value")
    await pymysql_store.delete("session_del")
    result = await pymysql_store.get("session_del")
    assert result is None
