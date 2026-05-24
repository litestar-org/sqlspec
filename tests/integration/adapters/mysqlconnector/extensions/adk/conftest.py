"""MysqlConnector ADK test fixtures."""

from collections.abc import AsyncGenerator

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.mysqlconnector import MysqlConnectorAsyncConfig
from sqlspec.adapters.mysqlconnector.adk import MysqlConnectorAsyncADKStore


@pytest.fixture
async def mysqlconnector_adk_store(mysql_service: MySQLService) -> "AsyncGenerator[MysqlConnectorAsyncADKStore, None]":
    """Create MysqlConnector ADK store with test database."""
    config = MysqlConnectorAsyncConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "autocommit": False,
            "use_pure": True,
        },
        extension_config={
            "adk": {
                "session_table": "test_sessions",
                "events_table": "test_events",
                "app_state_table": "test_app_state",
                "user_state_table": "test_user_state",
                "metadata_table": "test_adk_metadata",
            }
        },
    )

    try:
        store = MysqlConnectorAsyncADKStore(config)
        await store.create_tables()

        yield store

        async with config.provide_connection() as conn:
            cursor = await conn.cursor()
            try:
                await cursor.execute("DROP TABLE IF EXISTS test_events")
                await cursor.execute("DROP TABLE IF EXISTS test_sessions")
                await cursor.execute("DROP TABLE IF EXISTS test_user_state")
                await cursor.execute("DROP TABLE IF EXISTS test_app_state")
                await cursor.execute("DROP TABLE IF EXISTS test_adk_metadata")
                await conn.commit()
            finally:
                await cursor.close()
    finally:
        config.connection_instance = None


@pytest.fixture
async def mysqlconnector_adk_store_with_fk(
    mysql_service: MySQLService,
) -> "AsyncGenerator[MysqlConnectorAsyncADKStore, None]":
    """Create MysqlConnector ADK store with owner ID column."""
    config = MysqlConnectorAsyncConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "autocommit": False,
            "use_pure": True,
        },
        extension_config={
            "adk": {
                "session_table": "test_fk_sessions",
                "events_table": "test_fk_events",
                "app_state_table": "test_fk_app_state",
                "user_state_table": "test_fk_user_state",
                "metadata_table": "test_fk_adk_metadata",
                "owner_id_column": "tenant_id BIGINT NOT NULL REFERENCES test_tenants(id) ON DELETE CASCADE",
            }
        },
    )

    try:
        async with config.provide_connection() as conn:
            cursor = await conn.cursor()
            try:
                await cursor.execute("""
                    CREATE TABLE IF NOT EXISTS test_tenants (
                        id BIGINT PRIMARY KEY AUTO_INCREMENT,
                        name VARCHAR(128) NOT NULL UNIQUE
                    ) ENGINE=InnoDB
                """)
                await cursor.execute("INSERT INTO test_tenants (name) VALUES ('tenant1'), ('tenant2')")
                await conn.commit()
            finally:
                await cursor.close()

        store = MysqlConnectorAsyncADKStore(config)
        await store.create_tables()

        yield store

        async with config.provide_connection() as conn:
            cursor = await conn.cursor()
            try:
                await cursor.execute("DROP TABLE IF EXISTS test_fk_events")
                await cursor.execute("DROP TABLE IF EXISTS test_fk_sessions")
                await cursor.execute("DROP TABLE IF EXISTS test_fk_user_state")
                await cursor.execute("DROP TABLE IF EXISTS test_fk_app_state")
                await cursor.execute("DROP TABLE IF EXISTS test_fk_adk_metadata")
                await cursor.execute("DROP TABLE IF EXISTS test_tenants")
                await conn.commit()
            finally:
                await cursor.close()
    finally:
        config.connection_instance = None
