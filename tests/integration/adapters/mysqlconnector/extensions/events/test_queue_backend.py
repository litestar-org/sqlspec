# pyright: reportPrivateUsage=false, reportAttributeAccessIssue=false, reportArgumentType=false
"""MysqlConnector integration tests for the EventChannel queue backend."""

from typing import Any

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.mysqlconnector import MysqlConnectorAsyncConfig
from tests.integration.adapters._events_helpers import setup_async_event_channel

pytestmark = [pytest.mark.xdist_group("mysql"), pytest.mark.mysql_connector, pytest.mark.asyncio]


@pytest.mark.mysql
async def test_mysqlconnector_event_channel_queue_fallback(mysql_service: MySQLService, tmp_path: Any) -> None:
    """MysqlConnector configs publish, consume, and ack events via the queue backend."""
    migrations = tmp_path / "migrations"
    migrations.mkdir()

    config = MysqlConnectorAsyncConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "autocommit": True,
            "use_pure": True,
        },
        migration_config={
            "script_location": str(migrations),
            "include_extensions": ["events"],
            "version_table_name": "ddl_migrations_mysqlconn",
        },
        extension_config={"events": {"queue_table": "mysqlconn_event_queue"}},
    )

    _spec, channel = await setup_async_event_channel(config)

    assert channel._backend_name == "table_queue"

    event_id = await channel.publish("notifications", {"action": "mysql"})
    iterator = channel.iter_events("notifications", poll_interval=0.05)
    message = await iterator.__anext__()
    await iterator.aclose()
    await channel.ack(message.event_id)

    async with config.provide_session() as driver:
        row = await driver.select_one(
            "SELECT status FROM mysqlconn_event_queue WHERE event_id = :event_id", {"event_id": event_id}
        )

    assert message.payload["action"] == "mysql"
    assert row["status"] == "acked"


@pytest.mark.mysql
async def test_mysqlconnector_event_channel_multiple_messages(mysql_service: MySQLService, tmp_path: Any) -> None:
    """MysqlConnector queue backend handles multiple messages correctly."""
    migrations = tmp_path / "migrations"
    migrations.mkdir()

    config = MysqlConnectorAsyncConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "autocommit": True,
            "use_pure": True,
        },
        migration_config={
            "script_location": str(migrations),
            "include_extensions": ["events"],
            "version_table_name": "ddl_migrations_mysqlconn",
        },
        extension_config={"events": {"queue_table": "mysqlconn_event_queue"}},
    )

    _spec, channel = await setup_async_event_channel(config)

    event_ids = [
        await channel.publish("multi_test", {"index": 0}),
        await channel.publish("multi_test", {"index": 1}),
        await channel.publish("multi_test", {"index": 2}),
    ]

    received = []
    iterator = channel.iter_events("multi_test", poll_interval=0.05)
    for _ in range(3):
        message = await iterator.__anext__()
        received.append(message)
        await channel.ack(message.event_id)
    await iterator.aclose()

    received_ids = {m.event_id for m in received}
    assert received_ids == set(event_ids)


@pytest.mark.mysql
async def test_mysqlconnector_event_channel_nack_redelivery(mysql_service: MySQLService, tmp_path: Any) -> None:
    """MysqlConnector queue backend redelivers nacked messages."""
    migrations = tmp_path / "migrations"
    migrations.mkdir()

    config = MysqlConnectorAsyncConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "autocommit": True,
            "use_pure": True,
        },
        migration_config={
            "script_location": str(migrations),
            "include_extensions": ["events"],
            "version_table_name": "ddl_migrations_mysqlconn",
        },
        extension_config={"events": {"queue_table": "mysqlconn_event_queue"}},
    )

    _spec, channel = await setup_async_event_channel(config)

    event_id = await channel.publish("nack_test", {"retry": True})

    iterator = channel.iter_events("nack_test", poll_interval=0.05)
    message = await iterator.__anext__()
    await channel.nack(message.event_id)
    await iterator.aclose()

    async with config.provide_session() as driver:
        row = await driver.select_one(
            "SELECT status, attempts FROM mysqlconn_event_queue WHERE event_id = :event_id", {"event_id": event_id}
        )

    assert row["status"] == "pending"
    assert row["attempts"] == 2  # 1 from claim + 1 from nack
