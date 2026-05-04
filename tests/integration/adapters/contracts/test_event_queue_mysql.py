# pyright: reportPrivateUsage=false, reportAttributeAccessIssue=false, reportArgumentType=false
"""MySQL-family event queue contract tests."""

import inspect
from typing import Any

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.aiomysql import AiomysqlConfig
from sqlspec.adapters.asyncmy import AsyncmyConfig
from sqlspec.adapters.mysqlconnector import MysqlConnectorAsyncConfig
from sqlspec.adapters.pymysql import PyMysqlConfig
from tests.integration.adapters._events_helpers import setup_async_event_channel, setup_sync_event_channel

pytestmark = [pytest.mark.xdist_group("mysql"), pytest.mark.mysql]


def _mysql_config(adapter: str, mysql_service: MySQLService, tmp_path: Any) -> Any:
    migrations = tmp_path / f"{adapter}_migrations"
    migrations.mkdir()

    queue_table = f"{adapter}_event_queue"
    migration_config = {
        "script_location": str(migrations),
        "include_extensions": ["events"],
        "version_table_name": f"ddl_migrations_{adapter}",
    }
    extension_config: dict[str, Any] = {"events": {"queue_table": queue_table}}

    if adapter == "aiomysql":
        return AiomysqlConfig(
            connection_config={
                "host": mysql_service.host,
                "port": mysql_service.port,
                "user": mysql_service.user,
                "password": mysql_service.password,
                "db": mysql_service.db,
                "autocommit": True,
            },
            migration_config=migration_config,
            extension_config=extension_config,
        )
    if adapter == "asyncmy":
        return AsyncmyConfig(
            connection_config={
                "host": mysql_service.host,
                "port": mysql_service.port,
                "user": mysql_service.user,
                "password": mysql_service.password,
                "database": mysql_service.db,
                "autocommit": True,
            },
            migration_config=migration_config,
            extension_config=extension_config,
        )
    if adapter == "mysqlconnector":
        return MysqlConnectorAsyncConfig(
            connection_config={
                "host": mysql_service.host,
                "port": mysql_service.port,
                "user": mysql_service.user,
                "password": mysql_service.password,
                "database": mysql_service.db,
                "autocommit": True,
                "use_pure": True,
            },
            migration_config=migration_config,
            extension_config=extension_config,
        )
    return PyMysqlConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "autocommit": True,
        },
        migration_config=migration_config,
        extension_config=extension_config,
    )


async def _close_pool(config: Any) -> None:
    close_pool = getattr(config, "close_pool", None)
    if close_pool is None:
        return
    result = close_pool()
    if inspect.isawaitable(result):
        await result


@pytest.mark.parametrize(
    "adapter",
    [
        pytest.param("aiomysql", id="aiomysql"),
        pytest.param("asyncmy", id="asyncmy"),
        pytest.param("mysqlconnector", marks=pytest.mark.mysql_connector, id="mysqlconnector"),
        pytest.param("pymysql", marks=pytest.mark.pymysql, id="pymysql"),
    ],
)
async def test_mysql_family_event_channel_queue_fallback(
    adapter: str, mysql_service: MySQLService, tmp_path: Any
) -> None:
    """MySQL-family configs publish, consume, and ack events via the queue backend."""
    config = _mysql_config(adapter, mysql_service, tmp_path)
    queue_table = f"{adapter}_event_queue"

    if adapter == "pymysql":
        _spec, channel = setup_sync_event_channel(config)
        assert channel._backend_name == "table_queue"
        event_id = channel.publish("notifications", {"action": "mysql"})
        message = next(channel.iter_events("notifications", poll_interval=0.05))
        channel.ack(message.event_id)
        with config.provide_session() as driver:
            row = driver.select_one(
                f"SELECT status FROM {queue_table} WHERE event_id = :event_id", {"event_id": event_id}
            )
    else:
        try:
            _spec, channel = await setup_async_event_channel(config)
            assert channel._backend_name == "table_queue"
            event_id = await channel.publish("notifications", {"action": "mysql"})
            iterator = channel.iter_events("notifications", poll_interval=0.05)
            message = await iterator.__anext__()
            await iterator.aclose()
            await channel.ack(message.event_id)
            async with config.provide_session() as driver:
                row = await driver.select_one(
                    f"SELECT status FROM {queue_table} WHERE event_id = :event_id", {"event_id": event_id}
                )
        finally:
            await _close_pool(config)

    assert message.payload["action"] == "mysql"
    assert row["status"] == "acked"


@pytest.mark.parametrize(
    "adapter",
    [
        pytest.param("aiomysql", id="aiomysql"),
        pytest.param("asyncmy", id="asyncmy"),
        pytest.param("mysqlconnector", marks=pytest.mark.mysql_connector, id="mysqlconnector"),
    ],
)
async def test_mysql_family_event_channel_multiple_messages(
    adapter: str, mysql_service: MySQLService, tmp_path: Any
) -> None:
    """MySQL-family queue backends handle multiple messages correctly."""
    config = _mysql_config(adapter, mysql_service, tmp_path)
    try:
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
    finally:
        await _close_pool(config)

    assert {message.event_id for message in received} == set(event_ids)


@pytest.mark.parametrize(
    "adapter",
    [
        pytest.param("aiomysql", id="aiomysql"),
        pytest.param("asyncmy", id="asyncmy"),
        pytest.param("mysqlconnector", marks=pytest.mark.mysql_connector, id="mysqlconnector"),
    ],
)
async def test_mysql_family_event_channel_nack_redelivery(
    adapter: str, mysql_service: MySQLService, tmp_path: Any
) -> None:
    """MySQL-family queue backends redeliver nacked messages."""
    config = _mysql_config(adapter, mysql_service, tmp_path)
    queue_table = f"{adapter}_event_queue"
    try:
        _spec, channel = await setup_async_event_channel(config)
        event_id = await channel.publish("nack_test", {"retry": True})

        iterator = channel.iter_events("nack_test", poll_interval=0.05)
        message = await iterator.__anext__()
        await channel.nack(message.event_id)
        await iterator.aclose()

        async with config.provide_session() as driver:
            row = await driver.select_one(
                f"SELECT status, attempts FROM {queue_table} WHERE event_id = :event_id", {"event_id": event_id}
            )
    finally:
        await _close_pool(config)

    assert row["status"] == "pending"
    assert row["attempts"] == 2
