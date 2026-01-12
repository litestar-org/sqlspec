"""PyMySQL integration tests for the EventChannel queue backend."""

from typing import Any

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.pymysql import PyMysqlConfig
from tests.integration.adapters._events_helpers import setup_sync_event_channel

pytestmark = [pytest.mark.xdist_group("mysql"), pytest.mark.pymysql]


@pytest.mark.mysql
def test_pymysql_event_channel_queue_fallback(mysql_service: MySQLService, tmp_path: Any) -> None:
    """PyMySQL configs publish, consume, and ack events via the queue backend."""
    migrations = tmp_path / "migrations"
    migrations.mkdir()

    config = PyMysqlConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "autocommit": True,
        },
        migration_config={
            "script_location": str(migrations),
            "include_extensions": ["events"],
            "version_table_name": "ddl_migrations_pymysql",
        },
        extension_config={"events": {"queue_table": "pymysql_event_queue"}},
    )

    _spec, channel = setup_sync_event_channel(config)

    assert channel._backend_name == "table_queue"

    event_id = channel.publish("notifications", {"action": "mysql"})
    iterator = channel.iter_events("notifications", poll_interval=0.05)
    message = next(iterator)
    channel.ack(message.event_id)

    with config.provide_session() as driver:
        row = driver.select_one(
            "SELECT status FROM pymysql_event_queue WHERE event_id = :event_id", {"event_id": event_id}
        )

    assert message.payload["action"] == "mysql"
    assert row["status"] == "acked"
