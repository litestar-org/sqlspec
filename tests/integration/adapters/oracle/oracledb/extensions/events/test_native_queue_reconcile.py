"""End-to-end Oracle 23ai acceptance tests for native AQ and TxEventQ reconcile provisioning."""

import contextlib
from pathlib import Path
from typing import Literal
from uuid import uuid4

import pytest
from pytest_databases.docker.oracle import OracleService

from sqlspec import SQLSpec
from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleSyncConfig
from sqlspec.adapters.oracledb._storage import validate_oracle_identifier
from sqlspec.extensions.events import AsyncEventChannel, SyncEventChannel

pytestmark = pytest.mark.xdist_group("oracle")


@pytest.fixture(scope="module")
def oracle_native_reconcile_grants(oracle_23ai_service: OracleService) -> None:
    """Grant AQ_ADMINISTRATOR_ROLE and EXECUTE ON DBMS_AQADM to the fixture user via system."""
    app_user = validate_oracle_identifier(oracle_23ai_service.user, "user")
    admin_config = OracleSyncConfig(
        connection_config={
            "host": oracle_23ai_service.host,
            "port": oracle_23ai_service.port,
            "service_name": oracle_23ai_service.service_name,
            "user": "system",
            "password": oracle_23ai_service.system_password,
        }
    )
    try:
        with admin_config.provide_session() as driver:
            driver.execute(f"GRANT AQ_ADMINISTRATOR_ROLE, AQ_USER_ROLE TO {app_user}")
            driver.execute(f"GRANT EXECUTE ON DBMS_AQADM TO {app_user}")
            driver.execute(f"GRANT EXECUTE ON DBMS_AQ TO {app_user}")
    finally:
        admin_config.close_pool()


def _cleanup_native_objects_sync(
    config: OracleSyncConfig,
    *,
    backend: Literal["aq", "txeventq"],
    queue_names: tuple[str, ...],
    sql_table: str,
    version_table: str,
) -> None:
    """Best-effort cleanup of test queues, queue tables, and migration tables."""
    with contextlib.suppress(Exception), config.provide_session() as driver:
        for queue_name in queue_names:
            with contextlib.suppress(Exception):
                driver.execute(
                    "BEGIN DBMS_AQADM.STOP_QUEUE(queue_name => :queue_name); END;", {"queue_name": queue_name}
                )
            if backend == "aq":
                queue_table = f"{queue_name[:125]}_QT"
                with contextlib.suppress(Exception):
                    driver.execute(
                        "BEGIN DBMS_AQADM.DROP_QUEUE(queue_name => :queue_name); END;", {"queue_name": queue_name}
                    )
                with contextlib.suppress(Exception):
                    driver.execute(
                        "BEGIN DBMS_AQADM.DROP_QUEUE_TABLE(queue_table => :queue_table, force => TRUE); END;",
                        {"queue_table": queue_table},
                    )
            else:
                with contextlib.suppress(Exception):
                    driver.execute(
                        "BEGIN DBMS_AQADM.DROP_TRANSACTIONAL_EVENT_QUEUE(queue_name => :queue_name, force => TRUE); END;",
                        {"queue_name": queue_name},
                    )
        for tbl in (sql_table, version_table):
            with contextlib.suppress(Exception):
                driver.execute(f"DROP TABLE {tbl} PURGE")


@pytest.mark.parametrize("backend", ["aq", "txeventq"])
def test_oracle_reconcile_native_queue_round_trip(
    oracle_native_reconcile_grants: None,
    oracle_23ai_service: OracleService,
    tmp_path: Path,
    backend: Literal["aq", "txeventq"],
) -> None:
    """Running migrate_up() twice provisions one working AQ or TxEventQ queue and delivers an event."""
    _ = oracle_native_reconcile_grants
    suffix = uuid4().hex[:8].upper()
    queue_name = f"SQLSPEC_REC_{backend.upper()}_{suffix}"
    sql_table = f"SQLSPEC_EV_{suffix}"
    version_table = f"DDL_MIG_{suffix}"

    config = OracleSyncConfig(
        connection_config={
            "host": oracle_23ai_service.host,
            "port": oracle_23ai_service.port,
            "service_name": oracle_23ai_service.service_name,
            "user": oracle_23ai_service.user,
            "password": oracle_23ai_service.password,
        },
        migration_config={
            "script_location": str(tmp_path / f"migrations_{backend}"),
            "version_table_name": version_table,
        },
        extension_config={
            "events": {
                "backend": backend,
                "aq_queue": queue_name,
                "queue_table": sql_table,
                "migration_mode": "reconcile",
            }
        },
    )

    channel: SyncEventChannel | None = None
    try:
        config.migrate_up()
        config.migrate_up()

        with config.provide_session() as driver:
            queue_count = driver.select_value(
                "SELECT COUNT(*) FROM user_queues WHERE name = :name "
                "AND TRIM(enqueue_enabled) = 'YES' AND TRIM(dequeue_enabled) = 'YES'",
                {"name": queue_name},
            )
            assert queue_count == 1

        spec = SQLSpec()
        spec.add_config(config)
        channel = spec.event_channel(config)
        assert isinstance(channel, SyncEventChannel)
        assert channel.backend_name == backend

        event_id = channel.publish("alerts", {"action": "reconciled", "backend": backend})
        message = next(channel.iter_events("alerts", poll_interval=1.0))

        assert message.event_id == event_id
        assert message.payload == {"action": "reconciled", "backend": backend}
        channel.ack(message.event_id)
    finally:
        if channel is not None:
            channel.shutdown()
        _cleanup_native_objects_sync(
            config, backend=backend, queue_names=(queue_name,), sql_table=sql_table, version_table=version_table
        )
        config.close_pool()


async def test_oracle_async_reconcile_aq_channels_template_round_trip(
    oracle_native_reconcile_grants: None, oracle_23ai_service: OracleService, tmp_path: Path
) -> None:
    """Async migrate_up() twice provisions templated aq_channels queues and delivers events."""
    _ = oracle_native_reconcile_grants
    suffix = uuid4().hex[:8].upper()
    template = f"SQLSPEC_CH_{suffix}_{{channel}}"
    expected_queue = f"SQLSPEC_CH_{suffix}_ORDERS"
    sql_table = f"SQLSPEC_EV_{suffix}"
    version_table = f"DDL_MIG_{suffix}"

    async_config = OracleAsyncConfig(
        connection_config={
            "host": oracle_23ai_service.host,
            "port": oracle_23ai_service.port,
            "service_name": oracle_23ai_service.service_name,
            "user": oracle_23ai_service.user,
            "password": oracle_23ai_service.password,
            "min": 1,
            "max": 4,
        },
        migration_config={"script_location": str(tmp_path / "migrations_async"), "version_table_name": version_table},
        extension_config={
            "events": {
                "backend": "txeventq",
                "aq_queue": template,
                "aq_channels": ["orders"],
                "queue_table": sql_table,
                "migration_mode": "reconcile",
            }
        },
    )
    cleanup_config = OracleSyncConfig(
        connection_config={
            "host": oracle_23ai_service.host,
            "port": oracle_23ai_service.port,
            "service_name": oracle_23ai_service.service_name,
            "user": oracle_23ai_service.user,
            "password": oracle_23ai_service.password,
        }
    )

    channel: AsyncEventChannel | None = None
    try:
        await async_config.migrate_up()
        await async_config.migrate_up()

        async with async_config.provide_session() as driver:
            queue_count = await driver.select_value(
                "SELECT COUNT(*) FROM user_queues WHERE name = :name "
                "AND TRIM(enqueue_enabled) = 'YES' AND TRIM(dequeue_enabled) = 'YES'",
                {"name": expected_queue},
            )
            assert queue_count == 1

        spec = SQLSpec()
        spec.add_config(async_config)
        channel = spec.event_channel(async_config)
        assert isinstance(channel, AsyncEventChannel)
        assert channel.backend_name == "txeventq"

        event_id = await channel.publish("orders", {"order_id": 42})
        message = await anext(channel.iter_events("orders", poll_interval=1.0))

        assert message.event_id == event_id
        assert message.payload == {"order_id": 42}
        await channel.ack(message.event_id)
    finally:
        if channel is not None:
            await channel.shutdown()
        if async_config.connection_instance is not None:
            await async_config.close_pool()
        _cleanup_native_objects_sync(
            cleanup_config,
            backend="txeventq",
            queue_names=(expected_queue,),
            sql_table=sql_table,
            version_table=version_table,
        )
        cleanup_config.close_pool()
