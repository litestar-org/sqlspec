"""Shared provisioning helpers for Oracle events-backend integration tests."""

import contextlib
from collections.abc import Callable, Iterator

import pytest
from pytest_databases.docker.oracle import OracleService

from sqlspec.adapters.oracledb import OracleSyncConfig

DEFAULT_QUEUE_TABLE = "SQLSPEC_EVENTS_QUEUE_TABLE"
DEFAULT_QUEUE_NAME = "SQLSPEC_EVENTS_QUEUE"


def _app_config(oracle_service: OracleService) -> OracleSyncConfig:
    return OracleSyncConfig(
        connection_config={
            "host": oracle_service.host,
            "port": oracle_service.port,
            "service_name": oracle_service.service_name,
            "user": oracle_service.user,
            "password": oracle_service.password,
        }
    )


@contextlib.contextmanager
def classic_aq_queue(
    oracle_service: OracleService,
    *,
    queue_table: str = DEFAULT_QUEUE_TABLE,
    queue_name: str = DEFAULT_QUEUE_NAME,
    payload_type: str = "JSON",
) -> Iterator[None]:
    """Provision and tear down a classic Advanced Queuing queue as the app user."""

    config = _app_config(oracle_service)
    with config.provide_session() as session:
        session.execute_script(
            f"""
            DECLARE
                table_count INTEGER;
            BEGIN
                SELECT COUNT(*) INTO table_count FROM user_queue_tables WHERE queue_table = '{queue_table}';
                IF table_count = 0 THEN
                    dbms_aqadm.create_queue_table(queue_table => '{queue_table}', queue_payload_type => '{payload_type}');
                END IF;
                BEGIN
                    dbms_aqadm.create_queue(queue_name => '{queue_name}', queue_table => '{queue_table}');
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE != -24005 THEN
                            RAISE;
                        END IF;
                END;
                dbms_aqadm.start_queue(queue_name => '{queue_name}');
            END;
            """
        )
    try:
        yield
    finally:
        with contextlib.suppress(Exception):
            with config.provide_session() as session:
                session.execute_script(
                    f"""
                    BEGIN
                        BEGIN dbms_aqadm.stop_queue(queue_name => '{queue_name}'); EXCEPTION WHEN OTHERS THEN NULL; END;
                        BEGIN dbms_aqadm.drop_queue(queue_name => '{queue_name}'); EXCEPTION WHEN OTHERS THEN NULL; END;
                        BEGIN dbms_aqadm.drop_queue_table(queue_table => '{queue_table}'); EXCEPTION WHEN OTHERS THEN NULL; END;
                    END;
                    """
                )
        config.close_pool()


@pytest.fixture
def provision_classic_aq(
    oracle_aq_privileges: None, oracle_23ai_service: OracleService
) -> "Callable[..., contextlib.AbstractContextManager[None]]":
    """Return a factory that provisions a classic AQ queue (privileges guaranteed)."""

    def _factory(
        *, queue_table: str = DEFAULT_QUEUE_TABLE, queue_name: str = DEFAULT_QUEUE_NAME, payload_type: str = "JSON"
    ) -> "contextlib.AbstractContextManager[None]":
        return classic_aq_queue(
            oracle_23ai_service, queue_table=queue_table, queue_name=queue_name, payload_type=payload_type
        )

    return _factory
