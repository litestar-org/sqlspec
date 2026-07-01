"""Thin-mode JSON payload verification for the Oracle advanced_queue events backend.

Confirms that classic Advanced Queuing carries a structured JSON payload through a full
enqueue/dequeue cycle while the adapter is in its default thin mode (no Instant Client).
"""

from collections.abc import Generator

import pytest
from pytest_databases.docker.oracle import OracleService

from sqlspec import SQLSpec
from sqlspec.adapters.oracledb import OracleSyncConfig
from sqlspec.extensions.events import SyncEventChannel

pytestmark = pytest.mark.xdist_group("oracle")

_QUEUE_TABLE = "SQLSPEC_EVENTS_QUEUE_TABLE"
_QUEUE_NAME = "SQLSPEC_EVENTS_QUEUE"


@pytest.fixture
def oracle_aq_json_config(
    oracle_aq_privileges: None, oracle_23ai_service: OracleService
) -> Generator[OracleSyncConfig, None, None]:
    """Provision a JSON-payload AQ queue for the advanced_queue backend."""

    config = OracleSyncConfig(
        connection_config={
            "host": oracle_23ai_service.host,
            "port": oracle_23ai_service.port,
            "service_name": oracle_23ai_service.service_name,
            "user": oracle_23ai_service.user,
            "password": oracle_23ai_service.password,
        },
        extension_config={"events": {"backend": "advanced_queue"}},
    )

    with config.provide_session() as session:
        session.execute_script(
            f"""
            DECLARE
                table_count INTEGER;
            BEGIN
                SELECT COUNT(*) INTO table_count FROM user_queue_tables WHERE queue_table = '{_QUEUE_TABLE}';
                IF table_count = 0 THEN
                    dbms_aqadm.create_queue_table(queue_table => '{_QUEUE_TABLE}', queue_payload_type => 'JSON');
                END IF;
                BEGIN
                    dbms_aqadm.create_queue(queue_name => '{_QUEUE_NAME}', queue_table => '{_QUEUE_TABLE}');
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE != -24005 THEN
                            RAISE;
                        END IF;
                END;
                dbms_aqadm.start_queue(queue_name => '{_QUEUE_NAME}');
            END;
            """
        )
    try:
        yield config
    finally:
        try:
            with config.provide_session() as session:
                session.execute_script(
                    f"""
                    BEGIN
                        BEGIN dbms_aqadm.stop_queue(queue_name => '{_QUEUE_NAME}'); EXCEPTION WHEN OTHERS THEN NULL; END;
                        BEGIN dbms_aqadm.drop_queue(queue_name => '{_QUEUE_NAME}'); EXCEPTION WHEN OTHERS THEN NULL; END;
                        BEGIN dbms_aqadm.drop_queue_table(queue_table => '{_QUEUE_TABLE}'); EXCEPTION WHEN OTHERS THEN NULL; END;
                    END;
                    """
                )
        except Exception:  # pragma: no cover - cleanup best-effort
            pass
        config.close_pool()


def test_advanced_queue_json_round_trips_in_thin_mode(oracle_aq_json_config: OracleSyncConfig) -> None:
    """A nested JSON payload survives enqueue/dequeue unchanged with a thin-mode connection."""

    with oracle_aq_json_config.provide_session() as driver:
        assert driver.connection.thin is True

    spec = SQLSpec()
    spec.add_config(oracle_aq_json_config)
    channel = spec.event_channel(oracle_aq_json_config)
    assert isinstance(channel, SyncEventChannel)

    payload = {"action": "refresh", "nested": {"count": 3, "tags": ["a", "b"]}, "flag": True}
    event_id = channel.publish("alerts", payload)
    message = next(channel.iter_events("alerts", poll_interval=1.0))

    assert message.event_id == event_id
    assert message.payload["action"] == "refresh"
    assert message.payload["nested"] == {"count": 3, "tags": ["a", "b"]}
    assert message.payload["flag"] is True

    channel.ack(message.event_id)
