"""Oracle Advanced Queuing event channel integration tests."""

from collections.abc import Generator

import pytest
from pytest_databases.docker.oracle import OracleService

from sqlspec import SQLSpec
from sqlspec.adapters.oracledb import OracleSyncConfig
from sqlspec.extensions.events import SyncEventChannel

pytestmark = pytest.mark.xdist_group("oracle")


@pytest.fixture
def oracle_aq_config(oracle_23ai_service: OracleService) -> Generator[OracleSyncConfig, None, None]:
    """Provision Oracle config and ensure AQ queue exists for tests."""

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

    queue_table = "SQLSPEC_EVENTS_QUEUE_TABLE"
    queue_name = "SQLSPEC_EVENTS_QUEUE"

    created = False
    try:
        try:
            with config.provide_session() as session:
                session.execute_script(
                    f"""
                    DECLARE
                        table_count INTEGER;
                    BEGIN
                        SELECT COUNT(*) INTO table_count FROM user_queue_tables WHERE queue_table = '{queue_table}';
                        IF table_count = 0 THEN
                            dbms_aqadm.create_queue_table(queue_table => '{queue_table}', queue_payload_type => 'JSON');
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
            created = True
        except Exception:  # pragma: no cover - privilege detection path
            pytest.skip("Oracle AQ privileges unavailable")
        yield config
    finally:
        if created:
            try:
                with config.provide_session() as session:
                    session.execute_script(
                        f"""
                        BEGIN
                            BEGIN
                                dbms_aqadm.stop_queue(queue_name => '{queue_name}');
                            EXCEPTION
                                WHEN OTHERS THEN
                                    NULL;
                            END;
                            BEGIN
                                dbms_aqadm.drop_queue(queue_name => '{queue_name}');
                            EXCEPTION
                                WHEN OTHERS THEN
                                    NULL;
                            END;
                            BEGIN
                                dbms_aqadm.drop_queue_table(queue_table => '{queue_table}');
                            EXCEPTION
                                WHEN OTHERS THEN
                                    NULL;
                            END;
                        END;
                        """
                    )
            except Exception:  # pragma: no cover - cleanup best-effort
                pass
        config.close_pool()


def test_oracle_aq_publish_receive(oracle_aq_config: OracleSyncConfig) -> None:
    """AQ backend publishes and receives a JSON payload via EventChannel."""

    spec = SQLSpec()
    spec.add_config(oracle_aq_config)
    channel = spec.event_channel(oracle_aq_config)

    assert isinstance(channel, SyncEventChannel)

    event_id = channel.publish("alerts", {"action": "refresh"})
    iterator = channel.iter_events("alerts", poll_interval=1.0)
    message = next(iterator)

    assert message.event_id == event_id
    assert message.payload["action"] == "refresh"

    channel.ack(message.event_id)
