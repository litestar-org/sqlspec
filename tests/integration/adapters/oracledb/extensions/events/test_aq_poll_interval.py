"""Regression test for Oracle AQ backend poll_interval handling."""

import time
from collections.abc import Generator

import pytest
from pytest_databases.docker.oracle import OracleService

from sqlspec import SQLSpec
from sqlspec.adapters.oracledb import OracleSyncConfig

pytestmark = pytest.mark.xdist_group("oracle")

_AQ_WAIT_SECONDS = 10
_POLL_INTERVAL = 0.5
_LATENCY_TOLERANCE = 4.0


@pytest.fixture
def oracle_aq_poll_config(oracle_23ai_service: OracleService) -> Generator[OracleSyncConfig, None, None]:
    """Provision Oracle config with a high aq_wait_seconds and ensure the AQ queue exists."""

    config = OracleSyncConfig(
        connection_config={
            "host": oracle_23ai_service.host,
            "port": oracle_23ai_service.port,
            "service_name": oracle_23ai_service.service_name,
            "user": oracle_23ai_service.user,
            "password": oracle_23ai_service.password,
        },
        extension_config={"events": {"backend": "advanced_queue", "aq_wait_seconds": _AQ_WAIT_SECONDS}},
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
                            BEGIN dbms_aqadm.stop_queue(queue_name => '{queue_name}'); EXCEPTION WHEN OTHERS THEN NULL; END;
                            BEGIN dbms_aqadm.drop_queue(queue_name => '{queue_name}'); EXCEPTION WHEN OTHERS THEN NULL; END;
                            BEGIN dbms_aqadm.drop_queue_table(queue_table => '{queue_table}'); EXCEPTION WHEN OTHERS THEN NULL; END;
                        END;
                        """
                    )
            except Exception:  # pragma: no cover - cleanup best-effort
                pass
        config.close_pool()


def test_oracle_aq_dequeue_honors_caller_poll_interval(oracle_aq_poll_config: OracleSyncConfig) -> None:
    """dequeue() should return within poll_interval seconds, not aq_wait_seconds.

    The current backend passes settings.aq_wait_seconds straight to options.wait, ignoring
    the caller's poll_interval. A caller asking for a 0.5s heartbeat should not block for
    10s. Once the redesign respects min(poll_interval, aq_wait_seconds), this becomes
    cheap.
    """

    spec = SQLSpec()
    spec.add_config(oracle_aq_poll_config)
    channel = spec.event_channel(oracle_aq_poll_config)

    iterator = channel.iter_events("aq_poll_chan", poll_interval=_POLL_INTERVAL)

    start = time.monotonic()
    # Pull one timeout cycle — there are no enqueued messages so dequeue should return None
    # promptly. We rely on the channel's internal poll loop returning to the caller within
    # roughly poll_interval (the loop continues on None, so we'd see this in latency between
    # iterations if dequeue blocked for aq_wait_seconds instead).
    backend = channel._backend  # pyright: ignore[reportPrivateUsage]
    elapsed_first = _measure_single_dequeue(backend, "aq_poll_chan", _POLL_INTERVAL)

    iterator.close()
    elapsed_total = time.monotonic() - start
    assert elapsed_first <= _LATENCY_TOLERANCE, (
        f"dequeue blocked for {elapsed_first:.2f}s with poll_interval={_POLL_INTERVAL}s "
        f"(aq_wait_seconds={_AQ_WAIT_SECONDS}) — poll_interval was ignored"
    )
    assert elapsed_total <= _LATENCY_TOLERANCE + 1.0


def _measure_single_dequeue(backend: object, channel: str, poll_interval: float) -> float:
    """Time a single backend.dequeue() call directly to expose poll_interval drift."""
    start = time.monotonic()
    backend.dequeue(channel, poll_interval)  # type: ignore[attr-defined]
    return time.monotonic() - start
