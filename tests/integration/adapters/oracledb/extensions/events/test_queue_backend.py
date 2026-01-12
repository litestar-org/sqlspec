# pyright: reportPossiblyUnboundVariable=false, reportAttributeAccessIssue=false
"""OracleDB integration tests for the EventChannel queue backend."""

import asyncio
import os
from textwrap import dedent
from typing import TYPE_CHECKING

import pytest

from sqlspec import SQLSpec
from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleSyncConfig
from sqlspec.exceptions import SQLSpecError

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.xdist_group("oracle")

ORACLE_HOST = os.environ.get("ORACLE_TEST_HOST", "127.0.0.1")
ORACLE_PORT = int(os.environ.get("ORACLE_TEST_PORT", "1521"))
ORACLE_SERVICE = os.environ.get("ORACLE_TEST_SERVICE", "freepdb1")
ORACLE_USER = os.environ.get("ORACLE_TEST_USER", "app")
ORACLE_PASSWORD = os.environ.get("ORACLE_TEST_PASSWORD", "super-secret")


def _build_dsn() -> str:
    return f"{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}"


def _maybe_skip(reason: str) -> None:
    pytest.skip(reason)


def _prepare_queue_table() -> None:
    try:
        import oracledb
    except ImportError:  # pragma: no cover - optional dependency guard
        _maybe_skip("oracledb unavailable")

    try:
        connection = oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=_build_dsn())
    except oracledb.Error as error:  # pragma: no cover - connection error guard
        raise SQLSpecError(f"Cannot connect to Oracle: {error}") from error
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                dedent(
                    """
                    BEGIN
                        EXECUTE IMMEDIATE 'DROP TABLE sqlspec_event_queue PURGE';
                    EXCEPTION
                        WHEN OTHERS THEN
                            IF SQLCODE != -942 THEN
                                RAISE;
                            END IF;
                    END;
                    """
                )
            )
            cursor.execute(
                """
                CREATE TABLE sqlspec_event_queue (
                    event_id VARCHAR2(64) PRIMARY KEY,
                    channel VARCHAR2(128) NOT NULL,
                    payload_json CLOB NOT NULL,
                    metadata_json CLOB,
                    status VARCHAR2(32) DEFAULT 'pending',
                    available_at TIMESTAMP DEFAULT SYSTIMESTAMP,
                    lease_expires_at TIMESTAMP,
                    attempts NUMBER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT SYSTIMESTAMP,
                    acknowledged_at TIMESTAMP
                )
                """
            )
            cursor.execute(
                "CREATE INDEX idx_sqlspec_event_queue_channel_status ON sqlspec_event_queue(channel, status, available_at)"
            )
        connection.commit()
    except Exception as error:  # pragma: no cover - infrastructure guard
        raise SQLSpecError(str(error)) from error
    finally:
        connection.close()


@pytest.mark.oracle
def test_oracle_sync_event_channel_queue_fallback(tmp_path: "Path") -> None:
    """Queue-backed events work on Oracle via the extension migrations."""

    try:
        _prepare_queue_table()
    except SQLSpecError:  # pragma: no cover - service unavailable guard
        _maybe_skip("Oracle unavailable")

    config = OracleSyncConfig(
        connection_config={
            "dsn": _build_dsn(),
            "user": ORACLE_USER,
            "password": ORACLE_PASSWORD,
            "min": 1,
            "max": 2,
            "increment": 1,
        }
    )

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    event_id = channel.publish("notifications", {"action": "oracle"})
    iterator = channel.iter_events("notifications", poll_interval=0.5)
    message = next(iterator)
    channel.ack(message.event_id)

    with config.provide_session() as driver:
        row = driver.select_one(
            "SELECT status FROM sqlspec_event_queue WHERE event_id = :event_id", {"event_id": event_id}
        )

    assert row["status"] == "acked"

    config.close_pool()


@pytest.mark.oracle
@pytest.mark.asyncio
async def test_oracle_async_event_channel_queue_fallback(tmp_path: "Path") -> None:
    """Async Oracle configs also use the queue fallback."""

    try:
        _prepare_queue_table()
    except SQLSpecError:  # pragma: no cover - service unavailable guard
        _maybe_skip("Oracle unavailable")

    config = OracleAsyncConfig(
        connection_config={
            "dsn": _build_dsn(),
            "user": ORACLE_USER,
            "password": ORACLE_PASSWORD,
            "min": 1,
            "max": 2,
            "increment": 1,
        }
    )

    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)

    event_id = await channel.publish("notifications", {"action": "oracle_async"})
    iterator = channel.iter_events("notifications", poll_interval=0.5)
    try:
        message = await asyncio.wait_for(iterator.__anext__(), timeout=10)
    finally:
        await iterator.aclose()
    await channel.ack(message.event_id)

    async with config.provide_session() as driver:
        row = await driver.select_one(
            "SELECT status FROM sqlspec_event_queue WHERE event_id = :event_id", {"event_id": event_id}
        )

    assert row["status"] == "acked"

    await config.close_pool()
