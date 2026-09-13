"""Integration test verifying TypeCoercionCapabilities against observed PostgreSQL behavior."""

from datetime import datetime, timezone
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

if TYPE_CHECKING:
    from sqlspec.adapters.asyncpg import AsyncpgConfig
    from sqlspec.adapters.psqlpy import PsqlpyConfig
    from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig

pytestmark = pytest.mark.xdist_group("postgres")

SETUP_SQL = """
DROP TABLE IF EXISTS test_coercion CASCADE;
CREATE TABLE test_coercion (
    id SERIAL PRIMARY KEY,
    ts TIMESTAMPTZ,
    payload JSONB,
    uid UUID
);
"""
TEARDOWN_SQL = "DROP TABLE IF EXISTS test_coercion CASCADE;"


async def test_asyncpg_type_coercion_capabilities_observed(asyncpg_config: "AsyncpgConfig") -> None:
    """Verify AsyncpgConfig declared capabilities match observed runtime behavior."""
    caps = asyncpg_config.type_coercion_capabilities
    assert caps.datetime_binding == "native"
    assert caps.timestamp_precision == "microsecond"
    assert caps.json_columns_decoded is True
    assert caps.uuid_binding == "native"

    test_dt = datetime(2024, 1, 15, 12, 30, 45, 123456, tzinfo=timezone.utc)
    test_json = {"key": "value", "count": 42}
    test_uuid = uuid4()

    async with asyncpg_config.provide_session() as driver:
        await driver.execute_script(SETUP_SQL)
        try:
            await driver.execute(
                "INSERT INTO test_coercion (ts, payload, uid) VALUES (:ts, :payload, :uid)",
                {"ts": test_dt, "payload": test_json, "uid": test_uuid},
            )
            row = await driver.select_one("SELECT ts, payload, uid FROM test_coercion LIMIT 1")
            assert row is not None

            raw_ts, raw_payload, raw_uid = row["ts"], row["payload"], row["uid"]
            assert isinstance(raw_ts, datetime)
            assert raw_ts.microsecond == test_dt.microsecond
            assert isinstance(raw_payload, dict) == caps.json_columns_decoded
            assert raw_payload == test_json
            assert raw_uid == test_uuid
        finally:
            await driver.execute_script(TEARDOWN_SQL)


def test_psycopg_sync_type_coercion_capabilities_observed(psycopg_sync_config: "PsycopgSyncConfig") -> None:
    """Verify PsycopgSyncConfig declared capabilities match observed runtime behavior."""
    caps = psycopg_sync_config.type_coercion_capabilities
    assert caps.datetime_binding == "native"
    assert caps.timestamp_precision == "microsecond"
    assert caps.json_columns_decoded is True
    assert caps.uuid_binding == "native"

    test_dt = datetime(2024, 1, 15, 12, 30, 45, 123456, tzinfo=timezone.utc)
    test_json = {"key": "value", "count": 42}
    test_uuid = uuid4()

    with psycopg_sync_config.provide_session() as driver:
        driver.execute_script(SETUP_SQL)
        try:
            driver.execute(
                "INSERT INTO test_coercion (ts, payload, uid) VALUES (%(ts)s, %(payload)s, %(uid)s)",
                {"ts": test_dt, "payload": test_json, "uid": test_uuid},
            )
            row = driver.select_one("SELECT ts, payload, uid FROM test_coercion LIMIT 1")
            assert row is not None

            raw_ts, raw_payload, raw_uid = row["ts"], row["payload"], row["uid"]
            assert isinstance(raw_ts, datetime)
            assert raw_ts.microsecond == test_dt.microsecond
            assert isinstance(raw_payload, dict) == caps.json_columns_decoded
            assert raw_payload == test_json
            assert raw_uid == test_uuid
        finally:
            driver.execute_script(TEARDOWN_SQL)


async def test_psycopg_async_type_coercion_capabilities_observed(psycopg_async_config: "PsycopgAsyncConfig") -> None:
    """Verify PsycopgAsyncConfig declared capabilities match observed runtime behavior."""
    caps = psycopg_async_config.type_coercion_capabilities
    assert caps.datetime_binding == "native"
    assert caps.timestamp_precision == "microsecond"
    assert caps.json_columns_decoded is True
    assert caps.uuid_binding == "native"

    test_dt = datetime(2024, 1, 15, 12, 30, 45, 123456, tzinfo=timezone.utc)
    test_json = {"key": "value", "count": 42}
    test_uuid = uuid4()

    async with psycopg_async_config.provide_session() as driver:
        await driver.execute_script(SETUP_SQL)
        try:
            await driver.execute(
                "INSERT INTO test_coercion (ts, payload, uid) VALUES (%(ts)s, %(payload)s, %(uid)s)",
                {"ts": test_dt, "payload": test_json, "uid": test_uuid},
            )
            row = await driver.select_one("SELECT ts, payload, uid FROM test_coercion LIMIT 1")
            assert row is not None

            raw_ts, raw_payload, raw_uid = row["ts"], row["payload"], row["uid"]
            assert isinstance(raw_ts, datetime)
            assert raw_ts.microsecond == test_dt.microsecond
            assert isinstance(raw_payload, dict) == caps.json_columns_decoded
            assert raw_payload == test_json
            assert raw_uid == test_uuid
        finally:
            await driver.execute_script(TEARDOWN_SQL)


async def test_psqlpy_json_columns_match_capabilities(psqlpy_config: "PsqlpyConfig") -> None:
    """Native JSON and JSONB results are already decoded by psqlpy."""
    assert psqlpy_config.type_coercion_capabilities.json_columns_decoded is True
    async with psqlpy_config.provide_session() as driver:
        row = await driver.select_one(
            "SELECT $1::json AS payload, $2::jsonb AS binary_payload", ({"key": "value"}, {"count": 42})
        )
        assert row["payload"] == {"key": "value"}
        assert row["binary_payload"] == {"count": 42}
