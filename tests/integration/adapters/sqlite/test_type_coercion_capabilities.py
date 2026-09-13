"""Integration test verifying TypeCoercionCapabilities against observed SQLite behavior."""

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig

pytestmark = pytest.mark.xdist_group("sqlite")


def test_sqlite_type_coercion_capabilities_observed() -> None:
    """Verify SqliteConfig declared capabilities match observed runtime behavior."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    caps = config.type_coercion_capabilities
    assert caps.datetime_binding == "iso_text"
    assert caps.timestamp_precision == "microsecond"
    assert caps.json_columns_decoded is False
    assert caps.uuid_binding == "text"

    test_dt = datetime(2024, 1, 15, 12, 30, 45, 123456, tzinfo=timezone.utc)
    test_json = {"key": "value", "count": 42}
    test_uuid = uuid4()

    try:
        with config.provide_session() as driver:
            driver.execute_script(
                "CREATE TABLE test_coercion (id INTEGER PRIMARY KEY, ts TIMESTAMP, payload JSON, uid TEXT);"
            )
            driver.execute(
                "INSERT INTO test_coercion (id, ts, payload, uid) VALUES (:id, :ts, :payload, :uid)",
                {"id": 1, "ts": test_dt, "payload": test_json, "uid": test_uuid},
            )
            row = driver.select_one("SELECT ts, payload, uid FROM test_coercion WHERE id = :id", {"id": 1})
            assert row is not None

            raw_ts, raw_payload, raw_uid = row["ts"], row["payload"], row["uid"]
            assert isinstance(raw_payload, str) == (not caps.json_columns_decoded)
            assert isinstance(raw_uid, str) == (caps.uuid_binding == "text")
            if caps.datetime_binding == "iso_text":
                assert isinstance(raw_ts, str)
                parsed_dt = datetime.fromisoformat(raw_ts)
                assert parsed_dt.microsecond == test_dt.microsecond
    finally:
        config.close_pool()


async def test_aiosqlite_type_coercion_capabilities_observed() -> None:
    """Verify AiosqliteConfig declared capabilities match observed runtime behavior."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    caps = config.type_coercion_capabilities
    assert caps.datetime_binding == "iso_text"
    assert caps.timestamp_precision == "microsecond"
    assert caps.json_columns_decoded is False
    assert caps.uuid_binding == "text"

    test_dt = datetime(2024, 1, 15, 12, 30, 45, 123456, tzinfo=timezone.utc)
    test_json = {"key": "value", "count": 42}
    test_uuid = uuid4()

    try:
        async with config.provide_session() as driver:
            await driver.execute_script(
                "CREATE TABLE test_coercion (id INTEGER PRIMARY KEY, ts TIMESTAMP, payload JSON, uid TEXT);"
            )
            await driver.execute(
                "INSERT INTO test_coercion (id, ts, payload, uid) VALUES (:id, :ts, :payload, :uid)",
                {"id": 1, "ts": test_dt, "payload": test_json, "uid": test_uuid},
            )
            row = await driver.select_one("SELECT ts, payload, uid FROM test_coercion WHERE id = :id", {"id": 1})
            assert row is not None

            raw_ts, raw_payload, raw_uid = row["ts"], row["payload"], row["uid"]
            assert isinstance(raw_payload, str) == (not caps.json_columns_decoded)
            assert isinstance(raw_uid, str) == (caps.uuid_binding == "text")
            if caps.datetime_binding == "iso_text":
                assert isinstance(raw_ts, str)
                parsed_dt = datetime.fromisoformat(raw_ts)
                assert parsed_dt.microsecond == test_dt.microsecond
    finally:
        await config.close_pool()
