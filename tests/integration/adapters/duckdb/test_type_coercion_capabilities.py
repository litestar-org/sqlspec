"""Integration test verifying TypeCoercionCapabilities against observed DuckDB behavior."""

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from sqlspec.adapters.duckdb import DuckDBConfig

pytestmark = pytest.mark.xdist_group("duckdb")


def test_duckdb_type_coercion_capabilities_observed() -> None:
    """Verify DuckDBConfig declared capabilities match observed runtime behavior."""
    config = DuckDBConfig(connection_config={"database": ":memory:"})
    caps = config.type_coercion_capabilities
    assert caps.datetime_binding == "iso_text"
    assert caps.timestamp_precision == "microsecond"
    assert caps.json_columns_decoded is False
    assert caps.uuid_binding == "text"

    test_dt = datetime(2024, 1, 15, 12, 30, 45, 123456, tzinfo=timezone.utc)
    test_json = {"key": "value", "count": 42}
    test_uuid = uuid4()

    with config.provide_session() as driver:
        driver.execute_script(
            "CREATE TABLE test_coercion (id INTEGER PRIMARY KEY, ts TIMESTAMPTZ, payload JSON, uid VARCHAR);"
        )
        driver.execute(
            "INSERT INTO test_coercion (id, ts, payload, uid) VALUES (?, ?, ?, ?)", [1, test_dt, test_json, test_uuid]
        )
        row = driver.select_one("SELECT ts, payload, uid FROM test_coercion WHERE id = ?", [1])
        assert row is not None

        raw_ts, raw_payload, raw_uid = row["ts"], row["payload"], row["uid"]
        assert isinstance(raw_payload, str) == (not caps.json_columns_decoded)
        assert isinstance(raw_uid, str) == (caps.uuid_binding == "text")
        if isinstance(raw_ts, datetime):
            assert raw_ts.microsecond == test_dt.microsecond
        elif isinstance(raw_ts, str):
            parsed_dt = datetime.fromisoformat(raw_ts)
            assert parsed_dt.microsecond == test_dt.microsecond
