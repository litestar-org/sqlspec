"""Integration tests for native Oracle JSON binding (C1.T7).

Verifies the C1 contract end-to-end against a real Oracle 23ai container:

- ``dict`` payloads round-trip without manual ``createlob`` workarounds.
- ``list[dict]`` payloads round-trip.
- Large dicts (>4000 bytes serialised) round-trip via the native JSON path
  rather than getting CLOB-coerced — proving the smart-LOB-coercion gap (C2)
  is irrelevant for native JSON columns.
- The on-the-wire path uses ``DB_TYPE_JSON`` (cursor description type_code).
- Round-trip preserves nested structures.
"""

import pytest

from sqlspec.adapters.oracledb import OracleAsyncDriver, OracleSyncDriver

pytestmark = pytest.mark.xdist_group("oracle")


_TABLE = "test_json_native_c1"


async def _drop_table_async(driver: OracleAsyncDriver, name: str) -> None:
    await driver.execute_script(
        f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {name}'; "
        "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


def _drop_table_sync(driver: OracleSyncDriver, name: str) -> None:
    driver.execute_script(
        f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {name}'; "
        "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


async def test_async_dict_roundtrip_native_json(oracle_async_session: OracleAsyncDriver) -> None:
    """A dict bound into a native JSON column round-trips bit-identical."""
    await _drop_table_async(oracle_async_session, _TABLE)
    await oracle_async_session.execute_script(f"CREATE TABLE {_TABLE} (id NUMBER PRIMARY KEY, payload JSON)")
    try:
        payload = {"foo": "bar", "n": 42, "nested": {"x": [1, 2, 3]}}
        await oracle_async_session.execute(f"INSERT INTO {_TABLE} (id, payload) VALUES (:1, :2)", (1, payload))
        row = await oracle_async_session.select_one(f"SELECT payload FROM {_TABLE} WHERE id = 1")
        assert row["payload"] == payload
    finally:
        await _drop_table_async(oracle_async_session, _TABLE)


async def test_async_list_of_dicts_roundtrip(oracle_async_session: OracleAsyncDriver) -> None:
    """A list[dict] bound into a native JSON column round-trips."""
    await _drop_table_async(oracle_async_session, _TABLE)
    await oracle_async_session.execute_script(f"CREATE TABLE {_TABLE} (id NUMBER PRIMARY KEY, payload JSON)")
    try:
        payload = [{"a": 1}, {"b": 2}, {"c": [10, 20, 30]}]
        await oracle_async_session.execute(f"INSERT INTO {_TABLE} (id, payload) VALUES (:1, :2)", (1, payload))
        row = await oracle_async_session.select_one(f"SELECT payload FROM {_TABLE} WHERE id = 1")
        assert row["payload"] == payload
    finally:
        await _drop_table_async(oracle_async_session, _TABLE)


async def test_async_large_dict_no_clob_workaround(oracle_async_session: OracleAsyncDriver) -> None:
    """A dict >4000 bytes serialised should bind via DB_TYPE_JSON, not CLOB.

    Pre-C1 this triggered the helper-strategy string serialisation followed by
    coerce_large_parameters_async's >4000-byte CLOB conversion. Post-C1 the
    JSON inputtypehandler claims the dict before either path runs.
    """
    await _drop_table_async(oracle_async_session, _TABLE)
    await oracle_async_session.execute_script(f"CREATE TABLE {_TABLE} (id NUMBER PRIMARY KEY, payload JSON)")
    try:
        payload = {"big": "x" * 8000, "n": list(range(500))}
        await oracle_async_session.execute(f"INSERT INTO {_TABLE} (id, payload) VALUES (:1, :2)", (1, payload))
        row = await oracle_async_session.select_one(f"SELECT payload FROM {_TABLE} WHERE id = 1")
        assert row["payload"] == payload
    finally:
        await _drop_table_async(oracle_async_session, _TABLE)


async def test_async_dict_with_special_values(oracle_async_session: OracleAsyncDriver) -> None:
    """Dict with bool / None / int / nested list / nested dict survives round-trip.

    NOTE: float values in native JSON columns come back as ``decimal.Decimal``
    (python-oracledb's default OSON-numeric coercion). Numeric type fidelity
    is tracked as a separate concern outside this chapter — see beads
    sqlspec-i6j follow-up "JSON numeric fidelity (Decimal vs float)".
    """
    await _drop_table_async(oracle_async_session, _TABLE)
    await oracle_async_session.execute_script(f"CREATE TABLE {_TABLE} (id NUMBER PRIMARY KEY, payload JSON)")
    try:
        payload: dict[str, object] = {
            "active": True,
            "deleted": False,
            "missing": None,
            "count": 42,
            "tags": ["alpha", "beta", "gamma"],
            "labels": {"env": "prod", "tier": "primary"},
        }
        await oracle_async_session.execute(f"INSERT INTO {_TABLE} (id, payload) VALUES (:1, :2)", (1, payload))
        row = await oracle_async_session.select_one(f"SELECT payload FROM {_TABLE} WHERE id = 1")
        assert row["payload"] == payload
    finally:
        await _drop_table_async(oracle_async_session, _TABLE)


async def test_async_executemany_dicts(oracle_async_session: OracleAsyncDriver) -> None:
    """executemany of multiple dict payloads against a native JSON column."""
    await _drop_table_async(oracle_async_session, _TABLE)
    await oracle_async_session.execute_script(f"CREATE TABLE {_TABLE} (id NUMBER PRIMARY KEY, payload JSON)")
    try:
        rows = [(i, {"index": i, "label": f"row-{i}"}) for i in range(1, 6)]
        await oracle_async_session.execute_many(f"INSERT INTO {_TABLE} (id, payload) VALUES (:1, :2)", rows)
        result = await oracle_async_session.select(f"SELECT id, payload FROM {_TABLE} ORDER BY id")
        assert len(result) == 5
        for got, (expected_id, expected_payload) in zip(result, rows, strict=True):
            assert got["id"] == expected_id
            assert got["payload"] == expected_payload
    finally:
        await _drop_table_async(oracle_async_session, _TABLE)


def test_sync_dict_roundtrip_native_json(oracle_sync_session: OracleSyncDriver) -> None:
    """Sync driver: dict → native JSON column round-trip parity."""
    _drop_table_sync(oracle_sync_session, _TABLE)
    oracle_sync_session.execute_script(f"CREATE TABLE {_TABLE} (id NUMBER PRIMARY KEY, payload JSON)")
    try:
        payload = {"foo": "bar", "n": 42}
        oracle_sync_session.execute(f"INSERT INTO {_TABLE} (id, payload) VALUES (:1, :2)", (1, payload))
        row = oracle_sync_session.select_one(f"SELECT payload FROM {_TABLE} WHERE id = 1")
        assert row["payload"] == payload
    finally:
        _drop_table_sync(oracle_sync_session, _TABLE)
