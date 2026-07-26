"""Oracle JSON storage compatibility coverage."""

from decimal import Decimal

import pytest

from sqlspec.adapters.oracledb import OracleAsyncDriver, OracleClob, OracleJson
from sqlspec.utils.serializers import to_json

pytestmark = pytest.mark.xdist_group("oracle")


async def _drop_table(driver: "OracleAsyncDriver", table_name: str) -> None:
    await driver.execute_script(
        f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table_name}'; "
        "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


async def test_non_native_json_session_uses_oracle_18c(oracle_18c_async_session: "OracleAsyncDriver") -> None:
    """The non-native JSON lane must run against the real Oracle 18c service."""
    major = await oracle_18c_async_session.select_value(
        "SELECT TO_NUMBER(REGEXP_SUBSTR(version, '^[0-9]+')) FROM product_component_version "
        "WHERE product LIKE 'Oracle Database%'"
    )

    assert major == 18


async def test_native_json_round_trip_matrix(oracle_async_session: "OracleAsyncDriver") -> None:
    """Native JSON accepts direct, wrapped, sequence, and large Python payloads."""
    table_name = "json_modes_native"
    payloads: list[object] = [
        {"storage": "native", "number": 3.25},
        [{"item": 1}, {"item": 2}],
        {"large": "x" * 5000},
        OracleJson({"explicit": True}),
    ]
    await _drop_table(oracle_async_session, table_name)
    await oracle_async_session.execute_script(f"CREATE TABLE {table_name} (id NUMBER PRIMARY KEY, payload JSON)")

    try:
        for row_id, payload in enumerate(payloads, start=1):
            await oracle_async_session.execute(
                f"INSERT INTO {table_name} (id, payload) VALUES (:id, :payload)", {"id": row_id, "payload": payload}
            )

        rows = await oracle_async_session.select(f"SELECT id, payload FROM {table_name} ORDER BY id")

        assert rows[0]["payload"]["storage"] == "native"
        assert isinstance(rows[0]["payload"]["number"], Decimal)
        assert rows[1]["payload"] == [{"item": 1}, {"item": 2}]
        assert rows[2]["payload"]["large"] == "x" * 5000
        assert rows[3]["payload"] == {"explicit": True}
    finally:
        await _drop_table(oracle_async_session, table_name)


async def test_blob_is_json_round_trip_on_oracle_18c(oracle_18c_async_session: "OracleAsyncDriver") -> None:
    """Oracle 18c routes single and array-DML Python JSON through BLOB IS JSON storage."""
    table_name = "json_modes_blob"
    direct_payload = {"storage": "blob", "number": 3.25, "large": "x" * 5000}
    list_payload = [{"storage": "blob-list", "item": 1}, {"storage": "blob-list", "item": 2}]
    wrapped_payload = {"storage": "blob-wrapped", "explicit": True}
    await _drop_table(oracle_18c_async_session, table_name)
    await oracle_18c_async_session.execute_script(
        f"CREATE TABLE {table_name} (id NUMBER PRIMARY KEY, payload BLOB CHECK (payload IS JSON))"
    )

    try:
        await oracle_18c_async_session.execute(
            f"INSERT INTO {table_name} (id, payload) VALUES (:id, :payload)", {"id": 1, "payload": direct_payload}
        )
        await oracle_18c_async_session.execute_many(
            f"INSERT INTO {table_name} (id, payload) VALUES (:id, :payload)",
            [{"id": 2, "payload": list_payload}, {"id": 3, "payload": OracleJson(wrapped_payload)}],
        )

        rows = await oracle_18c_async_session.select(f"SELECT id, payload FROM {table_name} ORDER BY id")

        assert rows[0]["payload"] == direct_payload
        assert isinstance(rows[0]["payload"]["number"], float)
        assert rows[1]["payload"] == list_payload
        assert rows[2]["payload"] == wrapped_payload
    finally:
        await _drop_table(oracle_18c_async_session, table_name)


async def test_clob_is_json_round_trip_on_oracle_18c(oracle_18c_async_session: "OracleAsyncDriver") -> None:
    """Explicit CLOB storage still receives metadata-driven JSON decoding."""
    table_name = "json_modes_clob"
    payload = {"storage": "clob", "number": 3.25, "large": "x" * 5000}
    await _drop_table(oracle_18c_async_session, table_name)
    await oracle_18c_async_session.execute_script(
        f"CREATE TABLE {table_name} (id NUMBER PRIMARY KEY, payload CLOB CHECK (payload IS JSON))"
    )

    try:
        await oracle_18c_async_session.execute(
            f"INSERT INTO {table_name} (id, payload) VALUES (:id, :payload)",
            {"id": 1, "payload": OracleClob(to_json(payload))},
        )

        row = await oracle_18c_async_session.select_one(f"SELECT payload FROM {table_name} WHERE id = :id", {"id": 1})

        assert row["payload"] == payload
        assert isinstance(row["payload"]["number"], float)
    finally:
        await _drop_table(oracle_18c_async_session, table_name)
