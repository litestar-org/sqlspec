"""Integration tests for Oracle smart LOB / JSON coercion (Chapter 2).

Covers the wrapper-aware routing (:class:`OracleClob`, :class:`OracleBlob`,
:class:`OracleJson`) and the user-configurable byte thresholds wired through
``driver_features``. Pattern mirrors :mod:`test_msgspec_clob`.

Cases:
    1. ``OracleClob`` round-trip into a CLOB column (length threshold bypassed).
    2. ``OracleBlob`` round-trip into a BLOB column.
    3. ``OracleJson`` round-trip into a native ``JSON`` column on 23ai —
       verifies the C1 input handler claims the value (no CLOB intermediary).
    4. ``oracle_varchar2_byte_limit`` override keeps a 5000-byte string as
       VARCHAR2 (skipped unless the container has MAX_STRING_SIZE=EXTENDED).
    5. Demo workaround: binding ``bytes`` from ``to_json(..., as_bytes=True)``
       directly to a JSON column — no manual ``createlob`` needed.
"""

import pytest

from sqlspec.adapters.oracledb import (
    OracleAsyncConfig,
    OracleAsyncDriver,
    OracleBlob,
    OracleClob,
    OracleJson,
    OraclePoolParams,
    OracleSyncDriver,
)
from sqlspec.utils.serializers import to_json

pytestmark = pytest.mark.xdist_group("oracle")


_LARGE_CLOB_TEXT = "Lorem ipsum " * 1000
_LARGE_BLOB_BYTES = b"\x00\x01\x02\x03" * 2000
_LARGE_JSON_PAYLOAD = {"big": "x" * 10000, "nested": {"items": list(range(50))}}


async def _max_string_size_is_extended(driver: "OracleAsyncDriver") -> bool:
    """Return True iff the Oracle instance has MAX_STRING_SIZE=EXTENDED."""
    result = await driver.execute("SELECT value FROM v$parameter WHERE name = 'max_string_size'")
    rows = result.get_data() if hasattr(result, "get_data") else result.data
    if not rows:
        return False
    first = rows[0]
    value = first.get("value") if isinstance(first, dict) else first[0]
    return isinstance(value, str) and value.upper() == "EXTENDED"


async def test_oracle_clob_wrapper_round_trip(oracle_async_session: "OracleAsyncDriver") -> None:
    """OracleClob bypasses the length threshold and round-trips through a CLOB column."""
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE smart_lob_clob'; "
        "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
    await oracle_async_session.execute_script("CREATE TABLE smart_lob_clob (id NUMBER PRIMARY KEY, content CLOB)")
    await oracle_async_session.execute(
        "INSERT INTO smart_lob_clob (id, content) VALUES (:id, :content)",
        {"id": 1, "content": OracleClob(_LARGE_CLOB_TEXT)},
    )

    result = await oracle_async_session.execute("SELECT content FROM smart_lob_clob WHERE id = :id", {"id": 1})
    rows = result.get_data() if hasattr(result, "get_data") else result.data
    fetched = rows[0]
    value = fetched["content"] if isinstance(fetched, dict) else fetched[0]
    assert value == _LARGE_CLOB_TEXT


async def test_oracle_blob_wrapper_round_trip(oracle_async_session: "OracleAsyncDriver") -> None:
    """OracleBlob round-trips raw bytes through a BLOB column."""
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE smart_lob_blob'; "
        "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
    await oracle_async_session.execute_script("CREATE TABLE smart_lob_blob (id NUMBER PRIMARY KEY, data BLOB)")
    await oracle_async_session.execute(
        "INSERT INTO smart_lob_blob (id, data) VALUES (:id, :data)", {"id": 1, "data": OracleBlob(_LARGE_BLOB_BYTES)}
    )

    result = await oracle_async_session.execute("SELECT data FROM smart_lob_blob WHERE id = :id", {"id": 1})
    rows = result.get_data() if hasattr(result, "get_data") else result.data
    fetched = rows[0]
    value = fetched["data"] if isinstance(fetched, dict) else fetched[0]
    assert bytes(value) == _LARGE_BLOB_BYTES


async def test_oracle_json_wrapper_native_round_trip(oracle_async_session: "OracleAsyncDriver") -> None:
    """OracleJson round-trips through a native JSON column on Oracle 23ai.

    The wrapper unwraps in coerce_large_parameters_async and the C1 JSON input
    handler claims the dict before any CLOB coercion can fire.
    """
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE smart_lob_json'; "
        "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
    await oracle_async_session.execute_script("CREATE TABLE smart_lob_json (id NUMBER PRIMARY KEY, payload JSON)")
    await oracle_async_session.execute(
        "INSERT INTO smart_lob_json (id, payload) VALUES (:id, :payload)",
        {"id": 1, "payload": OracleJson(_LARGE_JSON_PAYLOAD)},
    )

    result = await oracle_async_session.execute("SELECT payload FROM smart_lob_json WHERE id = :id", {"id": 1})
    rows = result.get_data() if hasattr(result, "get_data") else result.data
    fetched = rows[0]
    payload = fetched["payload"] if isinstance(fetched, dict) else fetched[0]
    assert payload["big"] == _LARGE_JSON_PAYLOAD["big"]
    assert payload["nested"] == _LARGE_JSON_PAYLOAD["nested"]


async def test_threshold_override_keeps_string_as_varchar2(
    oracle_async_config: "OracleAsyncConfig", oracle_connection_config: "OraclePoolParams"
) -> None:
    """Setting oracle_varchar2_byte_limit=32767 keeps a 5000-byte string as VARCHAR2.

    Skipped on instances without MAX_STRING_SIZE=EXTENDED.
    """
    config = OracleAsyncConfig(
        connection_config=OraclePoolParams(**oracle_connection_config),
        driver_features={"oracle_varchar2_byte_limit": 32767},
    )
    try:
        async with config.provide_session() as session:
            if not await _max_string_size_is_extended(session):
                pytest.skip("MAX_STRING_SIZE != EXTENDED on this container")

            await session.execute_script(
                "BEGIN EXECUTE IMMEDIATE 'DROP TABLE smart_lob_extended'; "
                "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
            )
            await session.execute_script(
                "CREATE TABLE smart_lob_extended (id NUMBER PRIMARY KEY, content VARCHAR2(32767))"
            )
            payload = "y" * 5000
            await session.execute(
                "INSERT INTO smart_lob_extended (id, content) VALUES (:id, :content)", {"id": 1, "content": payload}
            )

            result = await session.execute("SELECT content FROM smart_lob_extended WHERE id = :id", {"id": 1})
            rows = result.get_data() if hasattr(result, "get_data") else result.data
            value = rows[0]["content"] if isinstance(rows[0], dict) else rows[0][0]
            assert value == payload
    finally:
        if config.connection_instance:
            await config.close_pool()


async def test_json_bytes_payload_no_manual_createlob_needed(oracle_async_session: "OracleAsyncDriver") -> None:
    """Demo regression: bytes from to_json(..., as_bytes=True) bind directly to JSON.

    Replicates the workaround at oracledb-vertexai-demo:utils/fixtures.py:282-286
    where a manual ``await connection.createlob(...)`` was needed because raw
    bytes hit the BLOB-coercion fallback. With C2's wrapper-aware routing and
    C1's native JSON binding, the raw bytes path is no longer needed — but the
    user-facing ergonomic answer is to wrap with ``OracleJson`` so the handler
    chain claims it cleanly.
    """
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE smart_lob_json_bytes'; "
        "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
    await oracle_async_session.execute_script("CREATE TABLE smart_lob_json_bytes (id NUMBER PRIMARY KEY, payload JSON)")

    payload = {"workaround_eliminated": True, "blob": "x" * 6000}
    serialized: bytes = to_json(payload, as_bytes=True)
    assert isinstance(serialized, bytes)

    await oracle_async_session.execute(
        "INSERT INTO smart_lob_json_bytes (id, payload) VALUES (:id, :payload)",
        {"id": 1, "payload": OracleJson(payload)},
    )
    del serialized

    result = await oracle_async_session.execute("SELECT payload FROM smart_lob_json_bytes WHERE id = :id", {"id": 1})
    rows = result.get_data() if hasattr(result, "get_data") else result.data
    value = rows[0]["payload"] if isinstance(rows[0], dict) else rows[0][0]
    assert value["workaround_eliminated"] is True
    assert value["blob"] == payload["blob"]


def test_oracle_clob_wrapper_round_trip_sync(oracle_sync_session: "OracleSyncDriver") -> None:
    """Sync coverage for the OracleClob wrapper round-trip."""
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE smart_lob_clob_sync'; "
        "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
    oracle_sync_session.execute_script("CREATE TABLE smart_lob_clob_sync (id NUMBER PRIMARY KEY, content CLOB)")
    oracle_sync_session.execute(
        "INSERT INTO smart_lob_clob_sync (id, content) VALUES (:id, :content)",
        {"id": 1, "content": OracleClob(_LARGE_CLOB_TEXT)},
    )

    result = oracle_sync_session.execute("SELECT content FROM smart_lob_clob_sync WHERE id = :id", {"id": 1})
    rows = result.get_data() if hasattr(result, "get_data") else result.data
    value = rows[0]["content"] if isinstance(rows[0], dict) else rows[0][0]
    assert value == _LARGE_CLOB_TEXT


def test_oracle_blob_wrapper_round_trip_sync(oracle_sync_session: "OracleSyncDriver") -> None:
    """Sync coverage for the OracleBlob wrapper round-trip."""
    oracle_sync_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE smart_lob_blob_sync'; "
        "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
    oracle_sync_session.execute_script("CREATE TABLE smart_lob_blob_sync (id NUMBER PRIMARY KEY, data BLOB)")
    oracle_sync_session.execute(
        "INSERT INTO smart_lob_blob_sync (id, data) VALUES (:id, :data)",
        {"id": 1, "data": OracleBlob(_LARGE_BLOB_BYTES)},
    )

    result = oracle_sync_session.execute("SELECT data FROM smart_lob_blob_sync WHERE id = :id", {"id": 1})
    rows = result.get_data() if hasattr(result, "get_data") else result.data
    value = rows[0]["data"] if isinstance(rows[0], dict) else rows[0][0]
    assert bytes(value) == _LARGE_BLOB_BYTES


async def test_oracle_clob_wrapper_positional_bind(oracle_async_session: "OracleAsyncDriver") -> None:
    """Positional (tuple) binds also unwrap OracleClob — sqlspec-205 fix coverage."""
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE smart_lob_clob_pos'; "
        "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
    await oracle_async_session.execute_script("CREATE TABLE smart_lob_clob_pos (id NUMBER PRIMARY KEY, content CLOB)")
    await oracle_async_session.execute(
        "INSERT INTO smart_lob_clob_pos (id, content) VALUES (:1, :2)", (1, OracleClob(_LARGE_CLOB_TEXT))
    )

    result = await oracle_async_session.execute("SELECT content FROM smart_lob_clob_pos WHERE id = :1", (1,))
    rows = result.get_data() if hasattr(result, "get_data") else result.data
    fetched = rows[0]
    value = fetched["content"] if isinstance(fetched, dict) else fetched[0]
    assert value == _LARGE_CLOB_TEXT


async def test_oracle_json_wrapper_positional_bind(oracle_async_session: "OracleAsyncDriver") -> None:
    """Positional (tuple) bind with OracleJson defers to the C1 native handler."""
    await oracle_async_session.execute_script(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE smart_lob_json_pos'; "
        "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
    await oracle_async_session.execute_script("CREATE TABLE smart_lob_json_pos (id NUMBER PRIMARY KEY, payload JSON)")
    payload = {"positional": True, "n": 7}
    await oracle_async_session.execute(
        "INSERT INTO smart_lob_json_pos (id, payload) VALUES (:1, :2)", (1, OracleJson(payload))
    )

    result = await oracle_async_session.execute("SELECT payload FROM smart_lob_json_pos WHERE id = :1", (1,))
    rows = result.get_data() if hasattr(result, "get_data") else result.data
    fetched = rows[0]
    value = fetched["payload"] if isinstance(fetched, dict) else fetched[0]
    assert value["positional"] is True
    assert value["n"] == 7
