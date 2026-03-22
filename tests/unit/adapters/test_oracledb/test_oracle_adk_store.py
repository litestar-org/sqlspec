"""Tests for Oracle ADK store Decimal coercion."""

from decimal import Decimal

from sqlspec.adapters.oracledb.adk.store import (
    JSONStorageType,
    OracleAsyncADKStore,
    OracleSyncADKStore,
    _event_json_column_ddl,
)


async def test_oracle_async_adk_store_deserialize_dict_coerces_decimal() -> None:
    store = OracleAsyncADKStore.__new__(OracleAsyncADKStore)  # type: ignore[call-arg]

    payload = {"value": Decimal("1.25"), "nested": {"score": Decimal("0.5")}}

    result = await store._deserialize_json_field(payload)  # type: ignore[attr-defined]

    assert result == {"value": 1.25, "nested": {"score": 0.5}}


async def test_oracle_async_adk_store_deserialize_state_dict_coerces_decimal() -> None:
    store = OracleAsyncADKStore.__new__(OracleAsyncADKStore)  # type: ignore[call-arg]

    payload = {"state": Decimal("2.0")}

    result = await store._deserialize_state(payload)  # type: ignore[attr-defined]

    assert result == {"state": 2.0}


def test_oracle_sync_adk_store_deserialize_dict_coerces_decimal() -> None:
    store = OracleSyncADKStore.__new__(OracleSyncADKStore)  # type: ignore[call-arg]

    payload = {"value": Decimal("3.14"), "items": [Decimal("1.0"), Decimal("2.0")]}

    result = store._deserialize_json_field(payload)  # type: ignore[attr-defined]

    assert result == {"value": 3.14, "items": [1.0, 2.0]}


def test_oracle_sync_adk_store_deserialize_state_dict_coerces_decimal() -> None:
    store = OracleSyncADKStore.__new__(OracleSyncADKStore)  # type: ignore[call-arg]

    payload = {"state": Decimal("5.0")}

    result = store._deserialize_state(payload)  # type: ignore[attr-defined]

    assert result == {"state": 5.0}


def test_oracle_event_json_column_ddl_prefers_blob_over_clob() -> None:
    assert _event_json_column_ddl(JSONStorageType.JSON_NATIVE) == "event_json JSON NOT NULL"
    assert _event_json_column_ddl(JSONStorageType.BLOB_JSON) == "event_json BLOB CHECK (event_json IS JSON) NOT NULL"
    assert _event_json_column_ddl(JSONStorageType.BLOB_PLAIN) == "event_json BLOB NOT NULL"


async def test_oracle_async_adk_store_serialize_event_json_uses_blob_for_non_native() -> None:
    store = OracleAsyncADKStore.__new__(OracleAsyncADKStore)  # type: ignore[call-arg]
    store._json_storage_type = JSONStorageType.BLOB_JSON  # type: ignore[attr-defined]

    result = await store._serialize_event_json({"value": 1})  # type: ignore[attr-defined]

    assert isinstance(result, bytes)
    assert b'"value":1' in result


def test_oracle_sync_adk_store_serialize_event_json_uses_blob_for_non_native() -> None:
    store = OracleSyncADKStore.__new__(OracleSyncADKStore)  # type: ignore[call-arg]
    store._json_storage_type = JSONStorageType.BLOB_JSON  # type: ignore[attr-defined]

    result = store._serialize_event_json({"value": 1})  # type: ignore[attr-defined]

    assert isinstance(result, bytes)
    assert b'"value":1' in result
