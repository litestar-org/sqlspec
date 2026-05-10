# pyright: reportPrivateUsage=false
"""Tests for Oracle ADK store behavior."""

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock

from sqlspec.adapters.oracledb.adk.store import (
    JSONStorageType,
    OracleAsyncADKMemoryStore,
    OracleAsyncADKStore,
    OracleSyncADKMemoryStore,
    OracleSyncADKStore,
    _event_json_column_ddl,
)


def _mock_config(adk_config: dict[str, object]) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"adk": adk_config}
    return config


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


def test_oracle_adk_session_table_applies_partition_compression_and_inmemory_clauses() -> None:
    config = _mock_config({
        "compression": {"enabled": True, "algorithm": "archive_high"},
        "in_memory": True,
        "partitioning": {"strategy": "range", "interval": "day"},
    })
    store = OracleAsyncADKStore(config)

    sql = store._get_create_sessions_table_sql_for_type(JSONStorageType.JSON_NATIVE)

    assert "COLUMN STORE COMPRESS FOR ARCHIVE HIGH" in sql
    assert "INMEMORY PRIORITY HIGH" in sql
    assert "PARTITION BY RANGE (create_time)" in sql
    assert "INTERVAL (NUMTODSINTERVAL(1, ''DAY''))" in sql


def test_oracle_adk_events_table_applies_hash_partitioning_and_table_options() -> None:
    config = _mock_config({
        "events_table_options": "TABLESPACE adk_data",
        "partitioning": {"strategy": "hash", "partition_count": 32},
    })
    store = OracleSyncADKStore(config)

    sql = store._get_create_events_table_sql_for_type(JSONStorageType.JSON_NATIVE)

    assert "TABLESPACE adk_data" in sql
    assert "PARTITION BY HASH (session_id) PARTITIONS 32" in sql


def test_oracle_adk_memory_table_applies_memory_specific_partition_key_and_compression() -> None:
    config = _mock_config({
        "compression": {"enabled": True, "algorithm": "oltp"},
        "memory_table_options": "TABLESPACE adk_memory",
        "partitioning": {"strategy": "hash", "memory_partition_key": "event_id", "partition_count": 8},
    })
    store = OracleAsyncADKMemoryStore(config)

    sql = store._get_create_memory_table_sql_for_type(JSONStorageType.JSON_NATIVE)

    assert "ROW STORE COMPRESS ADVANCED" in sql
    assert "TABLESPACE adk_memory" in sql
    assert "PARTITION BY HASH (event_id) PARTITIONS 8" in sql


def test_oracle_adk_sync_memory_table_uses_same_lifecycle_clauses() -> None:
    config = _mock_config({
        "compression": {"enabled": True, "algorithm": "query_high"},
        "partitioning": {"strategy": "range", "memory_partition_key": "inserted_at", "interval": "week"},
    })
    store = OracleSyncADKMemoryStore(config)

    sql = store._get_create_memory_table_sql_for_type(JSONStorageType.JSON_NATIVE)

    assert "COLUMN STORE COMPRESS FOR QUERY HIGH" in sql
    assert "PARTITION BY RANGE (inserted_at)" in sql
    assert "INTERVAL (NUMTODSINTERVAL(7, ''DAY''))" in sql


async def test_oracle_async_adk_memory_rows_to_records_deserializes_json_fields() -> None:
    store = OracleAsyncADKMemoryStore.__new__(OracleAsyncADKMemoryStore)  # type: ignore[call-arg]
    store._json_storage_type = JSONStorageType.BLOB_JSON
    timestamp = datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)
    row = (
        "memory-1",
        "session-1",
        "app",
        "user",
        "event-1",
        "assistant",
        timestamp,
        b'{"text":"hello"}',
        "hello",
        b'{"source":"unit"}',
        timestamp,
    )

    records = await store._rows_to_records([row])

    assert records == [
        {
            "id": "memory-1",
            "session_id": "session-1",
            "app_name": "app",
            "user_id": "user",
            "event_id": "event-1",
            "author": "assistant",
            "timestamp": timestamp,
            "content_json": {"text": "hello"},
            "content_text": "hello",
            "metadata_json": {"source": "unit"},
            "inserted_at": timestamp,
        }
    ]


def test_oracle_sync_adk_memory_rows_to_records_deserializes_json_fields() -> None:
    store = OracleSyncADKMemoryStore.__new__(OracleSyncADKMemoryStore)  # type: ignore[call-arg]
    store._json_storage_type = JSONStorageType.BLOB_JSON
    timestamp = datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)
    row = (
        "memory-2",
        "session-2",
        "app",
        "user",
        "event-2",
        "user",
        timestamp,
        b'{"text":"sync"}',
        "sync",
        b'{"source":"unit"}',
        timestamp,
    )

    records = store._rows_to_records([row])

    assert records[0]["content_json"] == {"text": "sync"}
    assert records[0]["metadata_json"] == {"source": "unit"}
    assert records[0]["content_text"] == "sync"
