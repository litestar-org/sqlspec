# pyright: reportPrivateUsage=false
"""Tests for Oracle ADK store behavior."""

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, cast, get_args, get_origin
from unittest.mock import MagicMock

from typing_extensions import NotRequired

from sqlspec.adapters.oracledb.adk import (
    JSONStorageType,
    OracleADKCompressionConfig,
    OracleADKConfig,
    OracleADKPartitionConfig,
    OracleAsyncADKMemoryStore,
    OracleAsyncADKStore,
    OracleSyncADKMemoryStore,
    OracleSyncADKStore,
)
from sqlspec.adapters.oracledb.adk.store import _event_data_column_ddl
from sqlspec.config import ADKConfig


def _mock_config(adk_config: dict[str, object]) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"adk": adk_config}
    return config


def test_oracle_adk_config_types_adapter_local_optimizations() -> None:
    """Oracle ADK optimization settings are typed on the adapter-local extension config."""

    assert cast("Any", ADKConfig).__optional_keys__ <= cast("Any", OracleADKConfig).__optional_keys__

    expected_types: dict[str, object] = {
        "in_memory": bool,
        "compression": OracleADKCompressionConfig,
        "partitioning": OracleADKPartitionConfig,
        "session_table_options": str,
        "events_table_options": str,
        "app_state_table_options": str,
        "user_state_table_options": str,
        "memory_table_options": str,
    }
    for feature_name, expected_type in expected_types.items():
        annotation = cast("Any", OracleADKConfig.__annotations__[feature_name])
        assert get_origin(annotation) is NotRequired
        assert get_args(annotation) == (expected_type,)

    for config_type, feature_types in (
        (OracleADKCompressionConfig, {"enabled": bool, "algorithm": str}),
        (
            OracleADKPartitionConfig,
            {
                "strategy": str,
                "partition_count": int,
                "partitions": int,
                "interval": str,
                "initial_less_than": str,
                "partition_key": str,
            },
        ),
    ):
        for feature_name, expected_type in feature_types.items():
            annotation = cast("Any", config_type.__annotations__[feature_name])
            assert get_origin(annotation) is NotRequired
            assert get_args(annotation) == (expected_type,)


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


def test_oracle_event_data_column_ddl_prefers_blob_over_clob() -> None:
    assert _event_data_column_ddl(JSONStorageType.JSON_NATIVE) == "event_data JSON NOT NULL"
    assert _event_data_column_ddl(JSONStorageType.BLOB_JSON) == "event_data BLOB CHECK (event_data IS JSON) NOT NULL"
    assert _event_data_column_ddl(JSONStorageType.BLOB_PLAIN) == "event_data BLOB NOT NULL"


async def test_oracle_async_adk_store_serialize_event_data_uses_blob_for_non_native() -> None:
    store = OracleAsyncADKStore.__new__(OracleAsyncADKStore)  # type: ignore[call-arg]
    store._json_storage_type = JSONStorageType.BLOB_JSON  # type: ignore[attr-defined]

    result = await store._serialize_event_data({"value": 1})  # type: ignore[attr-defined]

    assert isinstance(result, bytes)
    assert b'"value":1' in result


def test_oracle_sync_adk_store_serialize_event_data_uses_blob_for_non_native() -> None:
    store = OracleSyncADKStore.__new__(OracleSyncADKStore)  # type: ignore[call-arg]
    store._json_storage_type = JSONStorageType.BLOB_JSON  # type: ignore[attr-defined]

    result = store._serialize_event_data({"value": 1})  # type: ignore[attr-defined]

    assert isinstance(result, bytes)
    assert b'"value":1' in result


def test_oracle_adk_session_table_applies_partition_compression_and_inmemory_clauses() -> None:
    config = _mock_config({
        "compression": {"enabled": True, "algorithm": "archive_high"},
        "in_memory": True,
        "partitioning": {"strategy": "range", "interval": "day"},
    })
    store = OracleAsyncADKStore(config)

    sql = store._sessions_table_ddl_for_type(JSONStorageType.JSON_NATIVE)

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

    sql = store._events_table_ddl_for_type(JSONStorageType.JSON_NATIVE)

    assert "TABLESPACE adk_data" in sql
    assert "PARTITION BY HASH (session_id) PARTITIONS 32" in sql


def test_oracle_adk_memory_table_applies_memory_specific_partition_key_and_compression() -> None:
    config = _mock_config({
        "compression": {"enabled": True, "algorithm": "oltp"},
        "memory_table_options": "TABLESPACE adk_memory",
        "partitioning": {"strategy": "hash", "memory_partition_key": "event_id", "partition_count": 8},
    })
    store = OracleAsyncADKMemoryStore(config)

    sql = store._memory_table_ddl_for_type(JSONStorageType.JSON_NATIVE)

    assert "ROW STORE COMPRESS ADVANCED" in sql
    assert "TABLESPACE adk_memory" in sql
    assert "PARTITION BY HASH (event_id) PARTITIONS 8" in sql


def test_oracle_adk_sync_memory_table_uses_same_lifecycle_clauses() -> None:
    config = _mock_config({
        "compression": {"enabled": True, "algorithm": "query_high"},
        "partitioning": {"strategy": "range", "memory_partition_key": "inserted_at", "interval": "week"},
    })
    store = OracleSyncADKMemoryStore(config)

    sql = store._memory_table_ddl_for_type(JSONStorageType.JSON_NATIVE)

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
