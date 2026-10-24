# pyright: reportPrivateUsage=false
"""Tests for Oracle ADK store behavior."""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, NotRequired, Self, cast, get_args, get_origin
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

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
from sqlspec.adapters.oracledb.data_dictionary import OracleVersionCache, OracleVersionInfo
from sqlspec.config import ADKConfig
from sqlspec.exceptions import SQLSpecError


def _mock_config(adk_config: dict[str, object]) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"adk": adk_config}
    cache = OracleVersionCache()
    config._oracle_version_cache = cache
    return config


def _config_for_storage(storage_type: JSONStorageType) -> MagicMock:
    version = {
        JSONStorageType.JSON_NATIVE: OracleVersionInfo(21, 3, 0, compatible="21.0.0"),
        JSONStorageType.BLOB_JSON: OracleVersionInfo(19, 0, 0, compatible="19.0.0"),
        JSONStorageType.BLOB_PLAIN: OracleVersionInfo(11, 2, 0, compatible="11.2.0"),
    }[storage_type]
    config = MagicMock()
    cache = OracleVersionCache()
    cache.resolved = True
    cache.version = version
    config._oracle_version_cache = cache
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
    store._config = _config_for_storage(JSONStorageType.BLOB_JSON)  # type: ignore[attr-defined]

    result = await store._serialize_event_data({"value": 1})  # type: ignore[attr-defined]

    assert isinstance(result, bytes)
    assert b'"value":1' in result


def test_oracle_sync_adk_store_serialize_event_data_uses_blob_for_non_native() -> None:
    store = OracleSyncADKStore.__new__(OracleSyncADKStore)  # type: ignore[call-arg]
    store._config = _config_for_storage(JSONStorageType.BLOB_JSON)  # type: ignore[attr-defined]

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


def test_oracle_adk_state_tables_honor_partition_and_table_options() -> None:
    config = _mock_config({
        "app_state_table_options": "TABLESPACE app_state_data",
        "user_state_table_options": "TABLESPACE user_state_data",
        "partitioning": {
            "strategy": "hash",
            "partition_count": 8,
            "app_state_partition_key": "app_name",
            "user_state_partition_key": "user_id",
        },
    })

    for store in (OracleAsyncADKStore(config), OracleSyncADKStore(config)):
        app_sql = store._app_states_table_ddl_for_type(JSONStorageType.JSON_NATIVE)
        user_sql = store._user_states_table_ddl_for_type(JSONStorageType.JSON_NATIVE)

        assert "TABLESPACE app_state_data" in app_sql
        assert "PARTITION BY HASH (app_name) PARTITIONS 8" in app_sql
        assert "TABLESPACE user_state_data" in user_sql
        assert "PARTITION BY HASH (user_id) PARTITIONS 8" in user_sql


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
    store._config = _config_for_storage(JSONStorageType.BLOB_JSON)
    timestamp = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    row = (
        "memory-1",
        "session-1",
        "app",
        "user",
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
            "scope": "user",
            "event_id": "event-1",
            "author": "assistant",
            "timestamp": timestamp,
            "content_json": {"text": "hello"},
            "content_text": "hello",
            "metadata_json": {"source": "unit"},
            "inserted_at": timestamp,
            "embedding": None,
        }
    ]


def test_oracle_sync_adk_memory_rows_to_records_deserializes_json_fields() -> None:
    store = OracleSyncADKMemoryStore.__new__(OracleSyncADKMemoryStore)  # type: ignore[call-arg]
    store._config = _config_for_storage(JSONStorageType.BLOB_JSON)
    timestamp = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    row = (
        "memory-2",
        "session-2",
        "app",
        "user",
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
    assert records[0]["scope"] == "user"
    assert records[0]["embedding"] is None


def _sync_store_with_driver() -> "tuple[Any, MagicMock, MagicMock]":
    config = _config_for_storage(JSONStorageType.BLOB_JSON)
    config.extension_config = {"adk": {}}
    store = OracleSyncADKStore(config)
    driver = MagicMock()
    config.provide_session.return_value.__enter__.return_value = driver
    config.provide_session.return_value.__exit__.return_value = False
    return store, driver, config


def test_oracle_adk_create_tables_checks_existence() -> None:
    """create_tables consults get_tables and issues no CREATE TABLE when present."""

    store, driver, _ = _sync_store_with_driver()
    present = [
        store._session_table,
        store._events_table,
        store._app_state_table,
        store._user_state_table,
        store._metadata_table,
    ]
    driver.data_dictionary.get_tables.return_value = [{"table_name": name.upper()} for name in present]

    store.create_tables()

    driver.data_dictionary.get_tables.assert_called_once()
    executed = [str(call.args[0]) for call in driver.execute_script.call_args_list]
    assert all("CREATE TABLE" not in sql.upper() for sql in executed)


def test_oracle_adk_create_tables_creates_absent_tables() -> None:
    """create_tables issues CREATE TABLE with ORA-00955 race guards for tables the dictionary omits."""

    store, driver, _ = _sync_store_with_driver()
    driver.data_dictionary.get_tables.return_value = []

    store.create_tables()

    driver.data_dictionary.get_tables.assert_called_once()
    executed = " ".join(str(call.args[0]) for call in driver.execute_script.call_args_list)
    assert "CREATE TABLE" in executed.upper()
    assert "SQLCODE != -955" in executed


class _RecordingCursor:
    """Records the SQL and named binds a session listing issues."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.closed = False

    def __enter__(self) -> "Self":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def close(self) -> None:
        self.closed = True

    def execute(self, sql: str, params: "dict[str, Any] | None" = None) -> None:
        self.calls.append((sql, dict(params or {})))

    def fetchall(self) -> "list[Any]":
        return []


class _AsyncRecordingCursor(_RecordingCursor):
    async def execute(self, sql: str, params: "dict[str, Any] | None" = None) -> None:  # type: ignore[override]
        _RecordingCursor.execute(self, sql, params)

    async def fetchall(self) -> "list[Any]":  # type: ignore[override]
        return []


class _RecordingConnection:
    def __init__(self, cursor: _RecordingCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _RecordingCursor:
        return self._cursor

    def commit(self) -> None:
        return None

    async def commit_async(self) -> None:
        return None

    def __enter__(self) -> "Self":
        return self

    def __exit__(self, *_: Any) -> None:
        return None

    async def __aenter__(self) -> "Self":
        return self

    async def __aexit__(self, *_: Any) -> None:
        return None


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


def _async_session_store() -> "tuple[OracleAsyncADKStore, _RecordingCursor]":
    cursor = _AsyncRecordingCursor()
    config = _config_for_storage(JSONStorageType.BLOB_JSON)
    config.extension_config = {"adk": {}}
    config.provide_connection = lambda *_a, **_k: _RecordingConnection(cursor)
    return OracleAsyncADKStore(config), cursor


def _sync_session_store() -> "tuple[OracleSyncADKStore, _RecordingCursor]":
    cursor = _RecordingCursor()
    config = _config_for_storage(JSONStorageType.BLOB_JSON)
    config.extension_config = {"adk": {}}
    config.provide_connection = lambda *_a, **_k: _RecordingConnection(cursor)
    return OracleSyncADKStore(config), cursor


async def test_oracle_async_list_sessions_binds_order_and_page() -> None:
    """Oracle row limiting binds the page values as named parameters."""
    store, cursor = _async_session_store()

    await store.list_sessions("app", "u1", order_by="create_time", descending=False, limit=10, offset=20)

    sql, params = cursor.calls[0]
    assert _normalized(sql) == (
        "SELECT id, app_name, user_id, state, create_time, update_time "
        "FROM adk_session WHERE app_name = :app_name AND user_id = :user_id "
        "ORDER BY create_time ASC, id ASC "
        "OFFSET :page_offset ROWS FETCH NEXT :page_limit ROWS ONLY"
    )
    assert params == {"app_name": "app", "user_id": "u1", "page_limit": 10, "page_offset": 20}


def test_oracle_sync_list_sessions_binds_order_and_page() -> None:
    """The sync cursor path produces the same bounded query as the async path."""
    store, cursor = _sync_session_store()

    store.list_sessions("app", "u1", order_by="create_time", descending=False, limit=10, offset=20)

    sql, params = cursor.calls[0]
    assert _normalized(sql) == (
        "SELECT id, app_name, user_id, state, create_time, update_time "
        "FROM adk_session WHERE app_name = :app_name AND user_id = :user_id "
        "ORDER BY create_time ASC, id ASC "
        "OFFSET :page_offset ROWS FETCH NEXT :page_limit ROWS ONLY"
    )
    assert params == {"app_name": "app", "user_id": "u1", "page_limit": 10, "page_offset": 20}


async def test_oracle_async_list_sessions_defaults_to_recent_first_without_a_page() -> None:
    """The default listing keeps recent-first ordering and binds no page values."""
    store, cursor = _async_session_store()

    await store.list_sessions("app")

    sql, params = cursor.calls[0]
    assert _normalized(sql).endswith("WHERE app_name = :app_name ORDER BY update_time DESC, id DESC")
    assert params == {"app_name": "app"}


def test_oracle_sync_list_sessions_pages_an_unfiltered_listing() -> None:
    """Row limiting composes with an app-only listing."""
    store, cursor = _sync_session_store()

    store.list_sessions("app", limit=5)

    sql, params = cursor.calls[0]
    assert _normalized(sql).endswith(
        "ORDER BY update_time DESC, id DESC OFFSET :page_offset ROWS FETCH NEXT :page_limit ROWS ONLY"
    )
    assert params == {"app_name": "app", "page_limit": 5, "page_offset": 0}


async def test_oracle_async_list_sessions_zero_limit_never_queries() -> None:
    """A zero limit short-circuits before any database work."""
    store, cursor = _async_session_store()

    assert await store.list_sessions("app", limit=0) == []
    assert cursor.calls == []


def test_oracle_sync_list_sessions_zero_limit_never_queries() -> None:
    """A zero limit short-circuits before any database work on the sync path."""
    store, cursor = _sync_session_store()

    assert store.list_sessions("app", limit=0) == []
    assert cursor.calls == []


@pytest.mark.parametrize("storage_type", list(JSONStorageType))
def test_oracle_adk_memory_table_ddl_has_no_duplicate_columns(storage_type: JSONStorageType) -> None:
    """Memory table DDL must not declare duplicate app_name or user_id columns."""
    config = _mock_config({})
    for store in (OracleAsyncADKMemoryStore(config), OracleSyncADKMemoryStore(config)):
        sql = store._memory_table_ddl_for_type(storage_type)
        assert sql.count("app_name VARCHAR2(128) NOT NULL") == 1
        assert sql.count("user_id VARCHAR2(128) NOT NULL") == 1


def test_oracle_sync_list_sessions_closes_cursor() -> None:
    """Sync ADK store closes cursor via context manager."""
    store, cursor = _sync_session_store()
    store.list_sessions("app")
    assert cursor.closed is True


async def test_oracle_async_list_sessions_closes_cursor() -> None:
    """Async ADK store closes cursor via context manager."""
    store, cursor = _async_session_store()
    await store.list_sessions("app")
    assert cursor.closed is True


@pytest.mark.parametrize(
    "options",
    [
        pytest.param({"order_by": "id"}, id="unknown-order-column"),
        pytest.param({"limit": -1}, id="negative-limit"),
        pytest.param({"limit": True}, id="boolean-limit"),
        pytest.param({"offset": 5}, id="unbounded-offset"),
    ],
)
def test_oracle_sync_list_sessions_rejects_invalid_options(options: "dict[str, Any]") -> None:
    """Invalid ordering or paging fails before a connection is acquired."""
    store, cursor = _sync_session_store()

    with pytest.raises(ValueError):
        store.list_sessions("app", **options)

    assert cursor.calls == []


def test_oracle_sync_upsert_state_avoids_dual_state_bind_and_coerces_large_payload() -> None:
    """MERGE statements must not bind :state inside SELECT FROM DUAL and must coerce >2KB BLOBs."""
    cursor = _RecordingCursor()
    conn = _RecordingConnection(cursor)
    lob_mock = MagicMock()
    conn.createlob = MagicMock(return_value=lob_mock)  # type: ignore[attr-defined]
    config = _config_for_storage(JSONStorageType.BLOB_JSON)
    config.extension_config = {"adk": {}}
    config.provide_connection = lambda *_a, **_k: conn
    store = OracleSyncADKStore(config)

    large_state = {"blob": "x" * 5000}
    store.upsert_app_state("app", large_state)
    store.upsert_user_state("app", "u1", large_state)

    app_sql, app_params = cursor.calls[0]
    user_sql, user_params = cursor.calls[1]
    assert ":state AS state FROM DUAL" not in app_sql
    assert ":state AS state FROM DUAL" not in user_sql
    assert app_params["state"] is lob_mock
    assert user_params["state"] is lob_mock


def test_oracle_adk_memory_store_validates_table_identifier_and_drops_actual_indexes() -> None:
    """Memory stores validate _memory_table and drop the actual created index names."""
    bad_config = _mock_config({"memory_table": "bad; DROP TABLE x"})
    with pytest.raises((SQLSpecError, ValueError)):
        OracleSyncADKMemoryStore(bad_config)
    with pytest.raises((SQLSpecError, ValueError)):
        OracleAsyncADKMemoryStore(bad_config)

    fts_config = _mock_config({"memory_use_fts": True})
    sync_store = OracleSyncADKMemoryStore(fts_config)
    drop_sql = " ".join(sync_store._drop_memory_table_sql())
    assert "idx_adk_memory_app_scope_user_time" in drop_sql
    assert "idx_adk_memory_scope" in drop_sql
    assert "idx_adk_memory_fts" in drop_sql
    assert "idx_adk_memory_app_user_time" not in drop_sql


def test_oracle_adk_memory_store_preserves_raw_owner_id() -> None:
    """insert_memory_entries binds owner_id directly without str() coercion."""
    cursor = _RecordingCursor()
    conn = _RecordingConnection(cursor)
    config = _config_for_storage(JSONStorageType.JSON_NATIVE)
    config.extension_config = {"adk": {"owner_id_column": "owner_id RAW(16)"}}
    config.provide_connection = lambda *_a, **_k: conn
    store = OracleSyncADKMemoryStore(config)

    owner_uuid = uuid4()
    now = datetime.now(UTC)
    store.insert_memory_entries(
        [
            {
                "id": "m1",
                "session_id": "s1",
                "app_name": "app",
                "user_id": "u1",
                "scope": "user",
                "event_id": "e1",
                "author": "user",
                "timestamp": now,
                "content_json": {"a": 1},
                "content_text": "hello",
                "metadata_json": None,
                "inserted_at": now,
                "embedding": None,
            }
        ],
        owner_id=owner_uuid,
    )

    _, params = cursor.calls[0]
    assert params["owner_id"] is owner_uuid


def test_oracle_adk_owner_id_column_ddl_escapes_single_quotes() -> None:
    """owner_id_column DDL with single-quoted defaults is escaped inside EXECUTE IMMEDIATE."""
    config = _mock_config({"owner_id_column": "owner_id VARCHAR2(32) DEFAULT 'anon' NOT NULL"})
    session_store = OracleSyncADKStore(config)
    memory_store = OracleSyncADKMemoryStore(config)

    session_ddl = session_store._sessions_table_ddl_for_type(JSONStorageType.JSON_NATIVE)
    memory_ddl = memory_store._memory_table_ddl_for_type(JSONStorageType.JSON_NATIVE)

    assert "DEFAULT ''anon'' NOT NULL" in session_ddl
    assert "DEFAULT ''anon'' NOT NULL" in memory_ddl


def test_oracle_sync_adk_memory_fts_sanitizes_query_and_hybrid_rrf_fuses_candidates() -> None:
    cursor = _RecordingCursor()
    conn = _RecordingConnection(cursor)
    config = _config_for_storage(JSONStorageType.JSON_NATIVE)
    config.extension_config = {"adk": {"memory_use_fts": True}}
    config.provide_connection = lambda *_a, **_k: conn
    store = OracleSyncADKMemoryStore(config)

    store.search_entries(query="NOT (alpha & beta)", app_name="app", user_id="u1", limit=5)
    assert len(cursor.calls) == 1
    fts_sql, fts_params = cursor.calls[0]
    assert "CONTAINS(content_text, :query, 1) > 0" in fts_sql
    assert fts_params["query"] == "{NOT} ACCUM {alpha} ACCUM {beta}"

    cursor.calls.clear()
    assert store.search_entries(query="   !@#   ", app_name="app", user_id="u1", limit=5) == []
    assert cursor.calls == []

    store.search_entries(query="alpha beta", app_name="app", user_id="u1", limit=5, embedding=[0.1, 0.2, 0.3])
    assert len(cursor.calls) == 1
    rrf_sql, rrf_params = cursor.calls[0]
    assert "rrf_score" in rrf_sql
    assert "FETCH APPROX FIRST :top_k ROWS ONLY" in rrf_sql
    assert rrf_params["text_query"] == "{alpha} ACCUM {beta}"
    assert rrf_params["top_k"] == 5


@pytest.mark.anyio
async def test_oracle_async_adk_memory_fts_sanitizes_query_and_hybrid_rrf_fuses_candidates() -> None:
    cursor = _AsyncRecordingCursor()
    conn = _RecordingConnection(cursor)
    config = _config_for_storage(JSONStorageType.JSON_NATIVE)
    config.extension_config = {"adk": {"memory_use_fts": True}}
    config.provide_connection = lambda *_a, **_k: conn
    store = OracleAsyncADKMemoryStore(config)

    await store.search_entries(
        query="NOT (alpha & beta)", app_name="app", user_id="u1", limit=5, embedding=[0.1, 0.2, 0.3]
    )
    assert len(cursor.calls) == 1
    rrf_sql, rrf_params = cursor.calls[0]
    assert "rrf_score" in rrf_sql
    assert rrf_params["text_query"] == "{NOT} ACCUM {alpha} ACCUM {beta}"


def test_oracle_sync_adk_memory_hybrid_rrf_near_and_exact_fallback() -> None:
    """Sync ADK memory store forwards fts_operator='NEAR' and approximate=False."""
    cursor = _RecordingCursor()
    conn = _RecordingConnection(cursor)
    config = _config_for_storage(JSONStorageType.JSON_NATIVE)
    config.extension_config = {"adk": {"memory_use_fts": True}}
    config.provide_connection = lambda *_a, **_k: conn
    store = OracleSyncADKMemoryStore(config)

    store._search_entries_hybrid_rrf(
        "dark roast espresso", [0.1, 0.2, 0.3], "app", "u1", 5, fts_operator="NEAR", approximate=False
    )
    assert len(cursor.calls) == 1
    rrf_sql, rrf_params = cursor.calls[0]
    assert rrf_params["text_query"] == "NEAR(({dark}, {roast}, {espresso}))"
    assert "FETCH APPROX" not in rrf_sql
    assert "FETCH FIRST :top_k ROWS ONLY" in rrf_sql

    cursor.calls.clear()
    store._search_entries_hybrid_rrf(
        "   !@#   ", [0.1, 0.2, 0.3], "app", "u1", 5, fts_operator="NEAR", approximate=False
    )
    assert len(cursor.calls) == 1
    vec_sql, _ = cursor.calls[0]
    assert "FETCH APPROX" not in vec_sql
    assert "FETCH FIRST :top_k ROWS ONLY" in vec_sql


@pytest.mark.anyio
async def test_oracle_async_adk_memory_hybrid_rrf_near_and_exact_fallback() -> None:
    """Async ADK memory store forwards fts_operator='NEAR' and approximate=False."""
    cursor = _AsyncRecordingCursor()
    conn = _RecordingConnection(cursor)
    config = _config_for_storage(JSONStorageType.JSON_NATIVE)
    config.extension_config = {"adk": {"memory_use_fts": True}}
    config.provide_connection = lambda *_a, **_k: conn
    store = OracleAsyncADKMemoryStore(config)

    await store._search_entries_hybrid_rrf(
        "dark roast espresso", [0.1, 0.2, 0.3], "app", "u1", 5, fts_operator="NEAR", approximate=False
    )
    assert len(cursor.calls) == 1
    rrf_sql, rrf_params = cursor.calls[0]
    assert rrf_params["text_query"] == "NEAR(({dark}, {roast}, {espresso}))"
    assert "FETCH APPROX" not in rrf_sql
    assert "FETCH FIRST :top_k ROWS ONLY" in rrf_sql

    cursor.calls.clear()
    await store._search_entries_hybrid_rrf(
        "   !@#   ", [0.1, 0.2, 0.3], "app", "u1", 5, fts_operator="NEAR", approximate=False
    )
    assert len(cursor.calls) == 1
    vec_sql, _ = cursor.calls[0]
    assert "FETCH APPROX" not in vec_sql
    assert "FETCH FIRST :top_k ROWS ONLY" in vec_sql
