# pyright: reportPrivateUsage=false
"""Tests for Oracle ADK store behavior."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from inspect import isawaitable
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, NotRequired, Self, cast, get_args, get_origin
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sqlglot import exp, parse_one

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
from sqlspec.exceptions import ImproperConfigurationError, SQLSpecError
from sqlspec.extensions.adk import StoredEvent, StoredMemory

if TYPE_CHECKING:
    from collections.abc import Awaitable


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


def _config_for_vectors() -> MagicMock:
    config = _config_for_storage(JSONStorageType.JSON_NATIVE)
    config._oracle_version_cache.version = OracleVersionInfo(23, 4, 0, compatible="23.4.0")
    return config


def _memory_entry() -> StoredMemory:
    timestamp = datetime(2026, 5, 10, 12, tzinfo=UTC)
    return {
        "id": "memory-1",
        "session_id": "session-1",
        "app_name": "app",
        "user_id": "user",
        "scope": "user",
        "event_id": "event-1",
        "author": "user",
        "timestamp": timestamp,
        "content_json": {"text": "hello"},
        "content_text": "hello",
        "metadata_json": None,
        "inserted_at": timestamp,
        "embedding": None,
    }


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True])
@pytest.mark.parametrize("error_code", [1, 1400, 12899, 51803])
async def test_memory_batch_errors_commit_only_duplicate_skips(is_async: bool, error_code: int) -> None:
    import oracledb

    config = _config_for_storage(JSONStorageType.BLOB_JSON)
    config.extension_config = {"adk": {}}
    cursor = MagicMock()
    batch_error = SimpleNamespace(code=error_code, message=f"ORA-{error_code:05d}: rejected memory")
    cursor.getbatcherrors.return_value = [batch_error]
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    if is_async:
        cursor.executemany = AsyncMock()
        connection.commit = AsyncMock()
        connection.rollback = AsyncMock()
        config.provide_connection.return_value.__aenter__.return_value = connection
        store = OracleAsyncADKMemoryStore(config)
    else:
        config.provide_connection.return_value.__enter__.return_value = connection
        store = OracleSyncADKMemoryStore(config)

    entries: list[StoredMemory] = [_memory_entry(), {**_memory_entry(), "id": "memory-2"}]
    if error_code == 1:
        result: int | Awaitable[int] = store.insert_memory_entries(entries)
        if isawaitable(result):
            result = await result
        assert result == 1
        connection.commit.assert_called_once()
        connection.rollback.assert_not_called()
    else:
        with pytest.raises(oracledb.DatabaseError) as exc_info:
            result = store.insert_memory_entries(entries)
            if isawaitable(result):
                await result
        assert exc_info.value.args[0] is batch_error
        connection.commit.assert_not_called()
        connection.rollback.assert_called_once()


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True])
@pytest.mark.parametrize("use_fts", [False, True])
@pytest.mark.parametrize(
    "version",
    [
        OracleVersionInfo(19, 0, 0, compatible="19.0.0"),
        OracleVersionInfo(21, 3, 0, compatible="21.0.0"),
        OracleVersionInfo(23, 4, 0, compatible="23.0.0"),
    ],
)
async def test_legacy_memory_retains_text_storage_and_rejects_vectors(
    is_async: bool, use_fts: bool, version: OracleVersionInfo
) -> None:
    config = _config_for_storage(JSONStorageType.BLOB_JSON)
    config._oracle_version_cache.version = version
    config.extension_config = {"adk": {"memory_use_fts": use_fts}}
    cursor = MagicMock()
    cursor.getbatcherrors.return_value = []
    timestamp = _memory_entry()["timestamp"]
    cursor.fetchall.return_value = [
        (
            "memory-1",
            "session-1",
            "app",
            "user",
            "user",
            "event-1",
            "user",
            timestamp,
            b'{"text":"hello"}',
            "hello",
            None,
            timestamp,
            None,
        )
    ]
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    executed: list[str] = []

    def execute(sql: str, _params: Any) -> None:
        assert "NULL AS embedding" in sql
        executed.append(sql)

    if is_async:
        cursor.execute = AsyncMock(side_effect=execute)
        cursor.executemany = AsyncMock()
        cursor.fetchall = AsyncMock(return_value=cursor.fetchall.return_value)
        connection.commit = AsyncMock()
        config.provide_connection.return_value.__aenter__.return_value = connection
        store = OracleAsyncADKMemoryStore(config)
    else:
        cursor.execute.side_effect = execute
        config.provide_connection.return_value.__enter__.return_value = connection
        store = OracleSyncADKMemoryStore(config)

    ddl = store._memory_table_ddl()
    if isawaitable(ddl):
        ddl = await ddl

    create_sql = ddl.split("EXECUTE IMMEDIATE '", 1)[1].split("';", 1)[0].replace("''", "'")
    table = parse_one(create_sql, dialect="oracle")
    assert isinstance(table, exp.Create)
    assert {column.name for column in table.this.expressions if isinstance(column, exp.ColumnDef)} == {
        "id",
        "session_id",
        "app_name",
        "user_id",
        "scope",
        "event_id",
        "author",
        "timestamp",
        "content_json",
        "metadata_json",
        "content_text",
        "inserted_at",
    }
    assert "event_id VARCHAR2(128) NOT NULL," in create_sql
    assert "UNIQUE (app_name, user_id, event_id)" in create_sql

    inserted: int | Awaitable[int] = store.insert_memory_entries([_memory_entry()])
    if isawaitable(inserted):
        inserted = await inserted
    assert inserted == 1
    insert_sql, bind_rows = cursor.executemany.call_args.args
    assert "timestamp, content_json" in insert_sql
    assert set(bind_rows[0]) == set(_memory_entry()) - {"embedding"}

    records = store.search_entries("hello", "app", "user")
    if isawaitable(records):
        records = await records
    assert len(executed) == 1
    assert [(record["id"], record["embedding"]) for record in records] == [("memory-1", None)]

    with pytest.raises(ImproperConfigurationError, match=r"23\.4"):
        result = store.insert_memory_entries([{**_memory_entry(), "embedding": [0.1]}])
        if isawaitable(result):
            await result
    with pytest.raises(ImproperConfigurationError, match=r"23\.4"):
        result = store.search_entries("hello", "app", "user", embedding=[0.1])
        if isawaitable(result):
            await result


def test_oracle_adk_config_types_adapter_local_optimizations() -> None:
    """Oracle ADK optimization settings are typed on the adapter-local extension config."""

    assert cast("Any", ADKConfig).__optional_keys__ <= cast("Any", OracleADKConfig).__optional_keys__

    expected_types: dict[str, object] = {
        "vector_dimensions": int,
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


@pytest.mark.anyio
async def test_oracle_adk_memory_ddl_declares_embedding_vector() -> None:
    """Memory table DDL declares an embedding VECTOR column with configured dimensions."""
    config = _config_for_vectors()
    config.extension_config = {"adk": {}}
    store = OracleSyncADKMemoryStore(config)
    sql = store._memory_table_ddl()
    assert "embedding VECTOR(768, FLOAT32)" in sql

    custom_config = _config_for_vectors()
    custom_config.extension_config = {"adk": {"vector_dimensions": 1536}}
    custom_store = OracleAsyncADKMemoryStore(custom_config)
    custom_sql = await custom_store._memory_table_ddl()
    assert "embedding VECTOR(1536, FLOAT32)" in custom_sql


def test_oracle_adk_sync_insert_binds_embedding_array() -> None:
    """Sync memory store binds embeddings as float arrays."""
    import array

    config = _config_for_vectors()
    config.extension_config = {"adk": {}}
    store = OracleSyncADKMemoryStore(config)
    conn = MagicMock()
    cursor = MagicMock()
    cursor.rowcount = 1
    conn.cursor.return_value.__enter__.return_value = cursor
    config.provide_connection.return_value.__enter__.return_value = conn

    timestamp = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    entries: list[StoredMemory] = [
        {
            "id": "mem-1",
            "session_id": "sess-1",
            "app_name": "app",
            "user_id": "user",
            "scope": "user",
            "event_id": "ev-1",
            "author": "user",
            "timestamp": timestamp,
            "content_json": {"k": "v"},
            "content_text": "text",
            "metadata_json": None,
            "inserted_at": timestamp,
            "embedding": [0.1, 0.2, 0.3],
        }
    ]
    inserted = store.insert_memory_entries(entries)
    assert inserted == 1
    assert cursor.executemany.called
    _call_sql, call_params = cursor.executemany.call_args[0]
    first_param = call_params[0]
    assert "embedding" in first_param
    assert isinstance(first_param["embedding"], array.array)
    assert first_param["embedding"].typecode == "f"
    assert list(first_param["embedding"]) == pytest.approx([0.1, 0.2, 0.3])


def test_oracle_adk_memory_rows_to_records_decodes_embedding() -> None:
    """Memory rows to records round-trips vector embeddings as float lists."""
    import array

    store = OracleSyncADKMemoryStore.__new__(OracleSyncADKMemoryStore)  # type: ignore[call-arg]
    store._config = _config_for_storage(JSONStorageType.BLOB_JSON)
    timestamp = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    row = (
        "memory-vec",
        "session-vec",
        "app",
        "user",
        "user",
        "event-vec",
        "user",
        timestamp,
        b'{"text":"vec"}',
        "vec",
        b'{"source":"unit"}',
        timestamp,
        array.array("f", [0.1, 0.2, 0.3]),
    )
    records = store._rows_to_records([row])
    assert len(records) == 1
    assert records[0]["embedding"] == pytest.approx([0.1, 0.2, 0.3])


def test_adk_store_mutation_rolls_back_on_error() -> None:
    """ADK store mutation rolls back connection on error."""
    cursor = MagicMock()
    cursor.execute.side_effect = RuntimeError("mutation error")
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    config = _config_for_storage(JSONStorageType.BLOB_JSON)
    config.extension_config = {"adk": {}}
    config.provide_connection.return_value.__enter__.return_value = conn

    store = OracleSyncADKStore(config)
    with pytest.raises(RuntimeError, match="mutation error"):
        store.delete_session("app", "user", "sess-1")

    conn.rollback.assert_called_once()


def test_oracle_adk_memory_store_batch_insert() -> None:
    """ADK memory store executes batch insert via executemany."""
    cursor = MagicMock()
    cursor.rowcount = 2
    cursor.getbatcherrors.return_value = []
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    config = _config_for_storage(JSONStorageType.JSON_NATIVE)
    config.extension_config = {"adk": {}}
    config.provide_connection.return_value.__enter__.return_value = conn

    store = OracleSyncADKMemoryStore(config)
    now = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    entries: list[StoredMemory] = [
        {
            "id": "mem-1",
            "session_id": "sess-1",
            "app_name": "app",
            "user_id": "user",
            "scope": "user",
            "event_id": "ev-1",
            "author": "user",
            "timestamp": now,
            "content_json": {"k": 1},
            "content_text": "text 1",
            "metadata_json": None,
            "inserted_at": now,
            "embedding": None,
        },
        {
            "id": "mem-2",
            "session_id": "sess-1",
            "app_name": "app",
            "user_id": "user",
            "scope": "user",
            "event_id": "ev-2",
            "author": "user",
            "timestamp": now,
            "content_json": {"k": 2},
            "content_text": "text 2",
            "metadata_json": None,
            "inserted_at": now,
            "embedding": None,
        },
    ]
    inserted = store.insert_memory_entries(entries)
    assert inserted == 2
    cursor.executemany.assert_called_once()
    assert len(cursor.executemany.call_args[0][1]) == 2
    conn.commit.assert_called_once()


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

    def executemany(self, sql: str, params: Any = None, **_kwargs: Any) -> None:
        for p in params or []:
            self.calls.append((sql, dict(p or {})))

    def getbatcherrors(self) -> list[Any]:
        return []

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


@pytest.mark.parametrize("store_cls", [OracleSyncADKStore, OracleSyncADKMemoryStore])
def test_oracle_adk_owner_id_column_ddl_validates_and_renders_unquoted_defaults(store_cls: type[Any]) -> None:
    """owner_id_column accepts unquoted defaults and rejects single-quoted literals."""
    valid_config = _mock_config({"owner_id_column": "owner_id NUMBER(10) DEFAULT 0 NOT NULL"})
    store = store_cls(valid_config)
    ddl = (
        store._sessions_table_ddl_for_type(JSONStorageType.JSON_NATIVE)
        if isinstance(store, OracleSyncADKStore)
        else store._memory_table_ddl_for_type(JSONStorageType.JSON_NATIVE)
    )
    assert "owner_id NUMBER(10) DEFAULT 0 NOT NULL" in ddl

    quoted_config = _mock_config({"owner_id_column": "owner_id VARCHAR2(32) DEFAULT 'anon' NOT NULL"})
    with pytest.raises(ValueError, match="owner_id_column"):
        store_cls(quoted_config)


def test_oracle_sync_adk_memory_fts_sanitizes_query_and_hybrid_rrf_fuses_candidates() -> None:
    cursor = _RecordingCursor()
    conn = _RecordingConnection(cursor)
    config = _config_for_vectors()
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
    config = _config_for_vectors()
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
    config = _config_for_vectors()
    config.extension_config = {"adk": {"memory_use_fts": True, "fts_operator": "NEAR", "approximate": False}}
    config.provide_connection = lambda *_a, **_k: conn
    store = OracleSyncADKMemoryStore(config)

    store.search_entries("dark roast espresso", embedding=[0.1, 0.2, 0.3], app_name="app", user_id="u1", limit=5)
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
    config = _config_for_vectors()
    config.extension_config = {"adk": {"memory_use_fts": True, "fts_operator": "NEAR", "approximate": False}}
    config.provide_connection = lambda *_a, **_k: conn
    store = OracleAsyncADKMemoryStore(config)

    await store.search_entries("dark roast espresso", embedding=[0.1, 0.2, 0.3], app_name="app", user_id="u1", limit=5)
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


def _companion_mock_driver(*, is_async: bool, rows: list[dict[str, str]]) -> MagicMock:
    driver = MagicMock()
    if is_async:
        driver.data_dictionary.get_indexes = AsyncMock(return_value=rows)
        driver.execute = AsyncMock()
        driver.commit = AsyncMock()
    else:
        driver.data_dictionary.get_indexes = MagicMock(return_value=rows)
        driver.execute = MagicMock()
        driver.commit = MagicMock()
    return driver


async def _run_companion_hook(store: Any, driver: Any, *, is_async: bool) -> None:
    if is_async:
        await store.reconcile_companion_objects_async(driver)
    else:
        store.reconcile_companion_objects_sync(driver)


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True])
async def test_oracle_companions_create_only_missing_index(is_async: bool) -> None:
    config = _mock_config({})
    store = OracleAsyncADKStore(config) if is_async else OracleSyncADKStore(config)
    rows = [
        {"table_name": "ADK_SESSION", "index_name": "IDX_ADK_SESSION_APP_USER"},
        {"table_name": "ADK_EVENT", "index_name": "IDX_ADK_EVENT_SESSION"},
        {"table_name": "ADK_EVENT", "index_name": "IDX_ADK_EVENT_INVOCATION"},
        {"table_name": "ADK_EVENT", "index_name": "IDX_ADK_EVENT_TIMESTAMP"},
        {"table_name": "ADK_EVENT", "index_name": "IDX_ADK_EVENT_APP_TIMESTAMP"},
    ]
    driver = _companion_mock_driver(is_async=is_async, rows=rows)

    await _run_companion_hook(store, driver, is_async=is_async)

    executed = [call.args[0] for call in driver.execute.call_args_list]
    assert executed == ["CREATE INDEX idx_adk_session_update_time ON adk_session(update_time DESC)"]
    assert config._oracle_version_cache.resolved is False
    assert driver.commit.call_count == 0


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True])
async def test_oracle_companions_repeat_is_noop(is_async: bool) -> None:
    config = _mock_config({})
    store = OracleAsyncADKStore(config) if is_async else OracleSyncADKStore(config)
    rows = [
        {"table_name": "ADK_SESSION", "index_name": "IDX_ADK_SESSION_APP_USER"},
        {"table_name": "ADK_SESSION", "index_name": "IDX_ADK_SESSION_UPDATE_TIME"},
        {"table_name": "ADK_EVENT", "index_name": "IDX_ADK_EVENT_SESSION"},
        {"table_name": "ADK_EVENT", "index_name": "IDX_ADK_EVENT_INVOCATION"},
        {"table_name": "ADK_EVENT", "index_name": "IDX_ADK_EVENT_TIMESTAMP"},
        {"table_name": "ADK_EVENT", "index_name": "IDX_ADK_EVENT_APP_TIMESTAMP"},
    ]
    driver = _companion_mock_driver(is_async=is_async, rows=rows)

    await _run_companion_hook(store, driver, is_async=is_async)

    assert driver.execute.call_count == 0
    assert config._oracle_version_cache.resolved is False


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True])
async def test_oracle_companions_propagate_name_owned_by_other_table(is_async: bool) -> None:
    config = _mock_config({})
    store = OracleAsyncADKStore(config) if is_async else OracleSyncADKStore(config)
    rows = [{"table_name": "OTHER", "index_name": "IDX_ADK_SESSION_APP_USER"}]
    driver = _companion_mock_driver(is_async=is_async, rows=rows)
    driver.execute.side_effect = RuntimeError("ORA-00955: name is already used by an existing object")

    with pytest.raises(RuntimeError, match="ORA-00955"):
        await _run_companion_hook(store, driver, is_async=is_async)

    assert driver.execute.call_args_list[0].args[0] == (
        "CREATE INDEX idx_adk_session_app_user ON adk_session(app_name, user_id)"
    )


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True])
async def test_oracle_memory_companions_create_context_index(is_async: bool) -> None:
    config = _mock_config({"memory_use_fts": True})
    store = OracleAsyncADKMemoryStore(config) if is_async else OracleSyncADKMemoryStore(config)
    driver = _companion_mock_driver(is_async=is_async, rows=[])

    await _run_companion_hook(store, driver, is_async=is_async)

    executed = [call.args[0] for call in driver.execute.call_args_list]
    assert executed == [
        "CREATE INDEX idx_adk_memory_app_scope_user_time ON adk_memory(app_name, scope, user_id, timestamp DESC)",
        "CREATE INDEX idx_adk_memory_scope ON adk_memory(app_name, scope)",
        "CREATE INDEX idx_adk_memory_session ON adk_memory(session_id)",
        "CREATE INDEX idx_adk_memory_fts ON adk_memory(content_text) INDEXTYPE IS CTXSYS.CONTEXT",
    ]
    assert config._oracle_version_cache.resolved is False
    assert driver.commit.call_count == 0


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True])
async def test_oracle_memory_companions_omit_context_index_without_fts(is_async: bool) -> None:
    config = _mock_config({})
    store = OracleAsyncADKMemoryStore(config) if is_async else OracleSyncADKMemoryStore(config)
    driver = _companion_mock_driver(is_async=is_async, rows=[])

    await _run_companion_hook(store, driver, is_async=is_async)

    executed = [call.args[0] for call in driver.execute.call_args_list]
    assert len(executed) == 3
    assert all("CTXSYS" not in statement for statement in executed)
    assert config._oracle_version_cache.resolved is False


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True])
async def test_oracle_memory_companions_skip_disabled_store(is_async: bool) -> None:
    config = _mock_config({"enable_memory": False})
    store = OracleAsyncADKMemoryStore(config) if is_async else OracleSyncADKMemoryStore(config)
    driver = _companion_mock_driver(is_async=is_async, rows=[])

    await _run_companion_hook(store, driver, is_async=is_async)

    assert driver.data_dictionary.get_indexes.call_count == 0
    assert driver.execute.call_count == 0


@pytest.mark.parametrize(
    "storage_type", [JSONStorageType.JSON_NATIVE, JSONStorageType.BLOB_JSON, JSONStorageType.BLOB_PLAIN]
)
@pytest.mark.parametrize("is_async", [False, True])
def test_oracle_companion_specs_match_creation_ddl(storage_type: JSONStorageType, is_async: bool) -> None:
    from sqlspec.adapters.oracledb.adk.store import (
        _oracle_event_index_specs,
        _oracle_memory_index_specs,
        _oracle_missing_index_statements,
        _oracle_session_index_specs,
    )

    config = _mock_config({"memory_use_fts": True})
    session_store = OracleAsyncADKStore(config) if is_async else OracleSyncADKStore(config)
    memory_store = OracleAsyncADKMemoryStore(config) if is_async else OracleSyncADKMemoryStore(config)

    normalized_session_ddl = " ".join(session_store._sessions_table_ddl_for_type(storage_type).split())
    normalized_events_ddl = " ".join(session_store._events_table_ddl_for_type(storage_type).split())
    normalized_memory_ddl = " ".join(memory_store._memory_table_ddl_for_type(storage_type).split())

    for statement in _oracle_missing_index_statements(_oracle_session_index_specs(session_store.session_table), []):
        assert " ".join(statement.split()) in normalized_session_ddl
    for statement in _oracle_missing_index_statements(_oracle_event_index_specs(session_store.events_table), []):
        assert " ".join(statement.split()) in normalized_events_ddl
    for statement in _oracle_missing_index_statements(
        _oracle_memory_index_specs(memory_store.memory_table, use_fts=True), []
    ):
        assert " ".join(statement.split()) in normalized_memory_ddl


@pytest.mark.parametrize(
    ("target_storage_type", "catalog_json_type"),
    [(JSONStorageType.JSON_NATIVE, "BLOB"), (JSONStorageType.BLOB_PLAIN, "JSON")],
)
def test_oracle_reconcile_keeps_existing_json_storage(
    target_storage_type: JSONStorageType, catalog_json_type: str
) -> None:
    from sqlspec.migrations.schema import ensure_schema_sync

    config = _config_for_storage(target_storage_type)
    config.extension_config = {"adk": {}}
    config.statement_config.dialect = "oracle"
    store = OracleSyncADKStore(config)

    catalog_columns_by_table = {
        "adk_session": [
            {"column_name": "ID", "data_type": "VARCHAR2", "nullable": "N"},
            {"column_name": "APP_NAME", "data_type": "VARCHAR2", "nullable": "N"},
            {"column_name": "USER_ID", "data_type": "VARCHAR2", "nullable": "N"},
            {"column_name": "STATE", "data_type": catalog_json_type, "nullable": "N"},
            {"column_name": "CREATE_TIME", "data_type": "TIMESTAMP(6) WITH TIME ZONE", "nullable": "N"},
            {"column_name": "UPDATE_TIME", "data_type": "TIMESTAMP(6) WITH TIME ZONE", "nullable": "N"},
        ],
        "adk_event": [
            {"column_name": "ID", "data_type": "VARCHAR2", "nullable": "N"},
            {"column_name": "APP_NAME", "data_type": "VARCHAR2", "nullable": "N"},
            {"column_name": "USER_ID", "data_type": "VARCHAR2", "nullable": "N"},
            {"column_name": "SESSION_ID", "data_type": "VARCHAR2", "nullable": "N"},
            {"column_name": "INVOCATION_ID", "data_type": "VARCHAR2", "nullable": "Y"},
            {"column_name": "TIMESTAMP", "data_type": "TIMESTAMP(6) WITH TIME ZONE", "nullable": "N"},
            {"column_name": "EVENT_DATA", "data_type": catalog_json_type, "nullable": "N"},
        ],
        "adk_app_state": [
            {"column_name": "APP_NAME", "data_type": "VARCHAR2", "nullable": "N"},
            {"column_name": "STATE", "data_type": catalog_json_type, "nullable": "N"},
            {"column_name": "UPDATE_TIME", "data_type": "TIMESTAMP(6) WITH TIME ZONE", "nullable": "N"},
        ],
        "adk_user_state": [
            {"column_name": "APP_NAME", "data_type": "VARCHAR2", "nullable": "N"},
            {"column_name": "USER_ID", "data_type": "VARCHAR2", "nullable": "N"},
            {"column_name": "STATE", "data_type": catalog_json_type, "nullable": "N"},
            {"column_name": "UPDATE_TIME", "data_type": "TIMESTAMP(6) WITH TIME ZONE", "nullable": "N"},
        ],
        "adk_internal_metadata": [
            {"column_name": "KEY", "data_type": "VARCHAR2", "nullable": "N"},
            {"column_name": "VALUE", "data_type": "VARCHAR2", "nullable": "N"},
        ],
    }

    driver = MagicMock()
    driver.data_dictionary.get_tables.return_value = [
        {"table_name": table_name.upper()} for table_name in catalog_columns_by_table
    ]
    driver.data_dictionary.get_columns.side_effect = lambda _drv, table_name, **_kw: catalog_columns_by_table[
        str(table_name).casefold()
    ]

    targets = store.get_schema_targets_sync(driver)
    result = ensure_schema_sync(driver, targets, manage_schema=True, commit=False, strict=True)

    assert result.created_tables == []
    assert result.added_columns == {}
    assert all("ALTER" not in str(call.args[0]).upper() for call in driver.execute.call_args_list)
    assert driver.execute.call_count == 0


def _oracle_event(
    *, app_name: str = "app", user_id: str = "u1", session_id: str = "s1", event_id: str = "e1"
) -> StoredEvent:
    return {
        "id": event_id,
        "app_name": app_name,
        "user_id": user_id,
        "session_id": session_id,
        "invocation_id": "inv-1",
        "timestamp": datetime(2026, 5, 10, 12, 0, tzinfo=UTC),
        "event_data": {"text": "hello"},
    }


def _oracle_session_store_with_mocks(
    *, is_async: bool, fetchone_values: list[Any] | None = None
) -> tuple[Any, MagicMock, MagicMock]:
    config = _config_for_storage(JSONStorageType.JSON_NATIVE)
    config.extension_config = {"adk": {}}
    cursor = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    if is_async:
        cursor.execute = AsyncMock()
        cursor.fetchone = AsyncMock(side_effect=fetchone_values if fetchone_values is not None else [None])
        cursor.fetchall = AsyncMock(return_value=[])
        conn.commit = AsyncMock()
        conn.rollback = AsyncMock()
        config.provide_connection.return_value.__aenter__.return_value = conn
        return OracleAsyncADKStore(config), conn, cursor
    cursor.fetchone.side_effect = fetchone_values if fetchone_values is not None else [None]
    cursor.fetchall.return_value = []
    config.provide_connection.return_value.__enter__.return_value = conn
    return OracleSyncADKStore(config), conn, cursor


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True])
@pytest.mark.parametrize(
    ("renew_for", "expects_update"),
    [
        pytest.param(None, False, id="none"),
        pytest.param(0, False, id="zero-int"),
        pytest.param(-1, False, id="negative-int"),
        pytest.param(timedelta(0), False, id="zero-timedelta"),
        pytest.param(timedelta(milliseconds=200), True, id="subsecond-timedelta"),
        pytest.param(30, True, id="positive-int"),
    ],
)
async def test_oracle_adk_get_session_renews_only_for_positive_duration(
    is_async: bool, renew_for: int | timedelta | None, expects_update: bool
) -> None:
    """get_session executes the SYSTIMESTAMP touch only when renew_for is strictly positive."""
    now = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    store, _conn, cursor = _oracle_session_store_with_mocks(
        is_async=is_async, fetchone_values=[("s1", "app", "u1", '{"k":1}', now, now)]
    )
    maybe = store.get_session("app", "u1", "s1", renew_for=renew_for)
    result = await maybe if isawaitable(maybe) else maybe

    assert result is not None
    executed_sql = [_normalized(call.args[0]) for call in cursor.execute.call_args_list]
    has_update = any(sql.startswith("UPDATE adk_session SET update_time = SYSTIMESTAMP") for sql in executed_sql)
    assert has_update is expects_update


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True])
@pytest.mark.parametrize("owner_exists", [True, False])
async def test_oracle_adk_append_event_verifies_session_ownership_before_insert(
    is_async: bool, owner_exists: bool
) -> None:
    """append_event queries session ownership first and rolls back without inserting when missing."""
    store, conn, cursor = _oracle_session_store_with_mocks(
        is_async=is_async, fetchone_values=[(1,)] if owner_exists else [None]
    )
    event = _oracle_event()

    if owner_exists:
        maybe = store.append_event(event)
        if isawaitable(maybe):
            await maybe
        executed = [(_normalized(call.args[0]), call.args[1]) for call in cursor.execute.call_args_list]
        assert len(executed) == 2
        assert executed[0] == (
            (
                "SELECT 1 FROM adk_session WHERE id = :id AND app_name = :app_name AND user_id = :user_id "
                "FETCH FIRST 1 ROWS ONLY"
            ),
            {"id": "s1", "app_name": "app", "user_id": "u1"},
        )
        assert executed[1][0].startswith("INSERT INTO adk_event")
        assert conn.commit.call_count == 1
    else:
        with pytest.raises(ValueError, match="Session s1 not found for app='app', user='u1'"):
            maybe = store.append_event(event)
            if isawaitable(maybe):
                await maybe
        assert len(cursor.execute.call_args_list) == 1
        assert conn.commit.call_count == 0
        assert conn.rollback.call_count == 1


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True])
async def test_oracle_adk_append_event_and_update_state_rejects_mismatch_and_scopes_queries(is_async: bool) -> None:
    """Mismatched event scope fails before acquiring a connection; get_events and delete_session scope by owner."""
    store, conn, cursor = _oracle_session_store_with_mocks(is_async=is_async)
    bad_event = _oracle_event(user_id="other-user")

    with pytest.raises(ValueError, match="does not match"):
        maybe = store.append_event_and_update_state(bad_event, "app", "u1", "s1", {"turn": 1})
        if isawaitable(maybe):
            await maybe
    assert cursor.execute.call_count == 0

    maybe_events = store.get_events("app", "u1", "s1")
    events = await maybe_events if isawaitable(maybe_events) else maybe_events
    assert events == []
    events_sql = _normalized(cursor.execute.call_args.args[0])
    assert (
        "JOIN adk_session s ON e.session_id = s.id AND e.app_name = s.app_name AND e.user_id = s.user_id" in events_sql
    )
    assert "WHERE s.app_name = :app_name AND s.user_id = :user_id AND s.id = :session_id" in events_sql

    cursor.execute.reset_mock()
    maybe_del = store.delete_session("app", "u1", "s1")
    if isawaitable(maybe_del):
        await maybe_del
    delete_calls = [_normalized(call.args[0]) for call in cursor.execute.call_args_list]
    assert delete_calls == [
        "DELETE FROM adk_event WHERE app_name = :app_name AND user_id = :user_id AND session_id = :id",
        "DELETE FROM adk_session WHERE app_name = :app_name AND user_id = :user_id AND id = :id",
    ]
    assert conn.commit.call_count == 1


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True])
@pytest.mark.parametrize(
    ("app_name", "user_id", "expected_where", "expected_params"),
    [
        pytest.param(None, None, "session_id = :session_id", {"session_id": "s1"}, id="session-only"),
        pytest.param(
            "app",
            None,
            "session_id = :session_id AND app_name = :app_name",
            {"session_id": "s1", "app_name": "app"},
            id="with-app",
        ),
        pytest.param(
            None,
            "u1",
            "session_id = :session_id AND user_id = :user_id",
            {"session_id": "s1", "user_id": "u1"},
            id="with-user",
        ),
        pytest.param(
            "app",
            "u1",
            "session_id = :session_id AND app_name = :app_name AND user_id = :user_id",
            {"session_id": "s1", "app_name": "app", "user_id": "u1"},
            id="with-app-and-user",
        ),
    ],
)
async def test_oracle_adk_memory_delete_entries_by_session_scopes_owner_predicates(
    is_async: bool, app_name: str | None, user_id: str | None, expected_where: str, expected_params: dict[str, Any]
) -> None:
    """delete_entries_by_session appends :app_name and :user_id predicates when provided."""
    config = _config_for_storage(JSONStorageType.JSON_NATIVE)
    config.extension_config = {"adk": {}}
    cursor = MagicMock()
    cursor.rowcount = 2
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    if is_async:
        cursor.execute = AsyncMock()
        conn.commit = AsyncMock()
        config.provide_connection.return_value.__aenter__.return_value = conn
        store: Any = OracleAsyncADKMemoryStore(config)
        deleted = await store.delete_entries_by_session("s1", app_name=app_name, user_id=user_id)
    else:
        config.provide_connection.return_value.__enter__.return_value = conn
        store = OracleSyncADKMemoryStore(config)
        deleted = store.delete_entries_by_session("s1", app_name=app_name, user_id=user_id)

    assert deleted == 2
    sql, params = cursor.execute.call_args.args
    assert sql == f"DELETE FROM adk_memory WHERE {expected_where}"
    assert params == expected_params
