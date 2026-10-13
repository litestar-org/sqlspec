"""Unit tests for Spanner core performance helpers."""

import base64
from contextlib import nullcontext
from datetime import UTC, date, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from google.api_core import exceptions as api_exceptions
from google.cloud.spanner_v1 import param_types
from google.cloud.spanner_v1.data_types import JsonObject
from google.cloud.spanner_v1.types.type import TypeCode

from sqlspec.adapters.spanner._typing import SpannerSyncSessionContext
from sqlspec.adapters.spanner.adk.artifact_store import SpannerAsyncADKArtifactStore, SpannerSyncADKArtifactStore
from sqlspec.adapters.spanner.adk.store import SpannerSyncADKMemoryStore, SpannerSyncADKStore
from sqlspec.adapters.spanner.config import SpannerSyncConfig, SpannerSyncConnectionContext
from sqlspec.adapters.spanner.core import (
    build_param_type_signature,
    build_statement_config,
    chunk_mutation_rows,
    coerce_params,
    collect_rows,
    create_mapped_exception,
    execute_partitioned_dml_sync,
    infer_param_types,
    is_query_statement,
    resolve_column_names,
    resolve_row_plan,
    resolve_transaction_completion,
)
from sqlspec.adapters.spanner.data_dictionary import SpannerSyncDataDictionary
from sqlspec.adapters.spanner.driver import SpannerSyncDriver
from sqlspec.adapters.spanner.litestar.store import SpannerSyncStore
from sqlspec.adapters.spanner.type_converter import spanner_json, spanner_to_uuid
from sqlspec.core import SQL, TypedParameter
from sqlspec.exceptions import (
    DeadlockError,
    ImproperConfigurationError,
    OperationalError,
    PermissionDeniedError,
    QueryTimeoutError,
    UniqueViolationError,
)
from sqlspec.utils.serializers import from_json


def _field(name: str, code: int) -> SimpleNamespace:
    return SimpleNamespace(name=name, type_=SimpleNamespace(code=code))


def _json_object(value: str) -> object:
    return cast("Any", JsonObject).from_str(value)


def test_build_param_type_signature_empty_parameters() -> None:
    assert build_param_type_signature(None) == ()
    assert build_param_type_signature({}) == ()


def test_build_param_type_signature_tracks_key_type_pairs() -> None:
    signature = build_param_type_signature({"id": 1, "name": "alice"})

    assert signature == (("id", int, None), ("name", str, None))


def test_build_param_type_signature_separates_differently_typed_nulls() -> None:
    """Every NULL has the same runtime type, so the declared type must be part of the key."""
    as_text = build_param_type_signature({"value": TypedParameter(None, str)})
    as_number = build_param_type_signature({"value": TypedParameter(None, int)})

    assert as_text != as_number


def test_resolve_row_plan_returns_empty_plan_without_metadata() -> None:
    cache: dict[int, tuple[object, list[str], tuple[tuple[int, object], ...] | None]] = {}

    column_names, column_plan = resolve_row_plan(None, cache, json_deserializer=lambda value: value)

    assert column_names == []
    assert column_plan is None
    assert cache == {}


def test_resolve_row_plan_returns_empty_plan_for_non_json_columns() -> None:
    fields = [_field("id", TypeCode.INT64), _field("name", TypeCode.STRING)]
    cache: dict[int, tuple[object, list[str], tuple[tuple[int, object], ...] | None]] = {}

    column_names, column_plan = resolve_row_plan(fields, cache, json_deserializer=lambda value: value)

    assert column_names == ["id", "name"]
    assert column_plan is None
    assert len(cache) == 1


def test_resolve_row_plan_caches_json_metadata_and_plan() -> None:
    fields = [_field("id", TypeCode.INT64), _field("payload", TypeCode.JSON)]
    cache: dict[int, tuple[object, list[str], tuple[tuple[int, object], ...] | None]] = {}

    first_column_names, first_column_plan = resolve_row_plan(fields, cache, json_deserializer=lambda value: value)
    second_column_names, second_column_plan = resolve_row_plan(fields, cache, json_deserializer=lambda value: value)

    assert first_column_names == ["id", "payload"]
    assert first_column_names is second_column_names
    assert first_column_plan is not None
    assert first_column_plan is second_column_plan
    assert first_column_plan[0][0] == 1


def test_driver_row_plan_uses_changed_json_deserializer_with_reused_metadata() -> None:
    fields = [_field("payload", TypeCode.JSON)]
    rows = [('{"value":1}',)]
    driver = SpannerSyncDriver(
        cast("Any", object()), driver_features={"json_deserializer": lambda value: {"first": value}}
    )

    names, first_plan = driver._resolve_row_plan(fields)
    first_rows, _ = collect_rows(rows, fields, column_names=names, column_plan=first_plan)
    assert first_rows == [({"first": '{"value":1}'},)]

    driver.driver_features["json_deserializer"] = lambda value: {"second": value}
    names, second_plan = driver._resolve_row_plan(fields)
    second_rows, _ = collect_rows(rows, fields, column_names=names, column_plan=second_plan)
    assert second_rows == [({"second": '{"value":1}'},)]
    assert second_plan is not first_plan
    assert driver._resolve_row_plan(fields)[1] is second_plan


def test_collect_rows_returns_original_rows_without_a_plan() -> None:
    fields = [_field("id", TypeCode.INT64), _field("payload", TypeCode.STRING)]
    rows = [(1, '{"kind":"string"}')]

    data, column_names = collect_rows(rows, fields, column_names=["id", "payload"], column_plan=None)

    assert data is rows
    assert column_names == ["id", "payload"]


def test_collect_rows_normalizes_list_rows_without_a_plan() -> None:
    fields = [_field("id", TypeCode.INT64), _field("payload", TypeCode.STRING)]
    rows: list[Any] = [[1, '{"kind":"string"}']]

    data, column_names = collect_rows(rows, fields, column_names=["id", "payload"], column_plan=None)

    assert data == [(1, '{"kind":"string"}')]
    assert data is not rows
    assert column_names == ["id", "payload"]


def test_collect_rows_applies_json_plan_to_metadata_only() -> None:
    fields = [_field("id", TypeCode.INT64), _field("payload", TypeCode.JSON), _field("note", TypeCode.STRING)]
    rows = [(1, _json_object('{"kind":"payload"}'), '{"kind":"string"}')]
    json_calls: list[str] = []

    def json_deserializer(value: str) -> dict[str, str]:
        json_calls.append(value)
        return {"decoded": value}

    column_names, column_plan = resolve_row_plan(fields, {}, json_deserializer=json_deserializer)
    data, resolved_column_names = collect_rows(rows, fields, column_names=column_names, column_plan=column_plan)

    assert resolved_column_names == ["id", "payload", "note"]
    assert data == [(1, {"decoded": '{"kind":"payload"}'}, '{"kind":"string"}')]
    assert json_calls == ['{"kind":"payload"}']


def test_resolve_column_names_reuses_cached_fields() -> None:
    fields = [SimpleNamespace(name="id"), SimpleNamespace(name="name")]
    cache: dict[int, tuple[object, list[str]]] = {}

    first = resolve_column_names(fields, cache)
    second = resolve_column_names(fields, cache)

    assert first == ["id", "name"]
    assert second is first
    assert len(cache) == 1


def test_convert_json_row_value_zero_copy_unwrapping_with_default_deserializer(monkeypatch: Any) -> None:
    """Verify JsonObject is unwrapped directly without calling serialize() when using default from_json."""
    fields = [
        _field("obj", TypeCode.JSON),
        _field("arr", TypeCode.JSON),
        _field("scalar", TypeCode.JSON),
        _field("null_val", TypeCode.JSON),
    ]
    json_cls = cast("Any", JsonObject)
    json_obj = json_cls({"a": 1})
    json_arr = json_cls([1, 2, 3])
    json_scalar = json_cls("hello")
    json_null = json_cls(None)

    def fail_serialize(self: Any) -> str:
        msg = "serialize() should not be called when json_deserializer is from_json"
        raise AssertionError(msg)

    monkeypatch.setattr(JsonObject, "serialize", fail_serialize)

    column_names, column_plan = resolve_row_plan(fields, {}, json_deserializer=from_json)
    rows = [(json_obj, json_arr, json_scalar, json_null)]
    data, _ = collect_rows(rows, fields, column_names=column_names, column_plan=column_plan)

    assert data == [({"a": 1}, [1, 2, 3], "hello", None)]
    assert type(data[0][0]) is dict
    assert type(data[0][1]) is list


@pytest.mark.parametrize(
    ("state", "failed", "expected"),
    [
        ({"_transaction_id": b"txn"}, False, "commit"),
        ({"_mutations": [object()]}, False, "commit"),
        ({}, False, "skip"),
        ({"_transaction_id": b"txn"}, True, "rollback"),
        ({"_mutations": [object()]}, True, "rollback"),
        ({}, True, "rollback"),
        ({"_transaction_id": b"txn", "rolled_back": True}, False, "skip"),
        ({"_transaction_id": b"txn", "rolled_back": True}, True, "skip"),
        ({"_transaction_id": b"txn", "committed": object()}, False, "skip"),
        ({"_transaction_id": b"txn", "committed": object()}, True, "skip"),
    ],
    ids=[
        "begun-commits",
        "mutations-commit",
        "empty-skips",
        "failure-rolls-back",
        "failure-discards-buffered-mutations",
        "failure-before-begin-rolls-back",
        "rolled-back-skips-commit",
        "rolled-back-skips-rollback",
        "committed-skips-commit",
        "committed-skips-rollback",
    ],
)
def test_resolve_transaction_completion(state: dict[str, Any], failed: bool, expected: str) -> None:
    transaction = SimpleNamespace(**{
        "_transaction_id": None,
        "_mutations": [],
        "committed": None,
        "rolled_back": False,
        **state,
    })
    assert resolve_transaction_completion(transaction, failed=failed) == expected


@pytest.mark.parametrize(
    ("sql", "expected"),
    [("SELECT 1", True), ("WITH t AS (SELECT 1) SELECT * FROM t", True), ("UPDATE t SET a = 1 WHERE TRUE", False)],
)
def test_is_query_statement(sql: str, expected: bool) -> None:
    assert is_query_statement(sql, "spanner") is expected


def test_then_return_statement_returns_rows() -> None:
    """Verify ``THEN RETURN`` DML is classified as returning rows."""
    statement = SQL(
        "INSERT INTO t (id) VALUES (@id) THEN RETURN id", {"id": 1}, statement_config=build_statement_config()
    )
    statement.compile()

    assert statement.returns_rows()


def test_exception_mapping_permission_denied_unauthenticated_and_timeout() -> None:
    """Verify 403/401 -> PermissionDeniedError, 504 -> QueryTimeoutError, 503/429 -> OperationalError."""
    forbidden = cast("Any", api_exceptions.PermissionDenied)("access denied")
    unauthorized = cast("Any", api_exceptions.Unauthenticated)("invalid token")
    deadline = cast("Any", api_exceptions.DeadlineExceeded)("timed out")
    unavailable = cast("Any", api_exceptions.ServiceUnavailable)("unavailable")
    aborted = cast("Any", api_exceptions.Aborted)("aborted")
    too_many = cast("Any", api_exceptions.TooManyRequests)("quota")
    already_exists = cast("Any", api_exceptions.AlreadyExists)("exists")

    assert isinstance(create_mapped_exception(forbidden), PermissionDeniedError)
    assert isinstance(create_mapped_exception(unauthorized), PermissionDeniedError)
    assert isinstance(create_mapped_exception(deadline), QueryTimeoutError)
    assert isinstance(create_mapped_exception(unavailable), OperationalError)
    assert isinstance(create_mapped_exception(aborted), DeadlockError)
    assert isinstance(create_mapped_exception(too_many), OperationalError)
    assert isinstance(create_mapped_exception(already_exists), UniqueViolationError)


def test_connection_context_maps_implicit_commit_and_suppresses_rollback_errors() -> None:
    """Verify SpannerSyncConnectionContext maps commit GoogleAPICallError and suppresses rollback errors."""

    class _AbortingTxn:
        _transaction_id = b"tx1"
        _mutations: list[Any] = []
        committed = None
        rolled_back = False

        def __enter__(self) -> Any:
            return self

        def begin(self) -> None:
            return None

        def commit(self) -> None:
            raise cast("Any", api_exceptions.Aborted)("aborted on commit")

        def rollback(self) -> None:
            raise cast("Any", api_exceptions.ServiceUnavailable)("rollback failed")

    session_obj = SimpleNamespace(transaction=_AbortingTxn)
    sessions_manager = MagicMock()
    sessions_manager.get_session.return_value = session_obj
    database = SimpleNamespace(sessions_manager=sessions_manager)

    config = MagicMock()
    config.get_database.return_value = database
    config.driver_features = {}

    ctx = SpannerSyncConnectionContext(config, transaction=True)
    with pytest.raises(DeadlockError), ctx:
        pass

    ctx_rollback = SpannerSyncConnectionContext(config, transaction=True)
    with pytest.raises(RuntimeError, match="user boom"), ctx_rollback:
        raise RuntimeError("user boom")


def test_execute_script_passes_configured_timeout_to_ddl() -> None:
    """Verify dispatch_execute_script forwards configured timeout to execute_ddl_sync."""
    ddl_timeouts: list[Any] = []

    class _Operation:
        def result(self, timeout: Any = None) -> None:
            ddl_timeouts.append(timeout)

    class _Database:
        def update_ddl(self, _statements: list[str]) -> _Operation:
            return _Operation()

    db = _Database()

    class _TxConn:
        _session = SimpleNamespace(_database=db)

        def execute_update(self, *_args: Any, **_kwargs: Any) -> int:
            return 1

    driver = SpannerSyncDriver(cast("Any", _TxConn()), driver_features={"timeout": 42.0})
    script = SQL("CREATE TABLE t1 (id INT64) PRIMARY KEY (id); CREATE TABLE t2 (id INT64) PRIMARY KEY (id);")
    result = driver.dispatch_execute_script(driver.connection, script)

    assert result.statement_count == 2
    assert ddl_timeouts == [42.0]


def test_driver_mutation_helpers_and_partitioned_dml() -> None:
    """Verify write_insert/write_upsert/write_update/write_delete and execute_partitioned_dml."""
    mutations: list[tuple[str, str, list[str], Any]] = []
    pdml_calls: list[tuple[str, Any, Any]] = []

    class _Database:
        def execute_partitioned_dml(
            self, sql: str, *, params: Any = None, param_types: Any = None, **_kwargs: Any
        ) -> int:
            pdml_calls.append((sql, params, param_types))
            return 17

    db = _Database()

    class _TxConn:
        _session = SimpleNamespace(_database=db)

        def execute_update(self, *_args: Any, **_kwargs: Any) -> int:
            return 1

        def insert(self, table: str, columns: list[str], values: Any) -> None:
            mutations.append(("insert", table, columns, values))

        def insert_or_update(self, table: str, columns: list[str], values: Any) -> None:
            mutations.append(("upsert", table, columns, values))

        def update(self, table: str, columns: list[str], values: Any) -> None:
            mutations.append(("update", table, columns, values))

        def delete(self, table: str, keyset: Any) -> None:
            mutations.append(("delete", table, [], keyset))

    driver = SpannerSyncDriver(cast("Any", _TxConn()))
    driver.write_insert("users", ["id", "name"], [(1, "a")])
    driver.write_upsert("users", ["id", "name"], [(1, "b")])
    driver.write_update("users", ["id", "name"], [(1, "c")])
    driver.write_delete("users", "all_keys")
    affected = driver.execute_partitioned_dml("DELETE FROM users WHERE active = @active", {"active": False})

    assert [m[0] for m in mutations] == ["insert", "upsert", "update", "delete"]
    assert affected == 17
    assert pdml_calls[0][1] == {"active": False}
    assert pdml_calls[0][2] == {"active": param_types.BOOL}

    direct_affected = execute_partitioned_dml_sync(db, "DELETE FROM users WHERE TRUE")
    assert direct_affected == 17


def test_type_inference_and_coercion_cover_sequences_decimals_dates_uuids_and_typed_nulls() -> None:
    """Verify non-list sequences, typed None, Decimal/date/UUID array elements, and custom json_serializer."""
    uid = UUID("12345678-1234-5678-1234-567812345678")
    params = {
        "tuple_ints": (1, 2, 3),
        "decimals": [Decimal("1.25")],
        "dates": [date(2026, 5, 10)],
        "uuids": [uid],
        "leading_none": [None, 7],
        "null_int": TypedParameter(None, int),
    }
    coerced = coerce_params(params)
    inferred = infer_param_types(params)

    assert coerced is not None
    assert coerced["tuple_ints"] == [1, 2, 3]
    assert coerced["uuids"] == [str(uid)]
    assert coerced["null_int"] is None
    array_type = cast("Any", param_types.Array)
    assert inferred["tuple_ints"] == array_type(param_types.INT64)
    assert inferred["decimals"] == array_type(param_types.NUMERIC)
    assert inferred["dates"] == array_type(param_types.DATE)
    assert inferred["uuids"] == array_type(param_types.STRING)
    assert inferred["leading_none"] == array_type(param_types.INT64)
    assert inferred["null_int"] == param_types.INT64

    custom_calls: list[Any] = []

    def _custom_json(val: Any) -> str:
        custom_calls.append(val)
        return '{"x":2}'

    wrapped = spanner_json({"x": 1}, json_serializer=_custom_json)
    assert dict(wrapped) == {"x": 2}
    assert custom_calls == [{"x": 1}]
    assert spanner_to_uuid(uid.bytes) == uid


def test_chunk_mutation_rows_respects_mutation_limit() -> None:
    """Verify chunk_mutation_rows splits rows when cells exceed Spanner's 80,000 mutation limit."""
    columns = [f"c{i}" for i in range(10)]
    rows = [tuple(range(10)) for _ in range(8001)]
    chunks = chunk_mutation_rows(columns, rows, coerce_row=lambda d: d)
    assert [len(c) for c in chunks] == [8000, 1]


def test_litestar_store_uses_read_session_and_maps_transaction_errors() -> None:
    """Verify SpannerSyncStore uses provide_read_session, sharded exists, and maps write errors."""
    read_calls = 0
    now = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    encoded_val = base64.b64encode(b"val").decode("ascii")

    class _ReadDriver:
        def select_one_or_none(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
            return {"data": encoded_val, "expires_at": now}

    config = MagicMock()
    config.extension_config = {"litestar": {"shard_count": 4}}

    def provide_read_session() -> Any:
        nonlocal read_calls
        read_calls += 1
        return nullcontext(_ReadDriver())

    config.provide_read_session = provide_read_session
    database = MagicMock()
    database.run_in_transaction.side_effect = cast("Any", api_exceptions.PermissionDenied)("denied")
    config.get_database.return_value = database

    store = SpannerSyncStore(config)
    assert "shard_id = MOD(FARM_FINGERPRINT(@session_id), 4)" in store._build_exists_sql()
    assert store._get("k1") == b"val"
    assert store._exists("k1") is True
    assert read_calls == 2

    with pytest.raises(PermissionDeniedError):
        store._delete("k1")


def test_adk_store_honors_custom_json_serializers_and_maps_write_errors() -> None:
    """Verify ADK stores scope deletes, avoid tautologies, and map errors."""
    serialize_calls: list[Any] = []
    deserialize_calls: list[str] = []

    def custom_to_json(val: Any) -> str:
        serialize_calls.append(val)
        return '{"custom":true}'

    def custom_from_json(val: str) -> Any:
        deserialize_calls.append(val)
        return {"decoded": val}

    config = MagicMock()
    config.extension_config = {"adk": {"shard_count": 4}}
    config.driver_features = {"json_serializer": custom_to_json, "json_deserializer": custom_from_json}
    database = MagicMock()
    database.run_in_transaction.side_effect = cast("Any", api_exceptions.PermissionDenied)("denied")
    config.get_database.return_value = database

    store = SpannerSyncADKStore(config)
    assert store._json_serializer is custom_to_json
    assert store._json_deserializer is custom_from_json

    delete_stmts = store._build_delete_session_statements("app", "user", "s1")
    assert "app_name = @app_name AND user_id = @user_id" in delete_stmts[0][0]

    query_result = store._build_list_sessions_query("app", "user")
    assert query_result is not None
    list_sql, _, _ = query_result
    assert "FARM_FINGERPRINT(id)" not in list_sql

    with pytest.raises(PermissionDeniedError):
        store._run_write([("DELETE FROM adk_session WHERE TRUE", {}, {})])


def test_adk_memory_store_uses_snapshot_reads_and_vector_index_ddl() -> None:
    """Verify SpannerSyncADKMemoryStore uses snapshot read and vector index DDL."""
    config = MagicMock()
    config.extension_config = {"adk": {"vector_dimensions": 128, "vector_index_enabled": True}}
    config.driver_features = {}
    config.provide_connection.return_value = nullcontext(SimpleNamespace(execute_sql=lambda *_a, **_kw: []))
    store = SpannerSyncADKMemoryStore(config)

    assert store.search_entries("hello", "app", "user") == []
    config.provide_connection.assert_called_once_with(transaction=False)

    ddl_stmts = store._build_memory_table_ddl()
    assert any("WHERE embedding IS NOT NULL" in s for s in ddl_stmts)


def test_adk_artifact_store_maps_database_errors() -> None:
    """Verify SpannerSyncADKArtifactStore uses snapshot read and maps GoogleAPICallError."""
    config = MagicMock()
    config.extension_config = {"adk": {}}
    config.driver_features = {}
    config.provide_connection.side_effect = cast("Any", api_exceptions.PermissionDenied)("denied")

    store = SpannerSyncADKArtifactStore(config)
    assert isinstance(store, SpannerSyncADKArtifactStore)
    with pytest.raises(PermissionDeniedError):
        store.list_artifact_keys("app", "user")
    config.provide_connection.assert_called_once_with(transaction=False)

    async_store = SpannerAsyncADKArtifactStore(config)
    assert isinstance(async_store, SpannerAsyncADKArtifactStore)


def test_spanner_data_dictionary_passes_dialect_to_queries() -> None:
    """Verify SpannerSyncDataDictionary passes dialect to get_query."""
    dd = SpannerSyncDataDictionary()
    captured: list[str] = []

    class _Driver:
        def select(self, query: Any, **_kwargs: Any) -> list[Any]:
            captured.append(str(query))
            return []

    assert dd.get_tables(cast("Any", _Driver())) == []
    assert captured


def test_spanner_session_context_passes_active_driver_connection_on_exit() -> None:
    """Verify SpannerSyncSessionContext releases the active driver connection on exit."""
    released_conns: list[tuple[Any, Any]] = []
    initial_conn = SimpleNamespace(name="initial")
    renewed_conn = SimpleNamespace(name="renewed")

    context = SpannerSyncSessionContext(
        acquire_connection=lambda: initial_conn,
        release_connection=lambda conn, **kwargs: released_conns.append((conn, kwargs.get("exc_type"))),
        statement_config=SpannerSyncDriver(cast("Any", SimpleNamespace())).statement_config,
        driver_features={},
        prepare_driver=lambda drv: drv,
    )

    with pytest.raises(RuntimeError, match="boom"), context as drv:
        drv.connection = cast("Any", renewed_conn)
        raise RuntimeError("boom")

    assert released_conns == [(renewed_conn, RuntimeError)]


def test_spanner_config_rejects_min_sessions_and_closes_client_on_close_pool() -> None:
    """Verify SpannerSyncConfig rejects min_sessions and closes client on close_pool."""
    with pytest.raises(ImproperConfigurationError, match="min_sessions"):
        SpannerSyncConfig(connection_config={"project": "p", "instance_id": "i", "database_id": "d", "min_sessions": 2})

    closed: list[str] = []
    fake_client = SimpleNamespace(close=lambda: closed.append("client"))
    config = SpannerSyncConfig(
        connection_config={"project": "p", "instance_id": "i", "database_id": "d", "max_sessions": 2}
    )
    config._client = cast("Any", fake_client)
    config.connection_instance = cast("Any", SimpleNamespace(clear=lambda: closed.append("pool")))
    assert callable(config.provide_read_session)
    assert callable(config.provide_write_session)
    config.close_pool()

    assert closed == ["pool", "client"]
    assert config._client is None
