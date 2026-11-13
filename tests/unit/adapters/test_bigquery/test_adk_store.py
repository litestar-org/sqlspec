# pyright: reportPrivateUsage=false
"""Unit tests for BigQuery ADK store behavior."""

import inspect
from contextlib import nullcontext
from datetime import UTC, datetime
from typing import Any, NotRequired, cast, get_args, get_origin
from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.bigquery import BigQueryConfig
from sqlspec.adapters.bigquery.adk import BigQueryADKConfig, BigQueryADKRetentionConfig, BigQueryADKStore
from sqlspec.adapters.bigquery.litestar import BigQueryStore
from sqlspec.config import ADKConfig, ExtensionConfigs
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.adk import BaseSyncADKStore


def _make_store(
    adk_config: dict[str, object] | None = None, driver_features: dict[str, Any] | None = None
) -> BigQueryADKStore:
    adk_settings: dict[str, Any] = {"enable_memory": False}
    if adk_config:
        adk_settings.update(adk_config)
    extension_config: ExtensionConfigs = {"adk": adk_settings}
    config = BigQueryConfig(
        connection_config={"project": "test-project", "dataset_id": "test_dataset"},
        driver_features=driver_features,
        extension_config=extension_config,
    )
    return BigQueryADKStore(config)


def test_bigquery_adk_config_types_adapter_local_settings() -> None:
    """BigQuery ADK settings live on an adapter-local extension config."""

    assert cast("Any", ADKConfig).__optional_keys__ <= cast("Any", BigQueryADKConfig).__optional_keys__

    expected_types: dict[str, object] = {
        "session_lookup_window_days": int,
        "require_partition_filter": bool,
        "retention": BigQueryADKRetentionConfig,
    }
    for feature_name, expected_type in expected_types.items():
        annotation = cast("Any", BigQueryADKConfig.__annotations__[feature_name])
        assert get_origin(annotation) is NotRequired
        assert get_args(annotation) == (expected_type,)

    annotation = cast("Any", BigQueryADKRetentionConfig.__annotations__["event_ttl_seconds"])
    assert get_origin(annotation) is NotRequired
    assert get_args(annotation) == (int,)


def test_bigquery_adk_store_is_sync_contract() -> None:
    """BigQuery uses SQLSpec's synchronous ADK store boundary."""

    assert issubclass(BigQueryADKStore, BaseSyncADKStore)
    assert not inspect.iscoroutinefunction(BigQueryADKStore.create_tables)
    assert not getattr(BigQueryADKStore, "__abstractmethods__", set())


def test_bigquery_adk_store_instantiates_with_current_defaults() -> None:
    """Defaults expose the analytics-replica posture and current table names."""

    store = _make_store()

    assert store.session_table == "adk_session"
    assert store.events_table == "adk_event"
    assert store.app_state_table == "adk_app_state"
    assert store.user_state_table == "adk_user_state"
    assert store.metadata_table == "adk_internal_metadata"
    assert store._dataset_qualifier == "test_dataset."
    assert store._lookup_window_days == 30
    assert store._require_partition_filter is False
    assert store._partition_expiration_days is None


def test_bigquery_adk_store_reads_flat_extension_config_not_driver_features() -> None:
    """BigQuery ADK knobs are read from extension_config["adk"]."""

    store = _make_store(
        {"session_lookup_window_days": 7, "require_partition_filter": True},
        driver_features={"session_lookup_window_days": 2, "require_partition_filter": False},
    )

    assert store._lookup_window_days == 7
    assert store._require_partition_filter is True


def test_bigquery_adk_store_rejects_legacy_nested_bigquery_config() -> None:
    """Legacy nested ADK BigQuery blocks are rejected instead of silently ignored."""
    with pytest.raises(ImproperConfigurationError, match="bigquery"):
        _make_store({"bigquery": {"session_lookup_window_days": 7, "require_partition_filter": True}})


def test_bigquery_adk_store_derives_event_partition_expiration_from_retention() -> None:
    """Event TTL in seconds becomes BigQuery event partition expiration days."""

    store = _make_store({"retention": {"event_ttl_seconds": 86_401}})

    assert store._partition_expiration_days == 2


def test_bigquery_adk_session_ddl_is_partitioned_and_clustered_without_filter_by_default() -> None:
    """Sessions table DDL has DATE partitioning and no required filter by default."""

    store = _make_store()

    ddl = store._sessions_table_ddl()

    assert "PARTITION BY DATE(create_time)" in ddl
    assert "CLUSTER BY app_name, user_id, id" in ddl
    assert "test_dataset.adk_session" in ddl
    assert "require_partition_filter = TRUE" not in ddl


def test_bigquery_adk_event_ddl_clusters_and_carries_event_ttl_only() -> None:
    """Event TTL belongs on adk_event, not adk_session."""

    store = _make_store({"retention": {"event_ttl_seconds": 86400 * 30}})

    session_ddl = store._sessions_table_ddl()
    event_ddl = store._events_table_ddl()

    assert "PARTITION BY DATE(timestamp)" in event_ddl
    assert "CLUSTER BY app_name, user_id, session_id" in event_ddl
    assert "partition_expiration_days" not in session_ddl
    assert "partition_expiration_days = 30" in event_ddl


def test_bigquery_adk_explicit_partition_filter_adds_partition_predicates(monkeypatch: Any) -> None:
    """Opt-in partition-filter mode adds broad predicates to partitioned table access."""

    store = _make_store({"require_partition_filter": True})
    statements: list[str] = []

    def capture(_store: BigQueryADKStore, sql: str, parameters: Any = None) -> list[dict[str, Any]]:
        statements.append(sql)
        return []

    _capture_client(monkeypatch, store, capture)

    store.get_session("app", "user", "session")
    store._update_session_touch("app", "user", "session")
    store.update_session_state("app", "user", "session", {"turn": 1})
    store.delete_session("app", "user", "session")
    store.get_events("app", "user", "session")
    store.delete_expired_events(datetime.now(UTC))
    store.delete_idle_sessions(datetime.now(UTC))

    assert any("FROM test_dataset.adk_session" in sql and "create_time IS NOT NULL" in sql for sql in statements)
    assert any("UPDATE test_dataset.adk_session" in sql and "create_time IS NOT NULL" in sql for sql in statements)
    assert any("DELETE FROM test_dataset.adk_session" in sql and "create_time IS NOT NULL" in sql for sql in statements)
    assert any("FROM test_dataset.adk_event" in sql and "timestamp IS NOT NULL" in sql for sql in statements)
    assert any("DELETE FROM test_dataset.adk_event" in sql and "timestamp IS NOT NULL" in sql for sql in statements)


def test_bigquery_adk_get_events_reads_full_event_blob_without_json_value(monkeypatch: Any) -> None:
    """Event reads preserve nested event_data instead of JSON_VALUE scalar projections."""

    store = _make_store()
    timestamp = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    statements: list[str] = []

    def capture(_store: BigQueryADKStore, sql: str, parameters: Any = None) -> list[dict[str, Any]]:
        statements.append(sql)
        return [
            {
                "id": "event-1",
                "session_id": "session",
                "invocation_id": "inv-1",
                "timestamp": timestamp,
                "event_data": '{"content":{"parts":[{"text":"hello"}]},"actions":{"state_delta":{"x":1}}}',
                "app_name": "app",
                "user_id": "user",
            }
        ]

    _capture_client(monkeypatch, store, capture)

    events = store.get_events("app", "user", "session")

    assert len(events) == 1
    assert events[0]["event_data"] == {"content": {"parts": [{"text": "hello"}]}, "actions": {"state_delta": {"x": 1}}}
    assert "JSON_VALUE" not in statements[0]
    assert "e.event_data" in statements[0]


def test_bigquery_adk_decodes_json_bytes_as_utf8_not_latin1() -> None:
    """BigQuery JSON bytes are UTF-8 JSON, including non-ASCII payloads."""

    payload = '{"word":"café"}'.encode()

    assert BigQueryADKStore._decode_json(payload) == {"word": "café"}


def test_bigquery_adk_create_session_includes_owner_column_when_configured(monkeypatch: Any) -> None:
    """Owner columns from base ADK config are preserved by the BigQuery port."""

    store = _make_store({"owner_id_column": "tenant_id STRING"})
    statements: list[tuple[str, list[Any] | None]] = []

    def capture(_store: BigQueryADKStore, sql: str, parameters: list[Any] | None = None) -> list[dict[str, Any]]:
        statements.append((sql, parameters))
        return []

    _capture_client(monkeypatch, store, capture)

    store.create_session("session", "app", "user", {}, owner_id="tenant-1")

    sql, params = statements[0]
    assert "tenant_id" in sql
    assert params is not None
    assert any(
        getattr(param, "name", "") == "owner_id" and getattr(param, "value", None) == "tenant-1" for param in params
    )


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


def _capture_session_list(monkeypatch: Any, store: BigQueryADKStore) -> "list[tuple[str, list[Any]]]":
    calls: list[tuple[str, list[Any]]] = []

    def capture(_store: BigQueryADKStore, sql: str, parameters: Any = None) -> "list[dict[str, Any]]":
        calls.append((sql, list(parameters or [])))
        return []

    _capture_client(monkeypatch, store, capture)
    return calls


def test_bigquery_list_sessions_binds_order_and_page(monkeypatch: Any) -> None:
    """Explicit ordering renders inline while page bounds bind as named INT64 parameters."""
    store = _make_store()
    calls = _capture_session_list(monkeypatch, store)

    store.list_sessions("app", "u1", order_by="create_time", descending=False, limit=10, offset=20)

    sql, parameters = calls[0]
    assert _normalized(sql).endswith("ORDER BY create_time ASC, id ASC LIMIT @limit OFFSET @offset")
    assert [(p.name, p.type_, p.value) for p in parameters[-2:]] == [("limit", "INT64", 10), ("offset", "INT64", 20)]


def test_bigquery_list_sessions_defaults_to_recent_first_without_a_page(monkeypatch: Any) -> None:
    """The default listing keeps recent-first ordering and binds no page values."""
    store = _make_store()
    calls = _capture_session_list(monkeypatch, store)

    store.list_sessions("app")

    sql, parameters = calls[0]
    assert _normalized(sql).endswith("ORDER BY update_time DESC, id DESC")
    assert [p.name for p in parameters] == ["app_name", "window_start"]


def test_bigquery_list_sessions_composes_user_filter_with_a_page(monkeypatch: Any) -> None:
    """User filtering precedes the bound page values."""
    store = _make_store()
    calls = _capture_session_list(monkeypatch, store)

    store.list_sessions("app", "u1", limit=5)

    sql, parameters = calls[0]
    assert "AND user_id = @user_id" in sql
    assert [p.name for p in parameters] == ["app_name", "window_start", "user_id", "limit", "offset"]
    assert [p.value for p in parameters[-2:]] == [5, 0]


def test_bigquery_list_sessions_zero_limit_never_queries(monkeypatch: Any) -> None:
    """A zero limit short-circuits before any query is issued."""
    store = _make_store()
    calls = _capture_session_list(monkeypatch, store)

    assert store.list_sessions("app", limit=0) == []
    assert calls == []


@pytest.mark.parametrize(
    "options",
    [
        pytest.param({"order_by": "id"}, id="unknown-order-column"),
        pytest.param({"limit": -1}, id="negative-limit"),
        pytest.param({"limit": True}, id="boolean-limit"),
        pytest.param({"offset": 5}, id="unbounded-offset"),
    ],
)
def test_bigquery_list_sessions_rejects_invalid_options(monkeypatch: Any, options: "dict[str, Any]") -> None:
    """Invalid ordering or paging fails before a query is issued."""
    store = _make_store()
    calls = _capture_session_list(monkeypatch, store)

    with pytest.raises(ValueError):
        store.list_sessions("app", **options)

    assert calls == []


def test_bigquery_litestar_store_includes_partition_filter_when_required() -> None:
    """BigQueryStore appends the expires_at partition predicate when require_partition_filter is enabled."""
    executed: list[str] = []
    selected: list[str] = []
    now = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)

    class _FakeDriver:
        def select_one(self, sql: str, **_kwargs: Any) -> dict[str, Any]:
            selected.append(sql)
            return {"data": b"payload", "expires_at": now}

        def execute(self, sql: str, **_kwargs: Any) -> None:
            executed.append(sql)

    config = BigQueryConfig(
        connection_config={"project": "proj", "dataset_id": "ds"},
        extension_config={"litestar": {"require_partition_filter": True}},
    )
    config.provide_session = lambda *_args, **_kwargs: nullcontext(_FakeDriver())  # type: ignore[method-assign]
    store = BigQueryStore(config)

    store._get("s1", renew_for=60)
    store._set("s1", b"val", expires_in=60)
    store._delete("s1")
    store._delete_all()
    store._expires_in("s1")

    predicate = "expires_at IS NULL OR expires_at >= TIMESTAMP('1970-01-01 00:00:00+00')"
    target_predicate = "target.expires_at IS NULL OR target.expires_at >= TIMESTAMP('1970-01-01 00:00:00+00')"
    assert predicate in executed[0]
    assert target_predicate in executed[1]
    assert predicate in executed[2]
    assert predicate in executed[3]
    assert predicate in selected[1]


def test_bigquery_litestar_store_omits_partition_filter_by_default() -> None:
    """BigQueryStore omits the synthetic partition predicate when require_partition_filter is disabled."""
    executed: list[str] = []
    selected: list[str] = []
    now = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)

    class _FakeDriver:
        def select_one(self, sql: str, **_kwargs: Any) -> dict[str, Any]:
            selected.append(sql)
            return {"data": b"payload", "expires_at": now}

        def execute(self, sql: str, **_kwargs: Any) -> None:
            executed.append(sql)

    config = BigQueryConfig(connection_config={"project": "proj", "dataset_id": "ds"})
    config.provide_session = lambda *_args, **_kwargs: nullcontext(_FakeDriver())  # type: ignore[method-assign]
    store = BigQueryStore(config)

    store._get("s1", renew_for=60)
    store._set("s1", b"val", expires_in=60)
    store._delete("s1")
    store._delete_all()
    store._expires_in("s1")

    assert all("1970-01-01" not in sql for sql in executed)
    assert all("1970-01-01" not in sql for sql in selected)


def test_bigquery_adk_reconcile_target_order_and_dataset_qualification() -> None:
    """get_schema_targets_sync returns the five dataset-qualified ADK targets with BigQuery additive safety."""
    from unittest.mock import MagicMock

    store = _make_store({"owner_id_column": "tenant_id STRING"})
    targets = store.get_schema_targets_sync(MagicMock())

    assert [t.table_name for t in targets] == [
        "adk_session",
        "adk_event",
        "adk_app_state",
        "adk_user_state",
        "adk_internal_metadata",
    ]
    assert all(t.schema == "test_dataset" for t in targets)

    session_target, event_target, app_state_target, _, metadata_target = targets
    assert {"id", "app_name", "user_id", "create_time", "update_time"} <= session_target.unsafe_add_columns
    assert "tenant_id" not in session_target.unsafe_add_columns
    assert "state" not in session_target.unsafe_add_columns
    assert "invocation_id" not in event_target.unsafe_add_columns
    assert "event_data" not in event_target.unsafe_add_columns
    assert {"app_name", "update_time"} <= app_state_target.unsafe_add_columns
    assert {"key", "value"} <= metadata_target.unsafe_add_columns


def test_bigquery_adk_reconcile_adds_nullable_column_and_refuses_not_null() -> None:
    """Built-in ADK reconcile adds nullable BigQuery columns and refuses missing NOT NULL columns."""
    from unittest.mock import MagicMock

    from sqlspec.exceptions import MigrationError
    from sqlspec.extensions.adk.migrations.reconcile import reconcile as adk_reconcile
    from sqlspec.migrations.context import MigrationContext

    config = BigQueryConfig(
        connection_config={"project": "test-project", "dataset_id": "test_dataset"},
        extension_config={
            "adk": {"enable_memory": False, "owner_id_column": "tenant_id STRING", "migration_mode": "reconcile"}
        },
    )

    columns_by_table: dict[str, list[dict[str, str]]] = {
        "adk_session": [
            {"column_name": "id", "data_type": "STRING", "is_nullable": "NO"},
            {"column_name": "app_name", "data_type": "STRING", "is_nullable": "NO"},
            {"column_name": "user_id", "data_type": "STRING", "is_nullable": "NO"},
            {"column_name": "state", "data_type": "JSON", "is_nullable": "YES"},
            {"column_name": "create_time", "data_type": "TIMESTAMP", "is_nullable": "NO"},
            {"column_name": "update_time", "data_type": "TIMESTAMP", "is_nullable": "NO"},
        ],
        "adk_event": [
            {"column_name": "id", "data_type": "STRING", "is_nullable": "NO"},
            {"column_name": "app_name", "data_type": "STRING", "is_nullable": "NO"},
            {"column_name": "user_id", "data_type": "STRING", "is_nullable": "NO"},
            {"column_name": "session_id", "data_type": "STRING", "is_nullable": "NO"},
            {"column_name": "invocation_id", "data_type": "STRING", "is_nullable": "YES"},
            {"column_name": "timestamp", "data_type": "TIMESTAMP", "is_nullable": "NO"},
            {"column_name": "event_data", "data_type": "JSON", "is_nullable": "YES"},
        ],
        "adk_app_state": [
            {"column_name": "app_name", "data_type": "STRING", "is_nullable": "NO"},
            {"column_name": "state", "data_type": "JSON", "is_nullable": "YES"},
            {"column_name": "update_time", "data_type": "TIMESTAMP", "is_nullable": "NO"},
        ],
        "adk_user_state": [
            {"column_name": "app_name", "data_type": "STRING", "is_nullable": "NO"},
            {"column_name": "user_id", "data_type": "STRING", "is_nullable": "NO"},
            {"column_name": "state", "data_type": "JSON", "is_nullable": "YES"},
            {"column_name": "update_time", "data_type": "TIMESTAMP", "is_nullable": "NO"},
        ],
        "adk_internal_metadata": [
            {"column_name": "key", "data_type": "STRING", "is_nullable": "NO"},
            {"column_name": "value", "data_type": "STRING", "is_nullable": "NO"},
        ],
    }

    driver = MagicMock()
    driver.data_dictionary.get_tables.return_value = [{"table_name": name} for name in columns_by_table]
    driver.data_dictionary.get_columns.side_effect = lambda _drv, table, schema=None: columns_by_table[table]

    assert adk_reconcile(MigrationContext(config=config, dialect="bigquery", driver=driver)) is None
    assert driver.execute.call_count == 1
    executed_stmt = driver.execute.call_args.args[0]
    sql = executed_stmt.sql if hasattr(executed_stmt, "sql") else str(executed_stmt)
    assert "ALTER TABLE `test_dataset`.`adk_session`" in sql
    assert "ADD COLUMN `tenant_id` STRING" in sql
    driver.commit.assert_not_called()

    missing_not_null_cols = dict(columns_by_table)
    missing_not_null_cols["adk_app_state"] = [
        {"column_name": "app_name", "data_type": "STRING", "is_nullable": "NO"},
        {"column_name": "state", "data_type": "JSON", "is_nullable": "YES"},
    ]
    bad_driver = MagicMock()
    bad_driver.data_dictionary.get_tables.return_value = [{"table_name": name} for name in missing_not_null_cols]
    bad_driver.data_dictionary.get_columns.side_effect = lambda _drv, table, schema=None: missing_not_null_cols[table]

    with pytest.raises(MigrationError, match="update_time") as exc_info:
        adk_reconcile(MigrationContext(config=config, dialect="bigquery", driver=bad_driver))
    assert 'migration_mode="versioned"' in str(exc_info.value)


def _capture_client(monkeypatch: Any, store: BigQueryADKStore, capture: Any) -> None:
    client = MagicMock()

    def query(sql: str, *, job_config: Any = None) -> MagicMock:
        job = MagicMock()
        parameters = job_config.query_parameters if job_config is not None else None
        job.result.return_value = capture(store, sql, parameters)
        job.num_dml_affected_rows = 0
        return job

    client.query.side_effect = query
    monkeypatch.setattr(store._config, "provide_connection", lambda: nullcontext(client))
