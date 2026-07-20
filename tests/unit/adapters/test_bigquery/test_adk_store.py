# pyright: reportPrivateUsage=false
"""Unit tests for BigQuery ADK store behavior."""

import inspect
from datetime import datetime, timezone
from typing import Any, cast, get_args, get_origin

import pytest
from typing_extensions import NotRequired

from sqlspec.adapters.bigquery import BigQueryConfig
from sqlspec.adapters.bigquery.adk import BigQueryADKConfig, BigQueryADKRetentionConfig, BigQueryADKStore
from sqlspec.config import ADKConfig, ExtensionConfigs
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.adk import BaseSyncADKStore


def _make_store(
    adk_config: dict[str, object] | None = None, driver_features: dict[str, Any] | None = None
) -> BigQueryADKStore:
    adk_settings: dict[str, Any] = {"enable_memory": False, "include_memory_migration": False}
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

    monkeypatch.setattr(BigQueryADKStore, "_run_query", capture)

    store.get_session("app", "user", "session")
    store._update_session_touch("app", "user", "session")
    store.update_session_state("app", "user", "session", {"turn": 1})
    store.delete_session("app", "user", "session")
    store.get_events("app", "user", "session")
    store.delete_expired_events(datetime.now(timezone.utc))
    store.delete_idle_sessions(datetime.now(timezone.utc))

    assert any("FROM test_dataset.adk_session" in sql and "create_time IS NOT NULL" in sql for sql in statements)
    assert any("UPDATE test_dataset.adk_session" in sql and "create_time IS NOT NULL" in sql for sql in statements)
    assert any("DELETE FROM test_dataset.adk_session" in sql and "create_time IS NOT NULL" in sql for sql in statements)
    assert any("FROM test_dataset.adk_event" in sql and "timestamp IS NOT NULL" in sql for sql in statements)
    assert any("DELETE FROM test_dataset.adk_event" in sql and "timestamp IS NOT NULL" in sql for sql in statements)


def test_bigquery_adk_get_events_reads_full_event_blob_without_json_value(monkeypatch: Any) -> None:
    """Event reads preserve nested event_data instead of JSON_VALUE scalar projections."""

    store = _make_store()
    timestamp = datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)
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

    monkeypatch.setattr(BigQueryADKStore, "_run_query", capture)

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

    monkeypatch.setattr(BigQueryADKStore, "_run_query", capture)

    store.create_session("session", "app", "user", {}, owner_id="tenant-1")

    sql, params = statements[0]
    assert "tenant_id" in sql
    assert params is not None
    assert any(
        getattr(param, "name", "") == "owner_id" and getattr(param, "value", None) == "tenant-1" for param in params
    )
