"""Unit tests for the BigQuery ADK store (offline DDL + config wiring)."""

import asyncio
import importlib.util
from datetime import datetime, timezone
from typing import Any

import pytest

if importlib.util.find_spec("google.cloud.bigquery") is None:
    pytest.skip("google-cloud-bigquery not installed", allow_module_level=True)

from sqlspec.adapters.bigquery import BigQueryConfig
from sqlspec.adapters.bigquery.adk import BigQueryADKStore


def _make_store(extras: "dict[str, Any] | None" = None) -> BigQueryADKStore:
    extension: dict[str, Any] = {"adk": {"enable_memory": False, "include_memory_migration": False}}
    if extras:
        extension["adk"].update(extras)
    config = BigQueryConfig(
        connection_config={"project": "test-project", "dataset_id": "test_dataset"}, extension_config=extension
    )
    return BigQueryADKStore(config)


def test_bigquery_adk_store_instantiates_with_defaults() -> None:
    """Defaults expose the analytics-replica posture and qualified table names."""
    store = _make_store()
    assert store.session_table == "adk_session"
    assert store.events_table == "adk_event"
    assert store.app_state_table == "adk_app_state"
    assert store.user_state_table == "adk_user_state"
    assert store.metadata_table == "adk_metadata"
    assert store._dataset_qualifier == "test_dataset."
    assert store._lookup_window_days == 30
    assert store._require_partition_filter is False
    assert store._partition_expiration_days is None


def test_bigquery_adk_store_honours_session_lookup_window() -> None:
    """``bigquery.session_lookup_window_days`` from ``extension_config['adk']`` is propagated."""
    store = _make_store({"bigquery": {"session_lookup_window_days": 7}})
    assert store._lookup_window_days == 7


def test_bigquery_adk_store_derives_partition_expiration_from_retention() -> None:
    """Event TTL in seconds becomes BigQuery partition_expiration_days."""
    store = _make_store({"retention": {"event_ttl_seconds": 86400 * 30}})
    assert store._partition_expiration_days == 30


def test_bigquery_adk_store_honours_explicit_partition_filter_opt_in() -> None:
    """Partition filters are opt-in because BigQuery DML rejects unfiltered partitioned table touches."""
    store = _make_store({"bigquery": {"require_partition_filter": True}})

    assert store._require_partition_filter is True


def test_bigquery_adk_session_ddl_is_partitioned_and_clustered_without_filter_by_default() -> None:
    """Sessions table DDL has DATE partitioning + clustering on app_name/user_id."""
    store = _make_store()
    ddl = asyncio.run(store._get_create_sessions_table_sql())
    assert "PARTITION BY DATE(create_time)" in ddl
    assert "CLUSTER BY app_name, user_id, id" in ddl
    assert "test_dataset.adk_session" in ddl
    assert "require_partition_filter = TRUE" not in ddl


def test_bigquery_adk_events_ddl_clusters_on_session_id() -> None:
    """Events table DDL has DATE partitioning + clustering on session_id."""
    store = _make_store()
    ddl = asyncio.run(store._get_create_events_table_sql())
    assert "PARTITION BY DATE(timestamp)" in ddl
    assert "CLUSTER BY session_id" in ddl
    assert "test_dataset.adk_event" in ddl


def test_bigquery_adk_event_ttl_applies_only_to_event_partitions() -> None:
    """Session partitions must not inherit event TTL expiration."""
    store = _make_store({"retention": {"event_ttl_seconds": 86400 * 30}})

    session_ddl = asyncio.run(store._get_create_sessions_table_sql())
    event_ddl = asyncio.run(store._get_create_events_table_sql())

    assert "partition_expiration_days" not in session_ddl
    assert "partition_expiration_days = 30" in event_ddl


def test_bigquery_adk_explicit_partition_filter_adds_query_predicates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Opt-in partition-filter mode adds broad predicates to partitioned table DML."""
    store = _make_store({"bigquery": {"require_partition_filter": True}})
    statements: list[str] = []

    def capture(_store: BigQueryADKStore, sql: str, parameters: Any = None) -> list[dict[str, Any]]:
        statements.append(sql)
        return []

    monkeypatch.setattr(BigQueryADKStore, "_run_query", capture)

    store._get_session("app", "user", "session")
    store._update_session_touch("app", "user", "session")
    store._update_session_state("app", "user", "session", {"turn": 1})
    store._delete_session("app", "user", "session")
    store._get_events("app", "user", "session")
    store._delete_expired_events(datetime.now(timezone.utc))
    store._delete_idle_sessions(datetime.now(timezone.utc))

    assert any("FROM test_dataset.adk_session" in sql and "create_time IS NOT NULL" in sql for sql in statements)
    assert any("UPDATE test_dataset.adk_session" in sql and "create_time IS NOT NULL" in sql for sql in statements)
    assert any("DELETE FROM test_dataset.adk_session" in sql and "create_time IS NOT NULL" in sql for sql in statements)
    assert any("FROM test_dataset.adk_event" in sql and "timestamp IS NOT NULL" in sql for sql in statements)
    assert any("DELETE FROM test_dataset.adk_event" in sql and "timestamp IS NOT NULL" in sql for sql in statements)


def test_bigquery_adk_scoped_state_ddl_clustered() -> None:
    """Scoped-state tables cluster on their access keys."""
    store = _make_store()
    app_ddl = asyncio.run(store._get_create_app_states_table_sql())
    user_ddl = asyncio.run(store._get_create_user_states_table_sql())
    assert "CLUSTER BY app_name" in app_ddl
    assert "CLUSTER BY app_name, user_id" in user_ddl


def test_bigquery_adk_seed_metadata_uses_merge() -> None:
    """Metadata seed uses MERGE so re-runs are idempotent."""
    store = _make_store()
    seed = asyncio.run(store._get_seed_metadata_sql())
    assert "MERGE" in seed
    assert "schema_version" in seed


def test_bigquery_adk_drop_sql_orders_child_tables_first() -> None:
    """Drop statements must remove events before sessions to respect logical FK semantics."""
    store = _make_store()
    drops = store._get_drop_tables_sql()
    assert drops[0].endswith("adk_event")
    assert any(stmt.endswith("adk_session") for stmt in drops)
