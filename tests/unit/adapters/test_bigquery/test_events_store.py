"""Unit tests for BigQuery event store DDL emulator branching (regression for #473)."""

import pytest

from sqlspec.adapters.bigquery.config import BigQueryConfig
from sqlspec.adapters.bigquery.core import is_emulator_active_from_env
from sqlspec.adapters.bigquery.events import BigQueryEventQueueStore


def _make_store() -> BigQueryEventQueueStore:
    cfg = BigQueryConfig(
        connection_config={"project": "test", "dataset_id": "evt"},
        extension_config={"events": {"queue_table": "q"}},
    )
    return BigQueryEventQueueStore(cfg)


def test_is_emulator_active_from_env_reads_both_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BIGQUERY_EMULATOR_HOST", raising=False)
    monkeypatch.delenv("BIGQUERY_EMULATOR_HOST_HTTP", raising=False)
    assert is_emulator_active_from_env() is False

    monkeypatch.setenv("BIGQUERY_EMULATOR_HOST", "localhost:9050")
    assert is_emulator_active_from_env() is True

    monkeypatch.delenv("BIGQUERY_EMULATOR_HOST")
    monkeypatch.setenv("BIGQUERY_EMULATOR_HOST_HTTP", "http://localhost:9050")
    assert is_emulator_active_from_env() is True


def test_events_store_omits_cluster_by_against_emulator(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BIGQUERY_EMULATOR_HOST", "localhost:9050")
    store = _make_store()
    ddl = store._build_create_table_sql()
    assert "CLUSTER BY" not in ddl
    assert ddl.rstrip().endswith(")")


def test_events_store_keeps_cluster_by_without_emulator(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BIGQUERY_EMULATOR_HOST", raising=False)
    monkeypatch.delenv("BIGQUERY_EMULATOR_HOST_HTTP", raising=False)
    store = _make_store()
    ddl = store._build_create_table_sql()
    assert "CLUSTER BY channel, status, available_at" in ddl
