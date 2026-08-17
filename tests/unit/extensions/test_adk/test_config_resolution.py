"""Tests for ADK flat-config resolution."""

from typing import Any, cast

import pytest

from sqlspec.config import ADKConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.adk import _config_utils
from sqlspec.extensions.adk._config_utils import (
    _adk_artifact_store_config,
    _adk_memory_migration_enabled,
    _adk_memory_store_config,
    _adk_session_store_config,
    _adk_sessions_migration_enabled,
    _ADKSessionStoreConfig,
    _apply_owner_id,
    _ensure_adk_store_registration,
)


class _Config:
    extension_config: dict[str, dict[str, Any]]

    def __init__(self, adk_config: dict[str, Any]) -> None:
        self.extension_config = {"adk": adk_config}


def test_apply_owner_id_adds_only_configured_values() -> None:
    configured = cast("_ADKSessionStoreConfig", {})
    unconfigured = cast("_ADKSessionStoreConfig", {})

    _apply_owner_id(configured, {"owner_id_column": "tenant_id UUID"})
    _apply_owner_id(unconfigured, {})

    assert dict(configured) == {"owner_id_column": "tenant_id UUID"}
    assert not unconfigured


def test_adk_config_uses_flat_keys() -> None:
    """ADKConfig is a flat TypedDict; no per-adapter or nested negotiation blocks."""
    annotations = set(ADKConfig.__annotations__)
    expected_flat = {
        "session_table",
        "events_table",
        "app_state_table",
        "user_state_table",
        "metadata_table",
        "memory_table",
        "artifact_table",
        "owner_id_column",
    }
    forbidden_backend_settings = {
        "schema",
        "lifecycle",
        "capabilities",
        "optimizations",
        "oracle",
        "spanner",
        "adbc",
        "bigquery",
        "asyncpg",
        "in_memory",
        "shard_count",
        "session_table_options",
        "events_table_options",
        "memory_table_options",
        "expires_index_options",
        "fts_language",
        "schema_version",
        "partitioning",
        "retention",
        "compression",
        "sqlite_optimization",
        "enable_event_generated_columns",
        "enable_covering_indexes",
    }
    assert expected_flat <= annotations
    assert annotations.isdisjoint(forbidden_backend_settings)


def test_flat_schema_config_resolves_all_adk_table_names() -> None:
    config = _Config({
        "session_table": "agent_sessions",
        "events_table": "agent_events",
        "app_state_table": "agent_app_states",
        "user_state_table": "agent_user_states",
        "metadata_table": "agent_metadata",
        "owner_id_column": "tenant_id UUID",
    })

    resolved = _adk_session_store_config(config)

    assert resolved == {
        "session_table": "agent_sessions",
        "events_table": "agent_events",
        "app_state_table": "agent_app_states",
        "user_state_table": "agent_user_states",
        "metadata_table": "agent_metadata",
        "owner_id_column": "tenant_id UUID",
    }


def test_flat_memory_config_resolves_memory_store_settings() -> None:
    config = _Config({
        "enable_memory": False,
        "memory_table": "agent_memories",
        "memory_use_fts": True,
        "memory_max_results": 50,
    })

    resolved = _adk_memory_store_config(config)

    assert resolved["enable_memory"] is False
    assert resolved["memory_table"] == "agent_memories"
    assert resolved["use_fts"] is True
    assert resolved["max_results"] == 50
    assert resolved["vector_index_type"] == "hnsw"
    assert resolved["vector_dimensions"] == 768
    assert resolved["enable_bm25"] is False
    assert resolved["scann_num_leaves"] == 100
    assert resolved["scann_quantizer"] == "SQ8"


def test_flat_artifact_config_resolves_store_owned_table_only() -> None:
    config = _Config({"artifact_table": "agent_artifacts", "artifact_storage_uri": "s3://bucket/adk"})

    resolved = _adk_artifact_store_config(config)

    assert resolved == {"artifact_table": "agent_artifacts"}


def test_adk_config_has_no_separate_migration_include_flags() -> None:
    """Migration inclusion is not configurable per feature."""
    annotations = set(ADKConfig.__annotations__)

    assert "include_sessions_migration" not in annotations
    assert "include_memory_migration" not in annotations


def test_memory_migration_gate_follows_enable_memory() -> None:
    enabled = _Config({"enable_memory": True})
    disabled = _Config({"enable_memory": False})
    defaulted = _Config({})

    assert _adk_memory_migration_enabled(enabled) is True
    assert _adk_memory_migration_enabled(disabled) is False
    assert _adk_memory_migration_enabled(defaulted) is True


def test_sessions_migration_gate_follows_enable_sessions() -> None:
    enabled = _Config({"enable_sessions": True})
    disabled = _Config({"enable_sessions": False})
    defaulted = _Config({})

    assert _adk_sessions_migration_enabled(enabled) is True
    assert _adk_sessions_migration_enabled(disabled) is False
    assert _adk_sessions_migration_enabled(defaulted) is True


def test_retired_migration_include_keys_are_rejected() -> None:
    """Adapter configs reject the removed include flags through normal validation."""
    from sqlspec.adapters.sqlite import SqliteConfig

    for retired_key in ("include_sessions_migration", "include_memory_migration"):
        with pytest.raises(ImproperConfigurationError, match=retired_key):
            SqliteConfig(connection_config={"database": ":memory:"}, extension_config={"adk": {retired_key: False}})


def test_store_registration_skips_disabled_feature_store_classes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Disabled features do not require their adapter store class to exist."""
    from sqlspec.adapters.sqlite import SqliteConfig

    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"adk": {"enable_sessions": False, "enable_memory": True}},
    )
    resolved: list[str] = []

    def record_store_class(config: Any, store_suffix: str) -> object:
        resolved.append(store_suffix)
        return object

    monkeypatch.setattr(_config_utils, "_adk_adapter_store_class", record_store_class)

    _ensure_adk_store_registration(config)

    assert resolved == ["ADKMemoryStore"]
