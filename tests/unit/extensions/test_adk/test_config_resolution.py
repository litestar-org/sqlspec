"""Tests for ADK flat-config resolution."""

from typing import Any

from sqlspec.config import ADKConfig
from sqlspec.extensions.adk._config_utils import (
    _get_adk_artifact_store_config,
    _get_adk_memory_store_config,
    _get_adk_session_store_config,
    _is_adk_memory_migration_enabled,
)


class _Config:
    extension_config: dict[str, dict[str, Any]]

    def __init__(self, adk_config: dict[str, Any]) -> None:
        self.extension_config = {"adk": adk_config}


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

    resolved = _get_adk_session_store_config(config)

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

    resolved = _get_adk_memory_store_config(config)

    assert resolved == {"enable_memory": False, "memory_table": "agent_memories", "use_fts": True, "max_results": 50}


def test_flat_artifact_config_resolves_store_owned_table_only() -> None:
    config = _Config({"artifact_table": "agent_artifacts", "artifact_storage_uri": "s3://bucket/adk"})

    resolved = _get_adk_artifact_store_config(config)

    assert resolved == {"artifact_table": "agent_artifacts"}


def test_include_memory_migration_overrides_enable_memory() -> None:
    config = _Config({"enable_memory": True, "include_memory_migration": False})

    assert not _is_adk_memory_migration_enabled(config)


def test_include_memory_migration_defaults_to_enable_memory() -> None:
    enabled = _Config({"enable_memory": True})
    disabled = _Config({"enable_memory": False})

    assert _is_adk_memory_migration_enabled(enabled) is True
    assert _is_adk_memory_migration_enabled(disabled) is False
