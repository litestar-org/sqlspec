"""Tests for ADK clean-break configuration resolution."""

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


def test_adk_config_declares_nested_capability_sections() -> None:
    expected = {"schema", "memory", "search", "artifact", "optimizations", "oracle", "spanner", "adbc", "bigquery"}

    assert expected <= set(ADKConfig.__annotations__)


def test_nested_schema_config_resolves_all_adk_table_names() -> None:
    config = _Config({
        "session_table": "flat_sessions",
        "schema": {
            "session_table": "agent_sessions",
            "events_table": "agent_events",
            "app_state_table": "agent_app_states",
            "user_state_table": "agent_user_states",
            "metadata_table": "agent_metadata",
            "owner_id_column": "tenant_id UUID",
        },
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


def test_nested_memory_and_search_config_resolve_memory_store_settings() -> None:
    config = _Config({
        "enable_memory": True,
        "memory": {"enabled": False, "table": "agent_memories", "max_results": 50},
        "search": {"use_fts": True, "language": "simple"},
    })

    resolved = _get_adk_memory_store_config(config)

    assert resolved == {"enable_memory": False, "memory_table": "agent_memories", "use_fts": True, "max_results": 50}


def test_nested_artifact_config_resolves_table_and_storage_uri() -> None:
    config = _Config({
        "artifact_table": "flat_artifacts",
        "artifact_storage_uri": "file:///flat",
        "artifact": {"table": "agent_artifacts", "storage_uri": "s3://bucket/adk"},
    })

    resolved = _get_adk_artifact_store_config(config)

    assert resolved == {"artifact_table": "agent_artifacts", "storage_uri": "s3://bucket/adk"}


def test_schema_include_memory_migration_overrides_runtime_memory_enablement() -> None:
    config = _Config({"memory": {"enabled": True}, "schema": {"include_memory_migration": False}})

    assert not _is_adk_memory_migration_enabled(config)
