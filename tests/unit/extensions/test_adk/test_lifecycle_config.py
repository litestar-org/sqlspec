"""Tests for ADK lifecycle control resolution."""

from typing import Any

import pytest

from sqlspec.config import ADKConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.adk._config_utils import _get_adk_lifecycle_plan


class _Config:
    extension_config: dict[str, dict[str, Any]]

    def __init__(self, adk_config: dict[str, Any]) -> None:
        self.extension_config = {"adk": adk_config}


def test_adk_config_declares_lifecycle_section() -> None:
    assert "lifecycle" in ADKConfig.__annotations__


def test_default_lifecycle_plan_sets_indexing_controls_to_auto() -> None:
    plan = _get_adk_lifecycle_plan(_Config({}))

    assert plan.partitioning is None
    assert plan.retention is None
    assert plan.compression is None
    assert plan.indexing == {
        "generated_columns": "auto",
        "covering_indexes": "auto",
        "search_indexes": "auto",
        "json_indexes": "auto",
        "vector_indexes": "auto",
    }
    assert plan.table_options == {}


def test_nested_lifecycle_sections_override_flat_legacy_keys() -> None:
    plan = _get_adk_lifecycle_plan(
        _Config({
            "partitioning": {"strategy": "hash", "partition_count": 4},
            "retention": {"event_ttl_seconds": 60},
            "compression": {"enabled": False},
            "session_table_options": "flat-session-options",
            "lifecycle": {
                "partitioning": {"strategy": "range", "interval": "month"},
                "retention": {"event_ttl_seconds": 120},
                "indexing": {"generated_columns": "enable", "covering_indexes": "disable"},
                "compression": {"enabled": True, "algorithm": "zstd"},
                "table_options": {"sessions": "nested-session-options", "events": "nested-event-options"},
            },
        })
    )

    assert plan.partitioning == {"strategy": "range", "interval": "month"}
    assert plan.retention == {"event_ttl_seconds": 120}
    assert plan.compression == {"enabled": True, "algorithm": "zstd"}
    assert plan.indexing["generated_columns"] == "enable"
    assert plan.indexing["covering_indexes"] == "disable"
    assert plan.table_options == {"sessions": "nested-session-options", "events": "nested-event-options"}


def test_flat_table_options_are_normalized_when_lifecycle_options_are_absent() -> None:
    plan = _get_adk_lifecycle_plan(
        _Config({
            "session_table_options": "session-options",
            "events_table_options": "event-options",
            "memory_table_options": "memory-options",
            "expires_index_options": "expires-options",
        })
    )

    assert plan.table_options == {
        "sessions": "session-options",
        "events": "event-options",
        "memory": "memory-options",
        "expires_index": "expires-options",
    }


def test_invalid_lifecycle_indexing_mode_raises_configuration_error() -> None:
    config = _Config({"lifecycle": {"indexing": {"generated_columns": "sometimes"}}})

    with pytest.raises(ImproperConfigurationError):
        _get_adk_lifecycle_plan(config)
