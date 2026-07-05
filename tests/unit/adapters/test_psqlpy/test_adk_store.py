# pyright: reportPrivateUsage=false
"""Unit tests for psqlpy ADK store extension configuration."""

from typing import Any, cast, get_args, get_origin
from unittest.mock import MagicMock

from typing_extensions import NotRequired

from sqlspec.adapters.psqlpy.adk import PsqlpyADKConfig, PsqlpyADKStore
from sqlspec.config import ADKConfig


def _mock_config(adk_config: dict[str, object] | None = None) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"adk": adk_config or {}}
    return config


def test_psqlpy_adk_config_types_adapter_local_optimizations() -> None:
    """Psqlpy ADK optimization switches live on the adapter-local extension config."""

    assert cast("Any", ADKConfig).__optional_keys__ <= cast("Any", PsqlpyADKConfig).__optional_keys__
    assert cast("Any", PsqlpyADKConfig).__optional_keys__ - cast("Any", ADKConfig).__optional_keys__ == {
        "enable_event_generated_columns",
        "enable_covering_indexes",
    }

    for feature_name in ("enable_event_generated_columns", "enable_covering_indexes"):
        annotation = cast("Any", PsqlpyADKConfig.__annotations__[feature_name])
        assert get_origin(annotation) is NotRequired
        assert get_args(annotation) == (bool,)


async def test_psqlpy_adk_events_table_uses_plain_schema_by_default() -> None:
    """Psqlpy ADK optimization DDL stays opt-in through extension config."""

    store = PsqlpyADKStore(_mock_config())

    sql = await store._events_table_ddl()

    assert "author_gc" not in sql
    assert "node_path_gc" not in sql
    assert "INCLUDE (invocation_id)" not in sql


async def test_psqlpy_adk_events_table_applies_adapter_local_extension_config() -> None:
    """Psqlpy ADK extension settings enable PostgreSQL-specific event DDL."""

    store = PsqlpyADKStore(_mock_config({"enable_event_generated_columns": True, "enable_covering_indexes": True}))

    sql = await store._events_table_ddl()

    assert "author_gc VARCHAR(256) GENERATED ALWAYS AS (event_data->>'author') STORED" in sql
    assert "node_path_gc TEXT GENERATED ALWAYS AS (event_data->'node_info'->>'path') STORED" in sql
    assert "idx_adk_event_author_gc" in sql
    assert "idx_adk_event_node_path_gc" in sql
    assert "INCLUDE (invocation_id)" in sql
