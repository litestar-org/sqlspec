# pyright: reportPrivateUsage=false
"""Unit tests for asyncpg ADK store extension configuration."""

from typing import Any, cast, get_args, get_origin
from unittest.mock import MagicMock

from typing_extensions import NotRequired

from sqlspec.adapters.asyncpg.adk import AsyncpgADKConfig, AsyncpgADKStore
from sqlspec.config import ADKConfig


def _mock_config(adk_config: dict[str, object] | None = None) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"adk": adk_config or {}}
    return config


def test_asyncpg_adk_config_types_adapter_local_optimizations() -> None:
    """Asyncpg ADK optimizations are typed on the adapter-local extension config."""

    assert cast("Any", ADKConfig).__optional_keys__ <= cast("Any", AsyncpgADKConfig).__optional_keys__
    assert cast("Any", AsyncpgADKConfig).__optional_keys__ - cast("Any", ADKConfig).__optional_keys__ == {
        "autovacuum_analyze_scale_factor",
        "autovacuum_vacuum_scale_factor",
        "enable_event_generated_columns",
        "enable_covering_indexes",
        "fillfactor",
    }

    for feature_name in ("enable_event_generated_columns", "enable_covering_indexes"):
        annotation = cast("Any", AsyncpgADKConfig.__annotations__[feature_name])
        assert get_origin(annotation) is NotRequired
        assert get_args(annotation) == (bool,)


async def test_asyncpg_adk_events_table_uses_plain_schema_by_default() -> None:
    """Asyncpg ADK optimization DDL stays opt-in through adapter-local extension config."""

    store = AsyncpgADKStore(_mock_config())

    sql = await store._events_table_ddl()

    assert "author_gc" not in sql
    assert "node_path_gc" not in sql
    assert "INCLUDE (invocation_id)" not in sql


async def test_asyncpg_adk_events_table_applies_adapter_local_extension_config() -> None:
    """Asyncpg ADK extension settings enable PostgreSQL-specific event DDL."""

    store = AsyncpgADKStore(_mock_config({"enable_event_generated_columns": True, "enable_covering_indexes": True}))

    sql = await store._events_table_ddl()

    assert "author_gc VARCHAR(256) GENERATED ALWAYS AS (event_data->>'author') STORED" in sql
    assert "node_path_gc TEXT GENERATED ALWAYS AS (event_data->'node_info'->>'path') STORED" in sql
    assert "idx_adk_event_author_gc" in sql
    assert "idx_adk_event_node_path_gc" in sql
    assert "INCLUDE (invocation_id)" in sql


async def test_asyncpg_adk_event_table_applies_postgres_tuning_options() -> None:
    """Append-heavy ADK event tables honor PostgreSQL tuning settings."""
    store = AsyncpgADKStore(
        _mock_config({"fillfactor": 75, "autovacuum_vacuum_scale_factor": 0.1, "autovacuum_analyze_scale_factor": 0.2})
    )

    sql = await store._events_table_ddl()

    assert "fillfactor = 75" in sql
    assert "autovacuum_vacuum_scale_factor = 0.1" in sql
    assert "autovacuum_analyze_scale_factor = 0.2" in sql
