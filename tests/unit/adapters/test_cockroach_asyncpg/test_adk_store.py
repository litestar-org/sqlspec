# pyright: reportPrivateUsage=false
"""Unit tests for CockroachDB asyncpg ADK store extension configuration."""

from typing import Any, cast, get_args, get_origin
from unittest.mock import MagicMock

import pytest
from typing_extensions import NotRequired

from sqlspec.adapters.cockroach_asyncpg.adk import (
    CockroachAsyncpgADKConfig,
    CockroachAsyncpgADKMemoryStore,
    CockroachAsyncpgADKStore,
)
from sqlspec.config import ADKConfig


def _mock_config(adk_config: dict[str, object] | None = None) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"adk": adk_config or {}}
    return config


def test_cockroach_asyncpg_adk_config_types_adapter_local_optimizations() -> None:
    """Cockroach ADK optimizations are typed on the adapter-local extension config."""

    assert cast("Any", ADKConfig).__optional_keys__ <= cast("Any", CockroachAsyncpgADKConfig).__optional_keys__

    expected_types: dict[str, object] = {
        "table_locality": str,
        "session_table_locality": str,
        "events_table_locality": str,
        "app_state_table_locality": str,
        "user_state_table_locality": str,
        "metadata_table_locality": str,
        "memory_table_locality": str,
        "enable_hash_sharded_indexes": bool,
        "hash_shard_bucket_count": int,
        "enable_storing_indexes": bool,
        "enable_memory_trigram_index": bool,
    }
    for feature_name, expected_type in expected_types.items():
        annotation = cast("Any", CockroachAsyncpgADKConfig.__annotations__[feature_name])
        assert get_origin(annotation) is NotRequired
        assert get_args(annotation) == (expected_type,)


@pytest.mark.anyio
async def test_cockroach_asyncpg_adk_tables_use_plain_schema_by_default() -> None:
    """Cockroach ADK optimization DDL stays opt-in through adapter-local extension config."""

    store = CockroachAsyncpgADKStore(_mock_config())
    memory_store = CockroachAsyncpgADKMemoryStore(_mock_config())

    session_sql = await store._sessions_table_ddl()
    events_sql = await store._events_table_ddl()
    memory_sql = await memory_store._memory_table_ddl()

    assert "LOCALITY" not in session_sql
    assert "LOCALITY" not in events_sql
    assert "LOCALITY" not in memory_sql
    assert "USING HASH" not in session_sql
    assert "USING HASH" not in events_sql
    assert "USING HASH" not in memory_sql
    assert "STORING (" not in session_sql
    assert "STORING (" not in events_sql
    assert "gin_trgm_ops" not in memory_sql


@pytest.mark.anyio
async def test_cockroach_asyncpg_adk_tables_apply_locality_hash_and_storing_indexes() -> None:
    """Cockroach ADK extension settings enable CockroachDB-specific table and index DDL."""

    config = _mock_config({
        "table_locality": 'LOCALITY REGIONAL BY TABLE IN "us-east1"',
        "events_table_locality": "LOCALITY REGIONAL BY ROW",
        "enable_hash_sharded_indexes": True,
        "hash_shard_bucket_count": 8,
        "enable_storing_indexes": True,
    })
    store = CockroachAsyncpgADKStore(config)

    session_sql = await store._sessions_table_ddl()
    events_sql = await store._events_table_ddl()

    assert 'LOCALITY REGIONAL BY TABLE IN "us-east1"' in session_sql
    assert "LOCALITY REGIONAL BY ROW" in events_sql
    assert "idx_adk_session_update_time" in session_sql
    assert "ON adk_session(update_time DESC) USING HASH WITH (bucket_count = 8)" in session_sql
    assert "ON adk_event(session_id, timestamp ASC) USING HASH WITH (bucket_count = 8)" in events_sql
    assert "STORING (state, create_time, update_time)" in session_sql
    assert "STORING (invocation_id, event_data)" in events_sql


@pytest.mark.anyio
async def test_cockroach_asyncpg_memory_table_applies_trigram_index_and_locality() -> None:
    """Cockroach memory search DDL is enabled from extension_config["adk"], not driver features."""

    config = _mock_config({"memory_table_locality": "LOCALITY GLOBAL", "enable_memory_trigram_index": True})
    memory_store = CockroachAsyncpgADKMemoryStore(config)

    sql = await memory_store._memory_table_ddl()

    assert "LOCALITY GLOBAL" in sql
    assert "idx_adk_memory_content_trgm" in sql
    assert "ON adk_memory USING GIN (content_text gin_trgm_ops)" in sql
