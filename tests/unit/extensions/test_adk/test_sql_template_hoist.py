# pyright: reportPrivateUsage=false
"""Regression tests for adapter ADK SQL template ownership."""

import ast
import importlib
import inspect
from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.cockroach_psycopg.adk import (
    CockroachPsycopgAsyncADKMemoryStore,
    CockroachPsycopgAsyncADKStore,
    CockroachPsycopgSyncADKMemoryStore,
    CockroachPsycopgSyncADKStore,
)
from sqlspec.adapters.mysqlconnector.adk import (
    MysqlConnectorAsyncADKMemoryStore,
    MysqlConnectorAsyncADKStore,
    MysqlConnectorSyncADKMemoryStore,
    MysqlConnectorSyncADKStore,
)
from sqlspec.adapters.oracledb.adk import (
    JSONStorageType,
    OracleAsyncADKMemoryStore,
    OracleAsyncADKStore,
    OracleSyncADKMemoryStore,
    OracleSyncADKStore,
)
from sqlspec.adapters.psycopg.adk import (
    PsycopgAsyncADKMemoryStore,
    PsycopgAsyncADKStore,
    PsycopgSyncADKMemoryStore,
    PsycopgSyncADKStore,
)


def _mock_config(adk_config: dict[str, object]) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"adk": adk_config}
    return config


async def _session_ddl(store: object) -> dict[str, str]:
    return {
        "sessions": await store._sessions_table_ddl(),  # type: ignore[attr-defined]
        "events": await store._events_table_ddl(),  # type: ignore[attr-defined]
        "app_state": await store._app_states_table_ddl(),  # type: ignore[attr-defined]
        "user_state": await store._user_states_table_ddl(),  # type: ignore[attr-defined]
        "metadata": await store._metadata_table_ddl(),  # type: ignore[attr-defined]
        "seed": await store._metadata_seed_sql(),  # type: ignore[attr-defined]
    }


def _sync_session_ddl(store: object) -> dict[str, str]:
    return {
        "sessions": store._sessions_table_ddl(),  # type: ignore[attr-defined]
        "events": store._events_table_ddl(),  # type: ignore[attr-defined]
        "app_state": store._app_states_table_ddl(),  # type: ignore[attr-defined]
        "user_state": store._user_states_table_ddl(),  # type: ignore[attr-defined]
        "metadata": store._metadata_table_ddl(),  # type: ignore[attr-defined]
        "seed": store._metadata_seed_sql(),  # type: ignore[attr-defined]
    }


@pytest.mark.parametrize(
    "module_name",
    [
        "sqlspec.adapters.psycopg.adk.store",
        "sqlspec.adapters.cockroach_psycopg.adk.store",
        "sqlspec.adapters.mysqlconnector.adk.store",
        "sqlspec.adapters.oracledb.adk.store",
    ],
)
def test_adk_ddl_methods_reference_module_templates(module_name: str) -> None:
    module = importlib.import_module(module_name)
    tree = ast.parse(inspect.getsource(module))

    sql_owners = [
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and ("ddl" in node.name or "seed" in node.name)
    ]
    inline_templates = [node for owner in sql_owners for node in ast.walk(owner) if isinstance(node, ast.JoinedStr)]

    assert not inline_templates


@pytest.mark.anyio
async def test_psycopg_templates_bind_identically_for_sync_and_async_stores() -> None:
    config = _mock_config({
        "session_table": "agent_session",
        "events_table": "agent_event",
        "memory_table": "agent_memory",
        "owner_id_column": "tenant_id UUID REFERENCES tenant(id)",
        "enable_event_generated_columns": True,
        "enable_covering_indexes": True,
    })
    async_store = PsycopgAsyncADKStore(config)
    sync_store = PsycopgSyncADKStore(config)
    async_memory = PsycopgAsyncADKMemoryStore(config)
    sync_memory = PsycopgSyncADKMemoryStore(config)

    async_ddl = await _session_ddl(async_store)
    sync_ddl = _sync_session_ddl(sync_store)
    async_memory_ddl = await async_memory._memory_table_ddl()
    sync_memory_ddl = sync_memory._memory_table_ddl()

    assert async_ddl == sync_ddl
    assert async_memory_ddl == sync_memory_ddl
    assert "tenant_id UUID REFERENCES tenant(id)," in async_ddl["sessions"]
    assert "tenant_id UUID REFERENCES tenant(id)," in async_memory_ddl
    assert "author_gc VARCHAR(256) GENERATED ALWAYS AS (event_data->>'author') STORED" in async_ddl["events"]
    assert "node_path_gc TEXT GENERATED ALWAYS AS (event_data->'node_info'->>'path') STORED" in async_ddl["events"]
    assert "ON agent_event(session_id, timestamp ASC) INCLUDE (invocation_id)" in async_ddl["events"]


@pytest.mark.anyio
async def test_cockroach_templates_bind_identically_for_sync_and_async_stores() -> None:
    config = _mock_config({
        "session_table": "agent_session",
        "events_table": "agent_event",
        "memory_table": "agent_memory",
        "owner_id_column": "tenant_id UUID",
        "table_locality": "LOCALITY GLOBAL",
        "enable_hash_sharded_indexes": True,
        "hash_shard_bucket_count": 8,
        "enable_storing_indexes": True,
        "enable_memory_trigram_index": True,
    })
    async_store = CockroachPsycopgAsyncADKStore(config)
    sync_store = CockroachPsycopgSyncADKStore(config)
    async_memory = CockroachPsycopgAsyncADKMemoryStore(config)
    sync_memory = CockroachPsycopgSyncADKMemoryStore(config)

    async_ddl = await _session_ddl(async_store)
    sync_ddl = _sync_session_ddl(sync_store)
    async_memory_ddl = await async_memory._memory_table_ddl()
    sync_memory_ddl = sync_memory._memory_table_ddl()

    assert async_ddl == sync_ddl
    assert async_memory_ddl == sync_memory_ddl
    assert "tenant_id UUID," in async_ddl["sessions"]
    assert "LOCALITY GLOBAL" in async_ddl["sessions"]
    assert "USING HASH WITH (bucket_count = 8)" in async_ddl["events"]
    assert "STORING (invocation_id, event_data)" in async_ddl["events"]
    assert "ON agent_memory USING GIN (content_text gin_trgm_ops)" in async_memory_ddl


@pytest.mark.anyio
async def test_mysqlconnector_templates_bind_identically_for_sync_and_async_stores() -> None:
    config = _mock_config({
        "session_table": "agent_session",
        "events_table": "agent_event",
        "memory_table": "agent_memory",
        "owner_id_column": "tenant_id BIGINT",
        "enable_event_generated_columns": True,
        "enable_covering_indexes": True,
        "events_table_options": "COMMENT='agent-events'",
        "memory_table_options": "COMMENT='agent-memory'",
    })
    async_store = MysqlConnectorAsyncADKStore(config)
    sync_store = MysqlConnectorSyncADKStore(config)
    async_memory = MysqlConnectorAsyncADKMemoryStore(config)
    sync_memory = MysqlConnectorSyncADKMemoryStore(config)

    async_ddl = await _session_ddl(async_store)
    sync_ddl = _sync_session_ddl(sync_store)
    async_memory_ddl = await async_memory._memory_table_ddl()
    sync_memory_ddl = sync_memory._memory_table_ddl()

    assert async_ddl == sync_ddl
    assert async_memory_ddl == sync_memory_ddl
    assert "tenant_id BIGINT," in async_ddl["sessions"]
    assert "tenant_id BIGINT," in async_memory_ddl
    assert "author_gc VARCHAR(256) GENERATED ALWAYS AS" in async_ddl["events"]
    assert "INDEX idx_agent_event_session (session_id, timestamp ASC, invocation_id)" in async_ddl["events"]
    assert "COMMENT='agent-events'" in async_ddl["events"]
    assert "COMMENT='agent-memory'" in async_memory_ddl


@pytest.mark.anyio
async def test_oracle_templates_bind_identically_for_every_json_storage_type() -> None:
    config = _mock_config({
        "session_table": "agent_session",
        "events_table": "agent_event",
        "memory_table": "agent_memory",
        "owner_id_column": "tenant_id NUMBER",
        "partitioning": {"strategy": "hash", "partition_count": 8},
    })
    async_store = OracleAsyncADKStore(config)
    sync_store = OracleSyncADKStore(config)
    async_memory = OracleAsyncADKMemoryStore(config)
    sync_memory = OracleSyncADKMemoryStore(config)
    expected_columns = {
        JSONStorageType.JSON_NATIVE: ("state JSON NOT NULL", "event_data JSON NOT NULL", "content_json JSON"),
        JSONStorageType.BLOB_JSON: (
            "state BLOB CHECK (state IS JSON) NOT NULL",
            "event_data BLOB CHECK (event_data IS JSON) NOT NULL",
            "content_json BLOB CHECK (content_json IS JSON)",
        ),
        JSONStorageType.BLOB_PLAIN: ("state BLOB NOT NULL", "event_data BLOB NOT NULL", "content_json BLOB"),
    }

    for storage_type, (state_column, event_column, memory_column) in expected_columns.items():
        async_session_ddl = async_store._sessions_table_ddl_for_type(storage_type)
        sync_session_ddl = sync_store._sessions_table_ddl_for_type(storage_type)
        async_events_ddl = async_store._events_table_ddl_for_type(storage_type)
        sync_events_ddl = sync_store._events_table_ddl_for_type(storage_type)
        async_memory_ddl = async_memory._memory_table_ddl_for_type(storage_type)
        sync_memory_ddl = sync_memory._memory_table_ddl_for_type(storage_type)

        assert async_session_ddl == sync_session_ddl
        assert async_events_ddl == sync_events_ddl
        assert async_store._app_states_table_ddl_for_type(storage_type) == sync_store._app_states_table_ddl_for_type(
            storage_type
        )
        assert async_store._user_states_table_ddl_for_type(storage_type) == sync_store._user_states_table_ddl_for_type(
            storage_type
        )
        assert async_memory_ddl == sync_memory_ddl
        assert state_column in async_session_ddl
        assert event_column in async_events_ddl
        assert memory_column in async_memory_ddl
        assert (
            "update_time TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL, tenant_id NUMBER" in async_session_ddl
        )
        assert "tenant_id NUMBER," in async_memory_ddl
        assert "PARTITION BY HASH (id) PARTITIONS 8" in async_session_ddl

    assert await async_store._metadata_table_ddl() == sync_store._metadata_table_ddl()
    assert await async_store._metadata_seed_sql() == sync_store._metadata_seed_sql()
