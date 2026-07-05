# pyright: reportPrivateUsage=false
"""Unit tests for mysql-connector ADK store extension configuration."""

import asyncio
from typing import Any, cast, get_args, get_origin
from unittest.mock import MagicMock

from typing_extensions import NotRequired

from sqlspec.adapters.mysqlconnector.adk import (
    MysqlConnectorADKConfig,
    MysqlConnectorAsyncADKMemoryStore,
    MysqlConnectorAsyncADKStore,
    MysqlConnectorSyncADKMemoryStore,
    MysqlConnectorSyncADKStore,
)
from sqlspec.config import ADKConfig


def _mock_config(adk_config: dict[str, object] | None = None) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"adk": adk_config or {}}
    return config


def test_mysqlconnector_adk_config_types_adapter_local_mysql_options() -> None:
    """mysql-connector ADK MySQL options are typed on the adapter-local extension config."""

    assert cast("Any", ADKConfig).__optional_keys__ <= cast("Any", MysqlConnectorADKConfig).__optional_keys__

    expected_types: dict[str, object] = {
        "enable_event_generated_columns": bool,
        "enable_covering_indexes": bool,
        "session_table_options": str,
        "events_table_options": str,
        "app_state_table_options": str,
        "user_state_table_options": str,
        "memory_table_options": str,
    }
    for feature_name, expected_type in expected_types.items():
        annotation = cast("Any", MysqlConnectorADKConfig.__annotations__[feature_name])
        assert get_origin(annotation) is NotRequired
        assert get_args(annotation) == (expected_type,)


def test_mysqlconnector_async_adk_tables_use_plain_mysql_schema_by_default() -> None:
    """mysql-connector async ADK profile DDL stays opt-in through extension_config["adk"]."""

    store = MysqlConnectorAsyncADKStore(_mock_config())
    memory_store = MysqlConnectorAsyncADKMemoryStore(_mock_config())

    events_sql = asyncio.run(store._events_table_ddl())
    memory_sql = asyncio.run(memory_store._memory_table_ddl())

    assert "author_gc" not in events_sql
    assert "node_path_gc" not in events_sql
    assert "timestamp ASC, invocation_id" not in events_sql
    assert "COMMENT='adk-events'" not in events_sql
    assert "COMMENT='adk-memory'" not in memory_sql


def test_mysqlconnector_async_adk_tables_apply_adapter_local_mysql_profile() -> None:
    """mysql-connector async ADK options add generated columns, covering keys, and table options."""

    store = MysqlConnectorAsyncADKStore(
        _mock_config({
            "enable_event_generated_columns": True,
            "enable_covering_indexes": True,
            "session_table_options": "COMMENT='adk-session'",
            "events_table_options": "COMMENT='adk-events'",
            "app_state_table_options": "COMMENT='adk-app-state'",
            "user_state_table_options": "COMMENT='adk-user-state'",
        })
    )
    memory_store = MysqlConnectorAsyncADKMemoryStore(_mock_config({"memory_table_options": "COMMENT='adk-memory'"}))

    session_sql = asyncio.run(store._sessions_table_ddl())
    events_sql = asyncio.run(store._events_table_ddl())
    app_state_sql = asyncio.run(store._app_states_table_ddl())
    user_state_sql = asyncio.run(store._user_states_table_ddl())
    memory_sql = asyncio.run(memory_store._memory_table_ddl())

    assert (
        "author_gc VARCHAR(256) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(event_data, '$.author'))) STORED"
        in events_sql
    )
    assert (
        "node_path_gc VARCHAR(512) GENERATED ALWAYS AS "
        "(JSON_UNQUOTE(JSON_EXTRACT(event_data, '$.node_info.path'))) STORED" in events_sql
    )
    assert "INDEX idx_adk_event_author_gc (session_id, author_gc, timestamp ASC)" in events_sql
    assert "INDEX idx_adk_event_node_path_gc (session_id, node_path_gc, timestamp ASC)" in events_sql
    assert "INDEX idx_adk_event_scope (app_name, user_id, session_id, timestamp ASC, invocation_id)" in events_sql
    assert "INDEX idx_adk_event_session (session_id, timestamp ASC, invocation_id)" in events_sql
    assert "COMMENT='adk-session'" in session_sql
    assert "COMMENT='adk-events'" in events_sql
    assert "COMMENT='adk-app-state'" in app_state_sql
    assert "COMMENT='adk-user-state'" in user_state_sql
    assert "COMMENT='adk-memory'" in memory_sql


def test_mysqlconnector_sync_adk_tables_apply_same_mysql_profile() -> None:
    """mysql-connector sync ADK store uses the same extension_config["adk"] MySQL profile."""

    store = MysqlConnectorSyncADKStore(
        _mock_config({"enable_event_generated_columns": True, "enable_covering_indexes": True})
    )
    memory_store = MysqlConnectorSyncADKMemoryStore(_mock_config({"memory_table_options": "COMMENT='adk-memory'"}))

    events_sql = store._events_table_ddl()
    memory_sql = memory_store._memory_table_ddl()

    assert (
        "author_gc VARCHAR(256) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(event_data, '$.author'))) STORED"
        in events_sql
    )
    assert "INDEX idx_adk_event_session (session_id, timestamp ASC, invocation_id)" in events_sql
    assert "COMMENT='adk-memory'" in memory_sql
