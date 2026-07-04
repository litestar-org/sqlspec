# pyright: reportPrivateUsage=false
"""Unit tests for aiomysql ADK store extension configuration."""

import asyncio
from typing import Any, cast, get_args, get_origin
from unittest.mock import MagicMock

from typing_extensions import NotRequired

from sqlspec.adapters.aiomysql.adk import AiomysqlADKConfig, AiomysqlADKMemoryStore, AiomysqlADKStore
from sqlspec.config import ADKConfig


def _mock_config(adk_config: dict[str, object] | None = None) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"adk": adk_config or {}}
    return config


class _MysqlMissingTableError(Exception):
    errno = 1146


def test_aiomysql_adk_config_types_adapter_local_mysql_options() -> None:
    """aiomysql ADK MySQL options are typed on the adapter-local extension config."""

    assert cast("Any", ADKConfig).__optional_keys__ <= cast("Any", AiomysqlADKConfig).__optional_keys__

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
        annotation = cast("Any", AiomysqlADKConfig.__annotations__[feature_name])
        assert get_origin(annotation) is NotRequired
        assert get_args(annotation) == (expected_type,)


def test_aiomysql_adk_tables_use_plain_mysql_schema_by_default() -> None:
    """aiomysql ADK profile DDL stays opt-in through extension_config["adk"]."""

    store = AiomysqlADKStore(_mock_config())
    memory_store = AiomysqlADKMemoryStore(_mock_config())

    events_sql = asyncio.run(store._get_create_events_table_sql())
    memory_sql = asyncio.run(memory_store._get_create_memory_table_sql())

    assert "author_gc" not in events_sql
    assert "node_path_gc" not in events_sql
    assert "timestamp ASC, invocation_id" not in events_sql
    assert "COMMENT='adk-events'" not in events_sql
    assert "COMMENT='adk-memory'" not in memory_sql


def test_aiomysql_adk_tables_apply_adapter_local_mysql_profile() -> None:
    """aiomysql ADK MySQL options add generated columns, covering keys, and table options."""

    store = AiomysqlADKStore(
        _mock_config({
            "enable_event_generated_columns": True,
            "enable_covering_indexes": True,
            "session_table_options": "COMMENT='adk-session'",
            "events_table_options": "COMMENT='adk-events'",
            "app_state_table_options": "COMMENT='adk-app-state'",
            "user_state_table_options": "COMMENT='adk-user-state'",
        })
    )
    memory_store = AiomysqlADKMemoryStore(_mock_config({"memory_table_options": "COMMENT='adk-memory'"}))

    session_sql = asyncio.run(store._get_create_sessions_table_sql())
    events_sql = asyncio.run(store._get_create_events_table_sql())
    app_state_sql = asyncio.run(store._get_create_app_states_table_sql())
    user_state_sql = asyncio.run(store._get_create_user_states_table_sql())
    memory_sql = asyncio.run(memory_store._get_create_memory_table_sql())

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


def test_aiomysql_table_missing_uses_errno_attribute() -> None:
    from sqlspec.adapters.aiomysql.adk.store import _is_mysql_table_missing

    assert _is_mysql_table_missing(_MysqlMissingTableError()) is True
