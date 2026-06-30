# pyright: reportPrivateUsage=false
"""Unit tests for mssql-python ADK store wiring and T-SQL generation."""

from typing import Any, cast, get_args, get_origin
from unittest.mock import MagicMock

import pytest
from typing_extensions import NotRequired

from sqlspec.adapters.mssql_python.adk import MssqlPythonADKConfig, MssqlPythonAsyncADKStore, MssqlPythonSyncADKStore
from sqlspec.config import ADKConfig
from sqlspec.extensions.adk import BaseAsyncADKStore, BaseSyncADKStore


def _mock_config(adk_config: dict[str, object] | None = None) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"adk": adk_config or {}}
    return config


def test_mssql_python_adk_exports_sync_and_async_store_types() -> None:
    """The adapter exposes sync and async stores that implement the current ADK bases."""

    sync_store = MssqlPythonSyncADKStore(_mock_config())
    async_store = MssqlPythonAsyncADKStore(_mock_config())

    assert isinstance(sync_store, BaseSyncADKStore)
    assert isinstance(async_store, BaseAsyncADKStore)


def test_mssql_python_adk_config_extends_base_config_without_redeclaring_base_fields() -> None:
    """Adapter-local ADK config should only add MSSQL-specific fields."""

    assert cast("Any", ADKConfig).__optional_keys__ <= cast("Any", MssqlPythonADKConfig).__optional_keys__

    base_fields = set(ADKConfig.__annotations__)
    local_fields = set(MssqlPythonADKConfig.__annotations__) - base_fields
    assert local_fields == {"native_json"}
    annotation = cast("Any", MssqlPythonADKConfig.__annotations__["native_json"])
    assert get_origin(annotation) is NotRequired
    assert get_args(annotation) == (bool,)


def test_sync_store_reads_adk_table_names_from_extension_config() -> None:
    """Store table names come from extension_config['adk'], not driver features."""

    store = MssqlPythonSyncADKStore(
        _mock_config({
            "session_table": "custom_session",
            "events_table": "custom_event",
            "app_state_table": "custom_app_state",
            "user_state_table": "custom_user_state",
            "metadata_table": "custom_metadata",
        })
    )

    assert store.session_table == "custom_session"
    assert store.events_table == "custom_event"
    assert store.app_state_table == "custom_app_state"
    assert store.user_state_table == "custom_user_state"
    assert store.metadata_table == "custom_metadata"


def test_sync_store_generates_tsql_idempotent_schema_with_conservative_json() -> None:
    """The default MSSQL DDL uses idempotent sys.tables probes and NVARCHAR JSON storage."""

    store = MssqlPythonSyncADKStore(_mock_config())

    sessions_sql = store._get_create_sessions_table_sql()
    events_sql = store._get_create_events_table_sql()
    app_state_sql = store._get_create_app_states_table_sql()
    metadata_sql = store._get_seed_metadata_sql()

    assert "IF NOT EXISTS (SELECT 1 FROM sys.tables" in sessions_sql
    assert "schema_id = SCHEMA_ID(N'dbo')" in sessions_sql
    assert "row_id UNIQUEIDENTIFIER NOT NULL" in sessions_sql
    assert "DEFAULT NEWSEQUENTIALID()" in sessions_sql
    assert "state NVARCHAR(MAX) NOT NULL" in sessions_sql
    assert "DATETIME2(6)" in sessions_sql
    assert "sys.indexes" in sessions_sql
    assert "ON DELETE CASCADE" in events_sql
    assert "event_data NVARCHAR(MAX) NOT NULL" in events_sql
    assert "MERGE INTO [dbo].[adk_internal_metadata]" in metadata_sql
    assert "?" not in metadata_sql
    assert "N'schema_version'" in metadata_sql
    assert "state NVARCHAR(MAX) NOT NULL" in app_state_sql


def test_sync_store_can_force_native_json_from_extension_config() -> None:
    """MSSQL-native JSON is opt-in unless version detection proves support."""

    store = MssqlPythonSyncADKStore(_mock_config({"native_json": True}))

    assert "state JSON NOT NULL" in store._get_create_sessions_table_sql()
    assert "event_data JSON NOT NULL" in store._get_create_events_table_sql()


def test_sync_store_uses_tsql_upsert_and_top_limit() -> None:
    """Write/read SQL uses MERGE for scoped upserts and TOP for limited reads."""

    store = MssqlPythonSyncADKStore(_mock_config())

    app_upsert = store._get_upsert_app_state_sql()
    events_sql, params = store._get_events_query("app", "user", "session", limit=5)

    assert "MERGE INTO [dbo].[adk_app_state]" in app_upsert
    assert "WITH (HOLDLOCK)" in app_upsert
    assert "SELECT TOP (?)" in events_sql
    assert params[0] == 5
    assert "LIMIT" not in events_sql


@pytest.mark.anyio
async def test_async_store_generates_same_tsql_schema() -> None:
    """Async store DDL mirrors the sync store's T-SQL schema."""

    store = MssqlPythonAsyncADKStore(_mock_config())

    sessions_sql = await store._get_create_sessions_table_sql()
    events_sql = await store._get_create_events_table_sql()

    assert "IF NOT EXISTS (SELECT 1 FROM sys.tables" in sessions_sql
    assert "row_id UNIQUEIDENTIFIER NOT NULL" in sessions_sql
    assert "state NVARCHAR(MAX) NOT NULL" in sessions_sql
    assert "ON DELETE CASCADE" in events_sql
