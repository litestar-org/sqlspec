"""Unit tests for mssql_python ADK store DDL and registration."""

from types import SimpleNamespace
from typing import Any, cast

import pytest

from sqlspec.adapters.mssql_python import MssqlPythonAsyncConfig, MssqlPythonConfig
from sqlspec.adapters.mssql_python.adk import MssqlPythonADKStore, MssqlPythonAsyncADKStore
from sqlspec.extensions.adk._config_utils import _get_adk_adapter_store_class


def _config(extension_config: dict[str, Any] | None = None) -> Any:
    return SimpleNamespace(extension_config=extension_config or {"adk": {}})


@pytest.mark.anyio
async def test_async_adk_store_uses_tsql_idempotent_ddl() -> None:
    """ADK table DDL should use T-SQL idempotency and data types."""
    store = MssqlPythonAsyncADKStore(cast(MssqlPythonAsyncConfig, _config()))
    store._state_column_type = "NVARCHAR(MAX)"

    sessions_sql = await store._get_create_sessions_table_sql()
    events_sql = await store._get_create_events_table_sql()

    assert "IF OBJECT_ID(N'dbo.adk_session', N'U') IS NULL" in sessions_sql
    assert "state NVARCHAR(MAX) NOT NULL" in sessions_sql
    assert "DATETIME2(6)" in sessions_sql
    assert "SYSUTCDATETIME()" in sessions_sql
    assert "FOREIGN KEY (session_id) REFERENCES adk_session(id) ON DELETE CASCADE" in events_sql


@pytest.mark.anyio
async def test_sync_config_adk_store_name_resolves_for_migrations() -> None:
    """ADK migration lookup should resolve MssqlPythonConfig to MssqlPythonADKStore."""
    config = MssqlPythonConfig(connection_config={"server": "localhost"})

    store_type = _get_adk_adapter_store_class(config, "ADKStore")

    assert store_type is MssqlPythonADKStore


@pytest.mark.anyio
async def test_adk_drop_tables_uses_tsql_guards() -> None:
    """Drop statements should be FK-safe and idempotent in T-SQL."""
    store = MssqlPythonAsyncADKStore(cast(MssqlPythonAsyncConfig, _config()))

    statements = store._get_drop_tables_sql()

    assert statements[0].startswith("IF OBJECT_ID(N'dbo.adk_internal_metadata'")
    assert statements[-2].startswith("IF OBJECT_ID(N'dbo.adk_event'")
    assert statements[-1].startswith("IF OBJECT_ID(N'dbo.adk_session'")
