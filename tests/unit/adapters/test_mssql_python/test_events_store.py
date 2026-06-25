"""Unit tests for mssql_python event queue store DDL."""

from types import SimpleNamespace
from typing import Any, cast

from sqlspec.adapters.mssql_python import MssqlPythonConfig
from sqlspec.adapters.mssql_python.events.store import MssqlPythonEventQueueStore, _object_name


def _config(extension_config: dict[str, Any] | None = None) -> Any:
    return SimpleNamespace(extension_config=extension_config or {"events": {}})


def test_event_queue_store_uses_tsql_column_types_and_idempotency() -> None:
    """Event queue DDL should use T-SQL types and sys catalog guards."""
    store = MssqlPythonEventQueueStore(cast(MssqlPythonConfig, _config()))

    statements = store.create_statements()
    ddl = "\n".join(statements)

    assert "IF OBJECT_ID(N'[dbo].[sqlspec_event_queue]', N'U') IS NULL" in ddl
    assert "payload_json NVARCHAR(MAX) NOT NULL" in ddl
    assert "available_at DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME()" in ddl
    assert "IF NOT EXISTS (SELECT 1 FROM sys.indexes" in ddl


def test_event_queue_store_drop_uses_object_id_guard() -> None:
    """Event queue drop DDL should be idempotent for SQL Server."""
    store = MssqlPythonEventQueueStore(cast(MssqlPythonConfig, _config()))

    assert store.drop_statements() == [
        "IF OBJECT_ID(N'[dbo].[sqlspec_event_queue]', N'U') IS NOT NULL DROP TABLE sqlspec_event_queue;"
    ]


def test_object_name_preserves_bracket_quoted_dots() -> None:
    assert _object_name("[dbo.schema].[sqlspec.event.queue]") == "[dbo.schema].[sqlspec.event.queue]"
