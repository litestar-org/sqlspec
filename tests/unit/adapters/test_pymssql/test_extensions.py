"""pymssql extension package tests."""

import pytest

from sqlspec.adapters.pymssql.config import PymssqlConfig


def test_event_store_uses_tsql_column_types_and_idempotent_wrappers() -> None:
    """Event queue DDL should use SQL Server types and object-existence guards."""
    from sqlspec.adapters.pymssql.events.store import PymssqlEventQueueStore

    store = PymssqlEventQueueStore(PymssqlConfig(extension_config={"events": {"queue_table": "event_queue"}}))

    assert store._column_types() == ("NVARCHAR(MAX)", "NVARCHAR(MAX)", "DATETIME2(6)")
    assert store._timestamp_default() == "SYSUTCDATETIME()"
    assert "OBJECT_ID" in store._wrap_create_statement("CREATE TABLE event_queue (id INT)", "table")
    assert "sys.indexes" in store._wrap_create_statement("CREATE INDEX idx_events ON event_queue (channel)", "index")


def test_litestar_store_ddl_is_tsql_idempotent() -> None:
    """Litestar store DDL should be SQL Server-specific and idempotent."""
    from sqlspec.adapters.pymssql.litestar.store import PymssqlStore

    store = PymssqlStore(PymssqlConfig(extension_config={"litestar": {"session_table": "litestar_session"}}))
    ddl = store._get_create_table_sql()

    assert "IF NOT EXISTS" in ddl
    assert "CREATE TABLE litestar_session" in ddl
    assert "VARBINARY(MAX)" in ddl
    assert "SYSUTCDATETIME()" in ddl
    assert "%s" not in ddl


@pytest.mark.anyio
async def test_litestar_store_async_methods_bridge_sync_operations(monkeypatch: pytest.MonkeyPatch) -> None:
    """The async Litestar interface should bridge to sync methods through async_."""
    from sqlspec.adapters.pymssql.litestar.store import PymssqlStore

    calls: list[str] = []
    monkeypatch.setattr(PymssqlStore, "_create_table", lambda self: calls.append("create"))
    store = PymssqlStore(PymssqlConfig(extension_config={"litestar": {"session_table": "litestar_session"}}))

    await store.create_table()

    assert calls == ["create"]


def test_adk_store_ddl_uses_tsql_tables_and_json_fallback() -> None:
    """ADK DDL should use T-SQL table shape and NVARCHAR JSON fallback by default."""
    from sqlspec.adapters.pymssql.adk.store import PymssqlADKStore

    store = PymssqlADKStore(PymssqlConfig(extension_config={"adk": {}}))

    sessions_ddl = store._get_create_sessions_table_sql()
    events_ddl = store._get_create_events_table_sql()

    assert "CREATE TABLE" in sessions_ddl
    assert "NVARCHAR(MAX)" in sessions_ddl
    assert "SYSUTCDATETIME()" in sessions_ddl
    assert "event_data" in events_ddl
    assert "DATETIME2(6)" in events_ddl


def test_adk_store_can_force_native_json_column_type() -> None:
    """ADK config should allow native SQL Server JSON columns when requested."""
    from sqlspec.adapters.pymssql.adk.store import PymssqlADKStore

    store = PymssqlADKStore(PymssqlConfig(extension_config={"adk": {"native_json": True}}))

    assert "state JSON NOT NULL" in store._get_create_sessions_table_sql()
