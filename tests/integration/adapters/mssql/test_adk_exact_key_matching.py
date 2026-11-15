"""SQL Server ADK stores match owner keys exactly regardless of server collation."""

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest
from pytest_databases.docker.mssql import MSSQLService

from sqlspec.adapters.mssql_python import MssqlPythonConfig
from sqlspec.adapters.mssql_python.adk import MssqlPythonADKMemoryStore, MssqlPythonADKStore
from sqlspec.adapters.pymssql import PymssqlConfig
from sqlspec.adapters.pymssql.adk import PymssqlADKMemoryStore, PymssqlADKStore
from sqlspec.extensions.adk import StoredMemory
from tests.integration.fixtures.mssql import _mssql_connection_config, _mssql_python_connection_config

pytestmark = [pytest.mark.mssql, pytest.mark.xdist_group("mssql")]


def _stores(mssql_service: MSSQLService, adapter: str, suffix: str) -> "tuple[Any, Any, Any]":
    settings = {
        "session_table": f"adk_session_{suffix}",
        "events_table": f"adk_event_{suffix}",
        "app_state_table": f"adk_app_state_{suffix}",
        "user_state_table": f"adk_user_state_{suffix}",
        "memory_table": f"adk_memory_{suffix}",
        "enable_memory": True,
    }
    if adapter == "mssql_python":
        config: Any = MssqlPythonConfig(
            connection_config=_mssql_python_connection_config(mssql_service), extension_config={"adk": settings}
        )
        return config, MssqlPythonADKStore(config), MssqlPythonADKMemoryStore(config)
    config = PymssqlConfig(
        connection_config=_mssql_connection_config(mssql_service), extension_config={"adk": settings}
    )
    return config, PymssqlADKStore(config), PymssqlADKMemoryStore(config)


def _memory(session_id: str, user_id: str, event_id: str, text: str) -> StoredMemory:
    now = datetime(2026, 1, 1, tzinfo=UTC).replace(tzinfo=None)
    return StoredMemory(
        id=uuid4().hex,
        session_id=session_id,
        app_name="app",
        user_id=user_id,
        scope="user",
        event_id=event_id,
        author="user",
        timestamp=now,
        content_json={"text": text},
        content_text=text,
        metadata_json=None,
        inserted_at=now,
        embedding=None,
    )


@pytest.mark.parametrize("adapter", ["mssql_python", "pymssql"])
def test_mixed_case_owners_stay_separate(mssql_service: MSSQLService, adapter: str) -> None:
    config, store, memory = _stores(mssql_service, adapter, uuid4().hex[:8])
    try:
        store.create_tables()
        memory.create_tables()
        store.create_session("S1", "app", "alice", {"k": "alice"})
        store.create_session("S2", "app", "Alice", {"k": "Alice"})

        assert store.get_session("app", "Alice", "S1") is None
        assert [session["id"] for session in store.list_sessions("app", "alice")] == ["S1"]

        store.delete_session("app", "Alice", "S1")
        assert store.get_session("app", "alice", "S1") is not None

        memory.insert_memory_entries([_memory("S1", "alice", "e1", "alice latte")])
        assert memory.search_entries("latte", "app", "ALICE") == []
        assert len(memory.search_entries("latte", "app", "alice")) == 1
        assert memory.delete_entries_by_session("S1", "app", "ALICE") == 0
        assert memory.delete_entries_by_session("S1", "app", "alice") == 1
    finally:
        config.close_pool()
