# pyright: reportPrivateUsage=false
"""Cross-driver SQL parity between the mssql-python and pymssql extension stores."""

from typing import Any

import pytest

pytest.importorskip("mssql_python")
pytest.importorskip("pymssql")

from sqlspec.adapters.mssql_python import MssqlPythonConfig
from sqlspec.adapters.mssql_python.adk import MssqlPythonADKMemoryStore, MssqlPythonADKStore
from sqlspec.adapters.mssql_python.events import MssqlPythonEventQueueStore
from sqlspec.adapters.mssql_python.litestar import MssqlPythonStore
from sqlspec.adapters.pymssql import PymssqlConfig
from sqlspec.adapters.pymssql.adk import PymssqlADKMemoryStore, PymssqlADKStore
from sqlspec.adapters.pymssql.events import PymssqlEventQueueStore
from sqlspec.adapters.pymssql.litestar import PymssqlStore


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


def _qmark(sql: str) -> str:
    return sql.replace("%s", "?")


def test_adk_session_and_event_ddl_match() -> None:
    """ADK session and event table DDL and indexes match across drivers."""
    mssql_cfg = MssqlPythonConfig(extension_config={"adk": {"native_json": False}})
    pymssql_cfg = PymssqlConfig(extension_config={"adk": {}})
    mssql_store = MssqlPythonADKStore(mssql_cfg)
    pymssql_store = PymssqlADKStore(pymssql_cfg)

    assert _normalized(mssql_store._sessions_table_ddl()) == _normalized(pymssql_store._sessions_table_ddl())
    assert _normalized(mssql_store._events_table_ddl()) == _normalized(pymssql_store._events_table_ddl())
    assert mssql_store._index_specs() == pymssql_store._index_specs()
    assert mssql_store._drop_tables_sql() == pymssql_store._drop_tables_sql()


def test_adk_memory_ddl_and_indexes_match() -> None:
    """ADK memory table DDL and indexes match across drivers."""
    mssql_cfg = MssqlPythonConfig(extension_config={"adk": {"native_json": False}})
    pymssql_cfg = PymssqlConfig(extension_config={"adk": {}})
    mssql_memory = MssqlPythonADKMemoryStore(mssql_cfg)
    pymssql_memory = PymssqlADKMemoryStore(pymssql_cfg)

    assert _normalized(mssql_memory._memory_table_ddl()) == _normalized(pymssql_memory._memory_table_ddl())
    assert mssql_memory._memory_index_specs() == pymssql_memory._memory_index_specs()
    assert mssql_memory._drop_memory_table_sql() == pymssql_memory._drop_memory_table_sql()


def test_adk_session_list_query_matches_modulo_placeholder(monkeypatch: "pytest.MonkeyPatch") -> None:
    """ADK session list SQL matches modulo parameter placeholder style."""
    mssql_cfg = MssqlPythonConfig(extension_config={"adk": {"native_json": False}})
    pymssql_cfg = PymssqlConfig(extension_config={"adk": {}})
    calls: dict[str, tuple[str, tuple[Any, ...]]] = {}

    def capture_mssql(_store: Any, sql: str, params: "tuple[Any, ...]" = ()) -> "list[Any]":
        calls["mssql"] = (sql, tuple(params))
        return []

    def capture_pymssql(_store: Any, sql: str, params: "tuple[Any, ...]" = ()) -> "list[Any]":
        calls["pymssql"] = (sql, tuple(params))
        return []

    monkeypatch.setattr(MssqlPythonADKStore, "_execute_fetchall", capture_mssql)
    monkeypatch.setattr(PymssqlADKStore, "_execute_fetchall", capture_pymssql)
    MssqlPythonADKStore(mssql_cfg).list_sessions("app", "u1", limit=10, offset=20)
    PymssqlADKStore(pymssql_cfg).list_sessions("app", "u1", limit=10, offset=20)

    assert _normalized(calls["mssql"][0]) == _normalized(_qmark(calls["pymssql"][0]))
    assert calls["mssql"][1] == calls["pymssql"][1]


def test_event_queue_ddl_matches() -> None:
    """Event queue DDL statements match across drivers."""
    mssql_cfg = MssqlPythonConfig(extension_config={"events": {}})
    pymssql_cfg = PymssqlConfig(extension_config={"events": {}})

    assert (
        MssqlPythonEventQueueStore(mssql_cfg).create_statements()
        == PymssqlEventQueueStore(pymssql_cfg).create_statements()
    )
    assert (
        MssqlPythonEventQueueStore(mssql_cfg).drop_statements() == PymssqlEventQueueStore(pymssql_cfg).drop_statements()
    )


def test_litestar_store_ddl_matches() -> None:
    """Litestar session store DDL matches across drivers."""
    mssql_cfg = MssqlPythonConfig(extension_config={"litestar": {}})
    pymssql_cfg = PymssqlConfig(extension_config={"litestar": {}})

    assert _normalized(MssqlPythonStore(mssql_cfg)._table_ddl()) == _normalized(PymssqlStore(pymssql_cfg)._table_ddl())
    assert MssqlPythonStore(mssql_cfg)._drop_table_sql() == PymssqlStore(pymssql_cfg)._drop_table_sql()
