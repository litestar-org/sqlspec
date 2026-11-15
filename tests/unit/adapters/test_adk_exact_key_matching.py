"""Exact key matching in ADK stores on case-insensitive database engines."""

import importlib
import re
from contextlib import nullcontext
from typing import Any
from unittest.mock import MagicMock

import pytest

pytest.importorskip("google.adk")

_MSSQL = "col = ? AND col = ? COLLATE Latin1_General_100_BIN2"
_PYMSSQL = "col = %s AND col = %s COLLATE Latin1_General_100_BIN2"
_SYBASE = "col = %s AND CONVERT(VARBINARY(512), col) = CONVERT(VARBINARY(512), CONVERT(VARCHAR(128), %s))"
_SQLANYWHERE = "col = ? AND CAST(col AS BINARY(512)) = CAST(CAST(? AS VARCHAR(128)) AS BINARY(512))"
_TERADATA = "col = ? AND col (CASESPECIFIC) = ?"

STORES = [
    pytest.param(
        "sqlspec.adapters.mssql_python",
        "MssqlPythonConfig",
        "MssqlPythonADKStore",
        "MssqlPythonADKMemoryStore",
        _MSSQL,
        id="mssql_python",
    ),
    pytest.param(
        "sqlspec.adapters.pymssql", "PymssqlConfig", "PymssqlADKStore", "PymssqlADKMemoryStore", _PYMSSQL, id="pymssql"
    ),
    pytest.param(
        "sqlspec.adapters.sybase", "SybaseConfig", "SybaseADKStore", "SybaseADKMemoryStore", _SYBASE, id="sybase"
    ),
    pytest.param(
        "sqlspec.adapters.sqlanywhere",
        "SqlanywhereConfig",
        "SqlanywhereADKStore",
        "SqlanywhereADKMemoryStore",
        _SQLANYWHERE,
        id="sqlanywhere",
    ),
    pytest.param(
        "sqlspec.adapters.teradata",
        "TeradataConfig",
        "TeradataADKStore",
        "TeradataADKMemoryStore",
        _TERADATA,
        id="teradata",
    ),
]


def _predicate(template: str, column: str) -> str:
    return template.replace("col", column)


def _load(module: str, config_name: str, store_name: str, memory_name: str) -> "tuple[type, type, type]":
    adapter = importlib.import_module(module)
    adk = importlib.import_module(f"{module}.adk")
    return getattr(adapter, config_name), getattr(adk, store_name), getattr(adk, memory_name)


def _recording_config(config_cls: type) -> "tuple[Any, Any]":
    config = MagicMock(spec=config_cls)
    config.extension_config = {"adk": {"enable_memory": True}}
    connection = MagicMock()
    cursor = connection.cursor.return_value
    cursor.fetchone.return_value = None
    cursor.fetchall.return_value = []
    cursor.rowcount = 0
    config.provide_connection.side_effect = lambda *a, **k: nullcontext(connection)
    config.provide_read_connection = config.provide_connection
    return config, cursor


def _statements(cursor: Any) -> "list[tuple[str, tuple[Any, ...]]]":
    calls = list(cursor.execute.call_args_list) + list(cursor.executemany.call_args_list)
    return [(" ".join(call.args[0].split()), tuple(call.args[1]) if len(call.args) > 1 else ()) for call in calls]


def _find(statements: "list[tuple[str, tuple[Any, ...]]]", prefix: str) -> "tuple[str, tuple[Any, ...]]":
    return next(item for item in statements if item[0].startswith(prefix))


@pytest.mark.parametrize(("module", "config_name", "store_name", "memory_name", "template"), STORES)
def test_get_session_sql_matches_keys_exactly(
    module: str, config_name: str, store_name: str, memory_name: str, template: str
) -> None:
    config_cls, store_cls, _ = _load(module, config_name, store_name, memory_name)
    config, cursor = _recording_config(config_cls)

    store_cls(config).get_session("app", "alice", "s1")

    sql, params = _find(_statements(cursor), "SELECT")
    for column in ("app_name", "user_id", "id"):
        assert _predicate(template, column) in sql
    assert params == ("app", "app", "alice", "alice", "s1", "s1")


@pytest.mark.parametrize(("module", "config_name", "store_name", "memory_name", "template"), STORES)
def test_delete_session_sql_matches_keys_exactly(
    module: str, config_name: str, store_name: str, memory_name: str, template: str
) -> None:
    config_cls, store_cls, _ = _load(module, config_name, store_name, memory_name)
    config, cursor = _recording_config(config_cls)

    store_cls(config).delete_session("app", "alice", "s1")

    statements = _statements(cursor)
    events_sql, events_params = next(
        item for item in statements if item[0].startswith("DELETE") and "session_id" in item[0]
    )
    session_sql, session_params = next(
        item for item in statements if item[0].startswith("DELETE") and "session_id" not in item[0]
    )
    for column in ("app_name", "user_id", "session_id"):
        assert _predicate(template, column) in events_sql
    for column in ("app_name", "user_id", "id"):
        assert _predicate(template, column) in session_sql
    assert events_params == session_params == ("app", "app", "alice", "alice", "s1", "s1")


@pytest.mark.parametrize(("module", "config_name", "store_name", "memory_name", "template"), STORES)
def test_memory_search_and_delete_match_owner_exactly(
    module: str, config_name: str, store_name: str, memory_name: str, template: str
) -> None:
    config_cls, _, memory_cls = _load(module, config_name, store_name, memory_name)
    config, cursor = _recording_config(config_cls)
    memory = memory_cls(config)

    memory.search_entries("latte", "app", "alice", scope_filter="user")
    memory.delete_entries_by_session("s1", "app", "alice")

    statements = _statements(cursor)
    search_sql, search_params = next(item for item in statements if item[0].startswith("SELECT"))
    delete_sql, delete_params = next(item for item in statements if item[0].startswith("DELETE"))
    for column in ("app_name", "user_id"):
        assert _predicate(template, column) in search_sql
    for column in ("session_id", "app_name", "user_id"):
        assert _predicate(template, column) in delete_sql
    assert ("app", "app", "alice", "alice") == tuple(p for p in search_params if p in {"app", "alice"})
    assert delete_params == ("s1", "s1", "app", "app", "alice", "alice")


@pytest.mark.parametrize(
    ("module", "config_name", "store_name", "memory_name", "marker"),
    [
        pytest.param(
            "sqlspec.adapters.mssql_python",
            "MssqlPythonConfig",
            "MssqlPythonADKStore",
            "MssqlPythonADKMemoryStore",
            "Latin1_General_100_BIN2",
            id="mssql_python",
        ),
        pytest.param(
            "sqlspec.adapters.pymssql",
            "PymssqlConfig",
            "PymssqlADKStore",
            "PymssqlADKMemoryStore",
            "Latin1_General_100_BIN2",
            id="pymssql",
        ),
        pytest.param(
            "sqlspec.adapters.teradata",
            "TeradataConfig",
            "TeradataADKStore",
            "TeradataADKMemoryStore",
            "CASESPECIFIC",
            id="teradata",
        ),
    ],
)
def test_new_table_ddl_declares_exact_collation(
    module: str, config_name: str, store_name: str, memory_name: str, marker: str
) -> None:
    config_cls, store_cls, memory_cls = _load(module, config_name, store_name, memory_name)
    config, _ = _recording_config(config_cls)
    config.extension_config = {"adk": {"enable_memory": True, "native_json": False}}
    store = store_cls(config)
    memory = memory_cls(config)

    ddl = [store._sessions_table_ddl(), store._events_table_ddl(), memory._memory_table_ddl()]

    for statement in ddl:
        for column in ("id", "app_name", "user_id"):
            definition = re.search(rf"(?<![\w])\"?{column} N?VARCHAR\(128\)[^,]*", statement)
            assert definition is not None
            assert marker in definition.group(0)
