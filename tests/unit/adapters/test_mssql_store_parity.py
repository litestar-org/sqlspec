"""Cross-driver SQL parity between the mssql-python and pymssql extension stores."""

from typing import Any
from unittest.mock import MagicMock

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


def test_create_mapped_exception_ast_parity() -> None:
    """create_mapped_exception in mssql_python.core and pymssql.core must remain AST-equivalent."""
    import ast
    import inspect

    from sqlspec.adapters.mssql_python import core as mssql_core
    from sqlspec.adapters.pymssql import core as pymssql_core

    mssql_ast = ast.dump(ast.parse(inspect.getsource(mssql_core.create_mapped_exception)))
    pymssql_ast = ast.dump(ast.parse(inspect.getsource(pymssql_core.create_mapped_exception)))
    assert mssql_ast == pymssql_ast
    assert mssql_core._ERROR_CODE_MAPPING == pymssql_core._ERROR_CODE_MAPPING


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


def _qmark(sql: str) -> str:
    return sql.replace("%s", "?")


def _mock_session(monkeypatch: pytest.MonkeyPatch, config_cls: type[Any], driver: MagicMock) -> None:
    session_cm = MagicMock()
    session_cm.__enter__.return_value = driver
    session_cm.__exit__.return_value = False
    monkeypatch.setattr(config_cls, "provide_session", lambda _self: session_cm)


@pytest.mark.parametrize(
    ("store_cls", "config_cls"), [(MssqlPythonADKStore, MssqlPythonConfig), (PymssqlADKStore, PymssqlConfig)]
)
def test_adk_create_tables_creates_index_owned_by_other_table(
    store_cls: type[Any], config_cls: type[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A required index is created when an identically named index belongs to another table."""
    config = config_cls(extension_config={"adk": {"native_json": False}})
    driver = MagicMock()
    driver.data_dictionary.get_indexes.return_value = [
        {"schema_name": "dbo", "table_name": "other_table", "index_name": "idx_adk_session_app_user"}
    ]
    _mock_session(monkeypatch, config_cls, driver)
    monkeypatch.setattr(store_cls, "reconcile_schema", lambda _self, *, assume_existing=False: None)

    store = store_cls(config)
    store.create_tables()

    executed = [str(call.args[0]) for call in driver.execute.call_args_list]
    assert any("CREATE INDEX [idx_adk_session_app_user] ON [dbo].[adk_session]" in stmt for stmt in executed)


@pytest.mark.parametrize(
    ("store_cls", "config_cls"), [(MssqlPythonADKStore, MssqlPythonConfig), (PymssqlADKStore, PymssqlConfig)]
)
def test_adk_create_tables_skips_index_on_exact_target(
    store_cls: type[Any], config_cls: type[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """An index already existing on the exact target table is skipped case-insensitively."""
    config = config_cls(extension_config={"adk": {"native_json": False}})
    driver = MagicMock()
    driver.data_dictionary.get_indexes.return_value = [
        {"schema_name": "DBO", "table_name": "ADK_SESSION", "index_name": "IDX_ADK_SESSION_APP_USER"}
    ]
    _mock_session(monkeypatch, config_cls, driver)
    monkeypatch.setattr(store_cls, "reconcile_schema", lambda _self, *, assume_existing=False: None)

    store = store_cls(config)
    store.create_tables()

    executed = [str(call.args[0]) for call in driver.execute.call_args_list]
    assert not any("idx_adk_session_app_user" in stmt for stmt in executed)
    assert any("CREATE INDEX [idx_adk_session_update_time] ON [dbo].[adk_session]" in stmt for stmt in executed)


@pytest.mark.parametrize(
    ("store_cls", "config_cls"), [(MssqlPythonADKStore, MssqlPythonConfig), (PymssqlADKStore, PymssqlConfig)]
)
def test_adk_create_tables_uses_queried_schema_for_rows_without_schema(
    store_cls: type[Any], config_cls: type[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Catalog rows omitting schema_name default to the queried schema."""
    config = config_cls(extension_config={"adk": {"native_json": False}})
    driver = MagicMock()
    driver.data_dictionary.get_indexes.return_value = [
        {"table_name": "adk_session", "index_name": "idx_adk_session_app_user"}
    ]
    _mock_session(monkeypatch, config_cls, driver)
    monkeypatch.setattr(store_cls, "reconcile_schema", lambda _self, *, assume_existing=False: None)

    store = store_cls(config)
    store.create_tables()

    executed = [str(call.args[0]) for call in driver.execute.call_args_list]
    assert not any("idx_adk_session_app_user" in stmt for stmt in executed)


@pytest.mark.parametrize(
    ("store_cls", "config_cls"), [(MssqlPythonADKStore, MssqlPythonConfig), (PymssqlADKStore, PymssqlConfig)]
)
def test_adk_create_tables_keeps_configured_schemas_distinct(
    store_cls: type[Any], config_cls: type[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Distinct configured schemas do not cross-contaminate index existence checks."""
    config = config_cls(
        extension_config={
            "adk": {"native_json": False, "session_table": "app.sessions", "events_table": "audit.events"}
        }
    )
    driver = MagicMock()

    def mock_get_indexes(_driver: Any, schema: str | None = None) -> list[dict[str, Any]]:
        if schema == "audit":
            return [{"schema_name": "audit", "table_name": "sessions", "index_name": "idx_sessions_app_user"}]
        return []

    driver.data_dictionary.get_indexes.side_effect = mock_get_indexes
    _mock_session(monkeypatch, config_cls, driver)
    monkeypatch.setattr(store_cls, "reconcile_schema", lambda _self, *, assume_existing=False: None)

    store = store_cls(config)
    store.create_tables()

    executed = [str(call.args[0]) for call in driver.execute.call_args_list]
    assert any("CREATE INDEX [idx_sessions_app_user] ON [app].[sessions]" in stmt for stmt in executed)


@pytest.mark.parametrize(
    ("store_cls", "config_cls"), [(MssqlPythonADKStore, MssqlPythonConfig), (PymssqlADKStore, PymssqlConfig)]
)
def test_adk_bracketed_identifiers_normalize_in_helpers_and_catalog_rows(
    store_cls: type[Any], config_cls: type[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bracketed table and index names normalize cleanly across helpers and catalog output."""
    from sqlspec.adapters.arrow_odbc.adk.store import _split_tsql_table
    from sqlspec.adapters.mssql_python.core import split_schema_and_table as mssql_split
    from sqlspec.adapters.pymssql.core import split_schema_and_table as pymssql_split

    assert mssql_split("[app].[sessions]") == ("app", "sessions")
    assert pymssql_split("[app].[sessions]") == ("app", "sessions")
    assert _split_tsql_table("[app].[sessions]") == ("app", "sessions")

    config = config_cls(extension_config={"adk": {"native_json": False, "session_table": "app.sessions"}})
    driver = MagicMock()
    driver.data_dictionary.get_indexes.return_value = [
        {"schema_name": "[app]", "table_name": "[sessions]", "index_name": "[idx_sessions_app_user]"}
    ]
    _mock_session(monkeypatch, config_cls, driver)
    monkeypatch.setattr(store_cls, "reconcile_schema", lambda _self, *, assume_existing=False: None)

    store = store_cls(config)
    store.create_tables()

    executed = [str(call.args[0]) for call in driver.execute.call_args_list]
    assert not any("idx_sessions_app_user" in stmt for stmt in executed)


@pytest.mark.parametrize(
    ("memory_cls", "config_cls"),
    [(MssqlPythonADKMemoryStore, MssqlPythonConfig), (PymssqlADKMemoryStore, PymssqlConfig)],
)
def test_adk_memory_create_tables_creates_index_owned_by_other_table(
    memory_cls: type[Any], config_cls: type[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Memory store creates its index when an unrelated table owns the same index name."""
    config = config_cls(extension_config={"adk": {"native_json": False, "enable_memory": True}})
    driver = MagicMock()
    driver.data_dictionary.get_indexes.return_value = [
        {"schema_name": "dbo", "table_name": "other_table", "index_name": "idx_adk_memory_session"}
    ]
    _mock_session(monkeypatch, config_cls, driver)
    monkeypatch.setattr(memory_cls, "reconcile_schema", lambda _self, *, assume_existing=False: None)

    store = memory_cls(config)
    store.create_tables()

    executed = [str(call.args[0]) for call in driver.execute.call_args_list]
    assert any("CREATE INDEX [idx_adk_memory_session] ON [dbo].[adk_memory]" in stmt for stmt in executed)


def test_mssql_stores_companion_index_and_schema_target_parity() -> None:
    """Litestar SchemaTarget and CompanionIndex definitions match across pymssql and mssql-python."""
    mssql_store = MssqlPythonStore(MssqlPythonConfig(extension_config={"litestar": {}}))
    pymssql_store = PymssqlStore(PymssqlConfig(extension_config={"litestar": {}}))

    mssql_targets = mssql_store.get_schema_targets()
    pymssql_targets = pymssql_store.get_schema_targets()

    assert len(mssql_targets) == 1
    assert len(pymssql_targets) == 1
    assert mssql_targets[0].table_name == pymssql_targets[0].table_name == "litestar_session"
    assert mssql_targets[0].schema == pymssql_targets[0].schema == "dbo"
    assert [col.name for col in mssql_targets[0].create_table.columns] == [
        col.name for col in pymssql_targets[0].create_table.columns
    ]
    assert mssql_targets[0].constraint_columns == pymssql_targets[0].constraint_columns == frozenset({"session_id"})

    mssql_indexes = mssql_store.get_companion_indexes()
    pymssql_indexes = pymssql_store.get_companion_indexes()
    assert mssql_indexes == pymssql_indexes
    assert len(mssql_indexes) == 1
    assert mssql_indexes[0].name == "IX_litestar_session_expires_at"
    assert mssql_indexes[0].table == "litestar_session"
    assert mssql_indexes[0].schema == "dbo"
