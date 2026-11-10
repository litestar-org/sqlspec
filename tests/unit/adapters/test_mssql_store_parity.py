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
from sqlspec.data_dictionary.dialects.mssql import MssqlVersionInfo
from sqlspec.migrations.schema import ensure_schema_sync


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


@pytest.mark.parametrize(
    ("session_table", "expected_schema", "expected_table"),
    [
        ("litestar_session", "dbo", "litestar_session"),
        ("auth.user_sessions", "auth", "user_sessions"),
        ("[auth].[user_sessions]", "auth", "user_sessions"),
    ],
)
def test_mssql_stores_accept_qualified_session_table_names(
    session_table: str, expected_schema: str, expected_table: str
) -> None:
    """Both SQL Server Litestar stores accept 1- and 2-part session_table names and parse schema metadata."""
    mssql_store = MssqlPythonStore(MssqlPythonConfig(extension_config={"litestar": {"session_table": session_table}}))
    pymssql_store = PymssqlStore(PymssqlConfig(extension_config={"litestar": {"session_table": session_table}}))

    for store in (mssql_store, pymssql_store):
        target = store.get_schema_targets()[0]
        index = store.get_companion_indexes()[0]
        assert (target.schema, target.table_name) == (expected_schema, expected_table)
        assert (index.schema, index.table, index.name) == (
            expected_schema,
            expected_table,
            f"IX_{expected_table}_expires_at",
        )


@pytest.mark.parametrize(
    "invalid_name",
    [
        "",
        "   ",
        "a.b.c",
        "app..sess",
        ".sess",
        "sess.",
        "[unclosed",
        "bad;name",
        "[bad;name]",
        "x" * 64,
        f"auth.{'x' * 64}",
    ],
)
def test_mssql_stores_reject_invalid_qualified_session_table_names(invalid_name: str) -> None:
    """Both SQL Server Litestar stores raise ValueError on invalid session_table names."""
    with pytest.raises(ValueError):
        MssqlPythonStore(MssqlPythonConfig(extension_config={"litestar": {"session_table": invalid_name}}))
    with pytest.raises(ValueError):
        PymssqlStore(PymssqlConfig(extension_config={"litestar": {"session_table": invalid_name}}))


def _default_adk_index_rows(*, include_update_time: bool = True, uppercase: bool = False) -> list[dict[str, str]]:
    rows = [
        {"schema_name": "dbo", "table_name": "adk_session", "index_name": "idx_adk_session_app_user"},
        {"schema_name": "dbo", "table_name": "adk_event", "index_name": "idx_adk_event_scope"},
        {"schema_name": "dbo", "table_name": "adk_event", "index_name": "idx_adk_event_session"},
        {"schema_name": "dbo", "table_name": "adk_event", "index_name": "idx_adk_event_invocation"},
        {"schema_name": "dbo", "table_name": "adk_event", "index_name": "idx_adk_event_timestamp"},
        {"schema_name": "dbo", "table_name": "adk_event", "index_name": "idx_adk_event_app_timestamp"},
    ]
    if include_update_time:
        rows.append({"schema_name": "dbo", "table_name": "adk_session", "index_name": "idx_adk_session_update_time"})
    if uppercase:
        return [{key: val.upper() for key, val in row.items()} for row in rows]
    return rows


@pytest.mark.parametrize(
    ("store_cls", "config_cls"), [(MssqlPythonADKStore, MssqlPythonConfig), (PymssqlADKStore, PymssqlConfig)]
)
def test_adk_companions_create_only_missing_index(store_cls: type[Any], config_cls: type[Any]) -> None:
    """reconcile_companion_objects_sync creates only the missing index without calling commit."""
    store = store_cls(config_cls(extension_config={"adk": {}}))
    driver = MagicMock()
    driver.data_dictionary.get_indexes.return_value = _default_adk_index_rows(include_update_time=False)

    store.reconcile_companion_objects_sync(driver)

    executed = [str(call.args[0]) for call in driver.execute.call_args_list]
    assert executed == ["CREATE INDEX [idx_adk_session_update_time] ON [dbo].[adk_session] (update_time DESC)"]
    assert driver.commit.call_count == 0


@pytest.mark.parametrize(
    ("store_cls", "config_cls"), [(MssqlPythonADKStore, MssqlPythonConfig), (PymssqlADKStore, PymssqlConfig)]
)
def test_adk_companions_repeat_is_noop(store_cls: type[Any], config_cls: type[Any]) -> None:
    """Upper-cased catalog rows match case-insensitively and emit zero CREATE INDEX calls."""
    store = store_cls(config_cls(extension_config={"adk": {}}))
    driver = MagicMock()
    driver.data_dictionary.get_indexes.return_value = _default_adk_index_rows(include_update_time=True, uppercase=True)

    store.reconcile_companion_objects_sync(driver)

    assert driver.execute.call_count == 0
    assert driver.commit.call_count == 0


@pytest.mark.parametrize(
    ("store_cls", "config_cls"), [(MssqlPythonADKStore, MssqlPythonConfig), (PymssqlADKStore, PymssqlConfig)]
)
def test_adk_companions_create_index_owned_by_other_table(store_cls: type[Any], config_cls: type[Any]) -> None:
    """An identically named index on another table does not suppress companion creation."""
    store = store_cls(config_cls(extension_config={"adk": {}}))
    driver = MagicMock()
    driver.data_dictionary.get_indexes.return_value = [
        {"schema_name": "dbo", "table_name": "other_table", "index_name": "idx_adk_session_app_user"}
    ]

    store.reconcile_companion_objects_sync(driver)

    executed = [str(call.args[0]) for call in driver.execute.call_args_list]
    assert "CREATE INDEX [idx_adk_session_app_user] ON [dbo].[adk_session] (app_name, user_id)" in executed
    assert driver.commit.call_count == 0


@pytest.mark.parametrize(
    ("store_cls", "config_cls"), [(MssqlPythonADKStore, MssqlPythonConfig), (PymssqlADKStore, PymssqlConfig)]
)
def test_adk_companions_target_each_configured_schema(store_cls: type[Any], config_cls: type[Any]) -> None:
    """Two-part session and event table names query each distinct schema and qualify index DDL."""
    store = store_cls(
        config_cls(extension_config={"adk": {"session_table": "app.sessions", "events_table": "audit.events"}})
    )
    driver = MagicMock()
    queried_schemas: set[str] = set()

    def mock_get_indexes(_driver: Any, schema: str | None = None) -> list[dict[str, str]]:
        if schema is not None:
            queried_schemas.add(schema)
        return []

    driver.data_dictionary.get_indexes.side_effect = mock_get_indexes

    store.reconcile_companion_objects_sync(driver)

    executed = [str(call.args[0]) for call in driver.execute.call_args_list]
    assert queried_schemas == {"app", "audit"}
    assert "CREATE INDEX [idx_sessions_app_user] ON [app].[sessions] (app_name, user_id)" in executed
    assert "CREATE INDEX [idx_events_session] ON [audit].[events] (session_id, timestamp ASC)" in executed
    assert driver.commit.call_count == 0


@pytest.mark.parametrize(
    ("memory_cls", "config_cls"),
    [(MssqlPythonADKMemoryStore, MssqlPythonConfig), (PymssqlADKMemoryStore, PymssqlConfig)],
)
def test_adk_memory_companions_create_every_index(memory_cls: type[Any], config_cls: type[Any]) -> None:
    """Memory store companion reconciliation creates all 4 memory indexes without committing."""
    store = memory_cls(config_cls(extension_config={"adk": {"memory_table": "custom_memory"}}))
    driver = MagicMock()
    driver.data_dictionary.get_indexes.return_value = []

    store.reconcile_companion_objects_sync(driver)

    executed = [str(call.args[0]) for call in driver.execute.call_args_list]
    assert len(executed) == 4
    assert executed[0] == (
        "CREATE INDEX [idx_custom_memory_app_scope_user_time] ON [dbo].[custom_memory] "
        "(app_name, scope, user_id, timestamp DESC)"
    )
    assert "CREATE INDEX [idx_custom_memory_timestamp] ON [dbo].[custom_memory] (timestamp DESC)" in executed
    assert driver.commit.call_count == 0


@pytest.mark.parametrize(
    ("memory_cls", "config_cls"),
    [(MssqlPythonADKMemoryStore, MssqlPythonConfig), (PymssqlADKMemoryStore, PymssqlConfig)],
)
def test_adk_memory_companions_skip_disabled_store(memory_cls: type[Any], config_cls: type[Any]) -> None:
    """Disabled memory stores short-circuit both prepare_schema_sync and reconcile_companion_objects_sync."""
    store = memory_cls(config_cls(extension_config={"adk": {"enable_memory": False}}))
    driver = MagicMock()

    store.prepare_schema_sync(driver)
    store.reconcile_companion_objects_sync(driver)

    assert driver.data_dictionary.get_version.call_count == 0
    assert driver.data_dictionary.get_indexes.call_count == 0
    assert driver.execute.call_count == 0


@pytest.mark.parametrize(
    ("store_cls", "memory_cls", "config_cls"),
    [
        (MssqlPythonADKStore, MssqlPythonADKMemoryStore, MssqlPythonConfig),
        (PymssqlADKStore, PymssqlADKMemoryStore, PymssqlConfig),
    ],
)
def test_adk_prepare_resolves_json_on_supplied_driver(
    store_cls: type[Any], memory_cls: type[Any], config_cls: type[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """prepare_schema_sync resolves native JSON on the supplied driver without opening a second session, and propagates get_version errors."""

    def forbid_session(_self: Any) -> Any:
        msg = "nested session"
        raise AssertionError(msg)

    monkeypatch.setattr(config_cls, "provide_session", forbid_session)
    store = store_cls(config_cls(extension_config={"adk": {}}))
    memory_store = memory_cls(config_cls(extension_config={"adk": {"enable_memory": True}}))
    driver = MagicMock()
    driver.data_dictionary.get_version.return_value = MssqlVersionInfo(17, 0, 1000)

    store.prepare_schema_sync(driver)
    memory_store.prepare_schema_sync(driver)

    assert "state JSON NOT NULL" in store._sessions_table_ddl()
    assert "content_json JSON NOT NULL" in memory_store._memory_table_ddl()

    failing_store = store_cls(config_cls(extension_config={"adk": {}}))
    failing_driver = MagicMock()
    failing_driver.data_dictionary.get_version.side_effect = RuntimeError("catalog offline")
    with pytest.raises(RuntimeError, match="catalog offline"):
        failing_store.prepare_schema_sync(failing_driver)


@pytest.mark.parametrize(
    ("store_cls", "config_cls"), [(MssqlPythonADKStore, MssqlPythonConfig), (PymssqlADKStore, PymssqlConfig)]
)
def test_adk_prepare_keeps_configured_json_decision(store_cls: type[Any], config_cls: type[Any]) -> None:
    """Explicit native_json skips get_version inspection during prepare_schema_sync."""
    store = store_cls(config_cls(extension_config={"adk": {"native_json": False}}))
    driver = MagicMock()

    store.prepare_schema_sync(driver)

    assert driver.data_dictionary.get_version.call_count == 0
    assert "state NVARCHAR(MAX) NOT NULL" in store._sessions_table_ddl()


@pytest.mark.parametrize(
    ("store_cls", "config_cls"), [(MssqlPythonADKStore, MssqlPythonConfig), (PymssqlADKStore, PymssqlConfig)]
)
def test_adk_reconcile_keeps_existing_json_storage(store_cls: type[Any], config_cls: type[Any]) -> None:
    """Strict ensure_schema_sync accepts existing NVARCHAR(MAX) columns when target DDL resolves to JSON."""
    store = store_cls(config_cls(extension_config={"adk": {"native_json": True}}))
    driver = MagicMock()
    store.prepare_schema_sync(driver)
    targets = store.get_schema_targets_sync(driver)

    driver.data_dictionary.get_tables.return_value = [{"table_name": target.table_name} for target in targets]
    columns_by_table = {
        target.table_name: [
            {
                "column_name": column.name,
                "data_type": "nvarchar(max)" if column.dtype.upper() == "JSON" else column.dtype.lower(),
                "is_nullable": "NO" if column.not_null else "YES",
            }
            for column in target.create_table.columns
        ]
        for target in targets
    }
    driver.data_dictionary.get_columns.side_effect = lambda _drv, table_name, **_kw: columns_by_table[table_name]

    ensure_schema_sync(driver, targets, manage_schema=True, commit=False, strict=True)

    executed = [str(call.args[0]) for call in driver.execute.call_args_list]
    assert not any("ALTER" in stmt.upper() for stmt in executed)


@pytest.mark.parametrize(
    ("store_cls", "config_cls"), [(MssqlPythonADKStore, MssqlPythonConfig), (PymssqlADKStore, PymssqlConfig)]
)
def test_adk_companions_propagate_ddl_failure(store_cls: type[Any], config_cls: type[Any]) -> None:
    """DDL failures during companion index creation propagate immediately without inner commit."""
    store = store_cls(config_cls(extension_config={"adk": {}}))
    driver = MagicMock()
    driver.data_dictionary.get_indexes.return_value = []
    driver.execute.side_effect = [None, RuntimeError("boom")]

    with pytest.raises(RuntimeError, match="boom"):
        store.reconcile_companion_objects_sync(driver)

    assert driver.execute.call_count == 2
    assert driver.commit.call_count == 0
