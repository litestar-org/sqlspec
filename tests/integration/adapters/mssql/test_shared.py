"""Shared Microsoft SQL Server-family integration contracts."""

from collections.abc import Generator
from contextlib import contextmanager, suppress
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock

import pytest

from sqlspec import sql
from sqlspec.adapters.arrow_odbc import ArrowOdbcDriver
from sqlspec.adapters.arrow_odbc._typing import ArrowOdbcConnection
from sqlspec.adapters.pymssql import PymssqlConfig
from sqlspec.exceptions import SQLSpecError
from tests.integration.adapters._shared import install_shared_tests

install_shared_tests(globals(), "mssql")


@pytest.mark.parametrize(
    "case", [SimpleNamespace(adapter="pymssql", fixture_name="pymssql_connection_config")], ids=["pymssql"]
)
def test_pymssql_cached_dictionary_rows(case: Any, request: pytest.FixtureRequest, monkeypatch: Any) -> None:
    config = PymssqlConfig(connection_config={**request.getfixturevalue(case.fixture_name), "as_dict": True})
    executions = Mock()
    try:
        with config.provide_session() as driver:
            with_cursor = driver.with_cursor

            @contextmanager
            def counted_cursor(_driver: Any, connection: Any) -> Generator[Any, None, None]:
                with with_cursor(connection) as cursor:
                    proxy = SimpleNamespace(fetchall=cursor.fetchall, rowcount=cursor.rowcount)

                    def execute(sql: str, params: Any) -> None:
                        executions(sql, params)
                        cursor.execute(sql, params)
                        proxy.description = cursor.description
                        proxy.rowcount = cursor.rowcount

                    proxy.execute = execute
                    yield proxy

            monkeypatch.setattr(type(driver), "with_cursor", counted_cursor)
            for count in range(1, 4):
                result = driver.execute("SELECT 1 AS x UNION ALL SELECT 2 AS x ORDER BY x", ())
                assert result.get_data() == [{"x": 1}, {"x": 2}]
                assert executions.call_count == count
    finally:
        config.close_pool()


@pytest.mark.parametrize(
    "case", [SimpleNamespace(adapter="arrow_odbc", fixture_name="arrow_odbc_mssql_config")], ids=["arrow-odbc"]
)
def test_arrow_odbc_cached_select_executes_once(case: Any, request: pytest.FixtureRequest) -> None:
    with request.getfixturevalue(case.fixture_name).provide_session() as session:
        connection = session.connection
        proxy = SimpleNamespace(
            execute=Mock(wraps=connection.execute), read_arrow_batches=Mock(wraps=connection.read_arrow_batches)
        )
        driver = ArrowOdbcDriver(
            connection=cast("ArrowOdbcConnection", proxy), driver_features={"dbms_name": "Microsoft SQL Server"}
        )
        for count in range(1, 4):
            result = driver.execute("SELECT 1 AS x", ())
            assert result.get_data() == [{"x": 1}]
            assert proxy.read_arrow_batches.call_count == count
            proxy.execute.assert_not_called()


_RETURNING_CONFIGS = [
    pytest.param(
        SimpleNamespace(adapter="mssql_python", fixture_name="mssql_python_config"),
        marks=pytest.mark.mssql_python,
        id="mssql_python",
    ),
    pytest.param(
        SimpleNamespace(adapter="pymssql", fixture_name="pymssql_config"), marks=pytest.mark.pymssql, id="pymssql"
    ),
]
_RETURNING_TARGET = "returning_target"
_RETURNING_SOURCE = "returning_source"
_RETURNING_SETUP = f"""
IF OBJECT_ID('{_RETURNING_TARGET}') IS NOT NULL DROP TABLE {_RETURNING_TARGET};
IF OBJECT_ID('{_RETURNING_SOURCE}') IS NOT NULL DROP TABLE {_RETURNING_SOURCE};
CREATE TABLE {_RETURNING_TARGET} (id INT PRIMARY KEY, a INT, [a b] INT);
CREATE TABLE {_RETURNING_SOURCE} (id INT, b INT);
INSERT INTO {_RETURNING_TARGET} (id, a) VALUES (1, 10), (2, 20), (3, 30), (4, 40);
INSERT INTO {_RETURNING_SOURCE} VALUES (1, 7);
"""
_RETURNING_CLEANUP = f"""
IF OBJECT_ID('{_RETURNING_TARGET}') IS NOT NULL DROP TABLE {_RETURNING_TARGET};
IF OBJECT_ID('{_RETURNING_SOURCE}') IS NOT NULL DROP TABLE {_RETURNING_SOURCE};
"""


@pytest.mark.xdist_group("mssql")
@pytest.mark.parametrize("case", _RETURNING_CONFIGS)
def test_builder_returning_output_native(case: Any, request: pytest.FixtureRequest) -> None:
    target, source = _RETURNING_TARGET, _RETURNING_SOURCE
    cases = [
        (sql.insert(target, dialect="tsql").columns("id", "a").values(5, 50).returning("a"), [{"a": 50}]),
        (
            sql.insert(target, dialect="tsql").columns("id", "a").values(6, 60).returning("*"),
            [{"id": 6, "a": 60, "a b": None}],
        ),
        (
            sql.update(target, dialect="tsql").set(a=11).where_eq("id", 1).returning("a", f"{target}.id", "a + 1 AS x"),
            [{"a": 11, "id": 1, "x": 12}],
        ),
        (
            sql
            .update(target, dialect="tsql")
            .set(a=12)
            .where_eq("id", 1)
            .returning("DELETED.a AS old", "INSERTED.a AS new"),
            [{"old": 11, "new": 12}],
        ),
        (
            sql
            .update(target, dialect="tsql")
            .set(a=13)
            .from_(source, alias="s")
            .where(f"{target}.id = s.id")
            .returning("a", "s.b"),
            [{"a": 13, "b": 7}],
        ),
        (
            sql.update(target, dialect="tsql").set(a=14).where_eq("id", 2).returning("*"),
            [{"id": 2, "a": 14, "a b": None}],
        ),
        (sql.update(target, dialect="tsql").set(a=15).where_eq("id", 2).returning("[a b]"), [{"a b": None}]),
        (sql.delete(target, dialect="tsql").where_eq("id", 3).returning("a"), [{"a": 30}]),
        (sql.delete(target, dialect="tsql").where_eq("id", 4).returning("*"), [{"id": 4, "a": 40, "a b": None}]),
    ]
    config = request.getfixturevalue(case.fixture_name)
    with config.provide_session() as driver:
        driver.execute_script(_RETURNING_SETUP)
        try:
            for builder, expected in cases:
                assert driver.execute(builder).get_data() == expected
        finally:
            driver.execute_script(_RETURNING_CLEANUP)
            driver.commit()


@pytest.mark.xdist_group("mssql")
@pytest.mark.parametrize("case", _RETURNING_CONFIGS)
def test_builder_returning_output_rejected_on_triggered_table(case: Any, request: pytest.FixtureRequest) -> None:
    config = request.getfixturevalue(case.fixture_name)
    with config.provide_session() as driver:
        driver.execute_script(_RETURNING_SETUP)
        try:
            driver.execute_script(
                f"CREATE TRIGGER returning_target_trg ON {_RETURNING_TARGET} AFTER UPDATE AS BEGIN SET NOCOUNT ON; END"
            )
            builder = sql.update(_RETURNING_TARGET, dialect="tsql").set(a=9).where_eq("id", 1).returning("a")
            with pytest.raises(SQLSpecError, match="cannot have any enabled triggers"):
                driver.execute(builder)
        finally:
            driver.execute_script(_RETURNING_CLEANUP)
            driver.commit()


@pytest.mark.xdist_group("mssql")
@pytest.mark.parametrize("case", _RETURNING_CONFIGS)
def test_builder_aliased_dml_target_native(case: Any, request: pytest.FixtureRequest) -> None:
    target = f"{_RETURNING_TARGET} AS u"
    cases = [
        (sql.update(target, dialect="tsql").set(a=11).where_eq("u.id", 1).returning("a", "u.id"), [{"a": 11, "id": 1}]),
        (
            sql
            .update(target, dialect="tsql")
            .set(a=12)
            .from_(_RETURNING_SOURCE, alias="s")
            .where("u.id = s.id")
            .returning("DELETED.a AS old", "s.b"),
            [{"old": 11, "b": 7}],
        ),
        (sql.delete(target, dialect="tsql").where_eq("u.id", 2).returning("u.a"), [{"a": 20}]),
    ]
    config = request.getfixturevalue(case.fixture_name)
    with config.provide_session() as driver:
        driver.execute_script(_RETURNING_SETUP)
        try:
            for builder, expected in cases:
                assert driver.execute(builder).get_data() == expected
            assert driver.execute(sql.update(target, dialect="tsql").set(a=13).where_eq("u.id", 3)).rows_affected == 1
            assert driver.execute(sql.delete(target, dialect="tsql").where_eq("u.id", 4)).rows_affected == 1
            remaining = driver.execute(f"SELECT id, a FROM {_RETURNING_TARGET} ORDER BY id").get_data()
            assert remaining == [{"id": 1, "a": 12}, {"id": 3, "a": 13}]
        finally:
            driver.execute_script(_RETURNING_CLEANUP)
            driver.commit()


_ADK_RECONCILE_REPAIR_CONFIGS = [
    pytest.param(SimpleNamespace(adapter="pymssql"), marks=pytest.mark.pymssql, id="pymssql"),
    pytest.param(SimpleNamespace(adapter="mssql_python"), marks=pytest.mark.mssql_python, id="mssql-python"),
    pytest.param(SimpleNamespace(adapter="arrow_odbc"), id="arrow-odbc"),
]

_ADK_RECONCILE_ROLLBACK_CONFIGS = [
    pytest.param(SimpleNamespace(adapter="pymssql"), marks=pytest.mark.pymssql, id="pymssql"),
    pytest.param(SimpleNamespace(adapter="mssql_python"), marks=pytest.mark.mssql_python, id="mssql-python"),
]

_RC_ADK_TABLES = ("rc_event", "rc_session", "rc_app_state", "rc_user_state", "rc_meta", "rc_migrations")


def _build_mssql_adk_reconcile_config(
    adapter_key: str, request: pytest.FixtureRequest, migrations_dir: Path, *, mode: str
) -> tuple[Any, type[Any]]:
    adk_settings: dict[str, Any] = {
        "session_table": "rc_session",
        "events_table": "rc_event",
        "app_state_table": "rc_app_state",
        "user_state_table": "rc_user_state",
        "metadata_table": "rc_meta",
        "enable_memory": False,
        "migration_mode": mode,
    }
    mig_settings: dict[str, Any] = {"script_location": str(migrations_dir), "version_table_name": "rc_migrations"}
    if adapter_key == "pymssql":
        pytest.importorskip("pymssql")
        from sqlspec.adapters.pymssql.adk import PymssqlADKStore

        conn_cfg = dict(request.getfixturevalue("mssql_migration_connection_config"))
        return (
            PymssqlConfig(
                connection_config=conn_cfg, migration_config=mig_settings, extension_config={"adk": adk_settings}
            ),
            PymssqlADKStore,
        )
    if adapter_key == "mssql_python":
        pytest.importorskip("mssql_python")
        from sqlspec.adapters.mssql_python import MssqlPythonConfig
        from sqlspec.adapters.mssql_python.adk import MssqlPythonADKStore

        conn_cfg = dict(request.getfixturevalue("mssql_python_migration_connection_config"))
        return (
            MssqlPythonConfig(
                connection_config=conn_cfg, migration_config=mig_settings, extension_config={"adk": adk_settings}
            ),
            MssqlPythonADKStore,
        )
    pytest.importorskip("arrow_odbc")
    pytest.importorskip("pyarrow")
    from sqlspec.adapters.arrow_odbc import ArrowOdbcConfig
    from sqlspec.adapters.arrow_odbc.adk import ArrowOdbcADKStore

    mssql_service = request.getfixturevalue("mssql_service")
    return (
        ArrowOdbcConfig(
            connection_config={"connection_string": mssql_service.connection_string},
            driver_features={"dbms_name": "Microsoft SQL Server"},
            migration_config=mig_settings,
            extension_config={"adk": adk_settings},
        ),
        ArrowOdbcADKStore,
    )


def _cleanup_rc_adk_objects(config: Any) -> None:
    with suppress(Exception), config.provide_session() as driver:
        for table_name in _RC_ADK_TABLES:
            driver.execute(f"DROP TABLE IF EXISTS [dbo].[{table_name}]")
        driver.commit()


def _has_rc_session_update_time_index(config: Any) -> bool:
    with config.provide_session() as driver:
        rows = driver.select(
            "SELECT name FROM sys.indexes "
            "WHERE object_id = OBJECT_ID(N'dbo.rc_session') AND name = N'idx_rc_session_update_time'"
        )
        return len(rows) == 1


def _rc_ledger_versions(config: Any) -> list[str]:
    with config.provide_session() as driver:
        rows = driver.select("SELECT version_num FROM [dbo].[rc_migrations] ORDER BY version_num")
        return [str(row.get("version_num") or row.get("VERSION_NUM")) for row in rows]


@pytest.mark.xdist_group("mssql")
@pytest.mark.parametrize("case", _ADK_RECONCILE_REPAIR_CONFIGS)
def test_mssql_adk_reconcile_repairs_index_and_retains_ledger(
    case: Any, request: pytest.FixtureRequest, tmp_path: Path
) -> None:
    adapter_key = str(case.adapter)
    migrations_dir = tmp_path / f"adk_reconcile_{adapter_key}"
    migrations_dir.mkdir(parents=True, exist_ok=True)
    versioned_cfg, store_cls = _build_mssql_adk_reconcile_config(adapter_key, request, migrations_dir, mode="versioned")
    reconcile_cfg: Any = None
    _cleanup_rc_adk_objects(versioned_cfg)
    try:
        versioned_cfg.migrate_up(revision="ext_adk_0001", echo=False)
        versioned_store = store_cls(versioned_cfg)
        versioned_store.create_tables()
        versioned_store.create_session("s1", "app", "user", {"k": "v"})

        with versioned_cfg.provide_session() as driver:
            driver.execute("DROP INDEX [idx_rc_session_update_time] ON [dbo].[rc_session]")
            driver.commit()
        assert not _has_rc_session_update_time_index(versioned_cfg)

        reconcile_cfg, _ = _build_mssql_adk_reconcile_config(adapter_key, request, migrations_dir, mode="reconcile")
        reconcile_cfg.migrate_up(echo=False)
        assert _has_rc_session_update_time_index(reconcile_cfg)
        assert _rc_ledger_versions(reconcile_cfg) == ["ext_adk_0001"]

        reconcile_cfg.migrate_up(echo=False)
        assert _rc_ledger_versions(reconcile_cfg) == ["ext_adk_0001"]

        fetched = store_cls(reconcile_cfg).get_session("app", "user", "s1")
        assert fetched is not None
        assert fetched["id"] == "s1"
        assert fetched["state"] == {"k": "v"}
    finally:
        _cleanup_rc_adk_objects(versioned_cfg)
        versioned_cfg.close_pool()
        if reconcile_cfg is not None:
            reconcile_cfg.close_pool()


@pytest.mark.xdist_group("mssql")
@pytest.mark.parametrize("case", _ADK_RECONCILE_ROLLBACK_CONFIGS)
def test_mssql_adk_reconcile_rolls_back_failed_callback(
    case: Any, request: pytest.FixtureRequest, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    adapter_key = str(case.adapter)
    migrations_dir = tmp_path / f"adk_rollback_{adapter_key}"
    migrations_dir.mkdir(parents=True, exist_ok=True)
    versioned_cfg, store_cls = _build_mssql_adk_reconcile_config(adapter_key, request, migrations_dir, mode="versioned")
    reconcile_cfg: Any = None
    _cleanup_rc_adk_objects(versioned_cfg)
    try:
        versioned_cfg.migrate_up(revision="ext_adk_0001", echo=False)
        store_cls(versioned_cfg).create_tables()

        with versioned_cfg.provide_session() as driver:
            driver.execute("DROP INDEX [idx_rc_session_update_time] ON [dbo].[rc_session]")
            driver.commit()
        assert not _has_rc_session_update_time_index(versioned_cfg)

        reconcile_cfg, _ = _build_mssql_adk_reconcile_config(adapter_key, request, migrations_dir, mode="reconcile")
        with monkeypatch.context() as mp:
            mp.setattr(
                store_cls,
                "_index_specs",
                lambda _self: [
                    ("idx_rc_session_update_time", "rc_session", "update_time DESC"),
                    ("idx_rc_bad", "rc_session", "missing_column"),
                ],
            )
            with pytest.raises(SQLSpecError):
                reconcile_cfg.migrate_up(echo=False)

        assert not _has_rc_session_update_time_index(reconcile_cfg)
        assert _rc_ledger_versions(reconcile_cfg) == ["ext_adk_0001"]

        reconcile_cfg.migrate_up(echo=False)
        assert _has_rc_session_update_time_index(reconcile_cfg)
    finally:
        _cleanup_rc_adk_objects(versioned_cfg)
        versioned_cfg.close_pool()
        if reconcile_cfg is not None:
            reconcile_cfg.close_pool()
