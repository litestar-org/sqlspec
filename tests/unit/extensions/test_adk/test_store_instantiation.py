"""Smoke tests verifying all shipped ADK store classes are instantiable (not abstract).

Every shipped store class must be concrete — no unsatisfied abstract methods.
This catches bugs where stores have method signature mismatches with the base
class, such as cockroach, mysqlconnector sync, pymysql, and spanner stores
that are missing abstract method implementations added to the base contract.

The class list mirrors the shipped adapter ``adk`` exports and catches drift
when a concrete store no longer satisfies the base contract.
"""

import importlib
import inspect
from typing import cast

import pytest

from sqlspec.core import StatementConfig
from sqlspec.exceptions import SQLSpecError
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore
from sqlspec.extensions.adk.store import BaseAsyncADKStore, BaseSyncADKStore

SESSION_STORE_CLASSES = [
    "sqlspec.adapters.asyncpg.adk.AsyncpgADKStore",
    "sqlspec.adapters.aiomysql.adk.AiomysqlADKStore",
    "sqlspec.adapters.aiosqlite.adk.AiosqliteADKStore",
    "sqlspec.adapters.asyncmy.adk.AsyncmyADKStore",
    "sqlspec.adapters.adbc.adk.AdbcADKStore",
    "sqlspec.adapters.arrow_odbc.adk.ArrowOdbcADKStore",
    "sqlspec.adapters.cockroach_asyncpg.adk.CockroachAsyncpgADKStore",
    "sqlspec.adapters.cockroach_psycopg.adk.CockroachPsycopgAsyncADKStore",
    "sqlspec.adapters.cockroach_psycopg.adk.CockroachPsycopgSyncADKStore",
    "sqlspec.adapters.duckdb.adk.DuckDBADKStore",
    "sqlspec.adapters.mssql_python.adk.MssqlPythonADKStore",
    "sqlspec.adapters.mysqlconnector.adk.MysqlConnectorAsyncADKStore",
    "sqlspec.adapters.mysqlconnector.adk.MysqlConnectorSyncADKStore",
    "sqlspec.adapters.oracledb.adk.OracleAsyncADKStore",
    "sqlspec.adapters.oracledb.adk.OracleSyncADKStore",
    "sqlspec.adapters.psqlpy.adk.PsqlpyADKStore",
    "sqlspec.adapters.psycopg.adk.PsycopgAsyncADKStore",
    "sqlspec.adapters.psycopg.adk.PsycopgSyncADKStore",
    "sqlspec.adapters.pymssql.adk.PymssqlADKStore",
    "sqlspec.adapters.pymysql.adk.PyMysqlADKStore",
    "sqlspec.adapters.spanner.adk.SpannerAsyncADKStore",
    "sqlspec.adapters.spanner.adk.SpannerSyncADKStore",
    "sqlspec.adapters.sqlite.adk.SqliteADKStore",
]

MEMORY_STORE_CLASSES = [
    "sqlspec.adapters.asyncpg.adk.AsyncpgADKMemoryStore",
    "sqlspec.adapters.aiomysql.adk.AiomysqlADKMemoryStore",
    "sqlspec.adapters.aiosqlite.adk.AiosqliteADKMemoryStore",
    "sqlspec.adapters.asyncmy.adk.AsyncmyADKMemoryStore",
    "sqlspec.adapters.adbc.adk.AdbcADKMemoryStore",
    "sqlspec.adapters.arrow_odbc.adk.ArrowOdbcADKMemoryStore",
    "sqlspec.adapters.cockroach_asyncpg.adk.CockroachAsyncpgADKMemoryStore",
    "sqlspec.adapters.cockroach_psycopg.adk.CockroachPsycopgAsyncADKMemoryStore",
    "sqlspec.adapters.cockroach_psycopg.adk.CockroachPsycopgSyncADKMemoryStore",
    "sqlspec.adapters.duckdb.adk.DuckDBADKMemoryStore",
    "sqlspec.adapters.mssql_python.adk.MssqlPythonADKMemoryStore",
    "sqlspec.adapters.mysqlconnector.adk.MysqlConnectorAsyncADKMemoryStore",
    "sqlspec.adapters.mysqlconnector.adk.MysqlConnectorSyncADKMemoryStore",
    "sqlspec.adapters.oracledb.adk.OracleAsyncADKMemoryStore",
    "sqlspec.adapters.oracledb.adk.OracleSyncADKMemoryStore",
    "sqlspec.adapters.psqlpy.adk.PsqlpyADKMemoryStore",
    "sqlspec.adapters.psycopg.adk.PsycopgAsyncADKMemoryStore",
    "sqlspec.adapters.psycopg.adk.PsycopgSyncADKMemoryStore",
    "sqlspec.adapters.pymssql.adk.PymssqlADKMemoryStore",
    "sqlspec.adapters.pymysql.adk.PyMysqlADKMemoryStore",
    "sqlspec.adapters.spanner.adk.SpannerAsyncADKMemoryStore",
    "sqlspec.adapters.spanner.adk.SpannerSyncADKMemoryStore",
    "sqlspec.adapters.sqlite.adk.SqliteADKMemoryStore",
]

ALL_STORE_CLASSES = SESSION_STORE_CLASSES + MEMORY_STORE_CLASSES

SYNC_SESSION_STORE_CLASSES = [
    "sqlspec.adapters.adbc.adk.AdbcADKStore",
    "sqlspec.adapters.arrow_odbc.adk.ArrowOdbcADKStore",
    "sqlspec.adapters.cockroach_psycopg.adk.CockroachPsycopgSyncADKStore",
    "sqlspec.adapters.duckdb.adk.DuckDBADKStore",
    "sqlspec.adapters.mssql_python.adk.MssqlPythonADKStore",
    "sqlspec.adapters.mysqlconnector.adk.MysqlConnectorSyncADKStore",
    "sqlspec.adapters.oracledb.adk.OracleSyncADKStore",
    "sqlspec.adapters.psycopg.adk.PsycopgSyncADKStore",
    "sqlspec.adapters.pymssql.adk.PymssqlADKStore",
    "sqlspec.adapters.pymysql.adk.PyMysqlADKStore",
    "sqlspec.adapters.spanner.adk.SpannerSyncADKStore",
    "sqlspec.adapters.sqlite.adk.SqliteADKStore",
]

SYNC_MEMORY_STORE_CLASSES = [
    "sqlspec.adapters.adbc.adk.AdbcADKMemoryStore",
    "sqlspec.adapters.arrow_odbc.adk.ArrowOdbcADKMemoryStore",
    "sqlspec.adapters.cockroach_psycopg.adk.CockroachPsycopgSyncADKMemoryStore",
    "sqlspec.adapters.duckdb.adk.DuckDBADKMemoryStore",
    "sqlspec.adapters.mssql_python.adk.MssqlPythonADKMemoryStore",
    "sqlspec.adapters.mysqlconnector.adk.MysqlConnectorSyncADKMemoryStore",
    "sqlspec.adapters.oracledb.adk.OracleSyncADKMemoryStore",
    "sqlspec.adapters.psycopg.adk.PsycopgSyncADKMemoryStore",
    "sqlspec.adapters.pymssql.adk.PymssqlADKMemoryStore",
    "sqlspec.adapters.pymysql.adk.PyMysqlADKMemoryStore",
    "sqlspec.adapters.spanner.adk.SpannerSyncADKMemoryStore",
    "sqlspec.adapters.sqlite.adk.SqliteADKMemoryStore",
]


def _load_class(class_path: str) -> type:
    module_path, class_name = class_path.rsplit(".", 1)
    try:
        module = importlib.import_module(module_path)
    except ImportError as exc:
        pytest.skip(f"Module {module_path} not importable (missing optional dependency)")
        raise RuntimeError("pytest.skip did not raise") from exc
    return cast("type", getattr(module, class_name))


@pytest.mark.parametrize("class_path", ALL_STORE_CLASSES)
def test_store_has_no_abstract_methods(class_path: str) -> None:
    """Every shipped store class must be concrete (no unsatisfied abstract methods).

    A class with entries in ``__abstractmethods__`` cannot be instantiated and
    signals that the concrete store is missing one or more method implementations
    required by its base class contract.
    """
    cls = _load_class(class_path)
    abstract: set[str] = getattr(cls, "__abstractmethods__", set())
    assert not abstract, f"{class_path} has unsatisfied abstract methods: {abstract}"


@pytest.mark.parametrize("class_path", ALL_STORE_CLASSES)
def test_store_method_signatures_match_base_contract(class_path: str) -> None:
    """Every shipped concrete store keeps the base store method signatures."""
    cls = _load_class(class_path)

    if issubclass(cls, BaseAsyncADKMemoryStore):
        base: type = BaseAsyncADKMemoryStore
    elif issubclass(cls, BaseSyncADKMemoryStore):
        base = BaseSyncADKMemoryStore
    elif issubclass(cls, BaseAsyncADKStore):
        base = BaseAsyncADKStore
    else:
        base = BaseSyncADKStore

    for method_name in base.__abstractmethods__:
        base_signature = inspect.signature(getattr(base, method_name))
        concrete_signature = inspect.signature(getattr(cls, method_name))
        assert list(base_signature.parameters) == list(concrete_signature.parameters), (
            f"{class_path}.{method_name} parameters differ from {base.__name__}: "
            f"{concrete_signature} != {base_signature}"
        )


@pytest.mark.parametrize("class_path", SYNC_SESSION_STORE_CLASSES)
def test_sync_session_store_contract_methods_are_sync(class_path: str) -> None:
    """Sync-backed session stores expose sync methods, not async wrappers."""
    cls = _load_class(class_path)

    assert issubclass(cls, BaseSyncADKStore)
    assert not issubclass(cls, BaseAsyncADKStore)
    for method_name in BaseSyncADKStore.__abstractmethods__:
        assert not inspect.iscoroutinefunction(getattr(cls, method_name)), f"{class_path}.{method_name} is async"


@pytest.mark.parametrize("class_path", SYNC_MEMORY_STORE_CLASSES)
def test_sync_memory_store_contract_methods_are_sync(class_path: str) -> None:
    """Sync-backed memory stores expose sync methods, not async wrappers."""
    cls = _load_class(class_path)

    assert issubclass(cls, BaseSyncADKMemoryStore)
    assert not issubclass(cls, BaseAsyncADKMemoryStore)
    for method_name in BaseSyncADKMemoryStore.__abstractmethods__:
        assert not inspect.iscoroutinefunction(getattr(cls, method_name)), f"{class_path}.{method_name} is async"


def test_adk_store_registration_validator_resolves_sqlite_store_classes() -> None:
    """The migration registration validator resolves both SQLite ADK store classes."""
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.extensions.adk._config_utils import _ensure_adk_store_registration

    config = SqliteConfig(connection_config={"database": ":memory:"}, extension_config={"adk": {}})

    _ensure_adk_store_registration(config)


def test_adk_store_registration_validator_resolves_duckdb_store_classes() -> None:
    """The migration registration validator handles DuckDB store export casing."""
    from sqlspec.adapters.duckdb import DuckDBConfig
    from sqlspec.extensions.adk._config_utils import _ensure_adk_store_registration

    config = DuckDBConfig(connection_config={"database": ":memory:"}, extension_config={"adk": {}})

    _ensure_adk_store_registration(config)


@pytest.mark.parametrize(
    ("adapter", "prefix", "dialect"),
    [
        ("db2", "Db2Sync", "db2"),
        ("db2", "Db2Async", "db2"),
        ("arrow_odbc", "ArrowOdbc", "tsql"),
        ("arrow_odbc", "ArrowOdbc", "db2"),
        ("cockroach_asyncpg", "CockroachAsyncpg", "postgres"),
        ("cockroach_psycopg", "CockroachPsycopgSync", "postgres"),
        ("cockroach_psycopg", "CockroachPsycopgAsync", "postgres"),
        ("duckdb", "DuckDB", "duckdb"),
        ("mssql_python", "MssqlPython", "tsql"),
        ("pymssql", "Pymssql", "tsql"),
        ("sybase", "Sybase", "sybase"),
        ("sqlanywhere", "Sqlanywhere", "sqlanywhere"),
    ],
)
def test_qualified_table_names_reach_supporting_adk_stores(adapter: str, prefix: str, dialect: str) -> None:
    """Supported stores accept qualified tables through their public configuration."""
    session_tables = {
        "session_table": "app.sessions",
        "events_table": "app.events",
        "app_state_table": "app.app_state",
        "user_state_table": "app.user_state",
        "metadata_table": "app.metadata",
    }
    config = _load_class(f"sqlspec.adapters.{adapter}.{prefix}Config")(
        extension_config={"adk": {**session_tables, "memory_table": "app.memories"}},
        statement_config=StatementConfig(dialect=dialect),
    )

    session_store = _load_class(f"sqlspec.adapters.{adapter}.adk.{prefix}ADKStore")(config)
    memory_store = _load_class(f"sqlspec.adapters.{adapter}.adk.{prefix}ADKMemoryStore")(config)

    assert {name: getattr(session_store, name) for name in session_tables} == session_tables
    assert memory_store.memory_table == "app.memories"


@pytest.mark.parametrize("table", ["dataset.sessions", "project.dataset.sessions"])
def test_bigquery_adk_preserves_qualified_table_configuration(table: str) -> None:
    """BigQuery keeps dataset and project qualification supplied by the caller."""
    from sqlspec.adapters.bigquery import BigQueryConfig
    from sqlspec.adapters.bigquery.adk import BigQueryADKStore

    store = BigQueryADKStore(BigQueryConfig(extension_config={"adk": {"session_table": table, "enable_memory": False}}))

    assert store.session_table == table
    assert f"CREATE TABLE IF NOT EXISTS {table}" in store._sessions_table_ddl()


def test_adk_store_registration_validator_fails_fast_for_broken_store_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    """Broken adapter-to-store naming fails during config registration."""
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.extensions.adk import _config_utils

    def import_missing_store(path: str) -> object:
        if path.endswith("SqliteADKStore"):
            raise ImportError("missing test store")
        return importlib.import_module(path.rsplit(".", 1)[0])

    monkeypatch.setattr(_config_utils, "import_string", import_missing_store)
    monkeypatch.setattr(_config_utils, "_adk_exported_store_class", lambda _config, _suffix: None)

    with pytest.raises(SQLSpecError, match="Failed to import ADK store class"):
        SqliteConfig(connection_config={"database": ":memory:"}, extension_config={"adk": {}})
