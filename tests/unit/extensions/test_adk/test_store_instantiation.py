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

from sqlspec.exceptions import SQLSpecError
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore
from sqlspec.extensions.adk.store import BaseAsyncADKStore, BaseSyncADKStore

SESSION_STORE_CLASSES = [
    "sqlspec.adapters.asyncpg.adk.AsyncpgADKStore",
    "sqlspec.adapters.aiomysql.adk.AiomysqlADKStore",
    "sqlspec.adapters.aiosqlite.adk.AiosqliteADKStore",
    "sqlspec.adapters.asyncmy.adk.AsyncmyADKStore",
    "sqlspec.adapters.adbc.adk.AdbcADKStore",
    "sqlspec.adapters.cockroach_asyncpg.adk.CockroachAsyncpgADKStore",
    "sqlspec.adapters.cockroach_psycopg.adk.CockroachPsycopgAsyncADKStore",
    "sqlspec.adapters.cockroach_psycopg.adk.CockroachPsycopgSyncADKStore",
    "sqlspec.adapters.duckdb.adk.DuckdbADKStore",
    "sqlspec.adapters.mysqlconnector.adk.MysqlConnectorAsyncADKStore",
    "sqlspec.adapters.mysqlconnector.adk.MysqlConnectorSyncADKStore",
    "sqlspec.adapters.oracledb.adk.OracleAsyncADKStore",
    "sqlspec.adapters.oracledb.adk.OracleSyncADKStore",
    "sqlspec.adapters.psqlpy.adk.PsqlpyADKStore",
    "sqlspec.adapters.psycopg.adk.PsycopgAsyncADKStore",
    "sqlspec.adapters.psycopg.adk.PsycopgSyncADKStore",
    "sqlspec.adapters.pymysql.adk.PyMysqlADKStore",
    "sqlspec.adapters.spanner.adk.SpannerSyncADKStore",
    "sqlspec.adapters.sqlite.adk.SqliteADKStore",
]

MEMORY_STORE_CLASSES = [
    "sqlspec.adapters.asyncpg.adk.AsyncpgADKMemoryStore",
    "sqlspec.adapters.aiomysql.adk.AiomysqlADKMemoryStore",
    "sqlspec.adapters.aiosqlite.adk.AiosqliteADKMemoryStore",
    "sqlspec.adapters.asyncmy.adk.AsyncmyADKMemoryStore",
    "sqlspec.adapters.adbc.adk.AdbcADKMemoryStore",
    "sqlspec.adapters.cockroach_asyncpg.adk.CockroachAsyncpgADKMemoryStore",
    "sqlspec.adapters.cockroach_psycopg.adk.CockroachPsycopgAsyncADKMemoryStore",
    "sqlspec.adapters.cockroach_psycopg.adk.CockroachPsycopgSyncADKMemoryStore",
    "sqlspec.adapters.duckdb.adk.DuckdbADKMemoryStore",
    "sqlspec.adapters.mysqlconnector.adk.MysqlConnectorAsyncADKMemoryStore",
    "sqlspec.adapters.mysqlconnector.adk.MysqlConnectorSyncADKMemoryStore",
    "sqlspec.adapters.oracledb.adk.OracleAsyncADKMemoryStore",
    "sqlspec.adapters.oracledb.adk.OracleSyncADKMemoryStore",
    "sqlspec.adapters.psqlpy.adk.PsqlpyADKMemoryStore",
    "sqlspec.adapters.psycopg.adk.PsycopgAsyncADKMemoryStore",
    "sqlspec.adapters.psycopg.adk.PsycopgSyncADKMemoryStore",
    "sqlspec.adapters.pymysql.adk.PyMysqlADKMemoryStore",
    "sqlspec.adapters.spanner.adk.SpannerSyncADKMemoryStore",
    "sqlspec.adapters.sqlite.adk.SqliteADKMemoryStore",
]

ALL_STORE_CLASSES = SESSION_STORE_CLASSES + MEMORY_STORE_CLASSES

SYNC_SESSION_STORE_CLASSES = [
    "sqlspec.adapters.adbc.adk.AdbcADKStore",
    "sqlspec.adapters.cockroach_psycopg.adk.CockroachPsycopgSyncADKStore",
    "sqlspec.adapters.duckdb.adk.DuckdbADKStore",
    "sqlspec.adapters.mysqlconnector.adk.MysqlConnectorSyncADKStore",
    "sqlspec.adapters.oracledb.adk.OracleSyncADKStore",
    "sqlspec.adapters.psycopg.adk.PsycopgSyncADKStore",
    "sqlspec.adapters.pymysql.adk.PyMysqlADKStore",
    "sqlspec.adapters.spanner.adk.SpannerSyncADKStore",
    "sqlspec.adapters.sqlite.adk.SqliteADKStore",
]

SYNC_MEMORY_STORE_CLASSES = [
    "sqlspec.adapters.adbc.adk.AdbcADKMemoryStore",
    "sqlspec.adapters.cockroach_psycopg.adk.CockroachPsycopgSyncADKMemoryStore",
    "sqlspec.adapters.duckdb.adk.DuckdbADKMemoryStore",
    "sqlspec.adapters.mysqlconnector.adk.MysqlConnectorSyncADKMemoryStore",
    "sqlspec.adapters.oracledb.adk.OracleSyncADKMemoryStore",
    "sqlspec.adapters.psycopg.adk.PsycopgSyncADKMemoryStore",
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

    if issubclass(cls, BaseAsyncADKStore):
        base: type = BaseAsyncADKStore
    elif issubclass(cls, BaseSyncADKStore):
        base = BaseSyncADKStore
    elif issubclass(cls, BaseAsyncADKMemoryStore):
        base = BaseAsyncADKMemoryStore
    else:
        base = BaseSyncADKMemoryStore

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
