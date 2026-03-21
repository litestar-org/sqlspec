"""Smoke tests verifying all shipped ADK store classes are instantiable (not abstract).

Every shipped store class must be concrete — no unsatisfied abstract methods.
This catches bugs where stores have method signature mismatches with the base
class, such as cockroach, mysqlconnector sync, pymysql, and spanner stores
that are missing abstract method implementations added to the base contract.

NOTE: Some stores WILL fail this test currently — that is expected and
documents one of the bugs the ADK Clean-Break Overhaul (Ch1) is fixing.
"""

import importlib

import pytest

# Session stores (async)
ASYNC_SESSION_STORES = [
    "sqlspec.adapters.asyncpg.adk.store.AsyncpgADKStore",
    "sqlspec.adapters.aiosqlite.adk.store.AiosqliteADKStore",
    "sqlspec.adapters.asyncmy.adk.store.AsyncmyADKStore",
    "sqlspec.adapters.cockroach_asyncpg.adk.store.CockroachAsyncpgADKStore",
    "sqlspec.adapters.cockroach_psycopg.adk.store.CockroachPsycopgAsyncADKStore",
    "sqlspec.adapters.mysqlconnector.adk.store.MysqlConnectorAsyncADKStore",
    "sqlspec.adapters.oracledb.adk.store.OracleAsyncADKStore",
    "sqlspec.adapters.psqlpy.adk.store.PsqlpyADKStore",
    "sqlspec.adapters.psycopg.adk.store.PsycopgAsyncADKStore",
    # sqlite uses BaseAsyncADKStore despite being backed by a sync driver
    "sqlspec.adapters.sqlite.adk.store.SqliteADKStore",
]

# Session stores (sync)
SYNC_SESSION_STORES = [
    "sqlspec.adapters.adbc.adk.store.AdbcADKStore",
    "sqlspec.adapters.cockroach_psycopg.adk.store.CockroachPsycopgSyncADKStore",
    "sqlspec.adapters.duckdb.adk.store.DuckdbADKStore",
    "sqlspec.adapters.mysqlconnector.adk.store.MysqlConnectorSyncADKStore",
    "sqlspec.adapters.oracledb.adk.store.OracleSyncADKStore",
    "sqlspec.adapters.psycopg.adk.store.PsycopgSyncADKStore",
    "sqlspec.adapters.pymysql.adk.store.PyMysqlADKStore",
    "sqlspec.adapters.spanner.adk.store.SpannerSyncADKStore",
]

# Memory stores (async)
ASYNC_MEMORY_STORES = [
    "sqlspec.adapters.asyncpg.adk.store.AsyncpgADKMemoryStore",
    "sqlspec.adapters.aiosqlite.adk.store.AiosqliteADKMemoryStore",
    "sqlspec.adapters.asyncmy.adk.store.AsyncmyADKMemoryStore",
    "sqlspec.adapters.cockroach_asyncpg.adk.store.CockroachAsyncpgADKMemoryStore",
    "sqlspec.adapters.cockroach_psycopg.adk.store.CockroachPsycopgAsyncADKMemoryStore",
    "sqlspec.adapters.mysqlconnector.adk.store.MysqlConnectorAsyncADKMemoryStore",
    "sqlspec.adapters.oracledb.adk.store.OracleAsyncADKMemoryStore",
    "sqlspec.adapters.psqlpy.adk.store.PsqlpyADKMemoryStore",
    "sqlspec.adapters.psycopg.adk.store.PsycopgAsyncADKMemoryStore",
]

# Memory stores (sync)
SYNC_MEMORY_STORES = [
    "sqlspec.adapters.adbc.adk.store.AdbcADKMemoryStore",
    "sqlspec.adapters.cockroach_psycopg.adk.store.CockroachPsycopgSyncADKMemoryStore",
    "sqlspec.adapters.duckdb.adk.store.DuckdbADKMemoryStore",
    "sqlspec.adapters.mysqlconnector.adk.store.MysqlConnectorSyncADKMemoryStore",
    "sqlspec.adapters.oracledb.adk.store.OracleSyncADKMemoryStore",
    "sqlspec.adapters.psycopg.adk.store.PsycopgSyncADKMemoryStore",
    "sqlspec.adapters.pymysql.adk.store.PyMysqlADKMemoryStore",
    "sqlspec.adapters.spanner.adk.store.SpannerSyncADKMemoryStore",
    "sqlspec.adapters.sqlite.adk.store.SqliteADKMemoryStore",
]

ALL_STORE_CLASSES = ASYNC_SESSION_STORES + SYNC_SESSION_STORES + ASYNC_MEMORY_STORES + SYNC_MEMORY_STORES


@pytest.mark.parametrize("class_path", ALL_STORE_CLASSES)
def test_store_has_no_abstract_methods(class_path: str) -> None:
    """Every shipped store class must be concrete (no unsatisfied abstract methods).

    A class with entries in ``__abstractmethods__`` cannot be instantiated and
    signals that the concrete store is missing one or more method implementations
    required by its base class contract.
    """
    module_path, class_name = class_path.rsplit(".", 1)
    try:
        module = importlib.import_module(module_path)
    except ImportError:
        pytest.skip(f"Module {module_path} not importable (missing optional dependency)")
    cls = getattr(module, class_name)
    abstract = getattr(cls, "__abstractmethods__", set())
    assert not abstract, f"{class_path} has unsatisfied abstract methods: {abstract}"
