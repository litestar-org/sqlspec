"""Smoke tests verifying all shipped ADK store classes are instantiable (not abstract).

Every shipped store class must be concrete — no unsatisfied abstract methods.
This catches bugs where stores have method signature mismatches with the base
class, such as cockroach, mysqlconnector sync, pymysql, and spanner stores
that are missing abstract method implementations added to the base contract.

The class list mirrors the shipped adapter ``adk`` exports and catches drift
when a concrete store no longer satisfies the base contract.
"""

import importlib

import pytest

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
    abstract: set[str] = getattr(cls, "__abstractmethods__", set())
    assert not abstract, f"{class_path} has unsatisfied abstract methods: {abstract}"
