"""Factory helpers for SQLSpec registries used across docs demos."""

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("aiosqlite_registry", "duckdb_registry", "sqlite_registry")


def sqlite_registry(bind_key: str = "docs_sqlite") -> "SQLSpec":
    """Return a registry with a single SQLite configuration."""
    registry = SQLSpec()
    registry.add_config(SqliteConfig(bind_key=bind_key, pool_config={"database": ":memory:"}))
    return registry


def aiosqlite_registry(bind_key: str = "docs_aiosqlite") -> "SQLSpec":
    """Return a registry backed by an AioSQLite pool."""
    registry = SQLSpec()
    registry.add_config(
        AiosqliteConfig(
            bind_key=bind_key,
            pool_config={"database": ":memory:"},
            extension_config={"litestar": {"commit_mode": "autocommit"}},
        )
    )
    return registry


def duckdb_registry(bind_key: str = "docs_duckdb") -> "SQLSpec":
    """Return a registry with a DuckDB in-memory database."""
    registry = SQLSpec()
    registry.add_config(
        DuckDBConfig(
            bind_key=bind_key,
            pool_config={"database": ":memory:shared_docs"},
            extension_config={"litestar": {"commit_mode": "autocommit"}},
        )
    )
    return registry
