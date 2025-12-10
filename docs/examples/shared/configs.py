"""Factory helpers for SQLSpec registries used across docs demos."""

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("aiosqlite_registry", "duckdb_registry", "sqlite_registry")


def sqlite_registry(bind_key: str = "docs_sqlite") -> "tuple[SQLSpec, SqliteConfig]":
    """Return a registry with a single SQLite configuration."""
    registry = SQLSpec()
    config = registry.add_config(SqliteConfig(bind_key=bind_key, pool_config={"database": ":memory:"}))
    return registry, config


def aiosqlite_registry(bind_key: str = "docs_aiosqlite") -> "tuple[SQLSpec, AiosqliteConfig]":
    """Return a registry backed by an AioSQLite pool."""
    registry = SQLSpec()
    config = registry.add_config(
        AiosqliteConfig(
            bind_key=bind_key,
            pool_config={"database": ":memory:"},
            extension_config={"litestar": {"commit_mode": "autocommit"}},
        )
    )
    return registry, config


def duckdb_registry(bind_key: str = "docs_duckdb") -> "tuple[SQLSpec, DuckDBConfig]":
    """Return a registry with a DuckDB in-memory database."""
    registry = SQLSpec()
    config = registry.add_config(
        DuckDBConfig(
            bind_key=bind_key,
            pool_config={"database": ":memory:shared_docs"},
            extension_config={"litestar": {"commit_mode": "autocommit"}},
        )
    )
    return registry, config
