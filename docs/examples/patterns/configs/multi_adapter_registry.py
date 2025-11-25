"""Show how to register multiple adapters on a single SQLSpec instance."""

import os

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgPoolConfig
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("build_registry", "main")


def build_registry() -> "SQLSpec":
    """Create a registry with both sync and async adapters."""
    dsn = os.getenv("SQLSPEC_USAGE_PG_DSN", "postgresql://localhost/db")
    registry = SQLSpec()
    registry.add_config(SqliteConfig(bind_key="sync_sqlite", pool_config={"database": ":memory:"}))
    registry.add_config(AiosqliteConfig(bind_key="async_sqlite", pool_config={"database": ":memory:"}))
    registry.add_config(DuckDBConfig(bind_key="duckdb_docs", pool_config={"database": ":memory:docs_duck"}))
    registry.add_config(AsyncpgConfig(bind_key="asyncpg_docs", pool_config=AsyncpgPoolConfig(dsn=dsn)))
    return registry


def main() -> None:
    """Print summary of configured adapters and bind keys."""
    registry = build_registry()
    {
        "configs": [cfg.__class__.__name__ for cfg in registry.configs.values()],
        "bind_keys": [cfg.bind_key for cfg in registry.configs.values()],
    }


if __name__ == "__main__":
    main()
