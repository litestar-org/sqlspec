"""Dishka integration test fixtures and configuration."""

from typing import TYPE_CHECKING

import pytest

dishka = pytest.importorskip("dishka")

if TYPE_CHECKING:
    from dishka import Provider


@pytest.fixture
def simple_sqlite_provider() -> "Provider":
    """Create a simple Dishka provider that provides an SQLite config."""
    from dishka import Provider, Scope, provide

    from sqlspec.adapters.sqlite.config import SqliteConfig

    class DatabaseProvider(Provider):
        @provide(scope=Scope.APP)
        def get_database_config(self) -> SqliteConfig:
            config = SqliteConfig(pool_config={"database": ":memory:"})
            config.bind_key = "dishka_sqlite"
            return config

    return DatabaseProvider()


@pytest.fixture
def async_sqlite_provider() -> "Provider":
    """Create an async Dishka provider that provides an SQLite config."""
    import asyncio

    from dishka import Provider, Scope, provide

    from sqlspec.adapters.sqlite.config import SqliteConfig

    class AsyncDatabaseProvider(Provider):
        @provide(scope=Scope.APP)
        async def get_database_config(self) -> SqliteConfig:
            # Simulate some async work (e.g., fetching config from remote service)
            await asyncio.sleep(0.001)
            config = SqliteConfig(pool_config={"database": ":memory:"})
            config.bind_key = "async_dishka_sqlite"
            return config

    return AsyncDatabaseProvider()


@pytest.fixture
def multi_config_provider() -> "Provider":
    """Create a Dishka provider that provides multiple database configs."""
    from dishka import Provider, Scope, provide

    from sqlspec.adapters.duckdb.config import DuckDBConfig
    from sqlspec.adapters.sqlite.config import SqliteConfig

    class MultiDatabaseProvider(Provider):
        @provide(scope=Scope.APP)
        def get_sqlite_config(self) -> SqliteConfig:
            config = SqliteConfig(pool_config={"database": ":memory:"})
            config.bind_key = "dishka_multi_sqlite"
            config.migration_config = {"enabled": True, "script_location": "sqlite_migrations"}
            return config

        @provide(scope=Scope.APP)
        def get_duckdb_config(self) -> DuckDBConfig:
            config = DuckDBConfig(pool_config={"database": ":memory:"})
            config.bind_key = "dishka_multi_duckdb"
            config.migration_config = {"enabled": True, "script_location": "duckdb_migrations"}
            return config

    return MultiDatabaseProvider()


@pytest.fixture
def async_multi_config_provider() -> "Provider":
    """Create an async Dishka provider that provides multiple database configs."""
    import asyncio

    from dishka import Provider, Scope, provide

    from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
    from sqlspec.adapters.duckdb.config import DuckDBConfig
    from sqlspec.adapters.sqlite.config import SqliteConfig

    class AsyncMultiDatabaseProvider(Provider):
        @provide(scope=Scope.APP)
        async def get_sqlite_config(self) -> SqliteConfig:
            await asyncio.sleep(0.001)
            config = SqliteConfig(pool_config={"database": ":memory:"})
            config.bind_key = "async_multi_sqlite"
            config.migration_config = {"enabled": True}
            return config

        @provide(scope=Scope.APP)
        async def get_aiosqlite_config(self) -> AiosqliteConfig:
            await asyncio.sleep(0.001)
            config = AiosqliteConfig(pool_config={"database": ":memory:"})
            config.bind_key = "async_multi_aiosqlite"
            config.migration_config = {"enabled": True}
            return config

        @provide(scope=Scope.APP)
        async def get_duckdb_config(self) -> DuckDBConfig:
            await asyncio.sleep(0.001)
            config = DuckDBConfig(pool_config={"database": ":memory:"})
            config.bind_key = "async_multi_duckdb"
            config.migration_config = {"enabled": True}
            return config

    return AsyncMultiDatabaseProvider()
