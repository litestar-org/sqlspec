"""Dishka integration test fixtures and configuration."""

from typing import TYPE_CHECKING

import pytest

dishka = pytest.importorskip("dishka")

pytestmark = pytest.mark.xdist_group("dishka")

if TYPE_CHECKING:
    from dishka import Provider  # type: ignore[import-not-found]


@pytest.fixture
def simple_sqlite_provider() -> "Provider":
    """Create a simple Dishka provider that provides an SQLite config."""
    from dishka import Provider, Scope, provide  # pyright: ignore[reportMissingImports]

    from sqlspec.adapters.sqlite.config import SqliteConfig

    class DatabaseProvider(Provider):  # type: ignore[misc]
        @provide(scope=Scope.APP)  # type: ignore[misc]
        def get_database_config(self) -> SqliteConfig:
            config = SqliteConfig(connection_config={"database": ":memory:"})
            config.bind_key = "dishka_sqlite"
            return config

    return DatabaseProvider()


@pytest.fixture
def async_sqlite_provider() -> "Provider":
    """Create an async Dishka provider that provides an SQLite config."""
    import asyncio

    from dishka import Provider, Scope, provide  # pyright: ignore[reportMissingImports]

    from sqlspec.adapters.sqlite.config import SqliteConfig

    class AsyncDatabaseProvider(Provider):  # type: ignore[misc]
        @provide(scope=Scope.APP)  # type: ignore[misc]
        async def get_database_config(self) -> SqliteConfig:
            # Simulate some async work (e.g., fetching config from remote service)
            await asyncio.sleep(0.001)
            config = SqliteConfig(connection_config={"database": ":memory:"})
            config.bind_key = "async_dishka_sqlite"
            return config

    return AsyncDatabaseProvider()


@pytest.fixture
def multi_config_provider() -> "Provider":
    """Create a Dishka provider that provides multiple database configs."""
    from dishka import Provider, Scope, provide  # pyright: ignore[reportMissingImports]

    from sqlspec.adapters.duckdb.config import DuckDBConfig
    from sqlspec.adapters.sqlite.config import SqliteConfig

    class MultiDatabaseProvider(Provider):  # type: ignore[misc]
        @provide(scope=Scope.APP)  # type: ignore[misc]
        def get_sqlite_config(self) -> SqliteConfig:
            config = SqliteConfig(connection_config={"database": ":memory:"})
            config.bind_key = "dishka_multi_sqlite"
            config.migration_config = {"enabled": True, "script_location": "sqlite_migrations"}
            return config

        @provide(scope=Scope.APP)  # type: ignore[misc]
        def get_duckdb_config(self) -> DuckDBConfig:
            config = DuckDBConfig(connection_config={"database": ":memory:"})
            config.bind_key = "dishka_multi_duckdb"
            config.migration_config = {"enabled": True, "script_location": "duckdb_migrations"}
            return config

    return MultiDatabaseProvider()


@pytest.fixture
def async_multi_config_provider() -> "Provider":
    """Create an async Dishka provider that provides multiple database configs."""
    import asyncio

    from dishka import Provider, Scope, provide  # pyright: ignore[reportMissingImports]

    from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
    from sqlspec.adapters.duckdb.config import DuckDBConfig
    from sqlspec.adapters.sqlite.config import SqliteConfig

    class AsyncMultiDatabaseProvider(Provider):  # type: ignore[misc]
        @provide(scope=Scope.APP)  # type: ignore[misc]
        async def get_sqlite_config(self) -> SqliteConfig:
            await asyncio.sleep(0.001)
            config = SqliteConfig(connection_config={"database": ":memory:"})
            config.bind_key = "async_multi_sqlite"
            config.migration_config = {"enabled": True}
            return config

        @provide(scope=Scope.APP)  # type: ignore[misc]
        async def get_aiosqlite_config(self) -> AiosqliteConfig:
            await asyncio.sleep(0.001)
            config = AiosqliteConfig(connection_config={"database": ":memory:"})
            config.bind_key = "async_multi_aiosqlite"
            config.migration_config = {"enabled": True}
            return config

        @provide(scope=Scope.APP)  # type: ignore[misc]
        async def get_duckdb_config(self) -> DuckDBConfig:
            await asyncio.sleep(0.001)
            config = DuckDBConfig(connection_config={"database": ":memory:"})
            config.bind_key = "async_multi_duckdb"
            config.migration_config = {"enabled": True}
            return config

    return AsyncMultiDatabaseProvider()
