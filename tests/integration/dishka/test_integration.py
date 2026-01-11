"""Integration tests for Dishka DI framework with SQLSpec CLI."""

from pathlib import Path

import pytest
from click.testing import CliRunner

from sqlspec.cli import add_migration_commands

dishka = pytest.importorskip("dishka")

pytestmark = pytest.mark.xdist_group("dishka")


def test_simple_sync_dishka_provider(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test CLI with a simple synchronous Dishka provider."""
    runner = CliRunner()

    monkeypatch.chdir(tmp_path)

    config_module = '''
from dishka import make_container
from tests.integration.dishka.conftest import simple_sqlite_provider

def get_config_from_dishka():
    """Get config from Dishka container synchronously."""
    from sqlspec.adapters.sqlite.config import SqliteConfig

    # Create the provider directly (simulating the fixture)
    from dishka import Provider, provide, Scope

    class DatabaseProvider(Provider):
        @provide(scope=Scope.APP)
        def get_database_config(self) -> SqliteConfig:
            return SqliteConfig(
                connection_config={"database": ":memory:"},
                migration_config={"enabled": True, "script_location": "migrations"},
                bind_key="dishka_sqlite"
            )

    container = make_container(DatabaseProvider())
    with container() as request_container:
        return request_container.get(SqliteConfig)
'''
    Path("dishka_config.py").write_text(config_module)

    result = runner.invoke(
        add_migration_commands(), ["--config", "dishka_config.get_config_from_dishka", "show-config"]
    )

    assert result.exit_code == 0
    assert "dishka_sqlite" in result.output
    assert "Migration Enabled" in result.output or "migrations enabled" in result.output


def test_async_dishka_provider(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test CLI with an asynchronous Dishka provider."""
    runner = CliRunner()

    monkeypatch.chdir(tmp_path)

    config_module = '''
import asyncio
from dishka import make_async_container, Provider, provide, Scope
from sqlspec.adapters.sqlite.config import SqliteConfig

class AsyncDatabaseProvider(Provider):
    @provide(scope=Scope.APP)
    async def get_database_config(self) -> SqliteConfig:
        # Simulate some async work
        await asyncio.sleep(0.001)
        return SqliteConfig(
            connection_config={"database": ":memory:"},
            migration_config={"enabled": True, "script_location": "migrations"},
            bind_key="async_dishka_sqlite"
        )

async def get_async_config_from_dishka():
    """Get config from async Dishka container."""
    container = make_async_container(AsyncDatabaseProvider())
    async with container() as request_container:
        return await request_container.get(SqliteConfig)
'''
    Path("async_dishka_config.py").write_text(config_module)

    result = runner.invoke(
        add_migration_commands(), ["--config", "async_dishka_config.get_async_config_from_dishka", "show-config"]
    )

    assert result.exit_code == 0
    assert "async_dishka_sqlite" in result.output
    assert "Migration Enabled" in result.output or "migrations enabled" in result.output


def test_multi_config_dishka_provider(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test CLI with Dishka provider returning multiple configs."""
    runner = CliRunner()

    monkeypatch.chdir(tmp_path)

    config_module = '''
from dishka import make_container, Provider, provide, Scope
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.adapters.duckdb import DuckDBConfig

class MultiDatabaseProvider(Provider):
    @provide(scope=Scope.APP)
    def get_sqlite_config(self) -> SqliteConfig:
        return SqliteConfig(
            connection_config={"database": ":memory:"},
            migration_config={"enabled": True, "script_location": "sqlite_migrations"},
            bind_key="dishka_multi_sqlite"
        )

    @provide(scope=Scope.APP)
    def get_duckdb_config(self) -> DuckDBConfig:
        return DuckDBConfig(
            connection_config={"database": ":memory:"},
            migration_config={"enabled": True, "script_location": "duckdb_migrations"},
            bind_key="dishka_multi_duckdb"
        )

def get_multi_configs_from_dishka():
    """Get multiple configs from Dishka container."""
    container = make_container(MultiDatabaseProvider())
    with container() as request_container:
        sqlite_config = request_container.get(SqliteConfig)
        duckdb_config = request_container.get(DuckDBConfig)
        return [sqlite_config, duckdb_config]
'''
    Path("multi_dishka_config.py").write_text(config_module)

    result = runner.invoke(
        add_migration_commands(), ["--config", "multi_dishka_config.get_multi_configs_from_dishka", "show-config"]
    )

    assert result.exit_code == 0
    assert "dishka_multi_sqlite" in result.output
    assert "dishka_multi_duckdb" in result.output
    assert "2 configuration(s)" in result.output


def test_async_multi_config_dishka_provider(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test CLI with async Dishka provider returning multiple configs."""
    runner = CliRunner()

    monkeypatch.chdir(tmp_path)

    config_module = '''
import asyncio
from dishka import make_async_container, Provider, provide, Scope
from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.adapters.duckdb.config import DuckDBConfig
from sqlspec.adapters.aiosqlite.config import AiosqliteConfig

class AsyncMultiDatabaseProvider(Provider):
    @provide(scope=Scope.APP)
    async def get_sqlite_config(self) -> SqliteConfig:
        await asyncio.sleep(0.001)
        return SqliteConfig(
            connection_config={"database": ":memory:"},
            migration_config={"enabled": True},
            bind_key="async_multi_sqlite"
        )

    @provide(scope=Scope.APP)
    async def get_aiosqlite_config(self) -> AiosqliteConfig:
        await asyncio.sleep(0.001)
        return AiosqliteConfig(
            connection_config={"database": ":memory:"},
            migration_config={"enabled": True},
            bind_key="async_multi_aiosqlite"
        )

    @provide(scope=Scope.APP)
    async def get_duckdb_config(self) -> DuckDBConfig:
        await asyncio.sleep(0.001)
        return DuckDBConfig(
            connection_config={"database": ":memory:"},
            migration_config={"enabled": True},
            bind_key="async_multi_duckdb"
        )

async def get_async_multi_configs_from_dishka():
    """Get multiple configs from async Dishka container."""
    container = make_async_container(AsyncMultiDatabaseProvider())
    async with container() as request_container:
        sqlite_config = await request_container.get(SqliteConfig)
        aiosqlite_config = await request_container.get(AiosqliteConfig)
        duckdb_config = await request_container.get(DuckDBConfig)
        return [sqlite_config, aiosqlite_config, duckdb_config]
'''
    Path("async_multi_dishka_config.py").write_text(config_module)

    result = runner.invoke(
        add_migration_commands(),
        ["--config", "async_multi_dishka_config.get_async_multi_configs_from_dishka", "show-config"],
    )

    assert result.exit_code == 0
    assert "async_multi_sqlite" in result.output
    assert "async_multi_aiosqlite" in result.output
    assert "async_multi_duckdb" in result.output
    assert "3 configuration(s)" in result.output


def test_dishka_provider_with_dependencies(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test Dishka provider that has complex dependencies."""
    runner = CliRunner()

    monkeypatch.chdir(tmp_path)

    config_module = '''
from dishka import make_container, Provider, provide, Scope
from sqlspec.adapters.sqlite.config import SqliteConfig

class SettingsProvider(Provider):
    @provide(scope=Scope.APP)
    def get_database_url(self) -> str:
        return ":memory:"

    @provide(scope=Scope.APP)
    def get_bind_key(self) -> str:
        return "complex_dishka"

class DatabaseProvider(Provider):
    @provide(scope=Scope.APP)
    def get_database_config(self, database_url: str, bind_key: str) -> SqliteConfig:
        return SqliteConfig(
            connection_config={"database": database_url},
            migration_config={"enabled": True, "script_location": "complex_migrations"},
            bind_key=bind_key
        )

def get_complex_config_from_dishka():
    """Get config with dependencies from Dishka container."""
    container = make_container(SettingsProvider(), DatabaseProvider())
    with container() as request_container:
        return request_container.get(SqliteConfig)
'''
    Path("complex_dishka_config.py").write_text(config_module)

    result = runner.invoke(
        add_migration_commands(), ["--config", "complex_dishka_config.get_complex_config_from_dishka", "show-config"]
    )

    assert result.exit_code == 0
    assert "complex_dishka" in result.output
    assert "complex_migrations" in result.output


def test_dishka_error_handling(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test proper error handling when Dishka container fails."""
    runner = CliRunner()

    monkeypatch.chdir(tmp_path)

    config_module = '''
from dishka import make_container, Provider
from sqlspec.adapters.sqlite.config import SqliteConfig

class EmptyProvider(Provider):
    pass  # No providers for SqliteConfig

def get_failing_dishka_config():
    """Try to get config when no provider exists."""
    container = make_container(EmptyProvider())
    with container() as request_container:
        # This should raise an exception
        return request_container.get(SqliteConfig)
'''
    Path("failing_dishka_config.py").write_text(config_module)

    result = runner.invoke(
        add_migration_commands(), ["--config", "failing_dishka_config.get_failing_dishka_config", "show-config"]
    )

    assert result.exit_code == 1
    assert "Error loading config" in result.output
    assert "Failed to execute callable config" in result.output


def test_dishka_async_with_migration_commands(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that migration commands work with async Dishka configs."""
    runner = CliRunner()

    monkeypatch.chdir(tmp_path)

    config_module = '''
import asyncio
from dishka import make_async_container, Provider, provide, Scope
from sqlspec.adapters.sqlite.config import SqliteConfig

class MigrationProvider(Provider):
    @provide(scope=Scope.APP)
    async def get_database_config(self) -> SqliteConfig:
        await asyncio.sleep(0.001)
        return SqliteConfig(
            connection_config={"database": ":memory:"},
            migration_config={
                "enabled": True,
                "script_location": "dishka_migrations"
            },
            bind_key="migration_dishka"
        )

async def get_migration_config_from_dishka():
    """Get migration-enabled config from async Dishka container."""
    container = make_async_container(MigrationProvider())
    async with container() as request_container:
        return await request_container.get(SqliteConfig)
'''
    Path("migration_dishka_config.py").write_text(config_module)

    result = runner.invoke(
        add_migration_commands(),
        ["--config", "migration_dishka_config.get_migration_config_from_dishka", "show-config"],
    )

    assert result.exit_code == 0
    assert "migration_dishka" in result.output
    assert "dishka_migrations" in result.output or "Migration Enabled" in result.output


def test_dishka_with_config_validation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test Dishka integration with config validation enabled."""
    runner = CliRunner()

    monkeypatch.chdir(tmp_path)

    config_module = '''
import asyncio
from dishka import make_async_container, Provider, provide, Scope
from sqlspec.adapters.duckdb.config import DuckDBConfig

class ValidatedProvider(Provider):
    @provide(scope=Scope.APP)
    async def get_database_config(self) -> DuckDBConfig:
        await asyncio.sleep(0.001)
        return DuckDBConfig(
            connection_config={"database": ":memory:"},
            migration_config={"enabled": True},
            bind_key="validated_dishka"
        )

async def get_validated_config_from_dishka():
    """Get config for validation testing."""
    container = make_async_container(ValidatedProvider())
    async with container() as request_container:
        return await request_container.get(DuckDBConfig)
'''
    Path("validated_dishka_config.py").write_text(config_module)

    result = runner.invoke(
        add_migration_commands(),
        ["--config", "validated_dishka_config.get_validated_config_from_dishka", "--validate-config", "show-config"],
    )

    assert result.exit_code == 0
    assert "Successfully loaded 1 config(s)" in result.output
    assert "validated_dishka" in result.output


def test_real_world_dishka_scenario(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test a real-world scenario mimicking the user's issue."""
    runner = CliRunner()

    monkeypatch.chdir(tmp_path)

    Path("litestar_dishka_modular").mkdir()
    Path("litestar_dishka_modular/__init__.py").write_text("")
    Path("litestar_dishka_modular/sqlspec_main.py").write_text("")

    config_module = '''
"""Simulates the user's actual Dishka configuration."""
import asyncio
from typing import List
from dishka import make_async_container, Provider, provide, Scope
from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.adapters.duckdb.config import DuckDBConfig

class DatabaseConfigProvider(Provider):
    """Provider for database configurations."""

    @provide(scope=Scope.APP)
    async def get_primary_db_config(self) -> SqliteConfig:
        # Simulate loading config from environment or remote service
        await asyncio.sleep(0.002)  # Simulate I/O
        return SqliteConfig(
            connection_config={"database": ":memory:"},
            migration_config={
                "enabled": True,
                "script_location": "migrations/primary"
            },
            bind_key="primary_db"
        )

    @provide(scope=Scope.APP)
    async def get_analytics_db_config(self) -> DuckDBConfig:
        await asyncio.sleep(0.002)
        return DuckDBConfig(
            connection_config={"database": ":memory:"},
            migration_config={
                "enabled": True,
                "script_location": "migrations/analytics"
            },
            bind_key="analytics_db"
        )

async def main() -> List:
    """Main entry point - this is what the user was trying to call."""
    container = make_async_container(DatabaseConfigProvider())

    async with container() as request_container:
        primary_config = await request_container.get(SqliteConfig)
        analytics_config = await request_container.get(DuckDBConfig)
        return [primary_config, analytics_config]
'''
    Path("litestar_dishka_modular/sqlspec_main.py").write_text(config_module)

    result = runner.invoke(
        add_migration_commands(), ["--config", "litestar_dishka_modular.sqlspec_main.main", "show-config"]
    )

    assert result.exit_code == 0
    assert "primary_db" in result.output
    assert "analytics_db" in result.output
    assert "2 configuration(s)" in result.output


def test_dishka_provider_cleanup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that Dishka providers are properly cleaned up."""
    runner = CliRunner()

    monkeypatch.chdir(tmp_path)

    config_module = '''
import asyncio
from dishka import make_async_container, Provider, provide, Scope
from sqlspec.adapters.sqlite.config import SqliteConfig

cleanup_called = False

class CleanupProvider(Provider):
    @provide(scope=Scope.APP)
    async def get_database_config(self) -> SqliteConfig:
        await asyncio.sleep(0.001)
        return SqliteConfig(
            connection_config={"database": ":memory:"},
            migration_config={"enabled": True},
            bind_key="cleanup_test"
        )

    def __del__(self):
        global cleanup_called
        cleanup_called = True

async def get_cleanup_config():
    """Test that container cleanup works properly."""
    container = make_async_container(CleanupProvider())
    async with container() as request_container:
        config = await request_container.get(SqliteConfig)
        return config
'''
    Path("cleanup_dishka_config.py").write_text(config_module)

    result = runner.invoke(
        add_migration_commands(), ["--config", "cleanup_dishka_config.get_cleanup_config", "show-config"]
    )

    assert result.exit_code == 0
    assert "cleanup_test" in result.output
