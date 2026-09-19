"""Unit tests for config migration convenience methods.

Tests the 7 migration methods added to DatabaseConfigProtocol:
- migrate_up()
- migrate_down()
- get_current_migration()
- create_migration()
- init_migrations()
- stamp_migration()
- fix_migrations()

Tests cover all 4 base config classes:
- NoPoolSyncConfig (sync, no pool)
- NoPoolAsyncConfig (async, no pool)
- SyncDatabaseConfig (sync, pooled)
- AsyncDatabaseConfig (async, pooled)
"""

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier, Event
from unittest.mock import patch

import pytest

import sqlspec.config as config_module
import sqlspec.typing as typing_module
from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.adapters.asyncpg.config import AsyncpgConfig
from sqlspec.adapters.duckdb.config import DuckDBConfig
from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.config import (
    AsyncDatabaseConfig,
    ConnectionT,
    DriverT,
    NoPoolAsyncConfig,
    NoPoolSyncConfig,
    PoolT,
    SyncDatabaseConfig,
)
from sqlspec.loader import SQLFileLoader
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands
from sqlspec.migrations.tracker import AsyncMigrationTracker, SyncMigrationTracker


@pytest.mark.parametrize("key", ["script_location", "project_root"])
def test_invalid_builtin_migration_paths_remain_eager(key: str) -> None:
    """Deferring helpers must not defer built-in path errors."""
    with pytest.raises(TypeError):
        SqliteConfig(migration_config={key: None})


def test_invalid_migration_template_remains_eager() -> None:
    from sqlspec.migrations.templates import TemplateValidationError

    with pytest.raises(TemplateValidationError, match="string or list"):
        SqliteConfig(migration_config={"templates": {"sql": {"metadata": 42}}})


def test_migration_format_fallback_is_preserved() -> None:
    config = SqliteConfig(migration_config={"default_format": "unknown"})
    assert config.get_migration_commands()._template_settings.default_format == "sql"


def test_config_reexports_shared_type_variables() -> None:
    """Test that config exposes the canonical shared type variables."""
    assert ConnectionT is typing_module.ConnectionT
    assert PoolT is typing_module.PoolT
    assert DriverT is config_module.DriverT


def test_migration_methods_are_inherited_from_mixins() -> None:
    """Test that base configs inherit shared migration implementations."""
    assert config_module._SyncMigrationMixin in NoPoolSyncConfig.__mro__
    assert config_module._SyncMigrationMixin in SyncDatabaseConfig.__mro__
    assert config_module._AsyncMigrationMixin in NoPoolAsyncConfig.__mro__
    assert config_module._AsyncMigrationMixin in AsyncDatabaseConfig.__mro__
    assert "migrate_up" not in NoPoolSyncConfig.__dict__
    assert "migrate_up" not in SyncDatabaseConfig.__dict__
    assert "migrate_up" not in NoPoolAsyncConfig.__dict__
    assert "migrate_up" not in AsyncDatabaseConfig.__dict__


def test_sync_config_has_migration_methods() -> None:
    """Test that SyncDatabaseConfig has all migration methods."""
    assert hasattr(SyncDatabaseConfig, "migrate_up")
    assert hasattr(SyncDatabaseConfig, "migrate_down")
    assert hasattr(SyncDatabaseConfig, "get_current_migration")
    assert hasattr(SyncDatabaseConfig, "create_migration")
    assert hasattr(SyncDatabaseConfig, "init_migrations")
    assert hasattr(SyncDatabaseConfig, "stamp_migration")
    assert hasattr(SyncDatabaseConfig, "fix_migrations")


def test_async_config_has_migration_methods() -> None:
    """Test that AsyncDatabaseConfig has all migration methods."""
    assert hasattr(AsyncDatabaseConfig, "migrate_up")
    assert hasattr(AsyncDatabaseConfig, "migrate_down")
    assert hasattr(AsyncDatabaseConfig, "get_current_migration")
    assert hasattr(AsyncDatabaseConfig, "create_migration")
    assert hasattr(AsyncDatabaseConfig, "init_migrations")
    assert hasattr(AsyncDatabaseConfig, "stamp_migration")
    assert hasattr(AsyncDatabaseConfig, "fix_migrations")


def test_no_pool_sync_config_has_migration_methods() -> None:
    """Test that NoPoolSyncConfig has all migration methods."""
    assert hasattr(NoPoolSyncConfig, "migrate_up")
    assert hasattr(NoPoolSyncConfig, "migrate_down")
    assert hasattr(NoPoolSyncConfig, "get_current_migration")
    assert hasattr(NoPoolSyncConfig, "create_migration")
    assert hasattr(NoPoolSyncConfig, "init_migrations")
    assert hasattr(NoPoolSyncConfig, "stamp_migration")
    assert hasattr(NoPoolSyncConfig, "fix_migrations")


def test_no_pool_async_config_has_migration_methods() -> None:
    """Test that NoPoolAsyncConfig has all migration methods."""
    assert hasattr(NoPoolAsyncConfig, "migrate_up")
    assert hasattr(NoPoolAsyncConfig, "migrate_down")
    assert hasattr(NoPoolAsyncConfig, "get_current_migration")
    assert hasattr(NoPoolAsyncConfig, "create_migration")
    assert hasattr(NoPoolAsyncConfig, "init_migrations")
    assert hasattr(NoPoolAsyncConfig, "stamp_migration")
    assert hasattr(NoPoolAsyncConfig, "fix_migrations")


def test_sqlite_config_migrate_up_calls_commands(tmp_path: Path) -> None:
    """Test that SqliteConfig.migrate_up() delegates to SyncMigrationCommands.upgrade()."""
    migration_dir = tmp_path / "migrations"
    temp_db = str(tmp_path / "test.db")

    config = SqliteConfig(
        connection_config={"database": temp_db}, migration_config={"script_location": str(migration_dir)}
    )

    with patch.object(SyncMigrationCommands, "upgrade", return_value=None) as mock_upgrade:
        config.migrate_up(revision="head", allow_missing=True, auto_sync=False, dry_run=True)

        mock_upgrade.assert_called_once_with("head", True, False, True, use_logger=False, echo=None, summary_only=None)


def test_sqlite_config_migrate_down_calls_commands(tmp_path: Path) -> None:
    """Test that SqliteConfig.migrate_down() delegates to SyncMigrationCommands.downgrade()."""
    migration_dir = tmp_path / "migrations"
    temp_db = str(tmp_path / "test.db")

    config = SqliteConfig(
        connection_config={"database": temp_db}, migration_config={"script_location": str(migration_dir)}
    )

    with patch.object(SyncMigrationCommands, "downgrade", return_value=None) as mock_downgrade:
        config.migrate_down(revision="-2", dry_run=True)

        mock_downgrade.assert_called_once_with("-2", dry_run=True, use_logger=False, echo=None, summary_only=None)


def test_sqlite_config_get_current_migration_calls_commands(tmp_path: Path) -> None:
    """Test that SqliteConfig.get_current_migration() delegates to SyncMigrationCommands.current()."""
    migration_dir = tmp_path / "migrations"
    temp_db = str(tmp_path / "test.db")

    config = SqliteConfig(
        connection_config={"database": temp_db}, migration_config={"script_location": str(migration_dir)}
    )

    with patch.object(SyncMigrationCommands, "current", return_value="0001") as mock_current:
        result = config.get_current_migration(verbose=True)

        mock_current.assert_called_once_with(verbose=True)
        assert result == "0001"


def test_sqlite_config_create_migration_calls_commands(tmp_path: Path) -> None:
    """Test that SqliteConfig.create_migration() delegates to SyncMigrationCommands.revision()."""
    migration_dir = tmp_path / "migrations"
    temp_db = str(tmp_path / "test.db")

    config = SqliteConfig(
        connection_config={"database": temp_db}, migration_config={"script_location": str(migration_dir)}
    )

    with patch.object(SyncMigrationCommands, "revision", return_value=None) as mock_revision:
        config.create_migration(message="test migration", file_type="py")

        mock_revision.assert_called_once_with("test migration", "py")


def test_sqlite_config_init_migrations_calls_commands(tmp_path: Path) -> None:
    """Test that SqliteConfig.init_migrations() delegates to SyncMigrationCommands.init()."""
    migration_dir = tmp_path / "migrations"
    temp_db = str(tmp_path / "test.db")

    config = SqliteConfig(
        connection_config={"database": temp_db}, migration_config={"script_location": str(migration_dir)}
    )

    with patch.object(SyncMigrationCommands, "init", return_value=None) as mock_init:
        config.init_migrations(directory=str(migration_dir), package=False)

        mock_init.assert_called_once_with(str(migration_dir), False)


def test_sqlite_config_init_migrations_uses_default_directory(tmp_path: Path) -> None:
    """Test that SqliteConfig.init_migrations() uses script_location when directory not provided."""
    migration_dir = tmp_path / "migrations"
    temp_db = str(tmp_path / "test.db")

    config = SqliteConfig(
        connection_config={"database": temp_db}, migration_config={"script_location": str(migration_dir)}
    )

    with patch.object(SyncMigrationCommands, "init", return_value=None) as mock_init:
        config.init_migrations(package=True)

        mock_init.assert_called_once_with(str(migration_dir), True)


def test_sqlite_config_refreshes_migration_components_after_assignment(tmp_path: Path) -> None:
    """Late migration_config assignment should refresh cached migration helpers."""
    temp_db = str(tmp_path / "test.db")
    migration_dir = tmp_path / "migrations"
    migration_dir.mkdir()

    config = SqliteConfig(connection_config={"database": temp_db})

    original_commands = config.get_migration_commands()
    original_loader = config.get_migration_loader()

    config.migration_config = {"script_location": str(migration_dir)}

    refreshed_commands = config.get_migration_commands()
    refreshed_loader = config.get_migration_loader()

    assert refreshed_commands is not original_commands
    assert refreshed_loader is not original_loader
    assert refreshed_commands.migrations_path == migration_dir


def test_sqlite_config_stamp_migration_calls_commands(tmp_path: Path) -> None:
    """Test that SqliteConfig.stamp_migration() delegates to SyncMigrationCommands.stamp()."""
    migration_dir = tmp_path / "migrations"
    temp_db = str(tmp_path / "test.db")

    config = SqliteConfig(
        connection_config={"database": temp_db}, migration_config={"script_location": str(migration_dir)}
    )

    with patch.object(SyncMigrationCommands, "stamp", return_value=None) as mock_stamp:
        config.stamp_migration(revision="0001")

        mock_stamp.assert_called_once_with("0001")


def test_sqlite_config_fix_migrations_calls_commands(tmp_path: Path) -> None:
    """Test that SqliteConfig.fix_migrations() delegates to SyncMigrationCommands.fix()."""
    migration_dir = tmp_path / "migrations"
    temp_db = str(tmp_path / "test.db")

    config = SqliteConfig(
        connection_config={"database": temp_db}, migration_config={"script_location": str(migration_dir)}
    )

    with patch.object(SyncMigrationCommands, "fix", return_value=None) as mock_fix:
        config.fix_migrations(dry_run=True, update_database=False, yes=True)

        mock_fix.assert_called_once_with(True, False, True)


async def test_asyncpg_config_migrate_up_calls_commands(tmp_path: Path) -> None:
    """Test that AsyncpgConfig.migrate_up() delegates to AsyncMigrationCommands.upgrade()."""
    migration_dir = tmp_path / "migrations"

    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/test"},
        migration_config={"script_location": str(migration_dir)},
    )

    with patch.object(AsyncMigrationCommands, "upgrade", return_value=None) as mock_upgrade:
        await config.migrate_up(revision="0002", allow_missing=False, auto_sync=True, dry_run=False)

        mock_upgrade.assert_called_once_with("0002", False, True, False, use_logger=False, echo=None, summary_only=None)


async def test_asyncpg_config_migrate_down_calls_commands(tmp_path: Path) -> None:
    """Test that AsyncpgConfig.migrate_down() delegates to AsyncMigrationCommands.downgrade()."""
    migration_dir = tmp_path / "migrations"

    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/test"},
        migration_config={"script_location": str(migration_dir)},
    )

    with patch.object(AsyncMigrationCommands, "downgrade", return_value=None) as mock_downgrade:
        await config.migrate_down(revision="base", dry_run=False)

        mock_downgrade.assert_called_once_with("base", dry_run=False, use_logger=False, echo=None, summary_only=None)


async def test_asyncpg_config_get_current_migration_calls_commands(tmp_path: Path) -> None:
    """Test that AsyncpgConfig.get_current_migration() delegates to AsyncMigrationCommands.current()."""
    migration_dir = tmp_path / "migrations"

    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/test"},
        migration_config={"script_location": str(migration_dir)},
    )

    with patch.object(AsyncMigrationCommands, "current", return_value="0002") as mock_current:
        result = await config.get_current_migration(verbose=False)

        mock_current.assert_called_once_with(verbose=False)
        assert result == "0002"


async def test_asyncpg_config_create_migration_calls_commands(tmp_path: Path) -> None:
    """Test that AsyncpgConfig.create_migration() delegates to AsyncMigrationCommands.revision()."""
    migration_dir = tmp_path / "migrations"

    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/test"},
        migration_config={"script_location": str(migration_dir)},
    )

    with patch.object(AsyncMigrationCommands, "revision", return_value=None) as mock_revision:
        await config.create_migration(message="add users table", file_type="sql")

        mock_revision.assert_called_once_with("add users table", "sql")


async def test_asyncpg_config_init_migrations_calls_commands(tmp_path: Path) -> None:
    """Test that AsyncpgConfig.init_migrations() delegates to AsyncMigrationCommands.init()."""
    migration_dir = tmp_path / "migrations"

    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/test"},
        migration_config={"script_location": str(migration_dir)},
    )

    with patch.object(AsyncMigrationCommands, "init", return_value=None) as mock_init:
        await config.init_migrations(directory=str(migration_dir), package=True)

        mock_init.assert_called_once_with(str(migration_dir), True)


async def test_asyncpg_config_stamp_migration_calls_commands(tmp_path: Path) -> None:
    """Test that AsyncpgConfig.stamp_migration() delegates to AsyncMigrationCommands.stamp()."""
    migration_dir = tmp_path / "migrations"

    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/test"},
        migration_config={"script_location": str(migration_dir)},
    )

    with patch.object(AsyncMigrationCommands, "stamp", return_value=None) as mock_stamp:
        await config.stamp_migration(revision="0003")

        mock_stamp.assert_called_once_with("0003")


async def test_asyncpg_config_fix_migrations_calls_commands(tmp_path: Path) -> None:
    """Test that AsyncpgConfig.fix_migrations() delegates to AsyncMigrationCommands.fix()."""
    migration_dir = tmp_path / "migrations"

    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/test"},
        migration_config={"script_location": str(migration_dir)},
    )

    with patch.object(AsyncMigrationCommands, "fix", return_value=None) as mock_fix:
        await config.fix_migrations(dry_run=False, update_database=True, yes=False)

        mock_fix.assert_called_once_with(False, True, False)


def test_duckdb_pooled_config_migrate_up_calls_commands(tmp_path: Path) -> None:
    """Test that DuckDBConfig.migrate_up() delegates to SyncMigrationCommands.upgrade()."""
    migration_dir = tmp_path / "migrations"

    config = DuckDBConfig(
        connection_config={"database": ":memory:"}, migration_config={"script_location": str(migration_dir)}
    )

    with patch.object(SyncMigrationCommands, "upgrade", return_value=None) as mock_upgrade:
        config.migrate_up(revision="head", allow_missing=False, auto_sync=True, dry_run=False)

        mock_upgrade.assert_called_once_with("head", False, True, False, use_logger=False, echo=None, summary_only=None)


def test_duckdb_pooled_config_get_current_migration_calls_commands(tmp_path: Path) -> None:
    """Test that DuckDBConfig.get_current_migration() delegates to SyncMigrationCommands.current()."""
    migration_dir = tmp_path / "migrations"

    config = DuckDBConfig(
        connection_config={"database": ":memory:"}, migration_config={"script_location": str(migration_dir)}
    )

    with patch.object(SyncMigrationCommands, "current", return_value=None) as mock_current:
        result = config.get_current_migration(verbose=False)

        mock_current.assert_called_once_with(verbose=False)
        assert result is None


async def test_aiosqlite_async_config_migrate_up_calls_commands(tmp_path: Path) -> None:
    """Test that AiosqliteConfig.migrate_up() delegates to AsyncMigrationCommands.upgrade()."""
    migration_dir = tmp_path / "migrations"
    temp_db = str(tmp_path / "test.db")

    config = AiosqliteConfig(
        connection_config={"database": temp_db}, migration_config={"script_location": str(migration_dir)}
    )

    with patch.object(AsyncMigrationCommands, "upgrade", return_value=None) as mock_upgrade:
        await config.migrate_up(revision="head", allow_missing=True, auto_sync=True, dry_run=True)

        mock_upgrade.assert_called_once_with("head", True, True, True, use_logger=False, echo=None, summary_only=None)


def test_migrate_up_default_parameters_sync(tmp_path: Path) -> None:
    """Test that migrate_up() uses correct default parameter values for sync configs."""
    migration_dir = tmp_path / "migrations"
    temp_db = str(tmp_path / "test.db")

    config = SqliteConfig(
        connection_config={"database": temp_db}, migration_config={"script_location": str(migration_dir)}
    )

    with patch.object(SyncMigrationCommands, "upgrade", return_value=None) as mock_upgrade:
        config.migrate_up()

        mock_upgrade.assert_called_once_with("head", False, True, False, use_logger=False, echo=None, summary_only=None)


async def test_migrate_up_default_parameters_async(tmp_path: Path) -> None:
    """Test that migrate_up() uses correct default parameter values for async configs."""
    migration_dir = tmp_path / "migrations"

    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/test"},
        migration_config={"script_location": str(migration_dir)},
    )

    with patch.object(AsyncMigrationCommands, "upgrade", return_value=None) as mock_upgrade:
        await config.migrate_up()

        mock_upgrade.assert_called_once_with("head", False, True, False, use_logger=False, echo=None, summary_only=None)


def test_migrate_down_default_parameters_sync(tmp_path: Path) -> None:
    """Test that migrate_down() uses correct default parameter values for sync configs."""
    migration_dir = tmp_path / "migrations"
    temp_db = str(tmp_path / "test.db")

    config = SqliteConfig(
        connection_config={"database": temp_db}, migration_config={"script_location": str(migration_dir)}
    )

    with patch.object(SyncMigrationCommands, "downgrade", return_value=None) as mock_downgrade:
        config.migrate_down()

        mock_downgrade.assert_called_once_with("-1", dry_run=False, use_logger=False, echo=None, summary_only=None)


async def test_migrate_down_default_parameters_async(tmp_path: Path) -> None:
    """Test that migrate_down() uses correct default parameter values for async configs."""
    migration_dir = tmp_path / "migrations"

    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/test"},
        migration_config={"script_location": str(migration_dir)},
    )

    with patch.object(AsyncMigrationCommands, "downgrade", return_value=None) as mock_downgrade:
        await config.migrate_down()

        mock_downgrade.assert_called_once_with("-1", dry_run=False, use_logger=False, echo=None, summary_only=None)


def test_create_migration_default_file_type_sync(tmp_path: Path) -> None:
    """Test that create_migration() defaults to 'sql' file type for sync configs."""
    migration_dir = tmp_path / "migrations"
    temp_db = str(tmp_path / "test.db")

    config = SqliteConfig(
        connection_config={"database": temp_db}, migration_config={"script_location": str(migration_dir)}
    )

    with patch.object(SyncMigrationCommands, "revision", return_value=None) as mock_revision:
        config.create_migration(message="test migration")

        mock_revision.assert_called_once_with("test migration", "sql")


async def test_create_migration_default_file_type_async(tmp_path: Path) -> None:
    """Test that create_migration() defaults to 'sql' file type for async configs."""
    migration_dir = tmp_path / "migrations"

    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/test"},
        migration_config={"script_location": str(migration_dir)},
    )

    with patch.object(AsyncMigrationCommands, "revision", return_value=None) as mock_revision:
        await config.create_migration(message="test migration")

        mock_revision.assert_called_once_with("test migration", "sql")


def test_init_migrations_default_package_sync(tmp_path: Path) -> None:
    """Test that init_migrations() defaults to package=True for sync configs."""
    migration_dir = tmp_path / "migrations"
    temp_db = str(tmp_path / "test.db")

    config = SqliteConfig(
        connection_config={"database": temp_db}, migration_config={"script_location": str(migration_dir)}
    )

    with patch.object(SyncMigrationCommands, "init", return_value=None) as mock_init:
        config.init_migrations(directory=str(migration_dir))

        mock_init.assert_called_once_with(str(migration_dir), True)


async def test_init_migrations_default_package_async(tmp_path: Path) -> None:
    """Test that init_migrations() defaults to package=True for async configs."""
    migration_dir = tmp_path / "migrations"

    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/test"},
        migration_config={"script_location": str(migration_dir)},
    )

    with patch.object(AsyncMigrationCommands, "init", return_value=None) as mock_init:
        await config.init_migrations(directory=str(migration_dir))

        mock_init.assert_called_once_with(str(migration_dir), True)


def test_fix_migrations_default_parameters_sync(tmp_path: Path) -> None:
    """Test that fix_migrations() uses correct default parameter values for sync configs."""
    migration_dir = tmp_path / "migrations"
    temp_db = str(tmp_path / "test.db")

    config = SqliteConfig(
        connection_config={"database": temp_db}, migration_config={"script_location": str(migration_dir)}
    )

    with patch.object(SyncMigrationCommands, "fix", return_value=None) as mock_fix:
        config.fix_migrations()

        mock_fix.assert_called_once_with(False, True, False)


async def test_fix_migrations_default_parameters_async(tmp_path: Path) -> None:
    """Test that fix_migrations() uses correct default parameter values for async configs."""
    migration_dir = tmp_path / "migrations"

    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/test"},
        migration_config={"script_location": str(migration_dir)},
    )

    with patch.object(AsyncMigrationCommands, "fix", return_value=None) as mock_fix:
        await config.fix_migrations()

        mock_fix.assert_called_once_with(False, True, False)


def test_migration_helpers_are_independent_and_retryable(tmp_path: Path) -> None:
    attempts: list[str] = []

    class Tracker(SyncMigrationTracker):
        def __init__(self, version_table_name: str = "ddl_migrations") -> None:
            attempts.append(version_table_name)
            if len(attempts) == 1:
                raise RuntimeError("tracker temporarily unavailable")
            super().__init__(version_table_name)

    class Config(SqliteConfig):
        migration_tracker_type = Tracker

    config = Config(migration_config={"script_location": str(tmp_path)})
    assert attempts == []
    loader = config.get_migration_loader()
    with config.provide_session() as session:
        assert session.select_value("SELECT 1") == 1
    assert attempts == []
    with pytest.raises(RuntimeError, match="temporarily unavailable"):
        config.get_migration_commands()
    commands = config.get_migration_commands()
    assert config.get_migration_commands() is commands
    assert config.get_migration_loader() is loader
    assert isinstance(commands.tracker, Tracker)
    assert attempts == ["ddl_migrations", "ddl_migrations"]
    config.close_pool()


@pytest.mark.parametrize("kind", ["sync", "async", "no_pool_sync", "no_pool_async"])
def test_migration_tracker_initializes_once_per_config_family(kind: str, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    class SyncTracker(SyncMigrationTracker):
        def __init__(self, version_table_name: str = "ddl_migrations") -> None:
            calls.append(version_table_name)
            super().__init__(version_table_name)

    class AsyncTracker(AsyncMigrationTracker):
        def __init__(self, version_table_name: str = "ddl_migrations") -> None:
            calls.append(version_table_name)
            super().__init__(version_table_name)

    if kind == "no_pool_sync":
        config_type = pytest.importorskip("sqlspec.adapters.adbc").AdbcConfig
    elif kind == "no_pool_async":
        config_type = pytest.importorskip("sqlspec.adapters.mysqlconnector").MysqlConnectorAsyncConfig
    else:
        config_type = SqliteConfig if kind == "sync" else AiosqliteConfig
    tracker_type = AsyncTracker if kind in {"async", "no_pool_async"} else SyncTracker
    monkeypatch.setattr(config_type, "migration_tracker_type", tracker_type)
    config = config_type()
    assert calls == []
    config.get_migration_loader()
    assert calls == []
    commands = config.get_migration_commands()
    assert config.get_migration_commands() is commands
    assert type(commands.tracker) is tracker_type
    assert calls == ["ddl_migrations"]


@pytest.mark.parametrize("helper", ["commands", "loader"])
def test_concurrent_first_migration_access_shares_one_helper(
    helper: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[object] = []
    config = SqliteConfig(migration_config={"script_location": str(tmp_path)})
    barrier = Barrier(3)
    get_helper: Callable[[], object]
    if helper == "commands":

        class Tracker(SyncMigrationTracker):
            def __init__(self, version_table_name: str = "ddl_migrations") -> None:
                calls.append(self)
                super().__init__(version_table_name)

        monkeypatch.setattr(config, "migration_tracker_type", Tracker)
        get_helper = config.get_migration_commands
    else:
        original = SQLFileLoader.load_sql

        def load_sql(self: SQLFileLoader, *paths: str | Path) -> None:
            calls.append(self)
            original(self, *paths)

        monkeypatch.setattr(SQLFileLoader, "load_sql", load_sql)
        (tmp_path / "query.sql").write_text("-- name: value\nSELECT 7;\n")
        get_helper = config.get_migration_loader

    def get() -> object:
        barrier.wait(timeout=5)
        return get_helper()

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(get) for _ in range(2)]
        barrier.wait(timeout=5)
        results = [future.result(timeout=5) for future in futures]
    assert results[0] is results[1]
    assert len(calls) == 1


def test_migration_loader_failure_retries_without_publishing_partial_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "query.sql").write_text("-- name: value\nSELECT 7;\n")
    attempted: list[SQLFileLoader] = []
    original = SQLFileLoader.load_sql

    def load_sql(self: SQLFileLoader, *paths: str | Path) -> None:
        attempted.append(self)
        original(self, *paths)
        if len(attempted) == 1:
            raise RuntimeError("file temporarily unavailable")

    monkeypatch.setattr(SQLFileLoader, "load_sql", load_sql)
    config = SqliteConfig(migration_config={"script_location": str(tmp_path)})
    with pytest.raises(RuntimeError, match="temporarily unavailable"):
        config.get_migration_loader()
    loader = config.get_migration_loader()
    assert len(attempted) == 2
    assert loader is attempted[1]
    assert loader is not attempted[0]
    with config.provide_session() as session:
        assert session.select_value(loader.get_sql("value")) == 7
    config.close_pool()


def test_empty_migration_loader_notices_new_directory(tmp_path: Path) -> None:
    directory = tmp_path / "later"
    config = SqliteConfig(migration_config={"script_location": str(directory)})
    loader = config.get_migration_loader()
    assert loader.list_queries() == []
    directory.mkdir()
    (directory / "query.sql").write_text("-- name: value\nSELECT 9;\n")
    assert config.get_migration_loader() is loader
    with config.provide_session() as session:
        assert session.select_value(loader.get_sql("value")) == 9
    config.close_pool()


def test_supported_migration_mutations_refresh_only_affected_helpers(tmp_path: Path) -> None:
    extra = tmp_path / "extra.sql"
    extra.write_text("-- name: value\nSELECT 11;\n")
    extensions = tmp_path / "extension"
    extensions.mkdir()
    config = SqliteConfig(migration_config={"script_location": str(tmp_path / "absent")})
    config.load_migration_sql_files(extra)
    loader = config.get_migration_loader()
    commands = config.get_migration_commands()
    config.add_extension_migrations("example", extensions)
    updated = config.get_migration_commands()
    assert updated is not commands
    assert updated.runner.extension_migrations["example"] == extensions
    assert config.get_migration_loader() is loader
    config.add_extension_migrations("example", extensions)
    assert config.get_migration_commands() is updated
    assert config.remove_extension_migrations("example")
    assert config.get_migration_commands() is not updated
    assert config.get_migration_loader() is loader
    with config.provide_session() as session:
        assert session.select_value(loader.get_sql("value")) == 11
    config.set_migration_config({"version_table_name": "new_versions", "script_location": str(tmp_path / "absent")})
    assert config.get_migration_loader() is not loader
    assert config.get_migration_commands().tracker.version_table_name == "new_versions"
    assert config.get_migration_loader().list_queries() == []
    config.close_pool()


@pytest.mark.parametrize("helper", ["loader", "commands"])
def test_migration_assignment_waits_for_helper_publication(
    helper: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    old = tmp_path / "old"
    new = tmp_path / "new"
    old.mkdir()
    new.mkdir()
    (old / "query.sql").write_text("-- name: value\nSELECT 1;\n")
    (new / "query.sql").write_text("-- name: value\nSELECT 2;\n")
    config = SqliteConfig(migration_config={"script_location": str(old)})
    loading = Event()
    release = Event()
    assigning = Event()
    original = SQLFileLoader.load_sql

    def load_sql(self: SQLFileLoader, *paths: str | Path) -> None:
        if paths == (old,):
            loading.set()
            assert release.wait(timeout=5)
        original(self, *paths)

    def assign() -> None:
        assigning.set()
        config.set_migration_config({"script_location": str(new), "version_table_name": "new_versions"})

    get_helper: Callable[[], object]
    if helper == "loader":
        monkeypatch.setattr(SQLFileLoader, "load_sql", load_sql)
        get_helper = config.get_migration_loader
    else:

        class Tracker(SyncMigrationTracker):
            def __init__(self, version_table_name: str = "ddl_migrations") -> None:
                if version_table_name == "ddl_migrations":
                    loading.set()
                    assert release.wait(timeout=5)
                super().__init__(version_table_name)

        monkeypatch.setattr(config, "migration_tracker_type", Tracker)
        get_helper = config.get_migration_commands
    with ThreadPoolExecutor(max_workers=2) as executor:
        initial = executor.submit(get_helper)
        assert loading.wait(timeout=5)
        mutation = executor.submit(assign)
        assert assigning.wait(timeout=5)
        release.set()
        old_helper = initial.result(timeout=5)
        mutation.result(timeout=5)
    assert get_helper() is not old_helper
    new_loader = config.get_migration_loader()
    assert config.get_migration_commands().tracker.version_table_name == "new_versions"
    with config.provide_session() as session:
        assert session.select_value(new_loader.get_sql("value")) == 2
    config.close_pool()
