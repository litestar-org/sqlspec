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

from pathlib import Path
from unittest.mock import patch

import pytest

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.adapters.asyncpg.config import AsyncpgConfig
from sqlspec.adapters.duckdb.config import DuckDBConfig
from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.config import AsyncDatabaseConfig, NoPoolAsyncConfig, NoPoolSyncConfig, SyncDatabaseConfig
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands


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

        mock_upgrade.assert_called_once_with("head", True, False, True)


def test_sqlite_config_migrate_down_calls_commands(tmp_path: Path) -> None:
    """Test that SqliteConfig.migrate_down() delegates to SyncMigrationCommands.downgrade()."""
    migration_dir = tmp_path / "migrations"
    temp_db = str(tmp_path / "test.db")

    config = SqliteConfig(
        connection_config={"database": temp_db}, migration_config={"script_location": str(migration_dir)}
    )

    with patch.object(SyncMigrationCommands, "downgrade", return_value=None) as mock_downgrade:
        config.migrate_down(revision="-2", dry_run=True)

        mock_downgrade.assert_called_once_with("-2", dry_run=True)


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


@pytest.mark.asyncio
async def test_asyncpg_config_migrate_up_calls_commands(tmp_path: Path) -> None:
    """Test that AsyncpgConfig.migrate_up() delegates to AsyncMigrationCommands.upgrade()."""
    migration_dir = tmp_path / "migrations"

    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/test"},
        migration_config={"script_location": str(migration_dir)},
    )

    with patch.object(AsyncMigrationCommands, "upgrade", return_value=None) as mock_upgrade:
        await config.migrate_up(revision="0002", allow_missing=False, auto_sync=True, dry_run=False)

        mock_upgrade.assert_called_once_with("0002", False, True, False)


@pytest.mark.asyncio
async def test_asyncpg_config_migrate_down_calls_commands(tmp_path: Path) -> None:
    """Test that AsyncpgConfig.migrate_down() delegates to AsyncMigrationCommands.downgrade()."""
    migration_dir = tmp_path / "migrations"

    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/test"},
        migration_config={"script_location": str(migration_dir)},
    )

    with patch.object(AsyncMigrationCommands, "downgrade", return_value=None) as mock_downgrade:
        await config.migrate_down(revision="base", dry_run=False)

        mock_downgrade.assert_called_once_with("base", dry_run=False)


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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

        mock_upgrade.assert_called_once_with("head", False, True, False)


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


@pytest.mark.asyncio
async def test_aiosqlite_async_config_migrate_up_calls_commands(tmp_path: Path) -> None:
    """Test that AiosqliteConfig.migrate_up() delegates to AsyncMigrationCommands.upgrade()."""
    migration_dir = tmp_path / "migrations"
    temp_db = str(tmp_path / "test.db")

    config = AiosqliteConfig(
        connection_config={"database": temp_db}, migration_config={"script_location": str(migration_dir)}
    )

    with patch.object(AsyncMigrationCommands, "upgrade", return_value=None) as mock_upgrade:
        await config.migrate_up(revision="head", allow_missing=True, auto_sync=True, dry_run=True)

        mock_upgrade.assert_called_once_with("head", True, True, True)


def test_migrate_up_default_parameters_sync(tmp_path: Path) -> None:
    """Test that migrate_up() uses correct default parameter values for sync configs."""
    migration_dir = tmp_path / "migrations"
    temp_db = str(tmp_path / "test.db")

    config = SqliteConfig(
        connection_config={"database": temp_db}, migration_config={"script_location": str(migration_dir)}
    )

    with patch.object(SyncMigrationCommands, "upgrade", return_value=None) as mock_upgrade:
        config.migrate_up()

        mock_upgrade.assert_called_once_with("head", False, True, False)


@pytest.mark.asyncio
async def test_migrate_up_default_parameters_async(tmp_path: Path) -> None:
    """Test that migrate_up() uses correct default parameter values for async configs."""
    migration_dir = tmp_path / "migrations"

    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/test"},
        migration_config={"script_location": str(migration_dir)},
    )

    with patch.object(AsyncMigrationCommands, "upgrade", return_value=None) as mock_upgrade:
        await config.migrate_up()

        mock_upgrade.assert_called_once_with("head", False, True, False)


def test_migrate_down_default_parameters_sync(tmp_path: Path) -> None:
    """Test that migrate_down() uses correct default parameter values for sync configs."""
    migration_dir = tmp_path / "migrations"
    temp_db = str(tmp_path / "test.db")

    config = SqliteConfig(
        connection_config={"database": temp_db}, migration_config={"script_location": str(migration_dir)}
    )

    with patch.object(SyncMigrationCommands, "downgrade", return_value=None) as mock_downgrade:
        config.migrate_down()

        mock_downgrade.assert_called_once_with("-1", dry_run=False)


@pytest.mark.asyncio
async def test_migrate_down_default_parameters_async(tmp_path: Path) -> None:
    """Test that migrate_down() uses correct default parameter values for async configs."""
    migration_dir = tmp_path / "migrations"

    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/test"},
        migration_config={"script_location": str(migration_dir)},
    )

    with patch.object(AsyncMigrationCommands, "downgrade", return_value=None) as mock_downgrade:
        await config.migrate_down()

        mock_downgrade.assert_called_once_with("-1", dry_run=False)


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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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
