# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Unit tests for migration commands functionality.

Tests focused on MigrationCommands class behavior including:
- Async/sync command delegation
- Initialization behavior
- Configuration handling
- Error scenarios and edge cases
- Command routing and parameter passing
"""

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands, create_migration_commands

pytestmark = pytest.mark.xdist_group("migrations")


@pytest.fixture
def sync_config() -> SqliteConfig:
    """Create a sync database config for testing."""
    return SqliteConfig(connection_config={"database": ":memory:"})


@pytest.fixture
def async_config() -> AiosqliteConfig:
    """Create an async database config for testing."""
    return AiosqliteConfig(connection_config={"database": ":memory:"})


def test_migration_commands_sync_config_initialization(sync_config: SqliteConfig) -> None:
    """Test SyncMigrationCommands initializes correctly with sync config."""
    commands = SyncMigrationCommands(sync_config)
    assert commands is not None
    assert hasattr(commands, "runner")


def test_migration_commands_async_config_initialization(async_config: AiosqliteConfig) -> None:
    """Test AsyncMigrationCommands initializes correctly with async config."""
    commands = AsyncMigrationCommands(async_config)
    assert commands is not None
    assert hasattr(commands, "runner")


def test_migration_commands_sync_init_delegation(tmp_path: Path, sync_config: SqliteConfig) -> None:
    """Test that sync config init is delegated directly to sync implementation."""
    with patch.object(SyncMigrationCommands, "init") as mock_init:
        commands = SyncMigrationCommands(sync_config)

        migration_dir = str(tmp_path / "migrations")

        commands.init(migration_dir, package=False)

        mock_init.assert_called_once_with(migration_dir, package=False)


async def test_migration_commands_async_init_delegation(tmp_path: Path, async_config: AiosqliteConfig) -> None:
    """Test that async config init calls async method directly."""
    from typing import cast

    with patch.object(AsyncMigrationCommands, "init", new_callable=AsyncMock) as mock_init:
        commands = cast(AsyncMigrationCommands, create_migration_commands(async_config))

        migration_dir = str(tmp_path / "migrations")

        await commands.init(migration_dir, package=True)

        # Verify the async method was called directly
        mock_init.assert_called_once_with(migration_dir, package=True)


def test_migration_commands_sync_current_delegation(sync_config: SqliteConfig) -> None:
    """Test that sync config current is delegated directly to sync implementation."""
    with patch.object(SyncMigrationCommands, "current") as mock_current:
        commands = SyncMigrationCommands(sync_config)

        commands.current(verbose=True)

        mock_current.assert_called_once_with(verbose=True)


async def test_migration_commands_async_current_delegation(async_config: AiosqliteConfig) -> None:
    """Test that async config current calls async method directly."""
    from typing import cast

    with patch.object(AsyncMigrationCommands, "current", new_callable=AsyncMock) as mock_current:
        mock_current.return_value = "test_version"

        commands = cast(AsyncMigrationCommands, create_migration_commands(async_config))

        result = await commands.current(verbose=False)

        # Verify the async method was called directly
        mock_current.assert_called_once_with(verbose=False)
        assert result == "test_version"


def test_migration_commands_sync_upgrade_delegation(sync_config: SqliteConfig) -> None:
    """Test that sync config upgrade is delegated directly to sync implementation."""
    with patch.object(SyncMigrationCommands, "upgrade") as mock_upgrade:
        commands = SyncMigrationCommands(sync_config)

        commands.upgrade(revision="001")

        mock_upgrade.assert_called_once_with(revision="001")


async def test_migration_commands_async_upgrade_delegation(async_config: AiosqliteConfig) -> None:
    """Test that async config upgrade calls async method directly."""
    from typing import cast

    with patch.object(AsyncMigrationCommands, "upgrade", new_callable=AsyncMock) as mock_upgrade:
        commands = cast(AsyncMigrationCommands, create_migration_commands(async_config))

        await commands.upgrade(revision="002")

        # Verify the async method was called directly
        mock_upgrade.assert_called_once_with(revision="002")


def test_migration_commands_sync_downgrade_delegation(sync_config: SqliteConfig) -> None:
    """Test that sync config downgrade is delegated directly to sync implementation."""
    with patch.object(SyncMigrationCommands, "downgrade") as mock_downgrade:
        commands = SyncMigrationCommands(sync_config)

        commands.downgrade(revision="base")

        mock_downgrade.assert_called_once_with(revision="base")


async def test_migration_commands_async_downgrade_delegation(async_config: AiosqliteConfig) -> None:
    """Test that async config downgrade calls async method directly."""
    from typing import cast

    with patch.object(AsyncMigrationCommands, "downgrade", new_callable=AsyncMock) as mock_downgrade:
        commands = cast(AsyncMigrationCommands, create_migration_commands(async_config))

        await commands.downgrade(revision="001")

        # Verify the async method was called directly
        mock_downgrade.assert_called_once_with(revision="001")


def test_migration_commands_sync_stamp_delegation(sync_config: SqliteConfig) -> None:
    """Test that sync config stamp is delegated directly to sync implementation."""
    with patch.object(SyncMigrationCommands, "stamp") as mock_stamp:
        commands = SyncMigrationCommands(sync_config)

        commands.stamp("001")

        mock_stamp.assert_called_once_with("001")


async def test_migration_commands_async_stamp_delegation(async_config: AiosqliteConfig) -> None:
    """Test that async config stamp calls async method directly."""
    from typing import cast

    with patch.object(AsyncMigrationCommands, "stamp", new_callable=AsyncMock) as mock_stamp:
        commands = cast(AsyncMigrationCommands, create_migration_commands(async_config))

        await commands.stamp("002")

        # Verify the async method was called directly
        mock_stamp.assert_called_once_with("002")


def test_migration_commands_sync_revision_delegation(sync_config: SqliteConfig) -> None:
    """Test that sync config revision is delegated directly to sync implementation."""
    with patch.object(SyncMigrationCommands, "revision") as mock_revision:
        commands = SyncMigrationCommands(sync_config)

        commands.revision("Test revision", "sql")

        mock_revision.assert_called_once_with("Test revision", "sql")


async def test_migration_commands_async_revision_delegation(async_config: AiosqliteConfig) -> None:
    """Test that async config revision calls async method directly."""
    from typing import cast

    with patch.object(AsyncMigrationCommands, "revision", new_callable=AsyncMock) as mock_revision:
        commands = cast(AsyncMigrationCommands, create_migration_commands(async_config))

        await commands.revision("Test async revision", "python")

        # Verify the async method was called directly
        mock_revision.assert_called_once_with("Test async revision", "python")


def test_migration_commands_factory_returns_sync_for_sync_config(sync_config: SqliteConfig) -> None:
    """Test that sync config returns SyncMigrationCommands from factory."""
    commands: SyncMigrationCommands[SqliteConfig] | AsyncMigrationCommands[AiosqliteConfig] = create_migration_commands(
        sync_config
    )

    # Should return a SyncMigrationCommands instance
    assert isinstance(commands, SyncMigrationCommands)
    assert commands.config == sync_config

    # Methods should be synchronous, not async
    import inspect

    assert not inspect.iscoroutinefunction(commands.init)
    assert not inspect.iscoroutinefunction(commands.upgrade)
    assert not inspect.iscoroutinefunction(commands.downgrade)


def test_sync_migration_commands_initialization(sync_config: SqliteConfig) -> None:
    """Test SyncMigrationCommands proper initialization."""
    commands = SyncMigrationCommands(sync_config)

    assert commands.config == sync_config
    assert hasattr(commands, "tracker")
    assert hasattr(commands, "runner")


def test_async_migration_commands_initialization(async_config: AiosqliteConfig) -> None:
    """Test AsyncMigrationCommands proper initialization."""
    commands = AsyncMigrationCommands(async_config)

    assert commands.config == async_config
    assert hasattr(commands, "tracker")
    assert hasattr(commands, "runner")


def test_sync_migration_commands_init_creates_directory(tmp_path: Path, sync_config: SqliteConfig) -> None:
    """Test that SyncMigrationCommands init creates migration directory structure."""
    commands = SyncMigrationCommands(sync_config)

    migration_dir = tmp_path / "migrations"

    commands.init(str(migration_dir), package=True)

    assert migration_dir.exists()
    assert (migration_dir / "__init__.py").exists()


def test_sync_migration_commands_init_without_package(tmp_path: Path, sync_config: SqliteConfig) -> None:
    """Test that SyncMigrationCommands init creates directory without __init__.py when package=False."""
    commands = SyncMigrationCommands(sync_config)

    migration_dir = tmp_path / "migrations"

    commands.init(str(migration_dir), package=False)

    assert migration_dir.exists()
    assert not (migration_dir / "__init__.py").exists()


async def test_migration_commands_error_propagation(async_config: AiosqliteConfig) -> None:
    """Test that errors from underlying implementations are properly propagated."""
    from typing import cast

    with patch.object(AsyncMigrationCommands, "upgrade", side_effect=ValueError("Test error")):
        commands = cast(AsyncMigrationCommands, create_migration_commands(async_config))

        with pytest.raises(ValueError, match="Test error"):
            await commands.upgrade()


def test_migration_commands_sync_parameter_forwarding(sync_config: SqliteConfig) -> None:
    """Test that all parameters are properly forwarded to sync implementations."""
    with patch.object(SyncMigrationCommands, "upgrade") as mock_upgrade:
        commands: SyncMigrationCommands[SqliteConfig] | AsyncMigrationCommands[AiosqliteConfig] = (
            create_migration_commands(sync_config)
        )

        # Test with various parameter combinations
        commands.upgrade()
        mock_upgrade.assert_called_with()  # Called with no arguments, uses default

        commands.upgrade("specific_revision")
        mock_upgrade.assert_called_with("specific_revision")


def test_migration_commands_config_type_detection(sync_config: SqliteConfig, async_config: AiosqliteConfig) -> None:
    """Test that MigrationCommands work with their respective config types."""
    sync_commands = SyncMigrationCommands(sync_config)
    async_commands = AsyncMigrationCommands(async_config)

    assert sync_commands is not None
    assert async_commands is not None
    assert hasattr(sync_commands, "runner")
    assert hasattr(async_commands, "runner")


def test_sync_upgrade_empty_migration_folder(sync_config: SqliteConfig) -> None:
    """Test that sync upgrade shows helpful message when migration folder is empty."""
    from unittest.mock import MagicMock

    commands = SyncMigrationCommands(sync_config)

    mock_driver = MagicMock()
    with (
        patch.object(sync_config, "provide_session") as mock_session,
        patch("sqlspec.migrations.commands.console") as mock_console,
        patch.object(commands.runner, "get_migration_files", return_value=[]),
    ):
        mock_session.return_value.__enter__.return_value = mock_driver

        commands.upgrade()

        mock_console.print.assert_called_once()
        call_args = str(mock_console.print.call_args)
        assert "No migrations found" in call_args
        assert "sqlspec create-migration" in call_args


async def test_async_upgrade_empty_migration_folder(async_config: AiosqliteConfig) -> None:
    """Test that async upgrade shows helpful message when migration folder is empty."""
    commands = AsyncMigrationCommands(async_config)

    mock_driver = AsyncMock()
    mock_driver.driver_features = {}
    with (
        patch.object(async_config, "provide_session") as mock_session,
        patch("sqlspec.migrations.commands.console") as mock_console,
        patch.object(commands.runner, "get_migration_files", return_value=[]),
    ):
        mock_session.return_value.__aenter__.return_value = mock_driver

        await commands.upgrade()

        mock_console.print.assert_called_once()
        call_args = str(mock_console.print.call_args)
        assert "No migrations found" in call_args
        assert "sqlspec create-migration" in call_args


def test_sync_upgrade_already_at_latest_version(sync_config: SqliteConfig) -> None:
    """Test that sync upgrade shows 'already at latest' when all migrations are applied."""
    from unittest.mock import MagicMock

    commands = SyncMigrationCommands(sync_config)

    mock_driver = MagicMock()
    mock_migration_file = Path("/fake/migrations/0001_initial.sql")

    with (
        patch.object(sync_config, "provide_session") as mock_session,
        patch("sqlspec.migrations.commands.console") as mock_console,
        patch.object(commands.runner, "get_migration_files", return_value=[("0001", mock_migration_file)]),
        patch.object(commands.tracker, "get_applied_migrations", return_value=[{"version_num": "0001"}]),
    ):
        mock_session.return_value.__enter__.return_value = mock_driver

        commands.upgrade()

        mock_console.print.assert_called_once()
        call_args = str(mock_console.print.call_args)
        assert "Already at latest version" in call_args
        assert "No migrations found" not in call_args


async def test_async_upgrade_already_at_latest_version(async_config: AiosqliteConfig) -> None:
    """Test that async upgrade shows 'already at latest' when all migrations are applied."""
    commands = AsyncMigrationCommands(async_config)

    mock_driver = AsyncMock()
    mock_driver.driver_features = {}
    mock_migration_file = Path("/fake/migrations/0001_initial.sql")

    with (
        patch.object(async_config, "provide_session") as mock_session,
        patch("sqlspec.migrations.commands.console") as mock_console,
        patch.object(commands.runner, "get_migration_files", return_value=[("0001", mock_migration_file)]),
        patch.object(commands.tracker, "get_applied_migrations", return_value=[{"version_num": "0001"}]),
    ):
        mock_session.return_value.__aenter__.return_value = mock_driver

        await commands.upgrade()

        mock_console.print.assert_called_once()
        call_args = str(mock_console.print.call_args)
        assert "Already at latest version" in call_args
        assert "No migrations found" not in call_args
