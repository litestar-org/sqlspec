# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Tests for migration use_logger functionality.

Tests that the use_logger parameter correctly routes output to Python logger
instead of Rich console.
"""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands

pytestmark = pytest.mark.xdist_group("migrations")


@pytest.fixture
def sync_config() -> SqliteConfig:
    """Create a sync database config for testing."""
    return SqliteConfig(connection_config={"database": ":memory:"})


@pytest.fixture
def async_config() -> AiosqliteConfig:
    """Create an async database config for testing."""
    return AiosqliteConfig(connection_config={"database": ":memory:"})


@pytest.fixture
def sync_config_with_use_logger() -> SqliteConfig:
    """Create a sync database config with use_logger=True in migration_config."""
    return SqliteConfig(
        connection_config={"database": ":memory:"},
        migration_config={"use_logger": True},
    )


@pytest.fixture
def async_config_with_use_logger() -> AiosqliteConfig:
    """Create an async database config with use_logger=True in migration_config."""
    return AiosqliteConfig(
        connection_config={"database": ":memory:"},
        migration_config={"use_logger": True},
    )


class TestResolveUseLogger:
    """Test the _resolve_use_logger helper method."""

    def test_method_param_true_returns_true(self, sync_config: SqliteConfig) -> None:
        """When method parameter is True, return True regardless of config."""
        commands = SyncMigrationCommands(sync_config)
        assert commands._resolve_use_logger(True) is True

    def test_method_param_false_config_default_false(self, sync_config: SqliteConfig) -> None:
        """When method parameter is False and config has no default, return False."""
        commands = SyncMigrationCommands(sync_config)
        assert commands._resolve_use_logger(False) is False

    def test_method_param_false_config_default_true(self, sync_config_with_use_logger: SqliteConfig) -> None:
        """When method parameter is False and config default is True, return True."""
        commands = SyncMigrationCommands(sync_config_with_use_logger)
        assert commands._resolve_use_logger(False) is True

    def test_config_default_applies(self, sync_config_with_use_logger: SqliteConfig) -> None:
        """Config default should be used when method param is False."""
        commands = SyncMigrationCommands(sync_config_with_use_logger)
        # Even though we pass False, config has use_logger=True
        assert commands._resolve_use_logger(False) is True


class TestSyncUpgradeUseLogger:
    """Test use_logger parameter for sync upgrade command."""

    def test_use_logger_true_uses_logger(self, sync_config: SqliteConfig) -> None:
        """use_logger=True should output to logger instead of console."""
        commands = SyncMigrationCommands(sync_config)
        mock_driver = MagicMock()

        with (
            patch.object(sync_config, "provide_session") as mock_session,
            patch("sqlspec.migrations.commands.console") as mock_console,
            patch("sqlspec.migrations.commands.logger") as mock_logger,
            patch.object(commands.runner, "get_migration_files", return_value=[]),
        ):
            mock_session.return_value.__enter__.return_value = mock_driver

            commands.upgrade(use_logger=True)

            # Logger should be called with the message
            mock_logger.info.assert_called()
            call_args = str(mock_logger.info.call_args)
            assert "No migrations found" in call_args

            # Console should NOT be called for the main message
            for call in mock_console.print.call_args_list:
                assert "No migrations found" not in str(call)

    def test_use_logger_false_uses_console(self, sync_config: SqliteConfig) -> None:
        """use_logger=False (default) should output to Rich console."""
        commands = SyncMigrationCommands(sync_config)
        mock_driver = MagicMock()

        with (
            patch.object(sync_config, "provide_session") as mock_session,
            patch("sqlspec.migrations.commands.console") as mock_console,
            patch("sqlspec.migrations.commands.logger"),
            patch.object(commands.runner, "get_migration_files", return_value=[]),
        ):
            mock_session.return_value.__enter__.return_value = mock_driver

            commands.upgrade(use_logger=False)

            # Console should be called
            mock_console.print.assert_called()
            call_args = str(mock_console.print.call_args)
            assert "No migrations found" in call_args

    def test_config_default_use_logger_true(self, sync_config_with_use_logger: SqliteConfig) -> None:
        """MigrationConfig use_logger=True should be used as default."""
        commands = SyncMigrationCommands(sync_config_with_use_logger)
        mock_driver = MagicMock()

        with (
            patch.object(sync_config_with_use_logger, "provide_session") as mock_session,
            patch("sqlspec.migrations.commands.console"),
            patch("sqlspec.migrations.commands.logger") as mock_logger,
            patch.object(commands.runner, "get_migration_files", return_value=[]),
        ):
            mock_session.return_value.__enter__.return_value = mock_driver

            # No explicit parameter - should use config default
            commands.upgrade()

            # Logger should be called
            mock_logger.info.assert_called()
            call_args = str(mock_logger.info.call_args)
            assert "No migrations found" in call_args

    def test_already_at_latest_uses_logger(self, sync_config: SqliteConfig) -> None:
        """Test 'already at latest version' message goes to logger when use_logger=True."""
        commands = SyncMigrationCommands(sync_config)
        mock_driver = MagicMock()
        mock_migration_file = Path("/fake/migrations/0001_initial.sql")

        with (
            patch.object(sync_config, "provide_session") as mock_session,
            patch("sqlspec.migrations.commands.console"),
            patch("sqlspec.migrations.commands.logger") as mock_logger,
            patch.object(commands.runner, "get_migration_files", return_value=[("0001", mock_migration_file)]),
            patch.object(commands.tracker, "get_applied_migrations", return_value=[{"version_num": "0001"}]),
        ):
            mock_session.return_value.__enter__.return_value = mock_driver

            commands.upgrade(use_logger=True)

            mock_logger.info.assert_called()
            call_args = str(mock_logger.info.call_args)
            assert "Already at latest version" in call_args


class TestSyncDowngradeUseLogger:
    """Test use_logger parameter for sync downgrade command."""

    def test_use_logger_true_uses_logger(self, sync_config: SqliteConfig) -> None:
        """use_logger=True should output to logger instead of console."""
        commands = SyncMigrationCommands(sync_config)
        mock_driver = MagicMock()

        with (
            patch.object(sync_config, "provide_session") as mock_session,
            patch("sqlspec.migrations.commands.console"),
            patch("sqlspec.migrations.commands.logger") as mock_logger,
            patch.object(commands.tracker, "get_applied_migrations", return_value=[]),
        ):
            mock_session.return_value.__enter__.return_value = mock_driver

            commands.downgrade(use_logger=True)

            # Logger should be called
            mock_logger.info.assert_called()
            call_args = str(mock_logger.info.call_args)
            assert "No migrations to downgrade" in call_args

    def test_use_logger_false_uses_console(self, sync_config: SqliteConfig) -> None:
        """use_logger=False (default) should output to Rich console."""
        commands = SyncMigrationCommands(sync_config)
        mock_driver = MagicMock()

        with (
            patch.object(sync_config, "provide_session") as mock_session,
            patch("sqlspec.migrations.commands.console") as mock_console,
            patch("sqlspec.migrations.commands.logger"),
            patch.object(commands.tracker, "get_applied_migrations", return_value=[]),
        ):
            mock_session.return_value.__enter__.return_value = mock_driver

            commands.downgrade(use_logger=False)

            # Console should be called
            mock_console.print.assert_called()
            call_args = str(mock_console.print.call_args)
            assert "No migrations to downgrade" in call_args


class TestAsyncUpgradeUseLogger:
    """Test use_logger parameter for async upgrade command."""

    async def test_use_logger_true_uses_logger(self, async_config: AiosqliteConfig) -> None:
        """use_logger=True should output to logger instead of console."""
        commands = AsyncMigrationCommands(async_config)
        mock_driver = AsyncMock()
        mock_driver.driver_features = {}

        with (
            patch.object(async_config, "provide_session") as mock_session,
            patch("sqlspec.migrations.commands.console"),
            patch("sqlspec.migrations.commands.logger") as mock_logger,
            patch.object(commands.runner, "get_migration_files", return_value=[]),
        ):
            mock_session.return_value.__aenter__.return_value = mock_driver

            await commands.upgrade(use_logger=True)

            # Logger should be called
            mock_logger.info.assert_called()
            call_args = str(mock_logger.info.call_args)
            assert "No migrations found" in call_args

    async def test_use_logger_false_uses_console(self, async_config: AiosqliteConfig) -> None:
        """use_logger=False (default) should output to Rich console."""
        commands = AsyncMigrationCommands(async_config)
        mock_driver = AsyncMock()
        mock_driver.driver_features = {}

        with (
            patch.object(async_config, "provide_session") as mock_session,
            patch("sqlspec.migrations.commands.console") as mock_console,
            patch("sqlspec.migrations.commands.logger"),
            patch.object(commands.runner, "get_migration_files", return_value=[]),
        ):
            mock_session.return_value.__aenter__.return_value = mock_driver

            await commands.upgrade(use_logger=False)

            # Console should be called
            mock_console.print.assert_called()
            call_args = str(mock_console.print.call_args)
            assert "No migrations found" in call_args

    async def test_config_default_use_logger_true(self, async_config_with_use_logger: AiosqliteConfig) -> None:
        """MigrationConfig use_logger=True should be used as default."""
        commands = AsyncMigrationCommands(async_config_with_use_logger)
        mock_driver = AsyncMock()
        mock_driver.driver_features = {}

        with (
            patch.object(async_config_with_use_logger, "provide_session") as mock_session,
            patch("sqlspec.migrations.commands.console"),
            patch("sqlspec.migrations.commands.logger") as mock_logger,
            patch.object(commands.runner, "get_migration_files", return_value=[]),
        ):
            mock_session.return_value.__aenter__.return_value = mock_driver

            # No explicit parameter - should use config default
            await commands.upgrade()

            # Logger should be called
            mock_logger.info.assert_called()
            call_args = str(mock_logger.info.call_args)
            assert "No migrations found" in call_args


class TestAsyncDowngradeUseLogger:
    """Test use_logger parameter for async downgrade command."""

    async def test_use_logger_true_uses_logger(self, async_config: AiosqliteConfig) -> None:
        """use_logger=True should output to logger instead of console."""
        commands = AsyncMigrationCommands(async_config)
        mock_driver = AsyncMock()
        mock_driver.driver_features = {}

        with (
            patch.object(async_config, "provide_session") as mock_session,
            patch("sqlspec.migrations.commands.console"),
            patch("sqlspec.migrations.commands.logger") as mock_logger,
            patch.object(commands.tracker, "get_applied_migrations", return_value=[]),
        ):
            mock_session.return_value.__aenter__.return_value = mock_driver

            await commands.downgrade(use_logger=True)

            # Logger should be called
            mock_logger.info.assert_called()
            call_args = str(mock_logger.info.call_args)
            assert "No migrations to downgrade" in call_args

    async def test_use_logger_false_uses_console(self, async_config: AiosqliteConfig) -> None:
        """use_logger=False (default) should output to Rich console."""
        commands = AsyncMigrationCommands(async_config)
        mock_driver = AsyncMock()
        mock_driver.driver_features = {}

        with (
            patch.object(async_config, "provide_session") as mock_session,
            patch("sqlspec.migrations.commands.console") as mock_console,
            patch("sqlspec.migrations.commands.logger"),
            patch.object(commands.tracker, "get_applied_migrations", return_value=[]),
        ):
            mock_session.return_value.__aenter__.return_value = mock_driver

            await commands.downgrade(use_logger=False)

            # Console should be called
            mock_console.print.assert_called()
            call_args = str(mock_console.print.call_args)
            assert "No migrations to downgrade" in call_args


class TestConfigMigrateUpUseLogger:
    """Test use_logger parameter is passed through config.migrate_up()."""

    def test_sync_config_migrate_up_passes_use_logger(self, sync_config: SqliteConfig) -> None:
        """Test that SqliteConfig.migrate_up() passes use_logger to commands."""
        with patch.object(SyncMigrationCommands, "upgrade") as mock_upgrade:
            sync_config.migrate_up(use_logger=True)
            mock_upgrade.assert_called_once()
            assert mock_upgrade.call_args.kwargs.get("use_logger") is True

    def test_sync_config_migrate_down_passes_use_logger(self, sync_config: SqliteConfig) -> None:
        """Test that SqliteConfig.migrate_down() passes use_logger to commands."""
        with patch.object(SyncMigrationCommands, "downgrade") as mock_downgrade:
            sync_config.migrate_down(use_logger=True)
            mock_downgrade.assert_called_once()
            assert mock_downgrade.call_args.kwargs.get("use_logger") is True

    async def test_async_config_migrate_up_passes_use_logger(self, async_config: AiosqliteConfig) -> None:
        """Test that AiosqliteConfig.migrate_up() passes use_logger to commands."""
        with patch.object(AsyncMigrationCommands, "upgrade", new_callable=AsyncMock) as mock_upgrade:
            await async_config.migrate_up(use_logger=True)
            mock_upgrade.assert_called_once()
            assert mock_upgrade.call_args.kwargs.get("use_logger") is True

    async def test_async_config_migrate_down_passes_use_logger(self, async_config: AiosqliteConfig) -> None:
        """Test that AiosqliteConfig.migrate_down() passes use_logger to commands."""
        with patch.object(AsyncMigrationCommands, "downgrade", new_callable=AsyncMock) as mock_downgrade:
            await async_config.migrate_down(use_logger=True)
            mock_downgrade.assert_called_once()
            assert mock_downgrade.call_args.kwargs.get("use_logger") is True


class TestDryRunWithUseLogger:
    """Test dry_run mode with use_logger=True."""

    def test_sync_upgrade_dry_run_uses_logger(self, sync_config: SqliteConfig) -> None:
        """Dry run mode with use_logger=True should log the dry run message."""
        commands = SyncMigrationCommands(sync_config)
        mock_driver = MagicMock()

        with (
            patch.object(sync_config, "provide_session") as mock_session,
            patch("sqlspec.migrations.commands.console"),
            patch("sqlspec.migrations.commands.logger") as mock_logger,
            patch.object(commands.runner, "get_migration_files", return_value=[]),
        ):
            mock_session.return_value.__enter__.return_value = mock_driver

            commands.upgrade(dry_run=True, use_logger=True)

            # Should log the dry run message
            logger_calls = [str(call) for call in mock_logger.info.call_args_list]
            assert any("DRY RUN MODE" in call for call in logger_calls)

    def test_sync_downgrade_dry_run_uses_logger(self, sync_config: SqliteConfig) -> None:
        """Dry run mode with use_logger=True should log the dry run message."""
        commands = SyncMigrationCommands(sync_config)
        mock_driver = MagicMock()

        with (
            patch.object(sync_config, "provide_session") as mock_session,
            patch("sqlspec.migrations.commands.console"),
            patch("sqlspec.migrations.commands.logger") as mock_logger,
            patch.object(commands.tracker, "get_applied_migrations", return_value=[]),
        ):
            mock_session.return_value.__enter__.return_value = mock_driver

            commands.downgrade(dry_run=True, use_logger=True)

            # Should log the dry run message
            logger_calls = [str(call) for call in mock_logger.info.call_args_list]
            assert any("DRY RUN MODE" in call for call in logger_calls)
