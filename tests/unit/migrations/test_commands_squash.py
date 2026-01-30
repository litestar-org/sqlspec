# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Unit tests for Migration Commands squash functionality.

Tests for:
- SyncMigrationCommands.squash()
- AsyncMigrationCommands.squash()
"""

from pathlib import Path
from unittest.mock import Mock

import pytest

pytestmark = pytest.mark.xdist_group("migrations")


class TestSyncMigrationCommandsSquash:
    """Tests for SyncMigrationCommands.squash() method."""

    def test_squash_valid_range(self, tmp_path: Path) -> None:
        """Test squash succeeds with valid version range."""
        from sqlspec.migrations.commands import SyncMigrationCommands

        (tmp_path / "0001_initial.sql").write_text("-- name: migrate-0001-up\nCREATE TABLE t1 (id INT);")
        (tmp_path / "0002_users.sql").write_text("-- name: migrate-0002-up\nCREATE TABLE t2 (id INT);")
        (tmp_path / "0003_posts.sql").write_text("-- name: migrate-0003-up\nCREATE TABLE t3 (id INT);")

        config = Mock()
        config.is_async = False
        config.migration_tracker_type = Mock(return_value=Mock())
        config.migration_config = {"script_location": str(tmp_path)}
        config.provide_session = Mock()
        config.get_observability_runtime = Mock(return_value=None)

        commands = SyncMigrationCommands(config)

        commands.squash(start_version="0001", end_version="0003", description="release_1", dry_run=True)

    def test_squash_invalid_range_raises_error(self, tmp_path: Path) -> None:
        """Test squash raises error when end version is before start."""
        from sqlspec.exceptions import SquashValidationError
        from sqlspec.migrations.commands import SyncMigrationCommands

        (tmp_path / "0001_initial.sql").write_text("-- name: migrate-0001-up\nCREATE TABLE t1 (id INT);")
        (tmp_path / "0002_users.sql").write_text("-- name: migrate-0002-up\nCREATE TABLE t2 (id INT);")

        config = Mock()
        config.is_async = False
        config.migration_tracker_type = Mock(return_value=Mock())
        config.migration_config = {"script_location": str(tmp_path)}
        config.provide_session = Mock()
        config.get_observability_runtime = Mock(return_value=None)

        commands = SyncMigrationCommands(config)

        with pytest.raises(SquashValidationError, match="Invalid range"):
            commands.squash(start_version="0003", end_version="0001", description="release")

    def test_squash_dry_run_does_not_modify_files(self, tmp_path: Path) -> None:
        """Test dry_run mode previews without modifying files."""
        from sqlspec.migrations.commands import SyncMigrationCommands

        (tmp_path / "0001_initial.sql").write_text("-- name: migrate-0001-up\nCREATE TABLE t1 (id INT);")
        (tmp_path / "0002_users.sql").write_text("-- name: migrate-0002-up\nCREATE TABLE t2 (id INT);")

        config = Mock()
        config.is_async = False
        config.migration_tracker_type = Mock(return_value=Mock())
        config.migration_config = {"script_location": str(tmp_path)}
        config.provide_session = Mock()
        config.get_observability_runtime = Mock(return_value=None)

        commands = SyncMigrationCommands(config)
        commands.squash(start_version="0001", end_version="0002", description="release", dry_run=True)

        assert (tmp_path / "0001_initial.sql").exists()
        assert (tmp_path / "0002_users.sql").exists()

    def test_squash_update_database_false_skips_tracker(self, tmp_path: Path) -> None:
        """Test update_database=False skips tracker operations by verifying no DB session is opened."""
        from sqlspec.migrations.commands import SyncMigrationCommands

        (tmp_path / "0001_initial.sql").write_text("-- name: migrate-0001-up\nCREATE TABLE t1 (id INT);")
        (tmp_path / "0002_users.sql").write_text("-- name: migrate-0002-up\nCREATE TABLE t2 (id INT);")

        config = Mock()
        config.is_async = False
        tracker_mock = Mock()
        config.migration_tracker_type = Mock(return_value=tracker_mock)
        config.migration_config = {"script_location": str(tmp_path)}
        config.provide_session = Mock()
        config.get_observability_runtime = Mock(return_value=None)

        commands = SyncMigrationCommands(config)
        commands.squash(
            start_version="0001", end_version="0002", description="release", update_database=False, dry_run=True
        )

        config.provide_session.assert_not_called()


class TestAsyncMigrationCommandsSquash:
    """Tests for AsyncMigrationCommands.squash() method."""

    @pytest.mark.anyio
    async def test_async_squash_valid_range(self, tmp_path: Path) -> None:
        """Test async squash succeeds with valid version range."""
        from sqlspec.migrations.commands import AsyncMigrationCommands

        (tmp_path / "0001_initial.sql").write_text("-- name: migrate-0001-up\nCREATE TABLE t1 (id INT);")
        (tmp_path / "0002_users.sql").write_text("-- name: migrate-0002-up\nCREATE TABLE t2 (id INT);")

        config = Mock()
        config.is_async = True
        config.migration_tracker_type = Mock(return_value=Mock())
        config.migration_config = {"script_location": str(tmp_path)}
        config.get_observability_runtime = Mock(return_value=None)

        commands = AsyncMigrationCommands(config)

        await commands.squash(start_version="0001", end_version="0002", description="release", dry_run=True)

    @pytest.mark.anyio
    async def test_async_squash_invalid_range_raises_error(self, tmp_path: Path) -> None:
        """Test async squash raises error when end version is before start."""
        from sqlspec.exceptions import SquashValidationError
        from sqlspec.migrations.commands import AsyncMigrationCommands

        (tmp_path / "0001_initial.sql").write_text("-- name: migrate-0001-up\nCREATE TABLE t1 (id INT);")
        (tmp_path / "0002_users.sql").write_text("-- name: migrate-0002-up\nCREATE TABLE t2 (id INT);")

        config = Mock()
        config.is_async = True
        config.migration_tracker_type = Mock(return_value=Mock())
        config.migration_config = {"script_location": str(tmp_path)}
        config.get_observability_runtime = Mock(return_value=None)

        commands = AsyncMigrationCommands(config)

        with pytest.raises(SquashValidationError, match="Invalid range"):
            await commands.squash(start_version="0003", end_version="0001", description="release")
