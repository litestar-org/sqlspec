"""Unit tests for migration commands functionality.

Tests focused on MigrationCommands class behavior including:
- Async/sync command delegation
- Initialization behavior
- Configuration handling
- Error scenarios and edge cases
- Command routing and parameter passing
"""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import pytest

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.migrations.commands import AsyncMigrationCommands, MigrationCommands, SyncMigrationCommands

pytestmark = pytest.mark.xdist_group("migrations")


@pytest.fixture
def sync_config() -> SqliteConfig:
    """Create a sync database config for testing."""
    return SqliteConfig(pool_config={"database": ":memory:"})


@pytest.fixture
def async_config() -> AiosqliteConfig:
    """Create an async database config for testing."""
    return AiosqliteConfig(pool_config={"database": ":memory:"})


def test_migration_commands_sync_config_initialization(sync_config: SqliteConfig) -> None:
    """Test MigrationCommands initializes with sync implementation for sync config."""
    commands = MigrationCommands(sync_config)

    assert not commands._is_async
    assert isinstance(commands._impl, SyncMigrationCommands)


def test_migration_commands_async_config_initialization(async_config: AiosqliteConfig) -> None:
    """Test MigrationCommands initializes with async implementation for async config."""
    commands = MigrationCommands(async_config)

    assert commands._is_async
    assert isinstance(commands._impl, AsyncMigrationCommands)


def test_migration_commands_sync_init_delegation(sync_config: SqliteConfig) -> None:
    """Test that sync config init is delegated directly to sync implementation."""
    with patch.object(SyncMigrationCommands, "init") as mock_init:
        commands = MigrationCommands(sync_config)

        with tempfile.TemporaryDirectory() as temp_dir:
            migration_dir = str(Path(temp_dir) / "migrations")

            commands.init(migration_dir, package=False)

            mock_init.assert_called_once_with(migration_dir, package=False)


def test_migration_commands_async_init_delegation(async_config: AiosqliteConfig) -> None:
    """Test that async config init uses await_ wrapper."""
    with (
        patch.object(AsyncMigrationCommands, "init", new_callable=AsyncMock),
        patch("sqlspec.migrations.commands.await_") as mock_await,
    ):
        # Use AsyncMock and set up await_ to return a simple callable
        AsyncMock(return_value=None)
        mock_await.return_value = Mock(return_value=None)

        commands = MigrationCommands(async_config)

        with tempfile.TemporaryDirectory() as temp_dir:
            migration_dir = str(Path(temp_dir) / "migrations")

            commands.init(migration_dir, package=True)

            # Verify await_ was called with raise_sync_error=False
            mock_await.assert_called_once()
            call_args = mock_await.call_args
            assert call_args[1]["raise_sync_error"] is False


def test_migration_commands_sync_current_delegation(sync_config: SqliteConfig) -> None:
    """Test that sync config current is delegated directly to sync implementation."""
    with patch.object(SyncMigrationCommands, "current") as mock_current:
        commands = MigrationCommands(sync_config)

        commands.current(verbose=True)

        mock_current.assert_called_once_with(verbose=True)


def test_migration_commands_async_current_delegation(async_config: AiosqliteConfig) -> None:
    """Test that async config current uses await_ wrapper."""
    with (
        patch.object(AsyncMigrationCommands, "current", new_callable=AsyncMock),
        patch("sqlspec.migrations.commands.await_") as mock_await,
    ):
        # Set up await_ to return a callable that returns the expected value
        mock_await.return_value = Mock(return_value="test_version")

        commands = MigrationCommands(async_config)

        result = commands.current(verbose=False)

        # Verify await_ was called with raise_sync_error=False
        mock_await.assert_called_once()
        call_args = mock_await.call_args
        assert call_args[1]["raise_sync_error"] is False
        assert result == "test_version"


def test_migration_commands_sync_upgrade_delegation(sync_config: SqliteConfig) -> None:
    """Test that sync config upgrade is delegated directly to sync implementation."""
    with patch.object(SyncMigrationCommands, "upgrade") as mock_upgrade:
        commands = MigrationCommands(sync_config)

        commands.upgrade(revision="001")

        mock_upgrade.assert_called_once_with(revision="001")


def test_migration_commands_async_upgrade_delegation(async_config: AiosqliteConfig) -> None:
    """Test that async config upgrade uses await_ wrapper."""
    with (
        patch.object(AsyncMigrationCommands, "upgrade", new_callable=AsyncMock),
        patch("sqlspec.migrations.commands.await_") as mock_await,
    ):
        # Set up await_ to return a callable that returns None
        mock_await.return_value = Mock(return_value=None)

        commands = MigrationCommands(async_config)

        commands.upgrade(revision="002")

        # Verify await_ was called with raise_sync_error=False
        mock_await.assert_called_once()
        call_args = mock_await.call_args
        assert call_args[1]["raise_sync_error"] is False


def test_migration_commands_sync_downgrade_delegation(sync_config: SqliteConfig) -> None:
    """Test that sync config downgrade is delegated directly to sync implementation."""
    with patch.object(SyncMigrationCommands, "downgrade") as mock_downgrade:
        commands = MigrationCommands(sync_config)

        commands.downgrade(revision="base")

        mock_downgrade.assert_called_once_with(revision="base")


def test_migration_commands_async_downgrade_delegation(async_config: AiosqliteConfig) -> None:
    """Test that async config downgrade uses await_ wrapper."""
    with (
        patch.object(AsyncMigrationCommands, "downgrade", new_callable=AsyncMock),
        patch("sqlspec.migrations.commands.await_") as mock_await,
    ):
        # Set up await_ to return a callable that returns None
        mock_await.return_value = Mock(return_value=None)

        commands = MigrationCommands(async_config)

        commands.downgrade(revision="001")

        # Verify await_ was called with raise_sync_error=False
        mock_await.assert_called_once()
        call_args = mock_await.call_args
        assert call_args[1]["raise_sync_error"] is False


def test_migration_commands_sync_stamp_delegation(sync_config: SqliteConfig) -> None:
    """Test that sync config stamp is delegated directly to sync implementation."""
    with patch.object(SyncMigrationCommands, "stamp") as mock_stamp:
        commands = MigrationCommands(sync_config)

        commands.stamp("001")

        mock_stamp.assert_called_once_with("001")


def test_migration_commands_async_stamp_delegation(async_config: AiosqliteConfig) -> None:
    """Test that async config stamp uses await_ wrapper."""
    with (
        patch.object(AsyncMigrationCommands, "stamp", new_callable=AsyncMock),
        patch("sqlspec.migrations.commands.await_") as mock_await,
    ):
        # Set up await_ to return a callable that returns None
        mock_await.return_value = Mock(return_value=None)

        commands = MigrationCommands(async_config)

        commands.stamp("002")

        # Verify await_ was called with raise_sync_error=False
        mock_await.assert_called_once()
        call_args = mock_await.call_args
        assert call_args[1]["raise_sync_error"] is False


def test_migration_commands_sync_revision_delegation(sync_config: SqliteConfig) -> None:
    """Test that sync config revision is delegated directly to sync implementation."""
    with patch.object(SyncMigrationCommands, "revision") as mock_revision:
        commands = MigrationCommands(sync_config)

        commands.revision("Test revision", "sql")

        mock_revision.assert_called_once_with("Test revision", "sql")


def test_migration_commands_async_revision_delegation(async_config: AiosqliteConfig) -> None:
    """Test that async config revision uses await_ wrapper."""
    with (
        patch.object(AsyncMigrationCommands, "revision", new_callable=AsyncMock),
        patch("sqlspec.migrations.commands.await_") as mock_await,
    ):
        # Set up await_ to return a callable that returns None
        mock_await.return_value = Mock(return_value=None)

        commands = MigrationCommands(async_config)

        commands.revision("Test async revision", "python")

        # Verify await_ was called with raise_sync_error=False
        mock_await.assert_called_once()
        call_args = mock_await.call_args
        assert call_args[1]["raise_sync_error"] is False


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


def test_sync_migration_commands_init_creates_directory(sync_config: SqliteConfig) -> None:
    """Test that SyncMigrationCommands init creates migration directory structure."""
    commands = SyncMigrationCommands(sync_config)

    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"

        commands.init(str(migration_dir), package=True)

        assert migration_dir.exists()
        assert (migration_dir / "__init__.py").exists()


def test_sync_migration_commands_init_without_package(sync_config: SqliteConfig) -> None:
    """Test that SyncMigrationCommands init creates directory without __init__.py when package=False."""
    commands = SyncMigrationCommands(sync_config)

    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"

        commands.init(str(migration_dir), package=False)

        assert migration_dir.exists()
        assert not (migration_dir / "__init__.py").exists()


def test_migration_commands_error_propagation(async_config: AiosqliteConfig) -> None:
    """Test that errors from underlying implementations are properly propagated."""
    with (
        patch.object(AsyncMigrationCommands, "upgrade", side_effect=ValueError("Test error")),
        patch("sqlspec.migrations.commands.await_") as mock_await,
    ):
        # Set up await_ to raise the same error
        mock_await.return_value = Mock(side_effect=ValueError("Test error"))

        commands = MigrationCommands(async_config)

        with pytest.raises(ValueError, match="Test error"):
            commands.upgrade()


def test_migration_commands_parameter_forwarding(sync_config: SqliteConfig) -> None:
    """Test that all parameters are properly forwarded to underlying implementations."""
    with patch.object(SyncMigrationCommands, "upgrade") as mock_upgrade:
        commands = MigrationCommands(sync_config)

        # Test with various parameter combinations
        commands.upgrade()
        mock_upgrade.assert_called_with(revision="head")

        commands.upgrade("specific_revision")
        mock_upgrade.assert_called_with(revision="specific_revision")


def test_migration_commands_config_type_detection(sync_config: SqliteConfig, async_config: AiosqliteConfig) -> None:
    """Test that MigrationCommands correctly detects async vs sync configs."""
    sync_commands = MigrationCommands(sync_config)
    async_commands = MigrationCommands(async_config)

    assert not sync_commands._is_async
    assert async_commands._is_async

    assert isinstance(sync_commands._impl, SyncMigrationCommands)
    assert isinstance(async_commands._impl, AsyncMigrationCommands)
