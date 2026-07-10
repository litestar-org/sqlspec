"""Unit tests for migration commands functionality.

Tests focused on MigrationCommands class behavior including:
- Async/sync command delegation
- Initialization behavior
- Configuration handling
- Error scenarios and edge cases
- Command routing and parameter passing
"""

import inspect
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.exceptions import MigrationError
from sqlspec.migrations.base import BaseMigrationCommands
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands, create_migration_commands
from sqlspec.migrations.tracker import AsyncMigrationTracker, SyncMigrationTracker


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
        mock_current.assert_called_once_with(verbose=False)
        assert result == "test_version"


def test_sync_current_verbose_reuses_applied_migration_query(sync_config: SqliteConfig) -> None:
    """Verbose current output should derive the current version from its history query."""
    commands = SyncMigrationCommands(sync_config)
    driver = MagicMock()
    session = MagicMock()
    session.__enter__.return_value = driver
    commands.tracker = MagicMock()
    commands.tracker.get_applied_migrations.return_value = [
        {
            "version_num": "0001",
            "version_type": "sequential",
            "execution_sequence": 1,
            "description": "initial",
            "applied_at": "now",
            "execution_time_ms": 1,
            "checksum": "abc",
            "applied_by": "tester",
            "replaces": None,
        }
    ]

    with patch.object(sync_config, "provide_session", return_value=session):
        assert commands.current(verbose=True) == "0001"

    commands.tracker.get_current_version.assert_not_called()
    commands.tracker.get_applied_migrations.assert_called_once_with(driver)


async def test_async_current_verbose_reuses_applied_migration_query(async_config: AiosqliteConfig) -> None:
    """Async verbose current output should also issue one history query."""
    commands = AsyncMigrationCommands(async_config)
    driver = MagicMock()
    session = MagicMock()
    session.__aenter__ = AsyncMock(return_value=driver)
    session.__aexit__ = AsyncMock(return_value=None)
    commands.tracker = MagicMock()
    commands.tracker.ensure_tracking_table = AsyncMock()
    commands.tracker.get_current_version = AsyncMock()
    commands.tracker.get_applied_migrations = AsyncMock(
        return_value=[
            {
                "version_num": "0001",
                "version_type": "sequential",
                "execution_sequence": 1,
                "description": "initial",
                "applied_at": "now",
                "execution_time_ms": 1,
                "checksum": "abc",
                "applied_by": "tester",
                "replaces": None,
            }
        ]
    )

    with patch.object(async_config, "provide_session", return_value=session):
        assert await commands.current(verbose=True) == "0001"

    commands.tracker.get_current_version.assert_not_awaited()
    commands.tracker.get_applied_migrations.assert_awaited_once_with(driver)


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
        mock_revision.assert_called_once_with("Test async revision", "python")


def test_migration_commands_factory_returns_sync_for_sync_config(sync_config: SqliteConfig) -> None:
    """Test that sync config returns SyncMigrationCommands from factory."""
    commands: SyncMigrationCommands[SqliteConfig] | AsyncMigrationCommands[AiosqliteConfig] = create_migration_commands(
        sync_config
    )
    assert isinstance(commands, SyncMigrationCommands)
    assert commands.config == sync_config
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


class SchemaAwareSqliteConfig(SqliteConfig):
    supports_migration_schemas = True


class SchemaAwareAiosqliteConfig(AiosqliteConfig):
    supports_migration_schemas = True


class LegacySyncMigrationTracker(SyncMigrationTracker):
    def __init__(self, version_table_name: str = "ddl_migrations") -> None:
        super().__init__(version_table_name)


class LegacyAsyncMigrationTracker(AsyncMigrationTracker):
    def __init__(self, version_table_name: str = "ddl_migrations") -> None:
        super().__init__(version_table_name)


class LegacyTrackerSqliteConfig(SqliteConfig):
    migration_tracker_type = LegacySyncMigrationTracker


class LegacyTrackerAiosqliteConfig(AiosqliteConfig):
    migration_tracker_type = LegacyAsyncMigrationTracker


def test_sync_commands_keep_legacy_tracker_constructor_shape_without_schema(tmp_path: Path) -> None:
    config = LegacyTrackerSqliteConfig(
        connection_config={"database": ":memory:"}, migration_config={"script_location": str(tmp_path)}
    )
    commands = SyncMigrationCommands(config)
    assert isinstance(commands.tracker, LegacySyncMigrationTracker)
    assert commands.tracker.version_table == "ddl_migrations"


def test_async_commands_keep_legacy_tracker_constructor_shape_without_schema(tmp_path: Path) -> None:
    config = LegacyTrackerAiosqliteConfig(
        connection_config={"database": ":memory:"}, migration_config={"script_location": str(tmp_path)}
    )
    commands = AsyncMigrationCommands(config)
    assert isinstance(commands.tracker, LegacyAsyncMigrationTracker)
    assert commands.tracker.version_table == "ddl_migrations"


def test_sync_commands_pass_resolved_tracker_schema_when_supported(tmp_path: Path) -> None:
    config = SchemaAwareSqliteConfig(
        connection_config={"database": ":memory:"},
        migration_config={"script_location": str(tmp_path), "default_schema": "app_schema"},
    )
    commands = SyncMigrationCommands(config)
    assert commands.tracker.version_table_schema == "app_schema"
    assert commands.tracker.version_table == "app_schema.ddl_migrations"


def test_async_commands_pass_resolved_tracker_schema_when_supported(tmp_path: Path) -> None:
    config = SchemaAwareAiosqliteConfig(
        connection_config={"database": ":memory:"},
        migration_config={"script_location": str(tmp_path), "version_table_schema": "history_schema"},
    )
    commands = AsyncMigrationCommands(config)
    assert commands.tracker.version_table_schema == "history_schema"
    assert commands.tracker.version_table == "history_schema.ddl_migrations"


def test_sync_validate_migration_schema_raises_for_missing_schema(tmp_path: Path) -> None:
    config = SchemaAwareSqliteConfig(
        connection_config={"database": str(tmp_path / "db.sqlite")},
        migration_config={"default_schema": "missing_schema"},
    )
    commands = SyncMigrationCommands(config)
    driver = MagicMock()
    driver.has_schema.return_value = False
    with pytest.raises(MigrationError, match="Configured schema 'missing_schema' does not exist"):
        commands._validate_migration_schema(driver)
    driver.has_schema.assert_called_once_with("missing_schema")


async def test_async_validate_migration_schema_raises_for_missing_schema(tmp_path: Path) -> None:
    config = SchemaAwareAiosqliteConfig(
        connection_config={"database": str(tmp_path / "db.sqlite")},
        migration_config={"default_schema": "missing_schema"},
    )
    commands = AsyncMigrationCommands(config)
    driver = AsyncMock()
    driver.has_schema.return_value = False
    with pytest.raises(MigrationError, match="Configured schema 'missing_schema' does not exist"):
        await commands._validate_migration_schema(driver)
    driver.has_schema.assert_awaited_once_with("missing_schema")


def test_sync_validate_migration_schema_raises_when_adapter_does_not_support_it(sync_config: SqliteConfig) -> None:
    sync_config.set_migration_config({"default_schema": "any_schema"})
    commands = SyncMigrationCommands(sync_config)
    driver = MagicMock()
    with pytest.raises(MigrationError, match="does not support migration default schemas"):
        commands._validate_migration_schema(driver)
    driver.has_schema.assert_not_called()


async def test_async_validate_migration_schema_raises_when_adapter_does_not_support_it(
    async_config: AiosqliteConfig,
) -> None:
    async_config.set_migration_config({"default_schema": "any_schema"})
    commands = AsyncMigrationCommands(async_config)
    driver = AsyncMock()
    with pytest.raises(MigrationError, match="does not support migration default schemas"):
        await commands._validate_migration_schema(driver)
    driver.has_schema.assert_not_called()


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
        commands.upgrade()
        mock_upgrade.assert_called_with()
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
        patch.object(commands.tracker, "get_applied_migrations", return_value=[]),
        patch.object(commands.tracker, "ensure_tracking_table", return_value=None),
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


def test_commands_contract_base_revision_file_type_default_matches_concrete_commands() -> None:
    base_param = inspect.signature(BaseMigrationCommands.revision).parameters["file_type"]
    sync_param = inspect.signature(SyncMigrationCommands.revision).parameters["file_type"]
    async_param = inspect.signature(AsyncMigrationCommands.revision).parameters["file_type"]
    assert base_param.default is None
    assert sync_param.default is None
    assert async_param.default is None


def test_commands_contract_upgrade_documents_config_auto_sync_as_unoverridable() -> None:
    sync_source = inspect.getsource(SyncMigrationCommands.upgrade)
    async_source = inspect.getsource(AsyncMigrationCommands.upgrade)
    assert "config auto_sync=False cannot be overridden by the call-site flag" in sync_source
    assert "config auto_sync=False cannot be overridden by the call-site flag" in async_source
