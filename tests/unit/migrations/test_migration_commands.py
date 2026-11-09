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
from sqlspec.adapters.asyncpg.config import AsyncpgConfig
from sqlspec.adapters.psqlpy.config import PsqlpyConfig
from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgSyncConfig
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


def test_cached_sync_migration_commands_have_resolved_dialect(sync_config: SqliteConfig) -> None:
    """Cached sync migration commands inherit the config's resolved dialect."""
    commands = sync_config.get_migration_commands()

    assert commands.runner.context is not None
    assert commands.runner.context.dialect == "sqlite"


def test_cached_async_migration_commands_have_resolved_dialect(async_config: AiosqliteConfig) -> None:
    """Cached async migration commands inherit the config's resolved dialect."""
    commands = async_config.get_migration_commands()

    assert commands.runner.context is not None
    assert commands.runner.context.dialect == "sqlite"


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
    driver.data_dictionary.get_tables.return_value = [{"table_name": "ddl_migrations"}]
    session = MagicMock()
    session.__enter__.return_value = driver
    commands.tracker = MagicMock()
    commands.tracker.version_table_name = "ddl_migrations"
    commands.tracker.version_table_schema = None
    commands.tracker.catalog_schema_name.return_value = None
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
    driver.data_dictionary.get_tables = AsyncMock(return_value=[{"table_name": "ddl_migrations"}])
    session = MagicMock()
    session.__aenter__ = AsyncMock(return_value=driver)
    session.__aexit__ = AsyncMock(return_value=None)
    commands.tracker = MagicMock()
    commands.tracker.version_table_name = "ddl_migrations"
    commands.tracker.version_table_schema = None
    commands.tracker.catalog_schema_name.return_value = None
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


def test_sync_commands_reject_default_schema_when_adapter_lacks_support(sync_config: SqliteConfig) -> None:
    sync_config.set_migration_config({"default_schema": "any_schema"})
    with pytest.raises(MigrationError, match="SqliteConfig does not support migration default schemas"):
        SyncMigrationCommands(sync_config)


def test_async_commands_reject_default_schema_when_adapter_lacks_support(async_config: AiosqliteConfig) -> None:
    async_config.set_migration_config({"default_schema": "any_schema"})
    with pytest.raises(MigrationError, match="AiosqliteConfig does not support migration default schemas"):
        AsyncMigrationCommands(async_config)


def test_sync_commands_reject_version_table_schema_when_adapter_lacks_support(sync_config: SqliteConfig) -> None:
    sync_config.set_migration_config({"version_table_schema": "history"})
    with pytest.raises(
        MigrationError, match=r"does not support migration version table schemas.*version_table_schema='history'"
    ):
        SyncMigrationCommands(sync_config)


def test_async_commands_reject_version_table_schema_when_adapter_lacks_support(async_config: AiosqliteConfig) -> None:
    async_config.set_migration_config({"version_table_schema": "history"})
    with pytest.raises(
        MigrationError, match=r"does not support migration version table schemas.*version_table_schema='history'"
    ):
        AsyncMigrationCommands(async_config)


def test_commands_reject_default_schema_first_when_both_schema_options_are_set(sync_config: SqliteConfig) -> None:
    sync_config.set_migration_config({"default_schema": "any_schema", "version_table_schema": "history"})
    with pytest.raises(MigrationError, match="does not support migration default schemas"):
        SyncMigrationCommands(sync_config)


def test_commands_accept_empty_schema_options_when_adapter_lacks_support(sync_config: SqliteConfig) -> None:
    sync_config.set_migration_config({"default_schema": "", "version_table_schema": ""})
    commands = SyncMigrationCommands(sync_config)
    assert commands.tracker.version_table_schema is None


def test_commands_accept_qualified_version_table_when_adapter_lacks_support(sync_config: SqliteConfig) -> None:
    sync_config.set_migration_config({"version_table_name": "audit.ddl_migrations"})
    commands = SyncMigrationCommands(sync_config)
    assert commands.tracker.version_table_schema == "audit"


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


async def test_migration_commands_route_over_direct_dsn() -> None:
    """SyncMigrationCommands and AsyncMigrationCommands derive a session-mode config when direct_dsn is configured."""
    pooled_sync = PsycopgSyncConfig(
        connection_config={"conninfo": "postgresql://pooler:6432/db"},
        driver_features={"pooler_mode": "transaction", "direct_dsn": "postgresql://direct:5432/db"},
    )
    sync_commands = SyncMigrationCommands(pooled_sync)
    assert sync_commands.config is not pooled_sync
    assert sync_commands.config.connection_config["conninfo"] == "postgresql://direct:5432/db"
    assert sync_commands.config.driver_features["pooler_mode"] == "session"
    assert sync_commands.config.driver_features["direct_dsn"] is None

    mock_sync_driver = MagicMock()
    mock_sync_driver.data_dictionary.get_tables.return_value = [{"table_name": "ddl_migrations"}]
    with (
        patch.object(sync_commands.config, "provide_session") as mock_direct_session,
        patch.object(pooled_sync, "provide_session") as mock_pooled_session,
        patch("sqlspec.migrations.commands.console"),
    ):
        mock_direct_session.return_value.__enter__.return_value = mock_sync_driver
        sync_commands.tracker = MagicMock()
        sync_commands.tracker.version_table_name = "ddl_migrations"
        sync_commands.tracker.version_table_schema = None
        sync_commands.tracker.catalog_schema_name.return_value = None
        sync_commands.tracker.get_current_version.return_value = "0001"
        assert sync_commands.current() == "0001"
        mock_direct_session.assert_called_once()
        mock_pooled_session.assert_not_called()

    for async_cfg in (
        AsyncpgConfig(
            connection_config={"dsn": "postgresql://pooler:6432/db"},
            driver_features={"pooler_mode": "transaction", "direct_dsn": "postgresql://direct:5432/db"},
        ),
        PsycopgAsyncConfig(
            connection_config={"conninfo": "postgresql://pooler:6432/db"},
            driver_features={"pooler_mode": "transaction", "direct_dsn": "postgresql://direct:5432/db"},
        ),
        PsqlpyConfig(
            connection_config={"dsn": "postgresql://pooler:6432/db"},
            driver_features={"pooler_mode": "transaction", "direct_dsn": "postgresql://direct:5432/db"},
        ),
    ):
        async_commands = AsyncMigrationCommands(async_cfg)
        assert async_commands.config is not async_cfg
        assert async_commands.config.driver_features["pooler_mode"] == "session"
        assert async_commands.config.driver_features["direct_dsn"] is None
        mock_async_driver = AsyncMock()
        mock_async_driver.data_dictionary.get_tables = AsyncMock(return_value=[{"table_name": "ddl_migrations"}])
        with (
            patch.object(async_commands.config, "provide_session") as mock_direct_async_session,
            patch.object(async_cfg, "provide_session") as mock_pooled_async_session,
            patch("sqlspec.migrations.commands.console"),
        ):
            mock_direct_async_session.return_value.__aenter__.return_value = mock_async_driver
            async_commands.tracker = MagicMock()
            async_commands.tracker.version_table_name = "ddl_migrations"
            async_commands.tracker.version_table_schema = None
            async_commands.tracker.catalog_schema_name.return_value = None
            async_commands.tracker.ensure_tracking_table = AsyncMock()
            async_commands.tracker.get_current_version = AsyncMock(return_value="0001")
            assert await async_commands.current() == "0001"
            mock_direct_async_session.assert_called_once()
            mock_pooled_async_session.assert_not_called()


def _build_sqlite_mode_config(
    db_path: Path,
    migrations_dir: Path,
    *,
    is_async: bool,
    include_extensions: list[str] | None = None,
    extension_config: dict[str, dict[str, object]] | None = None,
    extra_migration_config: dict[str, object] | None = None,
) -> SqliteConfig | AiosqliteConfig:
    from typing import Any, cast

    migration_cfg: dict[str, Any] = {"script_location": str(migrations_dir)}
    if include_extensions is not None:
        migration_cfg["include_extensions"] = include_extensions
    if extra_migration_config:
        migration_cfg.update(extra_migration_config)
    ext_cfg = cast("Any", extension_config or {})
    if is_async:
        return AiosqliteConfig(
            connection_config={"database": str(db_path)}, migration_config=migration_cfg, extension_config=ext_cfg
        )
    return SqliteConfig(
        connection_config={"database": str(db_path)}, migration_config=migration_cfg, extension_config=ext_cfg
    )


async def _run_upgrade(config: SqliteConfig | AiosqliteConfig, **kwargs: object) -> None:
    from typing import Any, cast

    cmds = create_migration_commands(config)
    if isinstance(cmds, AsyncMigrationCommands):
        await cmds.upgrade(**cast("dict[str, Any]", kwargs))
    else:
        cmds.upgrade(**cast("dict[str, Any]", kwargs))


async def _run_verify(config: SqliteConfig | AiosqliteConfig, *, strict: bool = False) -> dict[str, object]:
    from typing import cast

    cmds = create_migration_commands(config)
    if isinstance(cmds, AsyncMigrationCommands):
        return cast("dict[str, object]", await cmds.verify(strict=strict, echo=False))
    return cast("dict[str, object]", cmds.verify(strict=strict, echo=False))


async def _run_stamp(config: SqliteConfig | AiosqliteConfig, revision: str) -> None:
    cmds = create_migration_commands(config)
    if isinstance(cmds, AsyncMigrationCommands):
        await cmds.stamp(revision)
    else:
        cmds.stamp(revision)


async def _fetch_rows(config: SqliteConfig | AiosqliteConfig, query: str) -> list[dict[str, object]]:
    from typing import cast

    if isinstance(config, AiosqliteConfig):
        async with config.provide_session() as driver:
            rows = await driver.select(query)
        return cast("list[dict[str, object]]", rows)
    with config.provide_session() as driver:
        rows = driver.select(query)
    return cast("list[dict[str, object]]", rows)


async def _close_config_pool(config: SqliteConfig | AiosqliteConfig) -> None:
    if isinstance(config, AiosqliteConfig):
        await config.close_pool()
    else:
        config.close_pool()


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_retained_reconcile_ledger_survives_checksum_collision_verify_stamp_and_mode_switch(
    tmp_path: Path, is_async: bool, caplog: pytest.LogCaptureFixture
) -> None:
    """Retained ext_<name>_* ledger rows survive auto-sync collisions, verify(strict=True), stamp, and mode toggles."""
    db_path = tmp_path / "ledger.db"
    core_dir = tmp_path / "migrations"
    core_dir.mkdir()
    ext_dir = tmp_path / "ext_demo"
    ext_dir.mkdir()

    shared_ddl = "-- name: migrate-0001-up\nCREATE TABLE demo_or_app (id INTEGER PRIMARY KEY);\n"
    (ext_dir / "0001_init.sql").write_text(shared_ddl, encoding="utf-8")
    (ext_dir / "reconcile.py").write_text("def reconcile(context):\n    return None\n", encoding="utf-8")

    cfg_v1 = _build_sqlite_mode_config(
        db_path,
        core_dir,
        is_async=is_async,
        include_extensions=["demo"],
        extension_config={"demo": {"migrations_path": ext_dir, "migration_mode": "versioned"}},
    )
    await _run_upgrade(cfg_v1, echo=False)
    snapshot_before = await _fetch_rows(
        cfg_v1, "SELECT version_num, checksum, execution_sequence FROM ddl_migrations ORDER BY execution_sequence"
    )
    await _close_config_pool(cfg_v1)
    assert len(snapshot_before) == 1
    assert snapshot_before[0]["version_num"] == "ext_demo_0001"
    retained_checksum = snapshot_before[0]["checksum"]

    (ext_dir / "0001_init.sql").write_text(
        "-- name: migrate-0001-up\nCREATE TABLE edited_ignored (id INTEGER PRIMARY KEY);\n", encoding="utf-8"
    )

    cfg_reconcile = _build_sqlite_mode_config(
        db_path,
        core_dir,
        is_async=is_async,
        include_extensions=["demo"],
        extension_config={"demo": {"migrations_path": ext_dir, "migration_mode": "reconcile"}},
    )
    cols_before = await _fetch_rows(cfg_reconcile, "PRAGMA table_info(ddl_migrations)")
    with caplog.at_level("WARNING", logger="sqlspec.migrations.commands"):
        await _run_upgrade(cfg_reconcile, use_logger=True)
        await _run_upgrade(cfg_reconcile, use_logger=True)
    assert not any("checksum" in rec.getMessage().lower() for rec in caplog.records)
    cols_after = await _fetch_rows(cfg_reconcile, "PRAGMA table_info(ddl_migrations)")
    snapshot_after_switch = await _fetch_rows(
        cfg_reconcile,
        "SELECT version_num, checksum, execution_sequence FROM ddl_migrations ORDER BY execution_sequence",
    )
    assert cols_after == cols_before
    assert snapshot_after_switch == snapshot_before
    cmds_check = create_migration_commands(cfg_reconcile)
    current_val = await cmds_check.current() if isinstance(cmds_check, AsyncMigrationCommands) else cmds_check.current()
    assert current_val == "ext_demo_0001"

    (core_dir / "0001_app.sql").write_text(shared_ddl, encoding="utf-8")
    if isinstance(cfg_reconcile, AiosqliteConfig):
        async with cfg_reconcile.provide_session() as drv:
            await drv.execute("DROP TABLE demo_or_app")
            await drv.commit()
    else:
        with cfg_reconcile.provide_session() as drv:
            drv.execute("DROP TABLE demo_or_app")
            drv.commit()

    caplog.clear()
    with caplog.at_level("WARNING", logger="sqlspec.migrations.commands"):
        await _run_upgrade(cfg_reconcile, use_logger=True)
    assert not any("checksum" in rec.getMessage().lower() for rec in caplog.records)

    rows_after_collision = await _fetch_rows(
        cfg_reconcile, "SELECT version_num, checksum FROM ddl_migrations ORDER BY execution_sequence"
    )
    assert rows_after_collision == [
        {"version_num": "ext_demo_0001", "checksum": retained_checksum},
        {"version_num": "0001", "checksum": retained_checksum},
    ]

    report = await _run_verify(cfg_reconcile, strict=True)
    assert report == {
        "valid": True,
        "orphaned_records": [],
        "checksum_mismatches": [],
        "pending_versions": [],
        "partial_squash_errors": [],
    }

    if isinstance(cfg_reconcile, AiosqliteConfig):
        async with cfg_reconcile.provide_session() as drv:
            await AsyncMigrationCommands(cfg_reconcile).tracker.record_migration(drv, "9999", "orphan", 0, "abc")
    else:
        with cfg_reconcile.provide_session() as drv:
            SyncMigrationCommands(cfg_reconcile).tracker.record_migration(drv, "9999", "orphan", 0, "abc")

    with pytest.raises(MigrationError, match=r"orphaned applied records \[9999\]"):
        await _run_verify(cfg_reconcile, strict=True)

    await _run_stamp(cfg_reconcile, "base")
    stamped_base = await _fetch_rows(cfg_reconcile, "SELECT version_num FROM ddl_migrations")
    assert [r["version_num"] for r in stamped_base] == ["ext_demo_0001"]

    await _run_stamp(cfg_reconcile, "0001")
    stamped_rev = await _fetch_rows(cfg_reconcile, "SELECT version_num FROM ddl_migrations")
    assert {r["version_num"] for r in stamped_rev} == {"0001", "ext_demo_0001"}
    await _close_config_pool(cfg_reconcile)

    (ext_dir / "0001_init.sql").write_text(shared_ddl, encoding="utf-8")
    (ext_dir / "0002_extra.sql").write_text(
        "-- name: migrate-0002-up\nCREATE TABLE demo_extra (id INTEGER PRIMARY KEY);\n", encoding="utf-8"
    )
    cfg_back_versioned = _build_sqlite_mode_config(
        db_path,
        core_dir,
        is_async=is_async,
        include_extensions=["demo"],
        extension_config={"demo": {"migrations_path": ext_dir, "migration_mode": "versioned"}},
    )
    await _run_upgrade(cfg_back_versioned, echo=False)
    final_versions = [
        r["version_num"]
        for r in await _fetch_rows(
            cfg_back_versioned, "SELECT version_num FROM ddl_migrations ORDER BY execution_sequence"
        )
    ]
    await _close_config_pool(cfg_back_versioned)
    assert final_versions == ["ext_demo_0001", "0001", "ext_demo_0002"]


def test_plan_version_reconciliation_excludes_retained_reconcile_rows_and_ignores_unparseable_versions(
    tmp_path: Path,
) -> None:
    """_plan_version_reconciliation excludes retained reconcile rows while renaming non-reconcile orphan matches."""
    from typing import cast

    from sqlspec.migrations.base import AppliedMigrationRecord

    core_dir = tmp_path / "migrations"
    core_dir.mkdir()
    ext_dir = tmp_path / "ext_demo"
    ext_dir.mkdir()
    (ext_dir / "reconcile.py").write_text("def reconcile(context):\n    return None\n", encoding="utf-8")

    core_file = core_dir / "0001_app.sql"
    core_file.write_text("-- name: migrate-0001-up\nSELECT 1;\n", encoding="utf-8")

    reconcile_cfg = _build_sqlite_mode_config(
        tmp_path / "plan1.db",
        core_dir,
        is_async=False,
        include_extensions=["demo"],
        extension_config={"demo": {"migrations_path": ext_dir, "migration_mode": "reconcile"}},
    )
    reconcile_cmds = SyncMigrationCommands(cast("SqliteConfig", reconcile_cfg))
    loaded = {"0001": reconcile_cmds.runner.load_migration(core_file, "0001")}
    checksum = loaded["0001"]["checksum"]
    applied: list[AppliedMigrationRecord] = [
        {
            "version_num": "ext_demo_0001",
            "version_type": "sequential",
            "execution_sequence": 1,
            "description": "init",
            "applied_at": "now",
            "execution_time_ms": 0,
            "checksum": checksum,
            "applied_by": "tester",
            "replaces": None,
        },
        {
            "version_num": "custom-unparseable-tag",
            "version_type": "sequential",
            "execution_sequence": 2,
            "description": "custom",
            "applied_at": "now",
            "execution_time_ms": 0,
            "checksum": "other",
            "applied_by": "tester",
            "replaces": None,
        },
    ]

    plan_reconcile = reconcile_cmds._plan_version_reconciliation([("0001", core_file)], applied, loaded)
    assert plan_reconcile.renames == ()
    assert plan_reconcile.applied_versions == ("ext_demo_0001", "custom-unparseable-tag")

    plain_cfg = _build_sqlite_mode_config(tmp_path / "plan2.db", core_dir, is_async=False)
    plain_cmds = SyncMigrationCommands(cast("SqliteConfig", plain_cfg))
    plan_plain = plain_cmds._plan_version_reconciliation([("0001", core_file)], applied, loaded)
    assert len(plan_plain.renames) == 1
    assert plan_plain.renames[0].old_version == "ext_demo_0001"
    assert plan_plain.renames[0].new_version == "0001"


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_upgrade_head_schedules_reconcilers_before_pending_and_repeats_at_head(
    tmp_path: Path, is_async: bool
) -> None:
    """upgrade('head') runs reconcilers in include_extensions order before pending migrations and repeats at head."""
    db_path = tmp_path / "head_sched.db"
    core_dir = tmp_path / "migrations"
    core_dir.mkdir()
    second_dir = tmp_path / "ext_second"
    second_dir.mkdir()
    first_dir = tmp_path / "ext_first"
    first_dir.mkdir()
    versioned_dir = tmp_path / "ext_versioned"
    versioned_dir.mkdir()

    (core_dir / "0001_app.sql").write_text(
        "-- name: migrate-0001-up\nCREATE TABLE app_t (id INTEGER PRIMARY KEY);\n", encoding="utf-8"
    )
    (versioned_dir / "0001_v.sql").write_text(
        "-- name: migrate-0001-up\nCREATE TABLE versioned_t (id INTEGER PRIMARY KEY);\n", encoding="utf-8"
    )

    callback_template = """
async def _run_async(driver, ext_name):
    await driver.execute("CREATE TABLE IF NOT EXISTS reconcile_log (seq INTEGER PRIMARY KEY AUTOINCREMENT, ext TEXT, app_seen INTEGER)")
    rows = await driver.select("SELECT COUNT(*) AS cnt FROM sqlite_master WHERE type = 'table' AND name = 'app_t'")
    await driver.execute("INSERT INTO reconcile_log (ext, app_seen) VALUES (:ext, :seen)", ext=ext_name, seen=rows[0]["cnt"])

def _run_sync(driver, ext_name):
    driver.execute("CREATE TABLE IF NOT EXISTS reconcile_log (seq INTEGER PRIMARY KEY AUTOINCREMENT, ext TEXT, app_seen INTEGER)")
    rows = driver.select("SELECT COUNT(*) AS cnt FROM sqlite_master WHERE type = 'table' AND name = 'app_t'")
    driver.execute("INSERT INTO reconcile_log (ext, app_seen) VALUES (:ext, :seen)", ext=ext_name, seen=rows[0]["cnt"])

def reconcile(context):
    if context.is_async_driver:
        return _run_async(context.driver, {ext_name!r})
    _run_sync(context.driver, {ext_name!r})
    return None
"""
    (second_dir / "reconcile.py").write_text(callback_template.format(ext_name="second"), encoding="utf-8")
    (first_dir / "reconcile.py").write_text(callback_template.format(ext_name="first"), encoding="utf-8")

    cfg = _build_sqlite_mode_config(
        db_path,
        core_dir,
        is_async=is_async,
        include_extensions=["second", "first", "versioned_ext"],
        extension_config={
            "second": {"migrations_path": second_dir, "migration_mode": "reconcile"},
            "first": {"migrations_path": first_dir, "migration_mode": "reconcile"},
            "versioned_ext": {"migrations_path": versioned_dir, "migration_mode": "versioned"},
        },
    )

    await _run_upgrade(cfg, echo=False)
    await _run_upgrade(cfg, echo=False)

    log_rows = await _fetch_rows(cfg, "SELECT ext, app_seen FROM reconcile_log ORDER BY seq")
    ledger_rows = await _fetch_rows(cfg, "SELECT version_num FROM ddl_migrations ORDER BY execution_sequence")
    await _close_config_pool(cfg)

    assert log_rows == [
        {"ext": "second", "app_seen": 0},
        {"ext": "first", "app_seen": 0},
        {"ext": "second", "app_seen": 1},
        {"ext": "first", "app_seen": 1},
    ]
    assert [r["version_num"] for r in ledger_rows] == ["0001", "ext_versioned_ext_0001"]


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_upgrade_empty_directory_and_summary_logging_with_reconcilers(
    tmp_path: Path, is_async: bool, caplog: pytest.LogCaptureFixture
) -> None:
    """Reconciler-only apps run on empty core directories, report Already at latest version, and log reconcilers_invoked."""
    db_path = tmp_path / "empty_core.db"
    core_dir = tmp_path / "empty_migs"
    core_dir.mkdir()
    demo_dir = tmp_path / "ext_demo"
    demo_dir.mkdir()

    (demo_dir / "reconcile.py").write_text(
        """
async def _async_op(driver):
    await driver.execute("CREATE TABLE IF NOT EXISTS solo_log (id INTEGER PRIMARY KEY AUTOINCREMENT)")
    await driver.execute("INSERT INTO solo_log DEFAULT VALUES")

def reconcile(context):
    if context.is_async_driver:
        return _async_op(context.driver)
    context.driver.execute("CREATE TABLE IF NOT EXISTS solo_log (id INTEGER PRIMARY KEY AUTOINCREMENT)")
    context.driver.execute("INSERT INTO solo_log DEFAULT VALUES")
    return None
""",
        encoding="utf-8",
    )

    cfg = _build_sqlite_mode_config(
        db_path,
        core_dir,
        is_async=is_async,
        include_extensions=["demo"],
        extension_config={"demo": {"migrations_path": demo_dir, "migration_mode": "reconcile"}},
    )

    with caplog.at_level("INFO", logger="sqlspec.migrations.commands"):
        await _run_upgrade(cfg, use_logger=True, summary_only=False)
    info_messages = [rec.getMessage() for rec in caplog.records]
    assert any("Reconciler demo (mode=reconcile) complete in" in msg for msg in info_messages)
    assert any("Already at latest version" in msg for msg in info_messages)
    assert not any("No migrations found" in msg for msg in info_messages)
    assert not any("Applied" in msg for msg in info_messages)

    caplog.clear()
    with caplog.at_level("INFO", logger="sqlspec.migrations.commands"):
        await _run_upgrade(cfg, use_logger=True, summary_only=True)
    summary_records = [rec for rec in caplog.records if rec.getMessage() == "migration.command.summary"]
    assert len(summary_records) == 1
    extra = getattr(summary_records[0], "extra_fields", {})
    assert extra["reconcilers_invoked"] == 1
    assert extra["applied_count"] == 0
    assert extra["pending_count"] == 0

    solo_rows = await _fetch_rows(cfg, "SELECT COUNT(*) AS cnt FROM solo_log")
    ledger_rows = await _fetch_rows(cfg, "SELECT version_num FROM ddl_migrations")
    await _close_config_pool(cfg)
    assert solo_rows == [{"cnt": 2}]
    assert ledger_rows == []


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_upgrade_reconciler_failure_stops_subsequent_work_and_strict_ordering_aborts_before_reconcilers(
    tmp_path: Path, is_async: bool, caplog: pytest.LogCaptureFixture
) -> None:
    """Mid-batch reconciler failure stops remaining work; OutOfOrderMigrationError and dry_run skip reconcilers."""
    from sqlspec.exceptions import OutOfOrderMigrationError

    db_path = tmp_path / "fail_stop.db"
    core_dir = tmp_path / "migs"
    core_dir.mkdir()
    (core_dir / "0001_app.sql").write_text(
        "-- name: migrate-0001-up\nCREATE TABLE app_after_fail (id INTEGER PRIMARY KEY);\n", encoding="utf-8"
    )

    first_dir = tmp_path / "ext_first"
    first_dir.mkdir()
    second_dir = tmp_path / "ext_second"
    second_dir.mkdir()
    third_dir = tmp_path / "ext_third"
    third_dir.mkdir()

    ok_tpl = """
async def _async_op(driver, name):
    await driver.execute("CREATE TABLE IF NOT EXISTS batch_log (name TEXT)")
    await driver.execute("INSERT INTO batch_log VALUES (:name)", name=name)

def reconcile(context):
    if context.is_async_driver:
        return _async_op(context.driver, {name!r})
    context.driver.execute("CREATE TABLE IF NOT EXISTS batch_log (name TEXT)")
    context.driver.execute("INSERT INTO batch_log VALUES (:name)", name={name!r})
    return None
"""
    (first_dir / "reconcile.py").write_text(ok_tpl.format(name="first"), encoding="utf-8")
    (second_dir / "reconcile.py").write_text(
        'def reconcile(context):\n    raise ValueError("boom")\n', encoding="utf-8"
    )
    (third_dir / "reconcile.py").write_text(ok_tpl.format(name="third"), encoding="utf-8")

    cfg = _build_sqlite_mode_config(
        db_path,
        core_dir,
        is_async=is_async,
        include_extensions=["first", "second", "third"],
        extension_config={
            "first": {"migrations_path": first_dir, "migration_mode": "reconcile"},
            "second": {"migrations_path": second_dir, "migration_mode": "reconcile"},
            "third": {"migrations_path": third_dir, "migration_mode": "reconcile"},
        },
    )

    with caplog.at_level("ERROR", logger="sqlspec.migrations.commands"), pytest.raises(MigrationError):
        await _run_upgrade(cfg, use_logger=True, summary_only=True, echo=False)
    fail_summaries = [rec for rec in caplog.records if rec.getMessage() == "migration.command.summary"]
    assert len(fail_summaries) == 1
    fail_extra = getattr(fail_summaries[0], "extra_fields", {})
    assert fail_extra["status"] == "failed"
    assert fail_extra["reconcilers_invoked"] == 1
    assert fail_extra["applied_count"] == 0

    batch_rows = await _fetch_rows(cfg, "SELECT name FROM batch_log")
    tables = {r["name"] for r in await _fetch_rows(cfg, "SELECT name FROM sqlite_master WHERE type = 'table'")}
    ledger_rows = await _fetch_rows(cfg, "SELECT version_num FROM ddl_migrations")
    await _close_config_pool(cfg)
    assert batch_rows == [{"name": "first"}]
    assert "app_after_fail" not in tables
    assert ledger_rows == []

    order_db = tmp_path / "strict_order.db"
    order_migs = tmp_path / "order_migs"
    order_migs.mkdir()
    (order_migs / "0002_second.sql").write_text(
        "-- name: migrate-0002-up\nCREATE TABLE second_t (id INTEGER PRIMARY KEY);\n", encoding="utf-8"
    )
    cfg_init = _build_sqlite_mode_config(order_db, order_migs, is_async=is_async)
    await _run_upgrade(cfg_init, echo=False)
    await _close_config_pool(cfg_init)

    (order_migs / "0001_late.sql").write_text(
        "-- name: migrate-0001-up\nCREATE TABLE late_t (id INTEGER PRIMARY KEY);\n", encoding="utf-8"
    )
    probe_dir = tmp_path / "ext_probe"
    probe_dir.mkdir()
    (probe_dir / "reconcile.py").write_text(ok_tpl.format(name="ran_unexpectedly"), encoding="utf-8")

    cfg_strict = _build_sqlite_mode_config(
        order_db,
        order_migs,
        is_async=is_async,
        include_extensions=["probe"],
        extension_config={"probe": {"migrations_path": probe_dir, "migration_mode": "reconcile"}},
        extra_migration_config={"strict_ordering": True},
    )
    with pytest.raises(OutOfOrderMigrationError):
        await _run_upgrade(cfg_strict, echo=False)
    strict_tables = {
        r["name"] for r in await _fetch_rows(cfg_strict, "SELECT name FROM sqlite_master WHERE type = 'table'")
    }
    await _close_config_pool(cfg_strict)
    assert "batch_log" not in strict_tables

    dry_dir = tmp_path / "ext_dry"
    dry_dir.mkdir()
    (dry_dir / "reconcile.py").write_text('raise RuntimeError("dry_run_imported")\n', encoding="utf-8")
    cfg_dry = _build_sqlite_mode_config(
        tmp_path / "dry.db",
        core_dir,
        is_async=is_async,
        include_extensions=["dry"],
        extension_config={"dry": {"migrations_path": dry_dir, "migration_mode": "reconcile"}},
    )
    await _run_upgrade(cfg_dry, dry_run=True, echo=False)
    await _close_config_pool(cfg_dry)


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_upgrade_reruns_converge_after_concurrent_duplicate_error_and_missing_schema_aborts_early(
    tmp_path: Path, is_async: bool
) -> None:
    """Re-running upgrade converges after a transient duplicate-object error, and missing default_schema aborts first."""
    db_path = tmp_path / "converge.db"
    core_dir = tmp_path / "converge_migs"
    core_dir.mkdir()
    ext_dir = tmp_path / "ext_converge"
    ext_dir.mkdir()
    flag_file = tmp_path / "fail_once.flag"
    flag_file.write_text("1", encoding="utf-8")

    (ext_dir / "reconcile.py").write_text(
        f"""
import sqlite3
from pathlib import Path

async def _async_op(driver):
    await driver.execute("CREATE TABLE IF NOT EXISTS demo_t (id INTEGER PRIMARY KEY)")
    flag = Path({str(flag_file)!r})
    if flag.exists():
        flag.unlink()
        raise sqlite3.OperationalError("table demo_t already exists")

def reconcile(context):
    if context.is_async_driver:
        return _async_op(context.driver)
    context.driver.execute("CREATE TABLE IF NOT EXISTS demo_t (id INTEGER PRIMARY KEY)")
    flag = Path({str(flag_file)!r})
    if flag.exists():
        flag.unlink()
        raise sqlite3.OperationalError("table demo_t already exists")
    return None
""",
        encoding="utf-8",
    )

    cfg = _build_sqlite_mode_config(
        db_path,
        core_dir,
        is_async=is_async,
        include_extensions=["demo"],
        extension_config={"demo": {"migrations_path": ext_dir, "migration_mode": "reconcile"}},
    )

    with pytest.raises(MigrationError) as first_exc:
        await _run_upgrade(cfg, echo=False)
    assert first_exc.value.__cause__ is not None

    await _run_upgrade(cfg, echo=False)
    tables = [
        r["name"]
        for r in await _fetch_rows(cfg, "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'demo_t'")
    ]
    ledger = await _fetch_rows(cfg, "SELECT version_num FROM ddl_migrations")
    await _close_config_pool(cfg)
    assert tables == ["demo_t"]
    assert ledger == []

    never_dir = tmp_path / "ext_never"
    never_dir.mkdir()
    (never_dir / "reconcile.py").write_text('raise RuntimeError("reconciler_should_not_run")\n', encoding="utf-8")
    schema_cls = SchemaAwareAiosqliteConfig if is_async else SchemaAwareSqliteConfig
    schema_cfg: SqliteConfig | AiosqliteConfig = schema_cls(
        connection_config={"database": str(tmp_path / "missing_schema.db")},
        migration_config={
            "script_location": str(core_dir),
            "include_extensions": ["never"],
            "default_schema": "missing",
        },
        extension_config={"never": {"migrations_path": never_dir, "migration_mode": "reconcile"}},
    )
    cmds = create_migration_commands(schema_cfg)
    if isinstance(cmds, AsyncMigrationCommands):
        mock_drv = AsyncMock()
        mock_drv.has_schema.return_value = False
        with patch.object(cmds.config, "provide_session") as mock_sess, pytest.raises(MigrationError, match="missing"):
            mock_sess.return_value.__aenter__.return_value = mock_drv
            await cmds.upgrade(echo=False)
    else:
        mock_drv = MagicMock()
        mock_drv.has_schema.return_value = False
        with patch.object(cmds.config, "provide_session") as mock_sess, pytest.raises(MigrationError, match="missing"):
            mock_sess.return_value.__enter__.return_value = mock_drv
            cmds.upgrade(echo=False)


async def _run_downgrade(config: SqliteConfig | AiosqliteConfig, **kwargs: object) -> None:
    from typing import Any, cast

    cmds = create_migration_commands(config)
    if isinstance(cmds, AsyncMigrationCommands):
        await cmds.downgrade(**cast("dict[str, Any]", kwargs))
    else:
        cmds.downgrade(**cast("dict[str, Any]", kwargs))


async def _run_current(config: SqliteConfig | AiosqliteConfig, *, verbose: bool = False) -> str | None:
    cmds = create_migration_commands(config)
    if isinstance(cmds, AsyncMigrationCommands):
        return await cmds.current(verbose=verbose)
    return cmds.current(verbose=verbose)


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_historical_upgrade_and_downgrade_refused_before_session_with_reconcilers(
    tmp_path: Path, is_async: bool, caplog: pytest.LogCaptureFixture
) -> None:
    """Non-head upgrade and all downgrades fail before session opening when reconcilers are enabled."""
    import sqlite3

    fresh_db = tmp_path / "fresh_guard.db"
    core_dir = tmp_path / "migrations"
    core_dir.mkdir()
    (core_dir / "0001_app.sql").write_text(
        "-- name: migrate-0001-up\nCREATE TABLE app_t (id INTEGER PRIMARY KEY);\n\n"
        "-- name: migrate-0001-down\nDROP TABLE app_t;\n",
        encoding="utf-8",
    )
    (core_dir / "0002_extra.sql").write_text(
        "-- name: migrate-0002-up\nCREATE TABLE extra_t (id INTEGER PRIMARY KEY);\n\n"
        "-- name: migrate-0002-down\nDROP TABLE extra_t;\n",
        encoding="utf-8",
    )

    poison_dir = tmp_path / "ext_demo_poison"
    poison_dir.mkdir()
    (poison_dir / "reconcile.py").write_text('raise RuntimeError("imported")\n', encoding="utf-8")

    cfg_fresh = _build_sqlite_mode_config(
        fresh_db,
        core_dir,
        is_async=is_async,
        include_extensions=["demo"],
        extension_config={"demo": {"migrations_path": poison_dir, "migration_mode": "reconcile"}},
    )

    with (
        caplog.at_level("ERROR", logger="sqlspec.migrations.commands"),
        pytest.raises(MigrationError, match=r"demo.*head"),
    ):
        await _run_upgrade(cfg_fresh, revision="0001", use_logger=True, summary_only=True, echo=False)
    summaries = [rec for rec in caplog.records if rec.getMessage() == "migration.command.summary"]
    assert len(summaries) == 1
    assert getattr(summaries[0], "extra_fields", {})["status"] == "failed"

    with pytest.raises(MigrationError, match=r"demo.*head"):
        await _run_upgrade(cfg_fresh, revision="0001", dry_run=True, echo=False)

    if fresh_db.exists():
        with sqlite3.connect(fresh_db) as conn:
            assert conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall() == []
    await _close_config_pool(cfg_fresh)

    migrated_db = tmp_path / "migrated_guard.db"
    noop_dir = tmp_path / "ext_demo_noop"
    noop_dir.mkdir()
    (noop_dir / "reconcile.py").write_text("def reconcile(context):\n    return None\n", encoding="utf-8")
    cfg_migrated = _build_sqlite_mode_config(
        migrated_db,
        core_dir,
        is_async=is_async,
        include_extensions=["demo"],
        extension_config={"demo": {"migrations_path": noop_dir, "migration_mode": "reconcile"}},
    )
    await _run_upgrade(cfg_migrated, echo=False)

    with pytest.raises(MigrationError, match="demo"):
        await _run_downgrade(cfg_migrated, revision="-1", echo=False)
    with pytest.raises(MigrationError, match="demo"):
        await _run_downgrade(cfg_migrated, revision="-1", dry_run=True, echo=False)

    ledger = [r["version_num"] for r in await _fetch_rows(cfg_migrated, "SELECT version_num FROM ddl_migrations")]
    tables = {r["name"] for r in await _fetch_rows(cfg_migrated, "SELECT name FROM sqlite_master WHERE type = 'table'")}
    await _close_config_pool(cfg_migrated)
    assert ledger == ["0001", "0002"]
    assert "app_t" in tables

    plain_db = tmp_path / "plain_hist.db"
    cfg_plain = _build_sqlite_mode_config(plain_db, core_dir, is_async=is_async)
    await _run_upgrade(cfg_plain, revision="0001", echo=False)
    assert [r["version_num"] for r in await _fetch_rows(cfg_plain, "SELECT version_num FROM ddl_migrations")] == [
        "0001"
    ]
    await _run_downgrade(cfg_plain, revision="-1", echo=False)
    assert await _fetch_rows(cfg_plain, "SELECT version_num FROM ddl_migrations") == []
    await _close_config_pool(cfg_plain)


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_current_stamp_fix_and_squash_never_import_reconcile_module(tmp_path: Path, is_async: bool) -> None:
    """current, stamp, fix, and squash never import reconcile.py and leave reconcile-mode numbered files untouched."""
    core_dir = tmp_path / "migrations"
    core_dir.mkdir()
    (core_dir / "0001_u.sql").write_text(
        "-- name: migrate-0001-up\nCREATE TABLE u_t (id INTEGER PRIMARY KEY);\n\n"
        "-- name: migrate-0001-down\nDROP TABLE u_t;\n",
        encoding="utf-8",
    )
    (core_dir / "0002_p.sql").write_text(
        "-- name: migrate-0002-up\nCREATE TABLE p_t (id INTEGER PRIMARY KEY);\n\n"
        "-- name: migrate-0002-down\nDROP TABLE p_t;\n",
        encoding="utf-8",
    )

    ext_dir = tmp_path / "ext_demo"
    ext_dir.mkdir()
    ext_sql = "-- name: migrate-20251011120000-up\nCREATE TABLE untouched (id INTEGER PRIMARY KEY);\n"
    (ext_dir / "20251011120000_init.sql").write_text(ext_sql, encoding="utf-8")
    (ext_dir / "reconcile.py").write_text('raise RuntimeError("poisoned_reconcile_module")\n', encoding="utf-8")

    cfg = _build_sqlite_mode_config(
        tmp_path / "tools.db",
        core_dir,
        is_async=is_async,
        include_extensions=["demo"],
        extension_config={"demo": {"migrations_path": ext_dir, "migration_mode": "reconcile"}},
    )
    cmds = create_migration_commands(cfg)
    if isinstance(cmds, AsyncMigrationCommands):
        assert await cmds.current(verbose=True) is None
        await cmds.stamp("base")
        await cmds.fix(dry_run=True, yes=True)
        await cmds.squash(start_version="0001", end_version="0002", description="rel", update_database=False, yes=True)
    else:
        assert cmds.current(verbose=True) is None
        cmds.stamp("base")
        cmds.fix(dry_run=True, yes=True)
        cmds.squash(start_version="0001", end_version="0002", description="rel", update_database=False, yes=True)
    await _close_config_pool(cfg)
    assert (ext_dir / "20251011120000_init.sql").read_text(encoding="utf-8") == ext_sql


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_upgrade_dry_run_preview_is_read_only_ordered_and_propagates_catalog_errors(
    tmp_path: Path, is_async: bool, caplog: pytest.LogCaptureFixture
) -> None:
    """upgrade(dry_run=True) previews reconcilers in order without creating/mutating ddl_migrations or importing callbacks."""
    import sqlite3

    from sqlspec.migrations.fix import MigrationFixer
    from sqlspec.migrations.version import generate_conversion_map

    fresh_db = tmp_path / "preview_fresh.db"
    sqlite3.connect(fresh_db).close()
    core_dir = tmp_path / "preview_migs"
    core_dir.mkdir()
    second_dir = tmp_path / "ext_second"
    second_dir.mkdir()
    first_dir = tmp_path / "ext_first"
    first_dir.mkdir()
    second_reconcile = second_dir / "reconcile.py"
    first_reconcile = first_dir / "reconcile.py"
    second_reconcile.write_text('raise RuntimeError("imported_second")\n', encoding="utf-8")
    first_reconcile.write_text('raise RuntimeError("imported_first")\n', encoding="utf-8")

    cfg_fresh = _build_sqlite_mode_config(
        fresh_db,
        core_dir,
        is_async=is_async,
        include_extensions=["second", "first"],
        extension_config={
            "second": {"migrations_path": second_dir, "migration_mode": "reconcile"},
            "first": {"migrations_path": first_dir, "migration_mode": "reconcile"},
        },
    )

    with patch("sqlspec.migrations.commands.console") as mock_console:
        await _run_upgrade(cfg_fresh, dry_run=True, echo=True)
    printed = "\n".join(str(call.args[0]) for call in mock_console.print.call_args_list if call.args)
    assert "second" in printed
    assert str(second_reconcile) in printed
    assert "live catalog inspection" in printed
    with sqlite3.connect(fresh_db) as conn:
        assert conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall() == []

    caplog.clear()
    with caplog.at_level("INFO", logger="sqlspec.migrations.commands"):
        await _run_upgrade(cfg_fresh, dry_run=True, use_logger=True, echo=False)
    messages = [rec.getMessage() for rec in caplog.records]
    idx_second = next(i for i, m in enumerate(messages) if "1/2: second" in m)
    idx_first = next(i for i, m in enumerate(messages) if "2/2: first" in m)
    assert idx_second < idx_first
    assert any("live catalog inspection" in m for m in messages)
    await _close_config_pool(cfg_fresh)

    retained_db = tmp_path / "preview_retained.db"
    retained_ext = tmp_path / "ext_ret"
    retained_ext.mkdir()
    (retained_ext / "0001_init.sql").write_text(
        "-- name: migrate-0001-up\nCREATE TABLE ret_t (id INTEGER PRIMARY KEY);\n", encoding="utf-8"
    )
    cfg_v = _build_sqlite_mode_config(
        retained_db,
        core_dir,
        is_async=is_async,
        include_extensions=["ret"],
        extension_config={"ret": {"migrations_path": retained_ext, "migration_mode": "versioned"}},
    )
    await _run_upgrade(cfg_v, echo=False)
    snap_before = await _fetch_rows(cfg_v, "SELECT * FROM ddl_migrations")
    await _close_config_pool(cfg_v)

    (retained_ext / "reconcile.py").write_text('raise RuntimeError("imported_ret")\n', encoding="utf-8")
    cfg_r = _build_sqlite_mode_config(
        retained_db,
        core_dir,
        is_async=is_async,
        include_extensions=["ret"],
        extension_config={"ret": {"migrations_path": retained_ext, "migration_mode": "reconcile"}},
    )
    caplog.clear()
    with caplog.at_level("INFO", logger="sqlspec.migrations.commands"):
        await _run_upgrade(cfg_r, dry_run=True, use_logger=True, echo=False)
    assert any("ret" in rec.getMessage() for rec in caplog.records)
    assert await _fetch_rows(cfg_r, "SELECT * FROM ddl_migrations") == snap_before
    await _close_config_pool(cfg_r)

    fixer_db = tmp_path / "preview_fixer.db"
    fixer_migs = tmp_path / "fixer_migs"
    fixer_migs.mkdir()
    (fixer_migs / "20251011120000_users.sql").write_text(
        "-- name: migrate-20251011120000-up\nCREATE TABLE users_f (id INTEGER PRIMARY KEY);\n", encoding="utf-8"
    )
    cfg_ts = _build_sqlite_mode_config(fixer_db, fixer_migs, is_async=is_async)
    await _run_upgrade(cfg_ts, echo=False)
    await _close_config_pool(cfg_ts)

    fixer = MigrationFixer(fixer_migs)
    conv_map = generate_conversion_map([("20251011120000", fixer_migs / "20251011120000_users.sql")])
    fixer.apply_renames(fixer.plan_renames(conv_map))

    noop_ext = tmp_path / "ext_noop"
    noop_ext.mkdir()
    (noop_ext / "reconcile.py").write_text("def reconcile(context):\n    return None\n", encoding="utf-8")
    cfg_fixed = _build_sqlite_mode_config(
        fixer_db,
        fixer_migs,
        is_async=is_async,
        include_extensions=["demo"],
        extension_config={"demo": {"migrations_path": noop_ext, "migration_mode": "reconcile"}},
    )
    await _run_upgrade(cfg_fixed, dry_run=True, echo=False)
    assert [r["version_num"] for r in await _fetch_rows(cfg_fixed, "SELECT version_num FROM ddl_migrations")] == [
        "20251011120000"
    ]
    await _run_upgrade(cfg_fixed, echo=False)
    assert [r["version_num"] for r in await _fetch_rows(cfg_fixed, "SELECT version_num FROM ddl_migrations")] == [
        "0001"
    ]
    await _close_config_pool(cfg_fixed)

    plain_migs = tmp_path / "plain_dry_migs"
    plain_migs.mkdir()
    (plain_migs / "0001_app.sql").write_text(
        "-- name: migrate-0001-up\nCREATE TABLE p_app (id INTEGER PRIMARY KEY);\n", encoding="utf-8"
    )
    cfg_plain_dry = _build_sqlite_mode_config(tmp_path / "plain_dry.db", plain_migs, is_async=is_async)
    caplog.clear()
    with caplog.at_level("INFO", logger="sqlspec.migrations.commands"):
        await _run_upgrade(cfg_plain_dry, dry_run=True, use_logger=True, echo=False)
    plain_msgs = [rec.getMessage() for rec in caplog.records]
    assert any("Would apply 0001" in m for m in plain_msgs)
    assert not any("reconciler" in m.lower() for m in plain_msgs)
    await _close_config_pool(cfg_plain_dry)

    err_cmds = create_migration_commands(cfg_fresh)
    if isinstance(err_cmds, AsyncMigrationCommands):
        err_drv = AsyncMock()
        err_drv.data_dictionary.get_tables = AsyncMock(side_effect=RuntimeError("catalog-down"))
        with (
            patch.object(err_cmds.config, "provide_session") as mock_sess,
            pytest.raises(RuntimeError, match="catalog-down"),
        ):
            mock_sess.return_value.__aenter__.return_value = err_drv
            await err_cmds.upgrade(dry_run=True, echo=False)
    else:
        err_drv = MagicMock()
        err_drv.data_dictionary.get_tables.side_effect = RuntimeError("catalog-down")
        with (
            patch.object(err_cmds.config, "provide_session") as mock_sess,
            pytest.raises(RuntimeError, match="catalog-down"),
        ):
            mock_sess.return_value.__enter__.return_value = err_drv
            err_cmds.upgrade(dry_run=True, echo=False)


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_current_is_read_only_and_verbose_renders_configured_reconcilers_table(
    tmp_path: Path, is_async: bool
) -> None:
    """current() never creates ddl_migrations on fresh DBs, and current(verbose=True) renders the reconcilers table."""
    import sqlite3

    from rich.table import Table

    plain_fresh_db = tmp_path / "plain_current.db"
    sqlite3.connect(plain_fresh_db).close()
    core_dir = tmp_path / "current_migs"
    core_dir.mkdir()
    cfg_plain = _build_sqlite_mode_config(plain_fresh_db, core_dir, is_async=is_async)
    assert await _run_current(cfg_plain, verbose=False) is None
    with sqlite3.connect(plain_fresh_db) as conn:
        assert conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall() == []
    await _close_config_pool(cfg_plain)

    ext_dir = tmp_path / "ext_demo_cur"
    ext_dir.mkdir()
    (ext_dir / "reconcile.py").write_text('raise RuntimeError("imported_current")\n', encoding="utf-8")

    rec_fresh_db = tmp_path / "rec_current_fresh.db"
    sqlite3.connect(rec_fresh_db).close()
    cfg_rec_fresh = _build_sqlite_mode_config(
        rec_fresh_db,
        core_dir,
        is_async=is_async,
        include_extensions=["demo"],
        extension_config={"demo": {"migrations_path": ext_dir, "migration_mode": "reconcile"}},
    )
    with patch("sqlspec.migrations.commands.console") as mock_console:
        assert await _run_current(cfg_rec_fresh, verbose=True) is None
    printed_tables = [
        call.args[0] for call in mock_console.print.call_args_list if call.args and isinstance(call.args[0], Table)
    ]
    assert [t.title for t in printed_tables] == ["Configured reconcilers (run on every head upgrade)"]
    assert printed_tables[0].row_count == 1
    with sqlite3.connect(rec_fresh_db) as conn:
        assert conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall() == []
    await _close_config_pool(cfg_rec_fresh)

    pop_db = tmp_path / "rec_current_pop.db"
    (core_dir / "0001_app.sql").write_text(
        "-- name: migrate-0001-up\nCREATE TABLE cur_app (id INTEGER PRIMARY KEY);\n", encoding="utf-8"
    )
    cfg_seed = _build_sqlite_mode_config(pop_db, core_dir, is_async=is_async)
    await _run_upgrade(cfg_seed, echo=False)
    await _close_config_pool(cfg_seed)

    cfg_rec_pop = _build_sqlite_mode_config(
        pop_db,
        core_dir,
        is_async=is_async,
        include_extensions=["demo"],
        extension_config={"demo": {"migrations_path": ext_dir, "migration_mode": "reconcile"}},
    )
    with patch("sqlspec.migrations.commands.console") as mock_console_v:
        assert await _run_current(cfg_rec_pop, verbose=True) == "0001"
    pop_tables = [
        call.args[0] for call in mock_console_v.print.call_args_list if call.args and isinstance(call.args[0], Table)
    ]
    assert [t.title for t in pop_tables] == ["Applied Migrations", "Configured reconcilers (run on every head upgrade)"]
    assert pop_tables[0].row_count == 1
    assert pop_tables[1].row_count == 1

    with patch("sqlspec.migrations.commands.console") as mock_console_nv:
        assert await _run_current(cfg_rec_pop, verbose=False) == "0001"
    nv_tables = [
        call.args[0] for call in mock_console_nv.print.call_args_list if call.args and isinstance(call.args[0], Table)
    ]
    assert nv_tables == []
    await _close_config_pool(cfg_rec_pop)
