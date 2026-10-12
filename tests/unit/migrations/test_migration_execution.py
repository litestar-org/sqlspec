# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Unit tests for migration execution.

Tests migration execution including:
- Migration tracking table creation and management
- Upgrade and downgrade execution with mocked dependencies
- Migration state tracking and version management
- Error handling and validation scenarios
- Migration file processing
"""

import ast
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from sqlspec.builder._ddl import CreateTable
from sqlspec.driver import ExecutionResult
from sqlspec.exceptions import MigrationError
from sqlspec.migrations import MigrationLoadError
from sqlspec.migrations._backup import create_backup, restore_backup
from sqlspec.migrations.base import (
    AppliedMigrationRecord,
    BaseMigrationCommands,
    BaseMigrationTracker,
    LoadedMigrationMetadata,
)
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands
from sqlspec.migrations.context import MigrationContext
from sqlspec.migrations.fix import MigrationFixer, MigrationRename
from sqlspec.migrations.loaders import PythonFileLoader, SQLFileLoader
from sqlspec.migrations.runner import AsyncMigrationRunner, SyncMigrationRunner
from sqlspec.migrations.schema import SchemaTarget, ensure_schema_async, ensure_schema_sync
from sqlspec.migrations.squash import MigrationSquasher, SquashPlan
from sqlspec.migrations.templates import build_template_settings
from sqlspec.migrations.tracker import AsyncMigrationTracker, SyncMigrationTracker
from sqlspec.migrations.utils import create_migration_file
from sqlspec.migrations.validation import validate_squash_idempotency, validate_squash_range
from sqlspec.migrations.version import (
    MigrationVersion,
    extract_migration_version,
    generate_conversion_map,
    parse_version,
)


@pytest.fixture
def temp_workspace_with_migrations(tmp_path: Path) -> Path:
    """Create a temporary workspace with migrations directory for tests."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    return tmp_path


class MockMigrationTracker(BaseMigrationTracker):
    """Mock migration tracker for testing."""

    def __init__(self, version_table_name: str = "test_migrations") -> None:
        super().__init__(version_table_name)
        self._applied_migrations: dict[str, AppliedMigrationRecord] = {}

    def ensure_tracking_table(self, driver: Any) -> None:
        """Mock ensure tracking table."""
        pass

    def get_current_version(self, driver: Any) -> str | None:
        """Mock get current version."""
        if not self._applied_migrations:
            return None
        return max(self._applied_migrations.keys())

    def get_applied_migrations(self, driver: Any) -> list[AppliedMigrationRecord]:
        """Mock get applied migrations."""
        return list(self._applied_migrations.values())

    def record_migration(
        self, driver: Any, version: str, description: str, execution_time_ms: int, checksum: str, *, commit: bool = True
    ) -> None:
        """Mock record migration."""
        _ = commit
        self._applied_migrations[version] = cast(
            "AppliedMigrationRecord",
            {
                "version_num": version,
                "description": description,
                "execution_time_ms": execution_time_ms,
                "checksum": checksum,
            },
        )

    def remove_migration(self, driver: Any, version: str, *, commit: bool = True) -> None:
        """Mock remove migration."""
        _ = commit
        if version in self._applied_migrations:
            del self._applied_migrations[version]


class MockMigrationRunner(SyncMigrationRunner):
    """Mock migration runner for testing."""

    def __init__(self, migrations_path: Path) -> None:
        super().__init__(migrations_path)
        self._executed_migrations: list[dict[str, Any]] = []

    def get_migration_files(self) -> list[tuple[str, Path]]:
        """Mock get migration files."""
        return super().get_migration_files()

    def load_migration(self, file_path: Path, version: str | None = None) -> LoadedMigrationMetadata:
        """Mock load migration."""
        return super().load_migration(file_path, version)

    def execute_upgrade(
        self,
        driver: Any,
        migration: LoadedMigrationMetadata,
        *,
        use_transaction: bool | None = None,
        on_success: Callable[[int], None] | None = None,
    ) -> Any:
        """Mock execute upgrade."""
        _ = driver, use_transaction, on_success
        sql = self._migration_sql(migration, "up")
        if sql:
            self._executed_migrations.append({"version": migration["version"], "direction": "up", "sql": sql})
            return Mock(spec=ExecutionResult)
        raise ValueError(f"No upgrade SQL for migration {migration['version']}")

    def execute_downgrade(
        self,
        driver: Any,
        migration: LoadedMigrationMetadata,
        *,
        use_transaction: bool | None = None,
        on_success: Callable[[int], None] | None = None,
    ) -> Any:
        """Mock execute downgrade."""
        _ = driver, use_transaction, on_success
        sql = self._migration_sql(migration, "down")
        if sql:
            self._executed_migrations.append({"version": migration["version"], "direction": "down", "sql": sql})
            return Mock(spec=ExecutionResult)
        return Mock(spec=ExecutionResult)

    def load_all_migrations(self) -> Any:
        """Mock load all migrations."""
        pass

    def get_executed_migrations(self) -> list[dict[str, Any]]:
        """Get executed migrations for testing."""
        return self._executed_migrations


def test_tracking_table_sql_generation() -> None:
    """Test migration tracking table SQL generation."""
    tracker = MockMigrationTracker("test_migrations")

    create_sql = tracker._tracking_table_ddl()

    assert hasattr(create_sql, "to_statement")
    stmt = create_sql.to_statement()
    assert "CREATE TABLE" in stmt.raw_sql.upper()
    assert "test_migrations" in stmt.raw_sql
    assert "version_num" in stmt.raw_sql
    assert "description" in stmt.raw_sql
    assert "applied_at" in stmt.raw_sql
    assert "execution_time_ms" in stmt.raw_sql
    assert "checksum" in stmt.raw_sql
    assert "applied_by" in stmt.raw_sql


def test_current_version_sql_generation() -> None:
    """Test current version query SQL generation."""
    tracker = MockMigrationTracker("test_migrations")

    version_sql = tracker._current_version_query()

    assert hasattr(version_sql, "to_statement")
    stmt = version_sql.to_statement()
    assert "SELECT" in stmt.raw_sql.upper()
    assert "version_num" in stmt.raw_sql
    assert "test_migrations" in stmt.raw_sql
    assert "ORDER BY" in stmt.raw_sql.upper()
    assert "LIMIT" in stmt.raw_sql.upper()


def test_applied_migrations_sql_generation() -> None:
    """Test applied migrations query SQL generation."""
    tracker = MockMigrationTracker("test_migrations")

    applied_sql = tracker._applied_migrations_query()

    assert hasattr(applied_sql, "to_statement")
    stmt = applied_sql.to_statement()
    assert "SELECT" in stmt.raw_sql.upper()
    assert "*" in stmt.raw_sql
    assert "test_migrations" in stmt.raw_sql.lower()
    assert "ORDER BY" in stmt.raw_sql.upper()
    assert "execution_sequence" in stmt.raw_sql.lower()


def test_record_migration_sql_generation() -> None:
    """Test migration recording SQL generation."""
    tracker = MockMigrationTracker("test_migrations")

    record_sql = tracker._record_migration_statement(
        version="0001",
        version_type="sequential",
        execution_sequence=1,
        description="test migration",
        execution_time_ms=250,
        checksum="abc123",
        applied_by="test_user",
    )

    assert hasattr(record_sql, "to_statement")
    stmt = record_sql.to_statement()
    assert "INSERT INTO" in stmt.raw_sql.upper()
    assert "test_migrations" in stmt.raw_sql
    assert "VALUES" in stmt.raw_sql.upper()

    params = stmt.parameters
    assert "0001" in str(params) or "0001" in stmt.raw_sql
    assert "test migration" in str(params) or "test migration" in stmt.raw_sql


def test_remove_migration_sql_generation() -> None:
    """Test migration removal SQL generation."""
    tracker = MockMigrationTracker("test_migrations")

    remove_sql = tracker._remove_migration_statement("0001")

    assert hasattr(remove_sql, "to_statement")
    stmt = remove_sql.to_statement()
    assert "DELETE" in stmt.raw_sql.upper()
    assert "test_migrations" in stmt.raw_sql
    assert "WHERE" in stmt.raw_sql.upper()
    assert "version_num" in stmt.raw_sql


def test_single_migration_upgrade_execution(temp_workspace_with_migrations: Path) -> None:
    """Test execution of a single migration upgrade."""
    migrations_dir = temp_workspace_with_migrations / "migrations"

    migration_file = migrations_dir / "0001_create_users.sql"
    migration_content = """
-- name: migrate-0001-up
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL
);

-- name: migrate-0001-down
DROP TABLE users;
"""
    migration_file.write_text(migration_content)

    runner = MockMigrationRunner(migrations_dir)
    MockMigrationTracker()
    mock_driver = Mock()

    with (
        patch("sqlspec.migrations.runner.get_migration_loader") as mock_get_loader,
        patch.object(type(runner.loader), "clear_cache"),
        patch.object(type(runner.loader), "load_sql"),
        patch.object(type(runner.loader), "has_query", return_value=True),
    ):
        mock_loader = Mock()
        mock_loader.validate_migration_file = Mock()
        mock_get_loader.return_value = mock_loader

        migration = runner.load_migration(migration_file)

    from unittest.mock import AsyncMock

    with patch.object(migration["loader"], "get_up_sql", new_callable=AsyncMock) as mock_get_up_sql:
        mock_get_up_sql.return_value = [
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE NOT NULL);"
        ]

        result = runner.execute_upgrade(mock_driver, migration)

        assert result is not None

        executed = runner.get_executed_migrations()
        assert len(executed) == 1
        assert executed[0]["version"] == "0001"
        assert executed[0]["direction"] == "up"


def test_single_migration_downgrade_execution(temp_workspace_with_migrations: Path) -> None:
    """Test execution of a single migration downgrade."""
    from unittest.mock import AsyncMock

    migrations_dir = temp_workspace_with_migrations / "migrations"

    migration_file = migrations_dir / "0001_create_users.sql"
    migration_content = """
-- name: migrate-0001-up
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL
);

-- name: migrate-0001-down
DROP TABLE users;
"""
    migration_file.write_text(migration_content)

    runner = MockMigrationRunner(migrations_dir)
    mock_driver = Mock()

    with (
        patch("sqlspec.migrations.runner.get_migration_loader") as mock_get_loader,
        patch.object(type(runner.loader), "clear_cache"),
        patch.object(type(runner.loader), "load_sql"),
        patch.object(type(runner.loader), "has_query", return_value=True),
    ):
        mock_loader = Mock()
        mock_loader.validate_migration_file = Mock()
        mock_get_loader.return_value = mock_loader

        migration = runner.load_migration(migration_file)

    with patch.object(migration["loader"], "get_down_sql", new_callable=AsyncMock) as mock_get_down_sql:
        mock_get_down_sql.return_value = ["DROP TABLE users;"]

        result = runner.execute_downgrade(mock_driver, migration)

        assert result is not None

        executed = runner.get_executed_migrations()
        assert len(executed) == 1
        assert executed[0]["version"] == "0001"
        assert executed[0]["direction"] == "down"


def test_multiple_migrations_execution_order(temp_workspace_with_migrations: Path) -> None:
    """Test execution order of multiple migrations."""
    from unittest.mock import AsyncMock

    migrations_dir = temp_workspace_with_migrations / "migrations"

    migrations = [
        ("0003_add_indexes.sql", "CREATE INDEX idx_users_email ON users(email);", "DROP INDEX idx_users_email;"),
        (
            "0001_create_users.sql",
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);",
            "DROP TABLE users;",
        ),
        (
            "0002_add_products.sql",
            "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL);",
            "DROP TABLE products;",
        ),
    ]

    for filename, up_sql, down_sql in migrations:
        migration_file = migrations_dir / filename
        version = filename.split("_")[0]
        content = f"""
-- name: migrate-{version}-up
{up_sql}

-- name: migrate-{version}-down
{down_sql}
"""
        migration_file.write_text(content)

    runner = MockMigrationRunner(migrations_dir)
    mock_driver = Mock()

    migration_files = runner.get_migration_files()

    assert len(migration_files) == 3
    assert migration_files[0][0] == "0001"
    assert migration_files[1][0] == "0002"
    assert migration_files[2][0] == "0003"

    with patch("sqlspec.migrations.runner.get_migration_loader") as mock_get_loader:
        mock_loader = Mock()
        mock_loader.validate_migration_file = Mock()
        mock_get_loader.return_value = mock_loader

        # Use patch.object to mock methods due to __slots__
        with (
            patch.object(type(runner.loader), "clear_cache"),
            patch.object(type(runner.loader), "load_sql"),
            patch.object(type(runner.loader), "has_query", return_value=True),
        ):
            sql_statements = [
                "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);",
                "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL);",
                "CREATE INDEX idx_users_email ON users(email);",
            ]

            for i, (version, file_path) in enumerate(migration_files):
                migration = runner.load_migration(file_path)

                with patch.object(migration["loader"], "get_up_sql", new_callable=AsyncMock) as mock_get_up_sql:
                    mock_get_up_sql.return_value = [sql_statements[i]]
                    result = runner.execute_upgrade(mock_driver, migration)
                    assert result is not None

        executed = runner.get_executed_migrations()
        assert len(executed) == 3
        assert executed[0]["version"] == "0001"
        assert executed[1]["version"] == "0002"
        assert executed[2]["version"] == "0003"


def test_migration_with_no_downgrade(temp_workspace_with_migrations: Path) -> None:
    """Test migration execution when no downgrade is available."""
    migrations_dir = temp_workspace_with_migrations / "migrations"

    migration_file = migrations_dir / "0001_irreversible.sql"
    migration_content = """
-- name: migrate-0001-up
CREATE TABLE irreversible_data AS
SELECT DISTINCT column1, column2 FROM legacy_table;
"""
    migration_file.write_text(migration_content)

    runner = MockMigrationRunner(migrations_dir)
    mock_driver = Mock()
    migration = runner.load_migration(migration_file)

    assert migration["has_upgrade"] is True
    assert migration["has_downgrade"] is False

    result = runner.execute_upgrade(mock_driver, migration)
    assert result is not None

    result = runner.execute_downgrade(mock_driver, migration)
    assert result is not None


def test_migration_state_recording() -> None:
    """Test recording migration state."""
    tracker = MockMigrationTracker()
    mock_driver = Mock()

    tracker.record_migration(
        mock_driver, version="0001", description="create users table", execution_time_ms=150, checksum="abc123def456"
    )

    applied_migrations = tracker.get_applied_migrations(mock_driver)
    assert len(applied_migrations) == 1

    migration = applied_migrations[0]
    assert migration["version_num"] == "0001"
    assert migration["description"] == "create users table"
    assert migration["execution_time_ms"] == 150
    assert migration["checksum"] == "abc123def456"


def test_current_version_tracking() -> None:
    """Test current version tracking."""
    tracker = MockMigrationTracker()
    mock_driver = Mock()

    assert tracker.get_current_version(mock_driver) is None

    migrations = [
        ("0001", "initial schema", 100, "hash1"),
        ("0002", "add users", 150, "hash2"),
        ("0003", "add indexes", 75, "hash3"),
    ]

    for version, desc, time_ms, checksum in migrations:
        tracker.record_migration(mock_driver, version, desc, time_ms, checksum)

    current = tracker.get_current_version(mock_driver)
    assert current == "0003"


def test_migration_removal() -> None:
    """Test migration removal from tracking."""
    tracker = MockMigrationTracker()
    mock_driver = Mock()

    tracker.record_migration(mock_driver, "0001", "first", 100, "hash1")
    tracker.record_migration(mock_driver, "0002", "second", 150, "hash2")
    tracker.record_migration(mock_driver, "0003", "third", 75, "hash3")

    assert len(tracker.get_applied_migrations(mock_driver)) == 3
    assert tracker.get_current_version(mock_driver) == "0003"

    tracker.remove_migration(mock_driver, "0003")

    assert len(tracker.get_applied_migrations(mock_driver)) == 2
    assert tracker.get_current_version(mock_driver) == "0002"

    tracker.remove_migration(mock_driver, "0001")

    migrations = tracker.get_applied_migrations(mock_driver)
    assert len(migrations) == 1
    assert migrations[0]["version_num"] == "0002"


def test_applied_migrations_ordering() -> None:
    """Test that applied migrations are returned in correct order."""
    tracker = MockMigrationTracker()
    mock_driver = Mock()

    migrations_data = [("0003", "third migration"), ("0001", "first migration"), ("0002", "second migration")]

    for version, desc in migrations_data:
        tracker.record_migration(mock_driver, version, desc, 100, f"hash_{version}")

    applied = tracker.get_applied_migrations(mock_driver)

    assert len(applied) == 3

    versions = [m["version_num"] for m in applied]
    assert "0001" in versions
    assert "0002" in versions
    assert "0003" in versions


def test_migration_execution_failure(temp_workspace_with_migrations: Path) -> None:
    """Test handling of migration execution failures."""
    migrations_dir = temp_workspace_with_migrations / "migrations"

    migration_file = migrations_dir / "0001_broken.sql"
    migration_content = """
-- name: migrate-0001-up
INVALID SQL STATEMENT THAT SHOULD FAIL;

-- name: migrate-0001-down
DROP TABLE IF EXISTS nonexistent_table;
"""
    migration_file.write_text(migration_content)

    runner = MockMigrationRunner(migrations_dir)
    mock_driver = Mock()

    with patch("sqlspec.migrations.runner.get_migration_loader") as mock_get_loader:
        mock_loader = Mock()
        mock_loader.validate_migration_file = Mock()
        mock_get_loader.return_value = mock_loader

        # Use patch on class methods since __slots__ prevents instance attribute assignment
        with (
            patch.object(type(runner.loader), "clear_cache"),
            patch.object(type(runner.loader), "load_sql"),
            patch.object(type(runner.loader), "has_query", return_value=True),
        ):
            migration = runner.load_migration(migration_file)

    async def mock_get_up_error(_file_path: Path) -> None:
        raise Exception("SQL syntax error")

    with patch.object(migration["loader"], "get_up_sql", new=mock_get_up_error):
        with pytest.raises(ValueError) as exc_info:
            runner.execute_upgrade(mock_driver, migration)

        assert "Failed to load upgrade for migration 0001" in str(exc_info.value)


def test_missing_upgrade_migration(temp_workspace_with_migrations: Path) -> None:
    """Test handling of missing upgrade migrations."""
    migrations_dir = temp_workspace_with_migrations / "migrations"

    migration_file = migrations_dir / "0001_downgrade_only.sql"
    migration_content = """
-- name: migrate-0001-down
DROP TABLE legacy_table;
"""
    migration_file.write_text(migration_content)

    runner = MockMigrationRunner(migrations_dir)

    with pytest.raises(MigrationLoadError, match="missing required 'up' query"):
        runner.load_migration(migration_file)


def test_corrupted_migration_file(temp_workspace_with_migrations: Path) -> None:
    """Test handling of corrupted migration files."""
    migrations_dir = temp_workspace_with_migrations / "migrations"

    migration_file = migrations_dir / "0001_corrupted.sql"
    migration_content = """
This is not a valid migration file format.
It has no proper named query structure.
-- name: incomplete
SELECT * FROM
"""
    migration_file.write_text(migration_content)

    runner = MockMigrationRunner(migrations_dir)

    with patch("sqlspec.migrations.runner.get_migration_loader") as mock_get_loader:
        mock_loader = Mock()
        mock_loader.validate_migration_file.side_effect = Exception("File validation failed")
        mock_get_loader.return_value = mock_loader

        with pytest.raises(Exception):
            runner.load_migration(migration_file)


def test_duplicate_version_detection(temp_workspace_with_migrations: Path) -> None:
    """Test detection of duplicate migration versions."""
    migrations_dir = temp_workspace_with_migrations / "migrations"

    file1 = migrations_dir / "0001_first.sql"
    file1.write_text("""
-- name: migrate-0001-up
CREATE TABLE first (id INTEGER);
""")

    file2 = migrations_dir / "0001_second.sql"
    file2.write_text("""
-- name: migrate-0001-up
CREATE TABLE second (id INTEGER);
""")

    runner = MockMigrationRunner(migrations_dir)

    files = runner.get_migration_files()

    versions = [version for version, _ in files]
    assert versions.count("0001") == 2


def test_upgrade_records_migration_returning_no_statements(temp_workspace_with_migrations: Path) -> None:
    """A migration whose up() returns no statements is recorded and never re-applied."""
    from sqlspec.adapters.sqlite.config import SqliteConfig
    from sqlspec.migrations.commands import SyncMigrationCommands

    migrations_dir = temp_workspace_with_migrations / "migrations"
    (migrations_dir / "0001_create_table.py").write_text(
        '''"""Create the example table."""


def up() -> list[str]:
    return ["CREATE TABLE example (id INTEGER)"]


def down() -> list[str]:
    return ["DROP TABLE example"]
'''
    )
    (migrations_dir / "0002_conditional_noop.py").write_text(
        '''"""Conditional migration with nothing to do."""


def up() -> list[str]:
    return []


def down() -> list[str]:
    return []
'''
    )

    config = SqliteConfig(
        connection_config={"database": ":memory:"}, migration_config={"script_location": str(migrations_dir)}
    )
    commands = SyncMigrationCommands(config)
    commands.tracker = MockMigrationTracker()

    commands.upgrade()

    with config.provide_session() as driver:
        applied = {record["version_num"] for record in commands.tracker.get_applied_migrations(driver)}
    assert applied == {"0001", "0002"}

    commands.upgrade()

    with config.provide_session() as driver:
        all_migrations = commands.runner.get_migration_files()
        applied_after = {record["version_num"] for record in commands.tracker.get_applied_migrations(driver)}
        pending = commands._collect_pending_migrations(all_migrations, applied_after, "head")
    assert applied_after == {"0001", "0002"}
    assert pending == []

    commands.downgrade("0001")

    with config.provide_session() as driver:
        remaining = {record["version_num"] for record in commands.tracker.get_applied_migrations(driver)}
    assert remaining == {"0001"}


def test_squash_update_database_reads_target_file_and_retains_backup_until_commit(tmp_path: Path) -> None:
    """Verify squash(update_database=True) does not fail with FileNotFoundError and rolls back on DB error."""
    f1 = tmp_path / "0001_first.sql"
    f1.write_text("-- name: migrate-0001-up\nSELECT 1;\n-- name: migrate-0001-down\nSELECT 1;\n", encoding="utf-8")
    f2 = tmp_path / "0002_second.sql"
    f2.write_text("-- name: migrate-0002-up\nSELECT 2;\n-- name: migrate-0002-down\nSELECT 2;\n", encoding="utf-8")

    config = MagicMock()
    config.migration_config = {"script_location": str(tmp_path)}
    config.extension_config = {}
    config.get_observability_runtime.return_value = None
    config.migration_tracker_type = SyncMigrationTracker

    driver = MagicMock()
    driver.driver_features = {"autocommit": False}
    config.provide_session.return_value.__enter__.return_value = driver

    commands = SyncMigrationCommands(config)
    commands.tracker.ensure_tracking_table = MagicMock()
    commands.tracker.is_squash_already_applied = MagicMock(return_value=True)
    commands.tracker.replace_with_squash = MagicMock()

    commands.squash("0001", "0002", "release", yes=True, update_database=True)
    commands.tracker.replace_with_squash.assert_called_once()
    assert (tmp_path / "0001_release.sql").exists()


def test_stamp_records_all_ancestor_migrations_and_supports_base(tmp_path: Path) -> None:
    """Verify stamp(revision) records all ancestor migrations up to revision and supports 'base'."""
    f1 = tmp_path / "0001_first.sql"
    f1.write_text("-- name: migrate-0001-up\nSELECT 1;\n", encoding="utf-8")
    f2 = tmp_path / "0002_second.sql"
    f2.write_text("-- name: migrate-0002-up\nSELECT 2;\n", encoding="utf-8")
    f3 = tmp_path / "0003_third.sql"
    f3.write_text("-- name: migrate-0003-up\nSELECT 3;\n", encoding="utf-8")

    config = MagicMock()
    config.migration_config = {"script_location": str(tmp_path)}
    config.extension_config = {}
    config.get_observability_runtime.return_value = None
    config.migration_tracker_type = SyncMigrationTracker

    driver = MagicMock()
    driver.driver_features = {"autocommit": False}
    config.provide_session.return_value.__enter__.return_value = driver

    commands = SyncMigrationCommands(config)
    commands.tracker.ensure_tracking_table = MagicMock()
    commands.tracker.record_migration = MagicMock()

    commands.stamp("0002")
    recorded_versions = [call.args[1] for call in commands.tracker.record_migration.call_args_list]
    assert recorded_versions == ["0001", "0002"]

    driver.execute.reset_mock()
    commands.stamp("base")
    assert driver.execute.called


def test_calculate_checksum_strips_version_headers_across_fix(tmp_path: Path) -> None:
    """Verify calculate_checksum produces identical checksum before and after fix rewrites -- Version:."""
    settings = build_template_settings({})
    mig_file = create_migration_file(tmp_path, "20251011120000", "init_schema", "sql", template_settings=settings)
    runner = SyncMigrationRunner(tmp_path)
    before_checksum = runner.calculate_checksum(mig_file.read_text(encoding="utf-8"))

    fixer = MigrationFixer(tmp_path)
    fixer.update_file_content(mig_file, "20251011120000", "0001")
    after_checksum = runner.calculate_checksum(mig_file.read_text(encoding="utf-8"))
    assert before_checksum == after_checksum


def test_migration_version_equality_and_ordering_antisymmetry() -> None:
    """Verify parse_version('1') and parse_version('0001') are equal and preserve antisymmetry."""
    v1 = parse_version("1")
    v2 = parse_version("0001")
    assert v1 == v2
    assert hash(v1) == hash(v2)
    assert not (v1 < v2)
    assert not (v1 > v2)
    assert v1 <= v2
    assert v1 >= v2


def test_downgrade_aborts_when_migration_lacks_downgrade(tmp_path: Path) -> None:
    """Verify downgrade aborts and does not remove tracking record when migration has no down SQL."""
    f1 = tmp_path / "0001_first.sql"
    f1.write_text("-- name: migrate-0001-up\nCREATE TABLE t1 (id INT);\n-- name: migrate-0001-down\nDROP TABLE t1;\n")
    f2 = tmp_path / "0002_second.sql"
    f2.write_text("-- name: migrate-0002-up\nCREATE TABLE t2 (id INT);\n")

    config = MagicMock()
    config.migration_config = {"script_location": str(tmp_path)}
    config.extension_config = {}
    config.get_observability_runtime.return_value = None
    config.migration_tracker_type = SyncMigrationTracker

    driver = MagicMock()
    driver.driver_features = {"autocommit": False}
    config.provide_session.return_value.__enter__.return_value = driver

    commands = SyncMigrationCommands(config)
    commands.tracker.ensure_tracking_table = MagicMock()
    commands.tracker.get_applied_migrations = MagicMock(
        return_value=[{"version_num": "0001", "description": "first"}, {"version_num": "0002", "description": "second"}]
    )
    commands.tracker.remove_migration = MagicMock()

    commands.downgrade(revision="base", echo=False)
    commands.tracker.remove_migration.assert_not_called()


def test_execute_migration_sql_resets_session_schema_before_on_success(tmp_path: Path) -> None:
    """Verify _execute_migration_sql resets session schema before invoking on_success."""
    config = MagicMock()
    config.supports_migration_schemas = True
    runner = SyncMigrationRunner(tmp_path, context=MigrationContext(config=config))
    driver = MagicMock()
    driver.has_schema.return_value = True
    call_order: list[str] = []
    driver.set_migration_session_schema.side_effect = lambda s: call_order.append(f"set:{s}")
    driver.reset_migration_session_schema.side_effect = lambda: call_order.append("reset")

    migration: Any = {"version": "0001", "schema": "custom_schema"}
    runner._execute_migration_sql(
        driver,
        ["CREATE TABLE t (id INT)"],
        migration=migration,
        use_transaction=True,
        on_success=lambda _: call_order.append("on_success"),
        start_time=0.0,
    )
    assert call_order.index("reset") < call_order.index("on_success")


def test_upgrade_failure_logs_failed_summary_and_records_partial_metrics(tmp_path: Path) -> None:
    """Verify upgrade logs status='failed' when a migration fails inside _apply_single_migration."""
    f1 = tmp_path / "0001_first.sql"
    f1.write_text("-- name: migrate-0001-up\nSELECT 1;\n", encoding="utf-8")

    config = MagicMock()
    config.migration_config = {"script_location": str(tmp_path)}
    config.extension_config = {}
    config.get_observability_runtime.return_value = None
    config.migration_tracker_type = SyncMigrationTracker

    driver = MagicMock()
    driver.driver_features = {"autocommit": False}
    config.provide_session.return_value.__enter__.return_value = driver

    commands = SyncMigrationCommands(config)
    commands.tracker.ensure_tracking_table = MagicMock()
    commands.tracker.get_applied_migrations = MagicMock(return_value=[])
    commands.runner.execute_upgrade = MagicMock(side_effect=RuntimeError("boom"))

    with patch("sqlspec.migrations.commands._log_command_summary") as mock_summary:
        commands.upgrade(echo=False)
        assert mock_summary.call_args.kwargs["status"] == "failed"
        assert isinstance(mock_summary.call_args.kwargs["error"], RuntimeError)


def test_synchronize_version_records_reconciles_with_pending_timestamp_present(tmp_path: Path) -> None:
    """Verify _synchronize_version_records reconciles fixed migrations even when another timestamp exists."""
    f1 = tmp_path / "0001_init.sql"
    f1.write_text("-- name: migrate-0001-up\nCREATE TABLE t1 (id INT);\n", encoding="utf-8")
    f2 = tmp_path / "20251011150000_new.sql"
    f2.write_text("-- name: migrate-20251011150000-up\nCREATE TABLE t2 (id INT);\n", encoding="utf-8")

    config = MagicMock()
    config.migration_config = {"script_location": str(tmp_path)}
    config.extension_config = {}
    config.get_observability_runtime.return_value = None
    config.migration_tracker_type = SyncMigrationTracker

    commands = SyncMigrationCommands(config)
    checksum_0001 = commands.runner.calculate_checksum(f1.read_text(encoding="utf-8"))
    commands.tracker.get_applied_migrations = MagicMock(
        return_value=[{"version_num": "20251011120000", "checksum": checksum_0001}]
    )
    commands.tracker.update_version_record = MagicMock()

    updated = commands._synchronize_version_records(MagicMock(), echo=False)
    assert updated == 1
    commands.tracker.update_version_record.assert_called_once()


def test_squash_when_target_filename_matches_first_source_file(tmp_path: Path) -> None:
    """Verify apply_squash and validate_squash_idempotency succeed when target_path == source_migrations[0]."""
    f1 = tmp_path / "0001_initial.sql"
    f1.write_text("-- name: migrate-0001-up\nSELECT 1;\n", encoding="utf-8")
    f2 = tmp_path / "0002_users.sql"
    f2.write_text("-- name: migrate-0002-up\nSELECT 2;\n", encoding="utf-8")

    assert validate_squash_idempotency([f1, f2], f1) == "ready"

    runner = SyncMigrationRunner(tmp_path)
    squasher = MigrationSquasher(tmp_path, runner)
    plans = squasher.plan_squash("0001", "0002", "initial")
    squasher.apply_squash(plans)

    assert f1.exists()
    assert not f2.exists()


def test_downgrade_to_core_revision_does_not_revert_extension_migrations(tmp_path: Path) -> None:
    """Verify _collect_revert_migrations('0001') ignores applied ext_* migrations."""
    config = MagicMock()
    config.migration_config = {"script_location": str(tmp_path)}
    config.extension_config = {}
    config.get_observability_runtime.return_value = None
    config.migration_tracker_type = SyncMigrationTracker

    commands = SyncMigrationCommands(config)
    applied: list[Any] = [{"version_num": "0001"}, {"version_num": "0002"}, {"version_num": "ext_adk_0001"}]
    to_revert = commands._collect_revert_migrations(applied, "0001")
    assert [m["version_num"] for m in to_revert] == ["0002"]


@pytest.mark.anyio
async def test_python_squash_with_none_downgrade_loads_cleanly(tmp_path: Path) -> None:
    """Verify generate_python_squash with down_sql=[] emits valid Python that PythonFileLoader loads."""
    runner = SyncMigrationRunner(tmp_path)
    squasher = MigrationSquasher(tmp_path, runner)
    target = tmp_path / "0001_squash.py"
    plan = SquashPlan(
        source_migrations=[("0001", tmp_path / "0001_a.sql")],
        target_version="0001",
        target_path=target,
        description="squash",
        source_versions=["0001"],
    )
    content = squasher.generate_python_squash(plan, up_sql=["SELECT 1;"], down_sql=[])
    target.write_text(content, encoding="utf-8")
    ast.parse(content)

    loader = PythonFileLoader(tmp_path)
    assert await loader.get_up_sql(target) == ["SELECT 1;"]
    assert await loader.get_down_sql(target) is None
    assert loader.has_downgrade(target) is False


def test_load_migration_does_not_execute_python_down_function(tmp_path: Path) -> None:
    """Verify SyncMigrationRunner.load_migration inspects has_downgrade without invoking down()."""
    py_file = tmp_path / "0001_side_effect.py"
    py_file.write_text(
        "def up():\n    return 'SELECT 1'\ndef down():\n    raise RuntimeError('down should not be called during load_migration')\n",
        encoding="utf-8",
    )
    runner = SyncMigrationRunner(tmp_path)
    metadata = runner.load_migration(py_file, "0001")
    assert metadata["has_downgrade"] is True


def test_is_squash_already_applied_raises_on_partial_overlap() -> None:
    """Verify SyncMigrationTracker.is_squash_already_applied raises MigrationError on partial overlap."""
    tracker = SyncMigrationTracker()
    driver = MagicMock()
    res = MagicMock()
    res.data = [{"version_num": "0001"}]
    res.get_data.return_value = res.data
    driver.execute.return_value = res

    with pytest.raises(MigrationError, match="Partial squash state detected"):
        tracker.is_squash_already_applied(driver, "0001", ["0001", "0002"])


@pytest.mark.anyio
async def test_ensure_schema_respects_autocommit() -> None:
    """Verify ensure_schema_sync and ensure_schema_async skip commit when autocommit=True."""
    ddl = CreateTable("test_tbl").column("id", "INTEGER")
    target = SchemaTarget("test_tbl", ddl)

    sync_driver = MagicMock()
    sync_driver.driver_features = {"autocommit": True}
    sync_driver.data_dictionary.get_tables.return_value = []
    ensure_schema_sync(sync_driver, [target], manage_schema=True, create_schema=True)
    sync_driver.commit.assert_not_called()

    async_driver = AsyncMock()
    async_driver.driver_features = {"autocommit": True}
    async_driver.data_dictionary.get_tables.return_value = []
    await ensure_schema_async(async_driver, [target], manage_schema=True, create_schema=True)
    async_driver.commit.assert_not_called()


def test_squash_preserves_transactional_and_schema_directives_and_extension_versions(tmp_path: Path) -> None:
    """Verify MigrationSquasher groups by transactional/schema directives and supports ext_* versions."""
    f1 = tmp_path / "ext_adk_0001_a.sql"
    f1.write_text("-- transactional: false\n-- schema: custom\n-- name: migrate-ext_adk_0001-up\nSELECT 1;\n")
    f2 = tmp_path / "ext_adk_0002_b.sql"
    f2.write_text("-- transactional: false\n-- schema: custom\n-- name: migrate-ext_adk_0002-up\nSELECT 2;\n")

    migrations = [("ext_adk_0001", f1), ("ext_adk_0002", f2)]
    in_range = validate_squash_range(migrations, "ext_adk_0001", "ext_adk_0002")
    assert len(in_range) == 2

    runner = SyncMigrationRunner(tmp_path)
    squasher = MigrationSquasher(tmp_path, runner)
    plans = squasher.plan_squash("ext_adk_0001", "ext_adk_0002", "release")
    assert len(plans) == 1
    content = squasher.generate_squashed_content(plans[0], ["SELECT 1;"], [])
    assert "-- transactional: false" in content
    assert "-- schema: custom" in content


def test_load_all_migrations_joins_multi_statement_python_migrations(tmp_path: Path) -> None:
    """Verify load_all_migrations joins multi-statement Python migrations instead of truncating to up_sql[0]."""
    py_file = tmp_path / "0001_multi.py"
    py_file.write_text("def up():\n    return ['SELECT 1', 'SELECT 2']\n", encoding="utf-8")
    runner = SyncMigrationRunner(tmp_path)
    all_queries = runner.load_all_migrations()
    assert "SELECT 2" in all_queries["migrate-0001-up"].sql


def test_extract_migration_version_zero_pads_extension_versions() -> None:
    """Verify extract_migration_version zero-pads sequential extension versions."""
    assert extract_migration_version("ext_adk_1_init") == "ext_adk_0001"
    assert extract_migration_version("1_init") == "0001"


@pytest.mark.anyio
async def test_python_loader_accepts_context_when_none_and_handles_keyword_only(tmp_path: Path) -> None:
    """Verify PythonFileLoader passes None to required positional context and skips keyword-only defaults."""
    f1 = tmp_path / "0001_ctx.py"
    f1.write_text(
        "def up(context):\n    return 'SELECT 1' if context is None else 'SELECT 2'\n"
        "def down(*, dry_run: bool = False):\n    return 'DROP TABLE t'\n",
        encoding="utf-8",
    )
    loader = PythonFileLoader(tmp_path, context=None)
    assert await loader.get_up_sql(f1) == ["SELECT 1"]
    assert await loader.get_down_sql(f1) == ["DROP TABLE t"]


def test_downgrade_records_reverted_metric(tmp_path: Path) -> None:
    """Verify downgrade records the 'reverted' metric."""
    f1 = tmp_path / "0001_first.sql"
    f1.write_text("-- name: migrate-0001-up\nSELECT 1;\n-- name: migrate-0001-down\nSELECT 1;\n", encoding="utf-8")

    config = MagicMock()
    config.migration_config = {"script_location": str(tmp_path)}
    config.extension_config = {}
    config.get_observability_runtime.return_value = None
    config.migration_tracker_type = SyncMigrationTracker

    driver = MagicMock()
    driver.driver_features = {"autocommit": False}
    config.provide_session.return_value.__enter__.return_value = driver

    commands = SyncMigrationCommands(config)
    commands.tracker.ensure_tracking_table = MagicMock()
    commands.tracker.get_applied_migrations = MagicMock(return_value=[{"version_num": "0001", "description": "first"}])
    commands.tracker.remove_migration = MagicMock()
    commands._record_command_metric = MagicMock()

    commands.downgrade(revision="-1", echo=False)
    commands._record_command_metric.assert_called_once_with("reverted", 1.0)


def test_load_metadata_cache_hit_respects_explicit_version(tmp_path: Path) -> None:
    """Verify BaseMigrationRunner._load_metadata updates version on cache hit when explicit version is passed."""
    f1 = tmp_path / "0001_init.sql"
    f1.write_text("-- name: migrate-0001-up\nSELECT 1;\n", encoding="utf-8")
    runner = SyncMigrationRunner(tmp_path)
    meta1 = runner._load_metadata(f1, None)
    assert meta1["version"] == "0001"
    meta2 = runner._load_metadata(f1, "ext_adk_0001")
    assert meta2["version"] == "ext_adk_0001"


def test_fixer_rollback_deletes_backup_and_create_backup_avoids_same_second_collision(tmp_path: Path) -> None:
    """Verify MigrationFixer.rollback deletes backup and create_backup handles multiple calls in same second."""
    mig_dir = tmp_path / "migrations"
    mig_dir.mkdir()
    (mig_dir / "0001_init.sql").write_text("SELECT 1;\n", encoding="utf-8")

    b1 = create_backup(mig_dir)
    b2 = create_backup(mig_dir)
    assert b1 != b2
    restore_backup(mig_dir, b1, delete_backup=True)
    restore_backup(mig_dir, b2, delete_backup=True)

    fixer = MigrationFixer(mig_dir)
    backup_path = fixer.create_backup()
    assert backup_path.exists()
    fixer.rollback()
    assert not backup_path.exists()
    assert fixer.backup_path is None


def test_dataclasses_and_exports_hygiene() -> None:
    """Verify MigrationVersion and MigrationRename use slots."""
    assert hasattr(MigrationVersion, "__slots__")
    assert hasattr(MigrationRename, "__slots__")
    _ = (
        AsyncMigrationCommands,
        AsyncMigrationRunner,
        AsyncMigrationTracker,
        BaseMigrationCommands,
        BaseMigrationTracker,
        MigrationContext,
        SQLFileLoader,
        generate_conversion_map,
    )
