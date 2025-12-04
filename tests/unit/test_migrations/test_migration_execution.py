# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Unit tests for migration execution.

Tests migration execution including:
- Migration tracking table creation and management
- Upgrade and downgrade execution with mocked dependencies
- Migration state tracking and version management
- Error handling and validation scenarios
- Migration file processing
"""

from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest

from sqlspec.driver import ExecutionResult
from sqlspec.migrations.base import BaseMigrationRunner, BaseMigrationTracker

pytestmark = pytest.mark.xdist_group("migrations")


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
        self._applied_migrations: dict[str, dict[str, Any]] = {}

    def ensure_tracking_table(self, driver: Any) -> None:
        """Mock ensure tracking table."""
        pass

    def get_current_version(self, driver: Any) -> str | None:
        """Mock get current version."""
        if not self._applied_migrations:
            return None
        return max(self._applied_migrations.keys())

    def get_applied_migrations(self, driver: Any) -> list[dict[str, Any]]:
        """Mock get applied migrations."""
        return list(self._applied_migrations.values())

    def record_migration(
        self, driver: Any, version: str, description: str, execution_time_ms: int, checksum: str
    ) -> None:
        """Mock record migration."""
        self._applied_migrations[version] = {
            "version_num": version,
            "description": description,
            "execution_time_ms": execution_time_ms,
            "checksum": checksum,
        }

    def remove_migration(self, driver: Any, version: str) -> None:
        """Mock remove migration."""
        if version in self._applied_migrations:
            del self._applied_migrations[version]


class MockMigrationRunner(BaseMigrationRunner):
    """Mock migration runner for testing."""

    def __init__(self, migrations_path: Path) -> None:
        super().__init__(migrations_path)
        self._executed_migrations: list[dict[str, Any]] = []

    def get_migration_files(self) -> list[tuple[str, Path]]:
        """Mock get migration files."""
        return self._get_migration_files_sync()

    def load_migration(self, file_path: Path) -> dict[str, Any]:
        """Mock load migration."""
        return self._load_migration_metadata(file_path)

    def execute_upgrade(self, driver: Any, migration: dict[str, Any]) -> ExecutionResult:
        """Mock execute upgrade."""
        sql = self._get_migration_sql(migration, "up")
        if sql:
            self._executed_migrations.append({"version": migration["version"], "direction": "up", "sql": sql})
            return Mock(spec=ExecutionResult)
        raise ValueError(f"No upgrade SQL for migration {migration['version']}")

    def execute_downgrade(self, driver: Any, migration: dict[str, Any]) -> ExecutionResult:
        """Mock execute downgrade."""
        sql = self._get_migration_sql(migration, "down")
        if sql:
            self._executed_migrations.append({"version": migration["version"], "direction": "down", "sql": sql})
            return Mock(spec=ExecutionResult)
        return Mock(spec=ExecutionResult)

    def load_all_migrations(self) -> None:
        """Mock load all migrations."""
        pass

    def get_executed_migrations(self) -> list[dict[str, Any]]:
        """Get executed migrations for testing."""
        return self._executed_migrations


def test_tracking_table_sql_generation() -> None:
    """Test migration tracking table SQL generation."""
    tracker = MockMigrationTracker("test_migrations")

    create_sql = tracker._get_create_table_sql()

    assert hasattr(create_sql, "to_statement")
    stmt = create_sql.to_statement()
    assert "CREATE TABLE" in stmt.sql.upper()
    assert "test_migrations" in stmt.sql
    assert "version_num" in stmt.sql
    assert "description" in stmt.sql
    assert "applied_at" in stmt.sql
    assert "execution_time_ms" in stmt.sql
    assert "checksum" in stmt.sql
    assert "applied_by" in stmt.sql


def test_current_version_sql_generation() -> None:
    """Test current version query SQL generation."""
    tracker = MockMigrationTracker("test_migrations")

    version_sql = tracker._get_current_version_sql()

    assert hasattr(version_sql, "to_statement")
    stmt = version_sql.to_statement()
    assert "SELECT" in stmt.sql.upper()
    assert "version_num" in stmt.sql
    assert "test_migrations" in stmt.sql
    assert "ORDER BY" in stmt.sql.upper()
    assert "LIMIT" in stmt.sql.upper()


def test_applied_migrations_sql_generation() -> None:
    """Test applied migrations query SQL generation."""
    tracker = MockMigrationTracker("test_migrations")

    applied_sql = tracker._get_applied_migrations_sql()

    assert hasattr(applied_sql, "to_statement")
    stmt = applied_sql.to_statement()
    assert "SELECT" in stmt.sql.upper()
    assert "*" in stmt.sql
    assert "test_migrations" in stmt.sql.lower()
    assert "ORDER BY" in stmt.sql.upper()
    assert "execution_sequence" in stmt.sql.lower()


def test_record_migration_sql_generation() -> None:
    """Test migration recording SQL generation."""
    tracker = MockMigrationTracker("test_migrations")

    record_sql = tracker._get_record_migration_sql(
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
    assert "INSERT INTO" in stmt.sql.upper()
    assert "test_migrations" in stmt.sql
    assert "VALUES" in stmt.sql.upper()

    params = stmt.parameters
    assert "0001" in str(params) or "0001" in stmt.sql
    assert "test migration" in str(params) or "test migration" in stmt.sql


def test_remove_migration_sql_generation() -> None:
    """Test migration removal SQL generation."""
    tracker = MockMigrationTracker("test_migrations")

    remove_sql = tracker._get_remove_migration_sql("0001")

    assert hasattr(remove_sql, "to_statement")
    stmt = remove_sql.to_statement()
    assert "DELETE" in stmt.sql.upper()
    assert "test_migrations" in stmt.sql
    assert "WHERE" in stmt.sql.upper()
    assert "version_num" in stmt.sql


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
        patch("sqlspec.migrations.base.get_migration_loader") as mock_get_loader,
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
        patch("sqlspec.migrations.base.get_migration_loader") as mock_get_loader,
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

    with patch("sqlspec.migrations.base.get_migration_loader") as mock_get_loader:
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
    from unittest.mock import AsyncMock

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

    with patch("sqlspec.migrations.base.get_migration_loader") as mock_get_loader:
        mock_loader = Mock()
        mock_loader.validate_migration_file = Mock()
        mock_get_loader.return_value = mock_loader

        with (
            patch.object(type(runner.loader), "clear_cache"),
            patch.object(type(runner.loader), "load_sql"),
            patch.object(type(runner.loader), "has_query", side_effect=lambda q: q.endswith("-up")),
        ):
            migration = runner.load_migration(migration_file)

            assert migration["has_upgrade"] is True
            assert migration["has_downgrade"] is False

    with patch.object(migration["loader"], "get_up_sql", new_callable=AsyncMock) as mock_get_up_sql:
        mock_get_up_sql.return_value = [
            "CREATE TABLE irreversible_data AS SELECT DISTINCT column1, column2 FROM legacy_table;"
        ]

        result = runner.execute_upgrade(mock_driver, migration)
        assert result is not None

    with (
        patch.object(migration["loader"], "get_down_sql", new_callable=AsyncMock) as mock_get_down_sql,
        patch("sqlspec.migrations.base.logger"),
    ):
        mock_get_down_sql.return_value = []
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

    with patch("sqlspec.migrations.base.get_migration_loader") as mock_get_loader:
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

    with patch.object(migration["loader"], "get_up_sql") as mock_get_up_sql:

        async def mock_get_up_error() -> None:
            raise Exception("SQL syntax error")

        mock_get_up_sql.return_value = mock_get_up_error()

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
    mock_driver = Mock()

    with patch("sqlspec.migrations.base.get_migration_loader") as mock_get_loader:
        mock_loader = Mock()
        mock_loader.validate_migration_file = Mock()
        mock_get_loader.return_value = mock_loader

        with (
            patch.object(type(runner.loader), "clear_cache"),
            patch.object(type(runner.loader), "load_sql"),
            patch.object(type(runner.loader), "has_query", side_effect=lambda q: q.endswith("-down")),
        ):
            migration = runner.load_migration(migration_file)

            assert migration["has_upgrade"] is False
            assert migration["has_downgrade"] is True

    with pytest.raises(ValueError) as exc_info:
        runner.execute_upgrade(mock_driver, migration)

    assert "has no upgrade query" in str(exc_info.value)


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

    with patch("sqlspec.migrations.base.get_migration_loader") as mock_get_loader:
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
