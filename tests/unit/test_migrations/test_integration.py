"""Integration tests for Python migration support."""

import tempfile
from pathlib import Path
from typing import Any, Optional

import pytest

from sqlspec.migrations.base import BaseMigrationRunner
from sqlspec.migrations.loaders import MigrationLoadError, get_migration_loader
from sqlspec.migrations.utils import create_migration_file


class MockSyncDriver:
    """Mock sync driver for testing."""

    def __init__(self) -> None:
        self.executed_sql: list[str] = []

    def execute(self, sql: Any) -> None:
        """Mock execute method."""
        # Handle both string SQL and SQL objects
        if hasattr(sql, "sql"):
            self.executed_sql.append(sql.sql)
        else:
            self.executed_sql.append(str(sql))


class SyncMigrationRunnerTest(BaseMigrationRunner["MockSyncDriver"]):
    """Test implementation of sync migration runner."""

    def __init__(self, migrations_dir: Path, project_root: Optional[Path] = None) -> None:
        """Initialize test migration runner.

        Args:
            migrations_dir: Directory containing migration files.
            project_root: Optional project root for Python imports.
        """
        super().__init__(migrations_dir)
        self.project_root = project_root

    def get_migration_files(self) -> "list[tuple[str, Path]]":
        """Get all migration files sorted by version."""
        return self._get_migration_files_sync()

    def load_migration(self, file_path: Path) -> "dict[str, Any]":
        """Load a migration file and extract its components."""
        return self._load_migration_metadata(file_path)

    def execute_upgrade(self, driver: "MockSyncDriver", migration: "dict[str, Any]") -> "tuple[None, int]":
        """Execute an upgrade migration."""
        upgrade_sql = self._get_migration_sql(migration, "up")
        if upgrade_sql is None:
            return None, 0
        driver.execute(upgrade_sql)
        return None, 100  # Mock execution time

    def execute_downgrade(self, driver: "MockSyncDriver", migration: "dict[str, Any]") -> "tuple[None, int]":
        """Execute a downgrade migration."""
        downgrade_sql = self._get_migration_sql(migration, "down")
        if downgrade_sql is None:
            return None, 0
        driver.execute(downgrade_sql)
        return None, 100  # Mock execution time

    def load_all_migrations(self) -> "dict[str, Any]":
        """Load all migrations."""
        return {}


class TestPythonMigrationIntegration:
    """Integration tests for Python migration functionality."""

    def test_create_python_migration_file(self) -> None:
        """Test creating a Python migration file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)

            result = create_migration_file(
                migrations_dir=migrations_dir, version="0001", message="test_migration", file_type="py"
            )

            assert result.exists()
            assert result.suffix == ".py"
            assert "testmigration" in result.name

            # Check content
            content = result.read_text()
            assert "def migrate_up()" in content
            assert "def migrate_down()" in content
            assert "Union[str, List[str]]" in content

    def test_sql_migration_file_creation(self) -> None:
        """Test creating a SQL migration file (existing functionality)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)

            result = create_migration_file(
                migrations_dir=migrations_dir, version="0001", message="test_migration", file_type="sql"
            )

            assert result.exists()
            assert result.suffix == ".sql"
            assert "testmigration" in result.name

            # Check content
            content = result.read_text()
            assert "-- name: migrate-" in content
            assert "-up" in content
            assert "-down" in content

    def test_python_migration_discovery(self) -> None:
        """Test that Python migrations are discovered alongside SQL ones."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)
            runner = SyncMigrationRunnerTest(migrations_dir)

            # Create both SQL and Python migrations
            sql_file = create_migration_file(migrations_dir, "0001", "sql_migration", "sql")
            py_file = create_migration_file(migrations_dir, "0002", "python_migration", "py")

            migrations = runner.get_migration_files()

            # Should find both files
            assert len(migrations) == 2
            versions = [version for version, _ in migrations]
            paths = [path for _, path in migrations]

            assert sql_file in paths
            assert py_file in paths
            assert len(set(versions)) == 2  # Different versions

    def test_python_migration_validation(self) -> None:
        """Test validation of Python migration files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)

            # Create valid Python migration
            valid_file = migrations_dir / "0001_valid.py"
            valid_file.write_text("""
def migrate_up():
    return "CREATE TABLE test (id INTEGER);"

def migrate_down():
    return "DROP TABLE test;"
""")

            loader = get_migration_loader(valid_file, migrations_dir)
            loader.validate_migration_file(valid_file)  # Should not raise

    def test_python_migration_validation_missing_up(self) -> None:
        """Test validation fails when migrate_up is missing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)

            # Create invalid Python migration
            invalid_file = migrations_dir / "0001_invalid.py"
            invalid_file.write_text("""
def some_other_function():
    return "CREATE TABLE test (id INTEGER);"
""")

            loader = get_migration_loader(invalid_file, migrations_dir)

            with pytest.raises(MigrationLoadError, match="missing required 'migrate_up' function"):
                loader.validate_migration_file(invalid_file)

    def test_python_migration_execution(self) -> None:
        """Test execution of Python migrations."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)
            runner = SyncMigrationRunnerTest(migrations_dir)
            driver = MockSyncDriver()

            # Create Python migration
            py_file = migrations_dir / "0001_test.py"
            py_file.write_text("""
def migrate_up():
    return "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);"

def migrate_down():
    return "DROP TABLE users;"
""")

            # Load and execute migration
            migration = runner.load_migration(py_file)
            runner.execute_upgrade(driver, migration)

            assert len(driver.executed_sql) == 1
            assert "CREATE TABLE users" in driver.executed_sql[0]

    def test_python_migration_downgrade(self) -> None:
        """Test downgrade execution of Python migrations."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)
            runner = SyncMigrationRunnerTest(migrations_dir)
            driver = MockSyncDriver()

            # Create Python migration
            py_file = migrations_dir / "0001_test.py"
            py_file.write_text("""
def migrate_up():
    return "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);"

def migrate_down():
    return "DROP TABLE users;"
""")

            # Load and execute downgrade
            migration = runner.load_migration(py_file)
            runner.execute_downgrade(driver, migration)

            assert len(driver.executed_sql) == 1
            assert "DROP TABLE users" in driver.executed_sql[0]

    def test_python_migration_no_downgrade(self) -> None:
        """Test Python migration without downgrade function."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)
            runner = SyncMigrationRunnerTest(migrations_dir)
            driver = MockSyncDriver()

            # Create Python migration without migrate_down
            py_file = migrations_dir / "0001_test.py"
            py_file.write_text("""
def migrate_up():
    return "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);"
""")

            # Load migration
            migration = runner.load_migration(py_file)

            # Should indicate no downgrade available
            assert migration["has_downgrade"] is False

            # Attempting downgrade should return None, 0
            result, time_ms = runner.execute_downgrade(driver, migration)
            assert result is None
            assert time_ms == 0
            assert len(driver.executed_sql) == 0

    def test_mixed_migration_loading(self) -> None:
        """Test loading mixed SQL and Python migrations."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)
            runner = SyncMigrationRunnerTest(migrations_dir)

            # Create SQL migration
            sql_file = migrations_dir / "0001_sql_migration.sql"
            sql_file.write_text("""
-- name: migrate-0001-up
CREATE TABLE sql_table (id INTEGER);

-- name: migrate-0001-down
DROP TABLE sql_table;
""")

            # Create Python migration
            py_file = migrations_dir / "0002_python_migration.py"
            py_file.write_text("""
def migrate_up():
    return "CREATE TABLE python_table (id INTEGER);"

def migrate_down():
    return "DROP TABLE python_table;"
""")

            # Load both migrations
            sql_migration = runner.load_migration(sql_file)
            py_migration = runner.load_migration(py_file)

            # Both should have upgrade and downgrade
            assert sql_migration["has_upgrade"] is True
            assert sql_migration["has_downgrade"] is True
            assert py_migration["has_upgrade"] is True
            assert py_migration["has_downgrade"] is True

            # Versions should be different
            assert sql_migration["version"] != py_migration["version"]

    def test_python_migration_with_list_return(self) -> None:
        """Test Python migration that returns a list of SQL statements."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)
            runner = SyncMigrationRunnerTest(migrations_dir)
            driver = MockSyncDriver()

            # Create Python migration returning list
            py_file = migrations_dir / "0001_test.py"
            py_file.write_text("""
def migrate_up():
    return [
        "CREATE TABLE users (id INTEGER PRIMARY KEY);",
        "CREATE INDEX idx_users_id ON users(id);"
    ]

def migrate_down():
    return [
        "DROP INDEX idx_users_id;",
        "DROP TABLE users;"
    ]
""")

            # Load and execute migration
            migration = runner.load_migration(py_file)
            runner.execute_upgrade(driver, migration)

            # Should execute the first SQL statement
            assert len(driver.executed_sql) == 1
            assert "CREATE TABLE users" in driver.executed_sql[0]

    def test_python_migration_async_functions(self) -> None:
        """Test Python migration with async functions."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)
            runner = SyncMigrationRunnerTest(migrations_dir)
            driver = MockSyncDriver()

            # Create Python migration with async functions
            py_file = migrations_dir / "0001_test.py"
            py_file.write_text("""
async def migrate_up():
    return "CREATE TABLE async_users (id INTEGER PRIMARY KEY);"

async def migrate_down():
    return "DROP TABLE async_users;"
""")

            # Load and execute migration
            migration = runner.load_migration(py_file)
            runner.execute_upgrade(driver, migration)

            assert len(driver.executed_sql) == 1
            assert "CREATE TABLE async_users" in driver.executed_sql[0]

    def test_python_migration_error_handling(self) -> None:
        """Test error handling in Python migrations."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)
            runner = SyncMigrationRunnerTest(migrations_dir)

            # Create Python migration with syntax error
            py_file = migrations_dir / "0001_test.py"
            py_file.write_text("""
def migrate_up(
    # Missing closing parenthesis - syntax error
    return "CREATE TABLE test (id INTEGER);"
""")

            # Should raise error during loading
            with pytest.raises(MigrationLoadError, match="Failed to execute migration module"):
                runner.load_migration(py_file)

    def test_python_migration_import_capabilities(self) -> None:
        """Test that Python migrations can import from the project."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)

            # Create a utility module in the project
            project_root = migrations_dir.parent
            (project_root / "pyproject.toml").touch()  # Mark as project root

            runner = SyncMigrationRunnerTest(migrations_dir, project_root)
            driver = MockSyncDriver()

            util_file = project_root / "migration_utils.py"
            util_file.write_text("""
def get_create_table_sql():
    return "CREATE TABLE imported_table (id INTEGER PRIMARY KEY);"
""")

            # Create Python migration that imports from the project
            py_file = migrations_dir / "0001_test.py"
            py_file.write_text("""
from migration_utils import get_create_table_sql

def migrate_up():
    return get_create_table_sql()

def migrate_down():
    return "DROP TABLE imported_table;"
""")

            # Load and execute migration
            migration = runner.load_migration(py_file)
            runner.execute_upgrade(driver, migration)

            assert len(driver.executed_sql) == 1
            assert "CREATE TABLE imported_table" in driver.executed_sql[0]
