"""Unit tests for MigrationRunner functionality.

Tests focused on MigrationRunner core functionality including:
- Migration discovery and loading
- Migration execution coordination
- Upgrade and downgrade operations
- Migration metadata management
- Error handling and validation

Uses CORE_ROUND_3 architecture with core.statement.SQL and related modules.
"""

import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest

from sqlspec.core.statement import SQL
from sqlspec.migrations.base import BaseMigrationRunner


def create_test_migration_runner(migrations_path: Path = Path("/test")) -> BaseMigrationRunner:
    """Create a test migration runner implementation."""

    class TestMigrationRunner(BaseMigrationRunner):
        def __init__(self, migrations_path: Path) -> None:
            super().__init__(migrations_path)

        def get_migration_files(self) -> Any:
            pass

        def load_migration(self, file_path: Path) -> Any:
            pass

        def execute_upgrade(self, driver: Any, migration: dict[str, Any]) -> Any:
            pass

        def execute_downgrade(self, driver: Any, migration: dict[str, Any]) -> Any:
            pass

        def load_all_migrations(self) -> Any:
            pass

    return TestMigrationRunner(migrations_path)


def create_migration_runner_with_sync_files(migrations_path: Path) -> BaseMigrationRunner:
    """Create a migration runner with sync file discovery."""

    class TestMigrationRunner(BaseMigrationRunner):
        def __init__(self, migrations_path: Path) -> None:
            super().__init__(migrations_path)

        def get_migration_files(self) -> Any:
            return self._get_migration_files_sync()

        def load_migration(self, file_path: Path) -> Any:
            _ = file_path
            pass

        def execute_upgrade(self, driver: Any, migration: dict[str, Any]) -> Any:
            _ = driver, migration
            pass

        def execute_downgrade(self, driver: Any, migration: dict[str, Any]) -> Any:
            _ = driver, migration
            pass

        def load_all_migrations(self) -> Any:
            pass

    return TestMigrationRunner(migrations_path)


def create_migration_runner_with_metadata(migrations_path: Path) -> BaseMigrationRunner:
    """Create a migration runner with metadata loading."""

    class TestMigrationRunner(BaseMigrationRunner):
        def __init__(self, migrations_path: Path) -> None:
            super().__init__(migrations_path)

        def get_migration_files(self) -> Any:
            return self._get_migration_files_sync()

        def load_migration(self, file_path: Path) -> Any:
            return self._load_migration_metadata(file_path)

        def execute_upgrade(self, driver: Any, migration: dict[str, Any]) -> Any:
            _ = driver, migration
            pass

        def execute_downgrade(self, driver: Any, migration: dict[str, Any]) -> Any:
            _ = driver, migration
            pass

        def load_all_migrations(self) -> Any:
            pass

    return TestMigrationRunner(migrations_path)


# Migration Runner Initialization Tests


def test_migration_runner_initialization() -> None:
    """Test basic MigrationRunner initialization."""
    migrations_path = Path("/test/migrations")
    runner = create_test_migration_runner()
    runner.migrations_path = migrations_path

    assert runner.migrations_path == migrations_path
    assert runner.loader is not None
    assert runner.project_root is None


def test_migration_runner_with_project_root() -> None:
    """Test MigrationRunner with project root set."""
    migrations_path = Path("/test/migrations")
    project_root = Path("/test/project")

    runner = create_test_migration_runner()
    runner.migrations_path = migrations_path
    runner.project_root = project_root

    assert runner.migrations_path == migrations_path
    assert runner.project_root == project_root


# File Discovery Tests


def test_get_migration_files_sorting() -> None:
    """Test that migration files are properly sorted by version."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migrations_path = Path(temp_dir)

        # Create migration files in non-sequential order
        (migrations_path / "0003_add_indexes.sql").write_text("-- Migration 3")
        (migrations_path / "0001_initial.sql").write_text("-- Migration 1")
        (migrations_path / "0010_final_touches.sql").write_text("-- Migration 10")
        (migrations_path / "0002_add_users.sql").write_text("-- Migration 2")

        runner = create_migration_runner_with_sync_files(migrations_path)
        files = runner.get_migration_files()

        # Should be sorted by version
        expected_order = ["0001", "0002", "0003", "0010"]
        actual_order = [version for version, _ in files]

        assert actual_order == expected_order


def test_get_migration_files_mixed_extensions() -> None:
    """Test migration file discovery with mixed SQL and Python files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migrations_path = Path(temp_dir)

        # Create mixed file types
        (migrations_path / "0001_schema.sql").write_text("-- SQL Migration")
        (migrations_path / "0002_data.py").write_text("# Python Migration")
        (migrations_path / "0003_more_schema.sql").write_text("-- Another SQL Migration")
        (migrations_path / "README.md").write_text("# Documentation")

        runner = create_migration_runner_with_sync_files(migrations_path)
        files = runner.get_migration_files()

        # Should include both SQL and Python, sorted by version
        assert len(files) == 3
        assert files[0][0] == "0001"
        assert files[1][0] == "0002"
        assert files[2][0] == "0003"

        # Check file extensions
        assert files[0][1].suffix == ".sql"
        assert files[1][1].suffix == ".py"
        assert files[2][1].suffix == ".sql"


# Metadata Loading Tests


def test_load_migration_metadata_integration() -> None:
    """Test full migration metadata loading process."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migrations_path = Path(temp_dir)

        migration_file = migrations_path / "0001_create_users.sql"
        migration_content = """
-- name: migrate-0001-up
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- name: migrate-0001-down
DROP TABLE users;
"""
        migration_file.write_text(migration_content)

        runner = create_migration_runner_with_metadata(migrations_path)

        # Mock the loader's has_query method
        runner.loader.clear_cache = Mock()
        runner.loader.load_sql = Mock()
        runner.loader.has_query = Mock(side_effect=lambda q: True)  # Both up and down exist

        with patch("sqlspec.migrations.base.get_migration_loader") as mock_get_loader:
            mock_loader = Mock()
            mock_loader.validate_migration_file = Mock()
            mock_get_loader.return_value = mock_loader

            metadata = runner.load_migration(migration_file)

        # Verify metadata structure
        assert metadata["version"] == "0001"
        assert metadata["description"] == "create_users"
        assert metadata["file_path"] == migration_file
        assert metadata["has_upgrade"] is True
        assert metadata["has_downgrade"] is True
        assert isinstance(metadata["checksum"], str)
        assert len(metadata["checksum"]) == 32  # MD5 length
        assert "loader" in metadata


def test_load_migration_metadata_python_file() -> None:
    """Test metadata loading for Python migration files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migrations_path = Path(temp_dir)

        migration_file = migrations_path / "0001_data_migration.py"
        python_content = '''
def up():
    """Upgrade migration."""
    return [
        "INSERT INTO users (name, email) VALUES ('admin', 'admin@example.com')",
        "UPDATE settings SET initialized = true"
    ]

def down():
    """Downgrade migration."""
    return [
        "UPDATE settings SET initialized = false",
        "DELETE FROM users WHERE name = 'admin'"
    ]
'''
        migration_file.write_text(python_content)

        runner = create_migration_runner_with_metadata(migrations_path)

        with (
            patch("sqlspec.migrations.base.get_migration_loader") as mock_get_loader,
            patch("sqlspec.migrations.base.run_") as mock_run,
        ):
            mock_loader = Mock()
            mock_loader.validate_migration_file = Mock()
            mock_loader.get_up_sql = Mock()
            mock_loader.get_down_sql = Mock()
            mock_get_loader.return_value = mock_loader

            # Mock successful down_sql execution
            mock_run.return_value = Mock(return_value=True)

            metadata = runner.load_migration(migration_file)

        assert metadata["version"] == "0001"
        assert metadata["description"] == "data_migration"
        assert metadata["has_upgrade"] is True
        assert metadata["has_downgrade"] is True


# SQL Generation Tests


def test_get_migration_sql_upgrade_success() -> None:
    """Test successful upgrade SQL generation."""
    runner = create_test_migration_runner()

    # Create mock migration with valid upgrade capability
    migration = {
        "version": "0001",
        "has_upgrade": True,
        "has_downgrade": False,
        "file_path": Path("/test/0001_test.sql"),
        "loader": Mock(),
    }

    with patch("sqlspec.migrations.base.run_") as mock_run:
        # Mock successful SQL generation - run_ should return a callable that returns the statements
        mock_run.return_value = Mock(return_value=["CREATE TABLE test (id INTEGER PRIMARY KEY);"])

        result = runner._get_migration_sql(migration, "up")

        # Should return SQL object with expected SQL text
        assert result is not None
        assert isinstance(result, SQL)
        assert result.sql == "CREATE TABLE test (id INTEGER PRIMARY KEY);"


def test_get_migration_sql_downgrade_success() -> None:
    """Test successful downgrade SQL generation."""
    runner = create_test_migration_runner()

    migration = {
        "version": "0001",
        "has_upgrade": True,
        "has_downgrade": True,
        "file_path": Path("/test/0001_test.sql"),
        "loader": Mock(),
    }

    with patch("sqlspec.migrations.base.run_") as mock_run:
        # Mock successful SQL generation - run_ should return a callable that returns the statements
        mock_run.return_value = Mock(return_value=["DROP TABLE test;"])

        result = runner._get_migration_sql(migration, "down")

        # Should return SQL object with expected SQL text
        assert result is not None
        assert isinstance(result, SQL)
        assert result.sql == "DROP TABLE test;"


def test_get_migration_sql_no_downgrade_warning() -> None:
    """Test warning when no downgrade is available."""
    runner = create_test_migration_runner()

    migration = {
        "version": "0001",
        "has_upgrade": True,
        "has_downgrade": False,  # No downgrade available
        "file_path": Path("/test/0001_test.sql"),
        "loader": Mock(),
    }

    with patch("sqlspec.migrations.base.logger") as mock_logger:
        result = runner._get_migration_sql(migration, "down")

        # Should return None and log warning
        assert result is None
        mock_logger.warning.assert_called_once_with("Migration %s has no downgrade query", "0001")


def test_get_migration_sql_no_upgrade_error() -> None:
    """Test error when no upgrade is available."""
    runner = create_test_migration_runner()

    migration = {
        "version": "0001",
        "has_upgrade": False,  # No upgrade available
        "has_downgrade": False,
        "file_path": Path("/test/0001_test.sql"),
        "loader": Mock(),
    }

    with pytest.raises(ValueError) as exc_info:
        runner._get_migration_sql(migration, "up")

    assert "Migration 0001 has no upgrade query" in str(exc_info.value)


def test_get_migration_sql_loader_exception_upgrade() -> None:
    """Test handling of loader exceptions during upgrade SQL generation."""
    runner = create_test_migration_runner()

    migration = {
        "version": "0001",
        "has_upgrade": True,
        "has_downgrade": False,
        "file_path": Path("/test/0001_test.sql"),
        "loader": Mock(),
    }

    with patch("sqlspec.migrations.base.run_") as mock_run:
        # Mock loader exception - run_ should return a callable that raises an exception
        mock_run.return_value = Mock(side_effect=Exception("Loader failed to parse migration"))

        with pytest.raises(ValueError) as exc_info:
            runner._get_migration_sql(migration, "up")

        assert "Failed to load upgrade for migration 0001" in str(exc_info.value)


def test_get_migration_sql_loader_exception_downgrade() -> None:
    """Test handling of loader exceptions during downgrade SQL generation."""
    runner = create_test_migration_runner()

    migration = {
        "version": "0001",
        "has_upgrade": True,
        "has_downgrade": True,
        "file_path": Path("/test/0001_test.sql"),
        "loader": Mock(),
    }

    with patch("sqlspec.migrations.base.run_") as mock_run, patch("sqlspec.migrations.base.logger") as mock_logger:
        # Mock loader exception for downgrade - run_ should return a callable that raises an exception
        mock_run.return_value = Mock(side_effect=Exception("Downgrade loader failed"))

        result = runner._get_migration_sql(migration, "down")

        # Should return None and log warning (not raise error for downgrade)
        assert result is None

        # Check that the warning was called with the migration version
        mock_logger.warning.assert_called_once()
        call_args = mock_logger.warning.call_args
        assert call_args[0][1] == "0001"  # Second argument should be the version


def test_get_migration_sql_empty_statements() -> None:
    """Test handling when migration loader returns empty statements."""
    runner = create_test_migration_runner()

    migration = {
        "version": "0001",
        "has_upgrade": True,
        "has_downgrade": False,
        "file_path": Path("/test/0001_test.sql"),
        "loader": Mock(),
    }

    with patch("sqlspec.migrations.base.run_") as mock_run:
        # Mock empty statements list - run_ should return a callable that returns an empty list
        mock_run.return_value = Mock(return_value=[])

        result = runner._get_migration_sql(migration, "up")

        # Should return None for empty statements
        assert result is None


def test_get_migration_sql_none_statements() -> None:
    """Test handling when migration loader returns None."""
    runner = create_test_migration_runner()

    migration = {
        "version": "0001",
        "has_upgrade": True,
        "has_downgrade": False,
        "file_path": Path("/test/0001_test.sql"),
        "loader": Mock(),
    }

    with patch("sqlspec.migrations.base.run_") as mock_run:
        # Mock None return - run_ should return a callable that returns None
        mock_run.return_value = Mock(return_value=None)

        result = runner._get_migration_sql(migration, "up")

        # Should return None
        assert result is None


# Error Handling Tests


def test_invalid_migration_version_handling() -> None:
    """Test handling of invalid migration version formats."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migrations_path = Path(temp_dir)

        # Create file with invalid version format
        invalid_file = migrations_path / "invalid_version_format.sql"
        invalid_file.write_text("CREATE TABLE test (id INTEGER);")

        runner = create_migration_runner_with_sync_files(migrations_path)
        files = runner.get_migration_files()

        # Should not include files with invalid version format
        assert len(files) == 0


def test_corrupted_migration_file_handling() -> None:
    """Test handling of corrupted migration files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migrations_path = Path(temp_dir)

        # Create corrupted migration file
        corrupted_file = migrations_path / "0001_corrupted.sql"
        corrupted_file.write_text("This is not a valid migration file content")

        runner = create_migration_runner_with_metadata(migrations_path)

        with patch("sqlspec.migrations.base.get_migration_loader") as mock_get_loader:
            mock_loader = Mock()
            mock_loader.validate_migration_file.side_effect = Exception("Validation failed")
            mock_get_loader.return_value = mock_loader

            # Should handle validation errors gracefully
            with pytest.raises(Exception):
                runner.load_migration(corrupted_file)


def test_missing_migrations_directory() -> None:
    """Test handling when migrations directory is missing."""
    nonexistent_path = Path("/nonexistent/migrations/directory")
    runner = create_migration_runner_with_sync_files(nonexistent_path)

    # Should handle missing directory gracefully
    files = runner.get_migration_files()
    assert files == []


# Performance Tests


def test_large_migration_file_handling() -> None:
    """Test handling of large migration files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migrations_path = Path(temp_dir)

        # Create large migration file
        large_file = migrations_path / "0001_large_migration.sql"

        # Generate large content
        large_content_parts = [
            """
-- name: migrate-0001-up
CREATE TABLE large_table (
    id INTEGER PRIMARY KEY,
    data TEXT
);
"""
        ]

        # Add many INSERT statements
        large_content_parts.extend(f"INSERT INTO large_table (data) VALUES ('data_{i:04d}');" for i in range(1000))

        large_content_parts.append("""
-- name: migrate-0001-down
DROP TABLE large_table;
""")

        large_content = "\n".join(large_content_parts)
        large_file.write_text(large_content)

        runner = create_migration_runner_with_metadata(migrations_path)

        # Mock the loader methods
        runner.loader.clear_cache = Mock()
        runner.loader.load_sql = Mock()
        runner.loader.has_query = Mock(return_value=True)

        with patch("sqlspec.migrations.base.get_migration_loader") as mock_get_loader:
            mock_loader = Mock()
            mock_loader.validate_migration_file = Mock()
            mock_get_loader.return_value = mock_loader

            # Should handle large file without issues
            metadata = runner.load_migration(large_file)

            assert metadata["version"] == "0001"
            assert metadata["description"] == "large_migration"
            assert len(metadata["checksum"]) == 32  # MD5 length


def test_many_migration_files_performance() -> None:
    """Test performance with many migration files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migrations_path = Path(temp_dir)

        # Create many migration files
        for i in range(100):
            migration_file = migrations_path / f"{i + 1:04d}_migration_{i}.sql"
            migration_file.write_text(f"""
-- name: migrate-{i + 1:04d}-up
CREATE TABLE test_table_{i} (id INTEGER PRIMARY KEY);

-- name: migrate-{i + 1:04d}-down
DROP TABLE test_table_{i};
""")

        runner = create_migration_runner_with_sync_files(migrations_path)

        # Should discover all files efficiently
        files = runner.get_migration_files()

        assert len(files) == 100

        # Verify sorting
        for i, (version, _) in enumerate(files):
            expected_version = f"{i + 1:04d}"
            assert version == expected_version
