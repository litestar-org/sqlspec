"""Unit tests for Migration class functionality.

Tests focused on Migration core functionality including:
- Migration creation and metadata management
- Version extraction and validation
- Checksum calculation and content verification
- Migration file structure and organization
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


class TestMigrationVersionExtraction:
    """Test migration version extraction functionality."""

    def test_extract_version_from_filename(self) -> None:
        """Test extracting version from migration filenames."""
        # Create a concrete test class for version extraction
        class TestMigrationRunner(BaseMigrationRunner):
            def __init__(self) -> None:
                self.migrations_path = Path("/test")
                self.loader = Mock()
                self.project_root = None

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

        runner = TestMigrationRunner()

        # Test valid version formats
        test_cases = [
            ("0001_initial.sql", "0001"),
            ("0002_add_users_table.sql", "0002"),
            ("0123_complex_migration.sql", "0123"),
            ("1_simple.sql", "0001"),  # Should be zero-padded
            ("42_meaning_of_life.sql", "0042"),  # Should be zero-padded
            ("9999_final_migration.sql", "9999"),
        ]

        for filename, expected_version in test_cases:
            result = runner._extract_version(filename)
            assert result == expected_version, f"Failed for {filename}: got {result}, expected {expected_version}"

    def test_extract_version_invalid_formats(self) -> None:
        """Test version extraction with invalid formats."""
        class TestMigrationRunner(BaseMigrationRunner):
            def __init__(self) -> None:
                self.migrations_path = Path("/test")
                self.loader = Mock()
                self.project_root = None

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

        runner = TestMigrationRunner()

        # Test invalid formats
        invalid_cases = [
            "no_version_here.sql",
            "abc_not_numeric.sql",
            "_empty_start.sql",
            "migration_without_number.sql",
            ".hidden_file.sql",
            "mixed_123abc_version.sql",
        ]

        for filename in invalid_cases:
            result = runner._extract_version(filename)
            assert result is None, f"Should return None for invalid filename: {filename}"


class TestMigrationChecksumCalculation:
    """Test migration content checksum calculation."""

    def test_calculate_checksum_basic(self) -> None:
        """Test basic checksum calculation."""
        class TestMigrationRunner(BaseMigrationRunner):
            def __init__(self) -> None:
                self.migrations_path = Path("/test")
                self.loader = Mock()
                self.project_root = None

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

        runner = TestMigrationRunner()

        content = "CREATE TABLE users (id INTEGER PRIMARY KEY);"
        checksum = runner._calculate_checksum(content)

        assert isinstance(checksum, str)
        assert len(checksum) == 32  # MD5 hex digest length

        # Same content should produce same checksum
        checksum2 = runner._calculate_checksum(content)
        assert checksum == checksum2

    def test_calculate_checksum_different_content(self) -> None:
        """Test that different content produces different checksums."""
        class TestMigrationRunner(BaseMigrationRunner):
            def __init__(self) -> None:
                self.migrations_path = Path("/test")
                self.loader = Mock()
                self.project_root = None

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

        runner = TestMigrationRunner()

        content1 = "CREATE TABLE users (id INTEGER PRIMARY KEY);"
        content2 = "CREATE TABLE products (id INTEGER PRIMARY KEY);"

        checksum1 = runner._calculate_checksum(content1)
        checksum2 = runner._calculate_checksum(content2)

        assert checksum1 != checksum2

    def test_calculate_checksum_unicode_content(self) -> None:
        """Test checksum calculation with Unicode content."""
        class TestMigrationRunner(BaseMigrationRunner):
            def __init__(self) -> None:
                self.migrations_path = Path("/test")
                self.loader = Mock()
                self.project_root = None

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

        runner = TestMigrationRunner()

        content = "-- Migration with unicode: 测试 файл עברית"
        checksum = runner._calculate_checksum(content)

        assert isinstance(checksum, str)
        assert len(checksum) == 32


class TestMigrationFilesDiscovery:
    """Test migration file discovery and sorting."""

    def test_get_migration_files_sync_empty_directory(self) -> None:
        """Test getting migration files from empty directory."""
        class TestMigrationRunner(BaseMigrationRunner):
            def __init__(self, migrations_path: Path) -> None:
                self.migrations_path = migrations_path
                self.loader = Mock()
                self.project_root = None

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

        with tempfile.TemporaryDirectory() as temp_dir:
            migrations_path = Path(temp_dir)
            runner = TestMigrationRunner(migrations_path)

            files = runner._get_migration_files_sync()
            assert files == []

    def test_get_migration_files_sync_nonexistent_directory(self) -> None:
        """Test getting migration files from nonexistent directory."""
        class TestMigrationRunner(BaseMigrationRunner):
            def __init__(self, migrations_path: Path) -> None:
                self.migrations_path = migrations_path
                self.loader = Mock()
                self.project_root = None

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

        nonexistent_path = Path("/nonexistent/migrations")
        runner = TestMigrationRunner(nonexistent_path)

        files = runner._get_migration_files_sync()
        assert files == []

    def test_get_migration_files_sync_with_sql_files(self) -> None:
        """Test getting migration files with SQL files."""
        class TestMigrationRunner(BaseMigrationRunner):
            def __init__(self, migrations_path: Path) -> None:
                self.migrations_path = migrations_path
                self.loader = Mock()
                self.project_root = None

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

        with tempfile.TemporaryDirectory() as temp_dir:
            migrations_path = Path(temp_dir)

            # Create migration files
            (migrations_path / "0001_initial.sql").write_text("-- Initial migration")
            (migrations_path / "0003_add_indexes.sql").write_text("-- Add indexes")
            (migrations_path / "0002_add_users.sql").write_text("-- Add users table")

            # Create non-migration files that should be ignored
            (migrations_path / "README.md").write_text("# Migrations")
            (migrations_path / "config.json").write_text("{}")

            runner = TestMigrationRunner(migrations_path)
            files = runner._get_migration_files_sync()

            # Should return sorted by version
            assert len(files) == 3
            assert files[0][0] == "0001"  # version
            assert files[1][0] == "0002"
            assert files[2][0] == "0003"

            # Check file paths
            assert files[0][1].name == "0001_initial.sql"
            assert files[1][1].name == "0002_add_users.sql"
            assert files[2][1].name == "0003_add_indexes.sql"

    def test_get_migration_files_sync_with_python_files(self) -> None:
        """Test getting migration files with Python files."""
        class TestMigrationRunner(BaseMigrationRunner):
            def __init__(self, migrations_path: Path) -> None:
                self.migrations_path = migrations_path
                self.loader = Mock()
                self.project_root = None

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

        with tempfile.TemporaryDirectory() as temp_dir:
            migrations_path = Path(temp_dir)

            # Create Python migration files
            (migrations_path / "0001_initial.py").write_text("# Python migration")
            (migrations_path / "0002_data_migration.py").write_text("# Data migration")

            runner = TestMigrationRunner(migrations_path)
            files = runner._get_migration_files_sync()

            assert len(files) == 2
            assert files[0][0] == "0001"
            assert files[1][0] == "0002"

    def test_get_migration_files_sync_mixed_types(self) -> None:
        """Test getting migration files with mixed SQL and Python files."""
        class TestMigrationRunner(BaseMigrationRunner):
            def __init__(self, migrations_path: Path) -> None:
                self.migrations_path = migrations_path
                self.loader = Mock()
                self.project_root = None

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

        with tempfile.TemporaryDirectory() as temp_dir:
            migrations_path = Path(temp_dir)

            # Create mixed migration files
            (migrations_path / "0001_initial.sql").write_text("-- SQL migration")
            (migrations_path / "0002_data_migration.py").write_text("# Python migration")
            (migrations_path / "0003_add_indexes.sql").write_text("-- Another SQL migration")

            runner = TestMigrationRunner(migrations_path)
            files = runner._get_migration_files_sync()

            assert len(files) == 3
            assert files[0][0] == "0001"  # Should be sorted by version regardless of type
            assert files[1][0] == "0002"
            assert files[2][0] == "0003"

    def test_get_migration_files_sync_hidden_files_ignored(self) -> None:
        """Test that hidden files are ignored."""
        class TestMigrationRunner(BaseMigrationRunner):
            def __init__(self, migrations_path: Path) -> None:
                self.migrations_path = migrations_path
                self.loader = Mock()
                self.project_root = None

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

        with tempfile.TemporaryDirectory() as temp_dir:
            migrations_path = Path(temp_dir)

            # Create visible and hidden files
            (migrations_path / "0001_visible.sql").write_text("-- Visible migration")
            (migrations_path / ".0002_hidden.sql").write_text("-- Hidden migration")
            (migrations_path / ".gitkeep").write_text("")

            runner = TestMigrationRunner(migrations_path)
            files = runner._get_migration_files_sync()

            # Should only include visible files
            assert len(files) == 1
            assert files[0][1].name == "0001_visible.sql"


class TestMigrationMetadataLoading:
    """Test migration metadata loading and validation."""

    def test_load_migration_metadata_sql_file(self) -> None:
        """Test loading metadata from SQL migration file."""
        class TestMigrationRunner(BaseMigrationRunner):
            def __init__(self, migrations_path: Path) -> None:
                self.migrations_path = migrations_path
                self.loader = Mock()
                self.loader.clear_cache = Mock()
                self.loader.load_sql = Mock()
                self.loader.has_query = Mock()
                self.project_root = None

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

        with tempfile.TemporaryDirectory() as temp_dir:
            migrations_path = Path(temp_dir)

            # Create SQL migration file
            migration_file = migrations_path / "0001_create_users.sql"
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

            runner = TestMigrationRunner(migrations_path)

            # Mock loader to indicate both upgrade and downgrade queries exist
            runner.loader.has_query.side_effect = lambda query: True

            with patch("sqlspec.migrations.base.get_migration_loader") as mock_get_loader:
                mock_loader = Mock()
                mock_loader.validate_migration_file = Mock()
                mock_get_loader.return_value = mock_loader

                metadata = runner._load_migration_metadata(migration_file)

            assert metadata["version"] == "0001"
            assert metadata["description"] == "create_users"
            assert metadata["file_path"] == migration_file
            assert metadata["has_upgrade"] is True
            assert metadata["has_downgrade"] is True
            assert isinstance(metadata["checksum"], str)
            assert len(metadata["checksum"]) == 32  # MD5 length

    def test_load_migration_metadata_python_file(self) -> None:
        """Test loading metadata from Python migration file."""
        class TestMigrationRunner(BaseMigrationRunner):
            def __init__(self, migrations_path: Path) -> None:
                self.migrations_path = migrations_path
                self.loader = Mock()
                self.project_root = None

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

        with tempfile.TemporaryDirectory() as temp_dir:
            migrations_path = Path(temp_dir)

            # Create Python migration file
            migration_file = migrations_path / "0001_data_migration.py"
            migration_content = '''
def up():
    """Upgrade migration."""
    return ["INSERT INTO users (name, email) VALUES ('admin', 'admin@example.com');"]

def down():
    """Downgrade migration."""
    return ["DELETE FROM users WHERE name = 'admin';"]
'''
            migration_file.write_text(migration_content)

            runner = TestMigrationRunner(migrations_path)

            with patch("sqlspec.migrations.base.get_migration_loader") as mock_get_loader, \
                 patch("sqlspec.migrations.base.run_") as mock_run:

                mock_loader = Mock()
                mock_loader.validate_migration_file = Mock()
                mock_loader.get_up_sql = Mock()
                mock_loader.get_down_sql = Mock()
                mock_get_loader.return_value = mock_loader

                # Mock run_ to simulate successful down_sql call
                mock_run.return_value = Mock(return_value=True)

                metadata = runner._load_migration_metadata(migration_file)

            assert metadata["version"] == "0001"
            assert metadata["description"] == "data_migration"
            assert metadata["file_path"] == migration_file
            assert metadata["has_upgrade"] is True
            assert metadata["has_downgrade"] is True

    def test_load_migration_metadata_no_downgrade(self) -> None:
        """Test loading metadata when no downgrade is available."""
        class TestMigrationRunner(BaseMigrationRunner):
            def __init__(self, migrations_path: Path) -> None:
                self.migrations_path = migrations_path
                self.loader = Mock()
                self.loader.clear_cache = Mock()
                self.loader.load_sql = Mock()
                self.loader.has_query = Mock()
                self.project_root = None

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

        with tempfile.TemporaryDirectory() as temp_dir:
            migrations_path = Path(temp_dir)

            # Create SQL migration file with only upgrade
            migration_file = migrations_path / "0001_irreversible.sql"
            migration_content = """
-- name: migrate-0001-up
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
"""
            migration_file.write_text(migration_content)

            runner = TestMigrationRunner(migrations_path)

            # Mock loader to indicate only upgrade query exists
            runner.loader.has_query.side_effect = lambda query: query.endswith("-up")

            with patch("sqlspec.migrations.base.get_migration_loader") as mock_get_loader:
                mock_loader = Mock()
                mock_loader.validate_migration_file = Mock()
                mock_get_loader.return_value = mock_loader

                metadata = runner._load_migration_metadata(migration_file)

            assert metadata["has_upgrade"] is True
            assert metadata["has_downgrade"] is False

    def test_load_migration_metadata_invalid_version(self) -> None:
        """Test loading metadata with invalid version format."""
        class TestMigrationRunner(BaseMigrationRunner):
            def __init__(self, migrations_path: Path) -> None:
                self.migrations_path = migrations_path
                self.loader = Mock()
                self.project_root = None

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

        with tempfile.TemporaryDirectory() as temp_dir:
            migrations_path = Path(temp_dir)

            # Create migration file with invalid name
            migration_file = migrations_path / "invalid_name.sql"
            migration_content = "CREATE TABLE test (id INTEGER);"
            migration_file.write_text(migration_content)

            runner = TestMigrationRunner(migrations_path)

            with patch("sqlspec.migrations.base.get_migration_loader") as mock_get_loader:
                mock_loader = Mock()
                mock_loader.validate_migration_file = Mock()
                mock_get_loader.return_value = mock_loader

                metadata = runner._load_migration_metadata(migration_file)

            assert metadata["version"] is None
            # Description extraction logic appears to parse the filename differently
            # when no valid version prefix is found
            assert metadata["description"] == "name"


class TestMigrationSQLGeneration:
    """Test migration SQL generation for upgrade and downgrade."""

    def test_get_migration_sql_upgrade(self) -> None:
        """Test getting upgrade SQL from migration."""
        class TestMigrationRunner(BaseMigrationRunner):
            def __init__(self) -> None:
                self.migrations_path = Path("/test")
                self.loader = Mock()
                self.project_root = None

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

        runner = TestMigrationRunner()

        # Create mock migration with upgrade capability
        migration = {
            "version": "0001",
            "has_upgrade": True,
            "has_downgrade": True,
            "file_path": Path("/test/0001_test.sql"),
            "loader": Mock()
        }

        with patch("sqlspec.migrations.base.run_") as mock_run, \
             patch("sqlspec.core.statement.SQL") as mock_sql:

            # Mock run_ to return a function that returns SQL statements
            mock_run.return_value = lambda file_path: ["CREATE TABLE test (id INTEGER);"]

            # Mock SQL constructor to return a mock object
            mock_sql_instance = Mock(spec=SQL)
            mock_sql.return_value = mock_sql_instance

            result = runner._get_migration_sql(migration, "up")

            # Should create SQL object with the statement
            assert result == mock_sql_instance
            mock_sql.assert_called_with("CREATE TABLE test (id INTEGER);")

    def test_get_migration_sql_downgrade(self) -> None:
        """Test getting downgrade SQL from migration."""
        class TestMigrationRunner(BaseMigrationRunner):
            def __init__(self) -> None:
                self.migrations_path = Path("/test")
                self.loader = Mock()
                self.project_root = None

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

        runner = TestMigrationRunner()

        migration = {
            "version": "0001",
            "has_upgrade": True,
            "has_downgrade": True,
            "file_path": Path("/test/0001_test.sql"),
            "loader": Mock()
        }

        with patch("sqlspec.migrations.base.run_") as mock_run, \
             patch("sqlspec.core.statement.SQL") as mock_sql:

            # Mock run_ to return SQL statements
            mock_run.return_value = ["DROP TABLE test;"]

            runner._get_migration_sql(migration, "down")

            # Should create SQL object with the statement
            assert mock_sql.called
            mock_sql.assert_called_with("DROP TABLE test;")

    def test_get_migration_sql_no_downgrade(self) -> None:
        """Test getting downgrade SQL when none available."""
        class TestMigrationRunner(BaseMigrationRunner):
            def __init__(self) -> None:
                self.migrations_path = Path("/test")
                self.loader = Mock()
                self.project_root = None

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

        runner = TestMigrationRunner()

        migration = {
            "version": "0001",
            "has_upgrade": True,
            "has_downgrade": False,  # No downgrade
            "file_path": Path("/test/0001_test.sql"),
            "loader": Mock()
        }

        with patch("sqlspec.migrations.base.logger") as mock_logger:
            result = runner._get_migration_sql(migration, "down")

            # Should return None and log warning
            assert result is None
            mock_logger.warning.assert_called_once()

    def test_get_migration_sql_no_upgrade_error(self) -> None:
        """Test error when trying to get upgrade SQL but none available."""
        class TestMigrationRunner(BaseMigrationRunner):
            def __init__(self) -> None:
                self.migrations_path = Path("/test")
                self.loader = Mock()
                self.project_root = None

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

        runner = TestMigrationRunner()

        migration = {
            "version": "0001",
            "has_upgrade": False,  # No upgrade
            "has_downgrade": False,
            "file_path": Path("/test/0001_test.sql"),
            "loader": Mock()
        }

        with pytest.raises(ValueError) as exc_info:
            runner._get_migration_sql(migration, "up")

        assert "has no upgrade query" in str(exc_info.value)

    def test_get_migration_sql_loader_error(self) -> None:
        """Test handling loader errors during SQL generation."""
        class TestMigrationRunner(BaseMigrationRunner):
            def __init__(self) -> None:
                self.migrations_path = Path("/test")
                self.loader = Mock()
                self.project_root = None

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

        runner = TestMigrationRunner()

        migration = {
            "version": "0001",
            "has_upgrade": True,
            "has_downgrade": True,
            "file_path": Path("/test/0001_test.sql"),
            "loader": Mock()
        }

        with patch("sqlspec.migrations.base.run_") as mock_run:
            # Mock run_ to raise an exception
            mock_run.side_effect = Exception("Loader error")

            # Should raise error for upgrade
            with pytest.raises(ValueError) as exc_info:
                runner._get_migration_sql(migration, "up")
            assert "Failed to load upgrade" in str(exc_info.value)

            # Should return None and log warning for downgrade
            with patch("sqlspec.migrations.base.logger") as mock_logger:
                result = runner._get_migration_sql(migration, "down")
                assert result is None
                mock_logger.warning.assert_called()

    def test_get_migration_sql_empty_statements(self) -> None:
        """Test handling when loader returns empty statements."""
        class TestMigrationRunner(BaseMigrationRunner):
            def __init__(self) -> None:
                self.migrations_path = Path("/test")
                self.loader = Mock()
                self.project_root = None

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

        runner = TestMigrationRunner()

        migration = {
            "version": "0001",
            "has_upgrade": True,
            "has_downgrade": False,
            "file_path": Path("/test/0001_test.sql"),
            "loader": Mock()
        }

        with patch("sqlspec.migrations.base.run_") as mock_run:
            # Mock run_ to return empty list
            mock_run.return_value = []

            result = runner._get_migration_sql(migration, "up")
            assert result is None
