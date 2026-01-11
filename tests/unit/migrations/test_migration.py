# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Unit tests for Migration class functionality.

Tests for Migration core functionality including:
- Migration creation and metadata management
- Version extraction and validation
- Checksum calculation and content verification
- Migration file structure and organization
- Error handling and validation
"""

from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest

from sqlspec.migrations.base import BaseMigrationRunner

pytestmark = pytest.mark.xdist_group("migrations")


class MockMigrationRunner(BaseMigrationRunner):
    """Mock migration runner implementation for unit tests."""

    def __init__(self, migrations_path: Path | None = None) -> None:
        if migrations_path is None:
            migrations_path = Path("/test")
        super().__init__(migrations_path)
        self.loader = Mock()
        self.project_root = None

    def get_migration_files(self) -> Any:
        return self._get_migration_files_sync()

    def load_migration(self, file_path: Path) -> Any:
        return self._load_migration_metadata(file_path)

    def execute_upgrade(self, driver: Any, migration: dict[str, Any]) -> Any:
        pass

    def execute_downgrade(self, driver: Any, migration: dict[str, Any]) -> Any:
        pass

    def load_all_migrations(self) -> Any:
        pass


def test_extract_version_from_filename() -> None:
    """Test extracting version from migration filenames."""
    runner = MockMigrationRunner()

    test_cases = [
        ("0001_initial.sql", "0001"),
        ("0002_add_users_table.sql", "0002"),
        ("0123_complex_migration.sql", "0123"),
        ("1_simple.sql", "0001"),
        ("42_meaning_of_life.sql", "0042"),
        ("9999_final_migration.sql", "9999"),
    ]

    for filename, expected_version in test_cases:
        result = runner._extract_version(filename)
        assert result == expected_version, f"Failed for {filename}: got {result}, expected {expected_version}"


def test_extract_version_invalid_formats() -> None:
    """Test version extraction with invalid formats."""
    runner = MockMigrationRunner()

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


def test_calculate_checksum_basic() -> None:
    """Test basic checksum calculation."""
    runner = MockMigrationRunner()

    content = "CREATE TABLE users (id INTEGER PRIMARY KEY);"
    checksum = runner._calculate_checksum(content)

    assert isinstance(checksum, str)
    assert len(checksum) == 32

    checksum2 = runner._calculate_checksum(content)
    assert checksum == checksum2


def test_calculate_checksum_different_content() -> None:
    """Test that different content produces different checksums."""
    runner = MockMigrationRunner()

    content1 = "CREATE TABLE users (id INTEGER PRIMARY KEY);"
    content2 = "CREATE TABLE products (id INTEGER PRIMARY KEY);"

    checksum1 = runner._calculate_checksum(content1)
    checksum2 = runner._calculate_checksum(content2)

    assert checksum1 != checksum2


def test_calculate_checksum_unicode_content() -> None:
    """Test checksum calculation with Unicode content."""
    runner = MockMigrationRunner()

    content = "-- Migration with unicode: 测试 файл עברית"
    checksum = runner._calculate_checksum(content)

    assert isinstance(checksum, str)
    assert len(checksum) == 32


def test_get_migration_files_sync_empty_directory(tmp_path: Path) -> None:
    """Test getting migration files from empty directory."""
    migrations_path = tmp_path
    runner = MockMigrationRunner(migrations_path)

    files = runner._get_migration_files_sync()
    assert files == []


def test_get_migration_files_sync_nonexistent_directory() -> None:
    """Test getting migration files from nonexistent directory."""
    nonexistent_path = Path("/nonexistent/migrations")
    runner = MockMigrationRunner(nonexistent_path)

    files = runner._get_migration_files_sync()
    assert files == []


def test_get_migration_files_sync_with_sql_files(tmp_path: Path) -> None:
    """Test getting migration files with SQL files."""
    migrations_path = tmp_path

    (migrations_path / "0001_initial.sql").write_text("-- Initial migration")
    (migrations_path / "0003_add_indexes.sql").write_text("-- Add indexes")
    (migrations_path / "0002_add_users.sql").write_text("-- Add users table")

    (migrations_path / "README.md").write_text("# Migrations")
    (migrations_path / "config.json").write_text("{}")

    runner = MockMigrationRunner(migrations_path)
    files = runner._get_migration_files_sync()

    assert len(files) == 3
    assert files[0][0] == "0001"
    assert files[1][0] == "0002"
    assert files[2][0] == "0003"

    assert files[0][1].name == "0001_initial.sql"
    assert files[1][1].name == "0002_add_users.sql"
    assert files[2][1].name == "0003_add_indexes.sql"


def test_get_migration_files_sync_with_python_files(tmp_path: Path) -> None:
    """Test getting migration files with Python files."""
    migrations_path = tmp_path

    (migrations_path / "0001_initial.py").write_text("# Initial migration")
    (migrations_path / "0002_data_migration.py").write_text("# Data migration")

    runner = MockMigrationRunner(migrations_path)
    files = runner._get_migration_files_sync()

    assert len(files) == 2
    assert files[0][0] == "0001"
    assert files[1][0] == "0002"


def test_get_migration_files_sync_mixed_types(tmp_path: Path) -> None:
    """Test getting migration files with mixed SQL and Python files."""
    migrations_path = tmp_path

    (migrations_path / "0001_initial.sql").write_text("-- SQL migration")
    (migrations_path / "0002_data_migration.py").write_text("# Python migration")
    (migrations_path / "0003_add_indexes.sql").write_text("-- Another SQL migration")

    runner = MockMigrationRunner(migrations_path)
    files = runner._get_migration_files_sync()

    assert len(files) == 3
    assert files[0][0] == "0001"
    assert files[1][0] == "0002"
    assert files[2][0] == "0003"


def test_get_migration_files_sync_hidden_files_ignored(tmp_path: Path) -> None:
    """Test that hidden files are ignored."""
    migrations_path = tmp_path

    (migrations_path / "0001_visible.sql").write_text("-- Visible migration")
    (migrations_path / ".0002_hidden.sql").write_text("-- Hidden migration")
    (migrations_path / ".gitkeep").write_text("")

    runner = MockMigrationRunner(migrations_path)
    files = runner._get_migration_files_sync()

    assert len(files) == 1
    assert files[0][1].name == "0001_visible.sql"


def test_load_migration_metadata_sql_file(tmp_path: Path) -> None:
    """Test loading metadata from SQL migration file."""
    migrations_path = tmp_path

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

    runner = MockMigrationRunner(migrations_path)

    runner.loader.clear_cache = Mock()
    runner.loader.load_sql = Mock()
    runner.loader.has_query = Mock(side_effect=lambda query: True)

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
    assert len(metadata["checksum"]) == 32


def test_load_migration_metadata_python_file_sync(tmp_path: Path) -> None:
    """Test loading metadata from Python migration file with sync functions."""
    migrations_path = tmp_path

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

    runner = MockMigrationRunner(migrations_path)

    with (
        patch("sqlspec.migrations.base.get_migration_loader") as mock_get_loader,
        patch("sqlspec.migrations.base.await_") as mock_await,
    ):
        mock_loader = Mock()
        mock_loader.validate_migration_file = Mock()
        mock_loader.get_up_sql = Mock()
        mock_loader.get_down_sql = Mock()
        mock_get_loader.return_value = mock_loader

        mock_await.return_value = Mock(return_value=True)

        metadata = runner._load_migration_metadata(migration_file)

    assert metadata["version"] == "0001"
    assert metadata["description"] == "data_migration"
    assert metadata["file_path"] == migration_file
    assert metadata["has_upgrade"] is True
    assert metadata["has_downgrade"] is True


def test_load_migration_metadata_python_file_async(tmp_path: Path) -> None:
    """Test loading metadata from Python migration file with async functions."""
    migrations_path = tmp_path

    migration_file = migrations_path / "0001_async_migration.py"
    migration_content = '''
import asyncio

async def up():
    """Upgrade migration."""
    await asyncio.sleep(0.001)
    return ["INSERT INTO users (name, email) VALUES ('admin', 'admin@example.com');"]

async def down():
    """Downgrade migration."""
    await asyncio.sleep(0.001)
    return ["DELETE FROM users WHERE name = 'admin';"]
'''
    migration_file.write_text(migration_content)

    runner = MockMigrationRunner(migrations_path)

    with (
        patch("sqlspec.migrations.base.get_migration_loader") as mock_get_loader,
        patch("sqlspec.migrations.base.await_") as mock_await,
    ):
        mock_loader = Mock()
        mock_loader.validate_migration_file = Mock()
        mock_loader.get_up_sql = Mock()
        mock_loader.get_down_sql = Mock()
        mock_get_loader.return_value = mock_loader

        mock_await.return_value = Mock(return_value=True)

        metadata = runner._load_migration_metadata(migration_file)

    assert metadata["version"] == "0001"
    assert metadata["description"] == "async_migration"
    assert metadata["file_path"] == migration_file
    assert metadata["has_upgrade"] is True
    assert metadata["has_downgrade"] is True


def test_load_migration_metadata_python_file_mixed(tmp_path: Path) -> None:
    """Test loading metadata from Python migration file with mixed sync/async functions."""
    migrations_path = tmp_path

    migration_file = migrations_path / "0001_mixed_migration.py"
    migration_content = '''
import asyncio

def up():
    """Sync upgrade migration."""
    return ["INSERT INTO users (name, email) VALUES ('admin', 'admin@example.com');"]

async def down():
    """Async downgrade migration."""
    await asyncio.sleep(0.001)
    return ["DELETE FROM users WHERE name = 'admin';"]
'''
    migration_file.write_text(migration_content)

    runner = MockMigrationRunner(migrations_path)

    with (
        patch("sqlspec.migrations.base.get_migration_loader") as mock_get_loader,
        patch("sqlspec.migrations.base.await_") as mock_await,
    ):
        mock_loader = Mock()
        mock_loader.validate_migration_file = Mock()
        mock_loader.get_up_sql = Mock()
        mock_loader.get_down_sql = Mock()
        mock_get_loader.return_value = mock_loader

        mock_await.return_value = Mock(return_value=True)

        metadata = runner._load_migration_metadata(migration_file)

    assert metadata["version"] == "0001"
    assert metadata["description"] == "mixed_migration"
    assert metadata["file_path"] == migration_file
    assert metadata["has_upgrade"] is True
    assert metadata["has_downgrade"] is True


def test_load_multiple_mixed_migrations(tmp_path: Path) -> None:
    """Test loading multiple migrations with mixed SQL and Python (sync/async) files."""
    migrations_path = tmp_path

    sql_migration = migrations_path / "0001_create_tables.sql"
    sql_content = """
-- up
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE posts (
    id INTEGER PRIMARY KEY,
    user_id INTEGER,
    title TEXT NOT NULL,
    content TEXT,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

-- down
DROP TABLE posts;
DROP TABLE users;
"""
    sql_migration.write_text(sql_content)

    python_sync_migration = migrations_path / "0002_seed_data.py"
    python_sync_content = '''
def up():
    """Sync upgrade migration to seed initial data."""
    return [
        "INSERT INTO users (name, email) VALUES ('admin', 'admin@example.com');",
        "INSERT INTO users (name, email) VALUES ('user1', 'user1@example.com');",
        "INSERT INTO posts (user_id, title, content) VALUES (1, 'Welcome Post', 'Welcome to our platform!');"
    ]

def down():
    """Sync downgrade migration to remove seeded data."""
    return [
        "DELETE FROM posts WHERE title = 'Welcome Post';",
        "DELETE FROM users WHERE email IN ('admin@example.com', 'user1@example.com');"
    ]
'''
    python_sync_migration.write_text(python_sync_content)

    python_async_migration = migrations_path / "0003_async_data_processing.py"
    python_async_content = '''
import asyncio

async def up():
    """Async upgrade migration for data processing."""
    await asyncio.sleep(0.001)
    return [
        "UPDATE users SET name = UPPER(name) WHERE id > 0;",
        "INSERT INTO posts (user_id, title, content) VALUES (2, 'Async Post', 'Posted via async migration');"
    ]

async def down():
    """Async downgrade migration to reverse data processing."""
    await asyncio.sleep(0.001)
    return [
        "DELETE FROM posts WHERE title = 'Async Post';",
        "UPDATE users SET name = LOWER(name) WHERE id > 0;"
    ]
'''
    python_async_migration.write_text(python_async_content)

    sql_migration2 = migrations_path / "0004_add_indexes.sql"
    sql_content2 = """
-- up
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_posts_user_id ON posts(user_id);
CREATE INDEX idx_posts_title ON posts(title);

-- down
DROP INDEX idx_posts_title;
DROP INDEX idx_posts_user_id;
DROP INDEX idx_users_email;
"""
    sql_migration2.write_text(sql_content2)

    python_mixed_migration = migrations_path / "0005_mixed_operations.py"
    python_mixed_content = '''
import asyncio

def up():
    """Sync upgrade for mixed operations."""
    return ["ALTER TABLE users ADD COLUMN last_login TIMESTAMP;"]

async def down():
    """Async downgrade for mixed operations."""
    await asyncio.sleep(0.001)
    return ["ALTER TABLE users DROP COLUMN last_login;"]
'''
    python_mixed_migration.write_text(python_mixed_content)

    runner = MockMigrationRunner(migrations_path)

    runner.loader.has_query = Mock(return_value=True)
    runner.loader.load_sql = Mock()
    runner.loader.clear_cache = Mock()

    migration_files = sorted(migrations_path.glob("*"), key=lambda p: p.name)

    with (
        patch("sqlspec.migrations.base.get_migration_loader") as mock_get_loader,
        patch("sqlspec.migrations.base.await_") as mock_await,
    ):
        mock_loader = Mock()
        mock_loader.validate_migration_file = Mock()
        mock_loader.get_up_sql = Mock()
        mock_loader.get_down_sql = Mock()
        mock_get_loader.return_value = mock_loader

        mock_await.return_value = Mock(return_value=True)

        all_metadata = []
        for migration_file in migration_files:
            metadata = runner._load_migration_metadata(migration_file)
            all_metadata.append(metadata)

    assert len(all_metadata) == 5

    sql_metadata = [m for m in all_metadata if m["file_path"].suffix == ".sql"]
    assert len(sql_metadata) == 2

    python_metadata = [m for m in all_metadata if m["file_path"].suffix == ".py"]
    assert len(python_metadata) == 3

    expected_migrations = [
        {"version": "0001", "description": "create_tables", "type": "sql"},
        {"version": "0002", "description": "seed_data", "type": "python_sync"},
        {"version": "0003", "description": "async_data_processing", "type": "python_async"},
        {"version": "0004", "description": "add_indexes", "type": "sql"},
        {"version": "0005", "description": "mixed_operations", "type": "python_mixed"},
    ]

    for i, expected in enumerate(expected_migrations):
        metadata = all_metadata[i]
        assert metadata["version"] == expected["version"]
        assert metadata["description"] == expected["description"]
        assert metadata["has_upgrade"] is True
        assert metadata["has_downgrade"] is True

        if expected["type"] == "sql":
            assert metadata["file_path"].suffix == ".sql"
        else:
            assert metadata["file_path"].suffix == ".py"


def test_load_migration_metadata_no_downgrade(tmp_path: Path) -> None:
    """Test loading metadata when no downgrade is available."""
    migrations_path = tmp_path

    migration_file = migrations_path / "0001_irreversible.sql"
    migration_content = """
-- name: migrate-0001-up
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
"""
    migration_file.write_text(migration_content)

    runner = MockMigrationRunner(migrations_path)

    runner.loader.clear_cache = Mock()
    runner.loader.load_sql = Mock()
    runner.loader.has_query = Mock(side_effect=lambda query: query.endswith("-up"))

    with patch("sqlspec.migrations.base.get_migration_loader") as mock_get_loader:
        mock_loader = Mock()
        mock_loader.validate_migration_file = Mock()
        mock_get_loader.return_value = mock_loader

        metadata = runner._load_migration_metadata(migration_file)

    assert metadata["has_upgrade"] is True
    assert metadata["has_downgrade"] is False


def test_load_migration_metadata_invalid_version(tmp_path: Path) -> None:
    """Test loading metadata with invalid version format."""
    migrations_path = tmp_path

    migration_file = migrations_path / "invalid_name.sql"
    migration_content = "CREATE TABLE test (id INTEGER);"
    migration_file.write_text(migration_content)

    runner = MockMigrationRunner(migrations_path)

    with patch("sqlspec.migrations.base.get_migration_loader") as mock_get_loader:
        mock_loader = Mock()
        mock_loader.validate_migration_file = Mock()
        mock_get_loader.return_value = mock_loader

        metadata = runner._load_migration_metadata(migration_file)

    assert metadata["version"] is None

    assert metadata["description"] == "name"


def test_get_migration_sql_upgrade() -> None:
    """Test getting upgrade SQL from migration."""
    runner = MockMigrationRunner()

    migration = {
        "version": "0001",
        "has_upgrade": True,
        "has_downgrade": True,
        "file_path": Path("/test/0001_test.sql"),
        "loader": Mock(),
    }

    with patch("sqlspec.migrations.base.await_") as mock_await:
        mock_await.return_value = lambda file_path: ["CREATE TABLE test (id INTEGER);"]

        result = runner._get_migration_sql(migration, "up")

        assert isinstance(result, list)
        assert result == ["CREATE TABLE test (id INTEGER);"]


def test_get_migration_sql_downgrade() -> None:
    """Test getting downgrade SQL from migration."""
    runner = MockMigrationRunner()

    migration = {
        "version": "0001",
        "has_upgrade": True,
        "has_downgrade": True,
        "file_path": Path("/test/0001_test.sql"),
        "loader": Mock(),
    }

    with patch("sqlspec.migrations.base.await_") as mock_await:
        mock_await.return_value = lambda file_path: ["DROP TABLE test;"]

        result = runner._get_migration_sql(migration, "down")

        assert isinstance(result, list)
        assert result == ["DROP TABLE test;"]


def test_get_migration_sql_no_downgrade() -> None:
    """Test getting downgrade SQL when none available."""
    runner = MockMigrationRunner()

    migration = {
        "version": "0001",
        "has_upgrade": True,
        "has_downgrade": False,
        "file_path": Path("/test/0001_test.sql"),
        "loader": Mock(),
    }

    with patch("sqlspec.migrations.base.logger") as mock_logger:
        result = runner._get_migration_sql(migration, "down")

        assert result is None
        mock_logger.warning.assert_called_once()


def test_get_migration_sql_no_upgrade_error() -> None:
    """Test error when trying to get upgrade SQL but none available."""
    runner = MockMigrationRunner()

    migration = {
        "version": "0001",
        "has_upgrade": False,
        "has_downgrade": False,
        "file_path": Path("/test/0001_test.sql"),
        "loader": Mock(),
    }

    with pytest.raises(ValueError) as exc_info:
        runner._get_migration_sql(migration, "up")

    assert "has no upgrade query" in str(exc_info.value)


def test_get_migration_sql_loader_error() -> None:
    """Test handling loader errors during SQL generation."""
    runner = MockMigrationRunner()

    migration = {
        "version": "0001",
        "has_upgrade": True,
        "has_downgrade": True,
        "file_path": Path("/test/0001_test.sql"),
        "loader": Mock(),
    }

    with patch("sqlspec.migrations.base.await_") as mock_await:
        mock_await.side_effect = Exception("Loader error")

        with pytest.raises(ValueError) as exc_info:
            runner._get_migration_sql(migration, "up")
        assert "Failed to load upgrade" in str(exc_info.value)

        with patch("sqlspec.migrations.base.logger") as mock_logger:
            result = runner._get_migration_sql(migration, "down")
            assert result is None
            mock_logger.warning.assert_called()


def test_get_migration_sql_empty_statements() -> None:
    """Test handling when loader returns empty statements."""
    runner = MockMigrationRunner()

    migration = {
        "version": "0001",
        "has_upgrade": True,
        "has_downgrade": False,
        "file_path": Path("/test/0001_test.sql"),
        "loader": Mock(),
    }

    with patch("sqlspec.migrations.base.await_") as mock_await:
        mock_await.return_value = lambda file_path: []

        result = runner._get_migration_sql(migration, "up")
        assert result is None
