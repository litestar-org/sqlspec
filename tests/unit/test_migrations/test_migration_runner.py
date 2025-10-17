# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Unit tests for MigrationRunner functionality.

Tests for MigrationRunner core functionality including:
- Migration discovery and loading
- Migration execution coordination
- Upgrade and downgrade operations
- Migration metadata management
- Error handling and validation
"""

import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest

from sqlspec.migrations.base import BaseMigrationRunner

pytestmark = pytest.mark.xdist_group("migrations")


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


def test_get_migration_files_sorting() -> None:
    """Test that migration files are properly sorted by version."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migrations_path = Path(temp_dir)

        (migrations_path / "0003_add_indexes.sql").write_text("-- Migration 3")
        (migrations_path / "0001_initial.sql").write_text("-- Migration 1")
        (migrations_path / "0010_final_touches.sql").write_text("-- Migration 10")
        (migrations_path / "0002_add_users.sql").write_text("-- Migration 2")

        runner = create_migration_runner_with_sync_files(migrations_path)
        files = runner.get_migration_files()

        expected_order = ["0001", "0002", "0003", "0010"]
        actual_order = [version for version, _ in files]

        assert actual_order == expected_order


def test_get_migration_files_mixed_extensions() -> None:
    """Test migration file discovery with mixed SQL and Python files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migrations_path = Path(temp_dir)

        (migrations_path / "0001_schema.sql").write_text("-- SQL Migration")
        (migrations_path / "0002_data.py").write_text("# Data migration")
        (migrations_path / "0003_more_schema.sql").write_text("-- Another SQL Migration")
        (migrations_path / "README.md").write_text("# README")

        runner = create_migration_runner_with_sync_files(migrations_path)
        files = runner.get_migration_files()

        assert len(files) == 3
        assert files[0][0] == "0001"
        assert files[1][0] == "0002"
        assert files[2][0] == "0003"

        assert files[0][1].suffix == ".sql"
        assert files[1][1].suffix == ".py"
        assert files[2][1].suffix == ".sql"


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

        with (
            patch.object(type(runner.loader), "clear_cache"),
            patch.object(type(runner.loader), "load_sql"),
            patch.object(type(runner.loader), "has_query", side_effect=lambda q: True),
        ):
            with patch("sqlspec.migrations.base.get_migration_loader") as mock_get_loader:
                mock_loader = Mock()
                mock_loader.validate_migration_file = Mock()
                mock_get_loader.return_value = mock_loader

                metadata = runner.load_migration(migration_file)

            assert metadata["version"] == "0001"
            assert metadata["description"] == "create_users"
            assert metadata["file_path"] == migration_file
            assert metadata["has_upgrade"] is True
            assert metadata["has_downgrade"] is True
            assert isinstance(metadata["checksum"], str)
            assert len(metadata["checksum"]) == 32
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
            patch("sqlspec.migrations.base.await_") as mock_await,
        ):
            mock_loader = Mock()
            mock_loader.validate_migration_file = Mock()
            mock_loader.get_up_sql = Mock()
            mock_loader.get_down_sql = Mock()
            mock_get_loader.return_value = mock_loader

            mock_await.return_value = Mock(return_value=True)

            metadata = runner.load_migration(migration_file)

        assert metadata["version"] == "0001"
        assert metadata["description"] == "data_migration"
        assert metadata["has_upgrade"] is True
        assert metadata["has_downgrade"] is True


def test_get_migration_sql_upgrade_success() -> None:
    """Test successful upgrade SQL generation."""
    runner = create_test_migration_runner()

    migration = {
        "version": "0001",
        "has_upgrade": True,
        "has_downgrade": False,
        "file_path": Path("/test/0001_test.sql"),
        "loader": Mock(),
    }

    with patch("sqlspec.migrations.base.await_") as mock_await:
        mock_await.return_value = Mock(return_value=["CREATE TABLE test (id INTEGER PRIMARY KEY);"])

        result = runner._get_migration_sql(migration, "up")

        assert result is not None
        assert isinstance(result, list)
        assert result == ["CREATE TABLE test (id INTEGER PRIMARY KEY);"]


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

    with patch("sqlspec.migrations.base.await_") as mock_await:
        mock_await.return_value = Mock(return_value=["DROP TABLE test;"])

        result = runner._get_migration_sql(migration, "down")

        assert result is not None
        assert isinstance(result, list)
        assert result == ["DROP TABLE test;"]


def test_get_migration_sql_no_downgrade_warning() -> None:
    """Test warning when no downgrade is available."""
    runner = create_test_migration_runner()

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
        mock_logger.warning.assert_called_once_with("Migration %s has no downgrade query", "0001")


def test_get_migration_sql_no_upgrade_error() -> None:
    """Test error when no upgrade is available."""
    runner = create_test_migration_runner()

    migration = {
        "version": "0001",
        "has_upgrade": False,
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

    with patch("sqlspec.migrations.base.await_") as mock_await:
        mock_await.return_value = Mock(side_effect=Exception("Loader failed to parse migration"))

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

    with patch("sqlspec.migrations.base.await_") as mock_await, patch("sqlspec.migrations.base.logger") as mock_logger:

        def mock_loader_function() -> None:
            raise Exception("Downgrade loader failed")

        mock_await.return_value = mock_loader_function

        result = runner._get_migration_sql(migration, "down")

        assert result is None

        mock_logger.warning.assert_called_once()
        call_args = mock_logger.warning.call_args
        assert call_args[0][1] == "0001"


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

    with patch("sqlspec.migrations.base.await_") as mock_await:
        mock_await.return_value = Mock(return_value=[])

        result = runner._get_migration_sql(migration, "up")

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

    with patch("sqlspec.migrations.base.await_") as mock_await:
        mock_await.return_value = Mock(return_value=None)

        result = runner._get_migration_sql(migration, "up")

        assert result is None


def test_invalid_migration_version_handling() -> None:
    """Test handling of invalid migration version formats."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migrations_path = Path(temp_dir)

        invalid_file = migrations_path / "invalid_version_format.sql"
        invalid_file.write_text("CREATE TABLE test (id INTEGER);")

        runner = create_migration_runner_with_sync_files(migrations_path)
        files = runner.get_migration_files()

        assert len(files) == 0


def test_corrupted_migration_file_handling() -> None:
    """Test handling of corrupted migration files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migrations_path = Path(temp_dir)

        corrupted_file = migrations_path / "0001_corrupted.sql"
        corrupted_file.write_text("This is not a valid migration file content")

        runner = create_migration_runner_with_metadata(migrations_path)

        with patch("sqlspec.migrations.base.get_migration_loader") as mock_get_loader:
            mock_loader = Mock()
            mock_loader.validate_migration_file.side_effect = Exception("Validation failed")
            mock_get_loader.return_value = mock_loader

            with pytest.raises(Exception):
                runner.load_migration(corrupted_file)


def test_missing_migrations_directory() -> None:
    """Test handling when migrations directory is missing."""
    nonexistent_path = Path("/nonexistent/migrations/directory")
    runner = create_migration_runner_with_sync_files(nonexistent_path)

    files = runner.get_migration_files()
    assert files == []


def test_large_migration_file_handling() -> None:
    """Test handling of large migration files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migrations_path = Path(temp_dir)

        large_file = migrations_path / "0001_large_migration.sql"

        large_content_parts = [
            """
-- name: migrate-0001-up
CREATE TABLE large_table (
    id INTEGER PRIMARY KEY,
    data TEXT
);
"""
        ]

        large_content_parts.extend(f"INSERT INTO large_table (data) VALUES ('data_{i:04d}');" for i in range(1000))

        large_content_parts.append("""
-- name: migrate-0001-down
DROP TABLE large_table;
""")

        large_content = "\n".join(large_content_parts)
        large_file.write_text(large_content)

        runner = create_migration_runner_with_metadata(migrations_path)

        with (
            patch.object(type(runner.loader), "clear_cache"),
            patch.object(type(runner.loader), "load_sql"),
            patch.object(type(runner.loader), "has_query", return_value=True),
        ):
            with patch("sqlspec.migrations.base.get_migration_loader") as mock_get_loader:
                mock_loader = Mock()
                mock_loader.validate_migration_file = Mock()
                mock_get_loader.return_value = mock_loader

                metadata = runner.load_migration(large_file)

                assert metadata["version"] == "0001"
                assert metadata["description"] == "large_migration"
                assert len(metadata["checksum"]) == 32


def test_many_migration_files_performance() -> None:
    """Test performance with many migration files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migrations_path = Path(temp_dir)

        for i in range(100):
            migration_file = migrations_path / f"{i + 1:04d}_migration_{i}.sql"
            migration_file.write_text(f"""
-- name: migrate-{i + 1:04d}-up
CREATE TABLE test_table_{i} (id INTEGER PRIMARY KEY);

-- name: migrate-{i + 1:04d}-down
DROP TABLE test_table_{i};
""")

        runner = create_migration_runner_with_sync_files(migrations_path)

        files = runner.get_migration_files()

        assert len(files) == 100

        for i, (version, _) in enumerate(files):
            expected_version = f"{i + 1:04d}"
            assert version == expected_version


def test_sql_loader_caches_files() -> None:
    """Test that SQL migration files leverage CoreSQLFileLoader caching.

    Verifies fix for bug #118 - duplicate SQL loading during migrations.
    The SQLFileLoader should NOT call clear_cache() before operations,
    allowing CoreSQLFileLoader's internal caching to work properly.
    """
    import asyncio

    from sqlspec.migrations.loaders import SQLFileLoader

    with tempfile.TemporaryDirectory() as temp_dir:
        migrations_path = Path(temp_dir)

        migration_file = migrations_path / "0001_test_migration.sql"
        migration_content = """
-- name: migrate-0001-up
CREATE TABLE test (id INTEGER PRIMARY KEY);

-- name: migrate-0001-down
DROP TABLE test;
"""
        migration_file.write_text(migration_content)

        sql_loader = SQLFileLoader()

        async def test_operations() -> None:
            sql_loader.validate_migration_file(migration_file)
            path_str = str(migration_file)
            assert path_str in sql_loader.sql_loader._files
            assert sql_loader.sql_loader.has_query("migrate-0001-up")
            assert sql_loader.sql_loader.has_query("migrate-0001-down")

            await sql_loader.get_up_sql(migration_file)
            assert path_str in sql_loader.sql_loader._files

            await sql_loader.get_down_sql(migration_file)
            assert path_str in sql_loader.sql_loader._files

        asyncio.run(test_operations())


def test_no_duplicate_loading_during_migration_execution() -> None:
    """Test that SQL files are loaded exactly once during migration execution.

    Verifies fix for issue #118 - validates that running a migration loads
    the SQL file only once, not multiple times. Checks that the file is in
    the loader's cache after validation and remains there throughout the workflow.
    """
    import asyncio

    from sqlspec.migrations.loaders import SQLFileLoader

    with tempfile.TemporaryDirectory() as temp_dir:
        migrations_path = Path(temp_dir)

        migration_file = migrations_path / "0001_create_users.sql"
        migration_content = """
-- name: migrate-0001-up
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    username TEXT NOT NULL
);

-- name: migrate-0001-down
DROP TABLE users;
"""
        migration_file.write_text(migration_content)

        sql_loader = SQLFileLoader()

        async def test_migration_workflow() -> None:
            sql_loader.validate_migration_file(migration_file)

            path_str = str(migration_file)
            assert path_str in sql_loader.sql_loader._files, "File should be loaded after validation"
            assert sql_loader.sql_loader.has_query("migrate-0001-up")
            assert sql_loader.sql_loader.has_query("migrate-0001-down")

            file_count_after_validation = len(sql_loader.sql_loader._files)

            await sql_loader.get_up_sql(migration_file)
            file_count_after_up = len(sql_loader.sql_loader._files)
            assert file_count_after_validation == file_count_after_up, "get_up_sql should not load additional files"

            await sql_loader.get_down_sql(migration_file)
            file_count_after_down = len(sql_loader.sql_loader._files)
            assert file_count_after_up == file_count_after_down, "get_down_sql should not load additional files"

        asyncio.run(test_migration_workflow())


def test_sql_file_loader_counter_accuracy_single_file() -> None:
    """Test SQLFileLoader caching behavior for single file loading.

    Verifies fix for issue #118 (Solution 2) - ensures that load_sql()
    properly caches files. First call should load and parse the file,
    second call should return immediately from cache without reparsing.
    """
    from sqlspec.loader import SQLFileLoader

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        test_file = temp_path / "test_queries.sql"
        test_content = """
-- name: get_user
SELECT * FROM users WHERE id = :id;

-- name: list_users
SELECT * FROM users;

-- name: delete_user
DELETE FROM users WHERE id = :id;
"""
        test_file.write_text(test_content)

        loader = SQLFileLoader()

        loader.load_sql(test_file)
        path_str = str(test_file)
        assert path_str in loader._files, "First load should add file to cache"
        assert len(loader._queries) == 3, "First load should parse 3 queries"

        query_count_before_reload = len(loader._queries)
        file_count_before_reload = len(loader._files)

        loader.load_sql(test_file)

        assert len(loader._queries) == query_count_before_reload, "Second load should not add new queries (cached)"
        assert len(loader._files) == file_count_before_reload, "Second load should not add new files (cached)"


def test_sql_file_loader_counter_accuracy_directory() -> None:
    """Test SQLFileLoader caching behavior for directory loading.

    Verifies that _load_directory() properly caches files and doesn't
    reload them on subsequent calls.
    """
    from sqlspec.loader import SQLFileLoader

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        file1 = temp_path / "queries1.sql"
        file1.write_text("""
-- name: query1
SELECT 1;
""")

        file2 = temp_path / "queries2.sql"
        file2.write_text("""
-- name: query2
SELECT 2;
""")

        loader = SQLFileLoader()

        loader.load_sql(temp_path)
        assert len(loader._files) == 2, "First load should add 2 files to cache"
        assert len(loader._queries) == 2, "First load should parse 2 queries"

        query_count_before_reload = len(loader._queries)
        file_count_before_reload = len(loader._files)

        loader.load_sql(temp_path)

        assert len(loader._queries) == query_count_before_reload, "Second load should not add new queries (all cached)"
        assert len(loader._files) == file_count_before_reload, "Second load should not add new files (all cached)"


def test_migration_workflow_single_load_design() -> None:
    """Test that migration workflow respects single-load design.

    Verifies fix for issue #118 (Solution 1) - confirms that:
    1. validate_migration_file() loads the file and parses queries
    2. get_up_sql() retrieves queries WITHOUT reloading the file
    3. get_down_sql() retrieves queries WITHOUT reloading the file

    All three operations should use the same cached file.
    """
    import asyncio

    from sqlspec.migrations.loaders import SQLFileLoader

    with tempfile.TemporaryDirectory() as temp_dir:
        migrations_path = Path(temp_dir)

        migration_file = migrations_path / "0001_test.sql"
        migration_content = """
-- name: migrate-0001-up
CREATE TABLE test_table (id INTEGER);

-- name: migrate-0001-down
DROP TABLE test_table;
"""
        migration_file.write_text(migration_content)

        sql_loader = SQLFileLoader()

        async def test_workflow() -> None:
            sql_loader.validate_migration_file(migration_file)

            path_str = str(migration_file)
            assert path_str in sql_loader.sql_loader._files, "File should be loaded after validation"
            assert sql_loader.sql_loader.has_query("migrate-0001-up")
            assert sql_loader.sql_loader.has_query("migrate-0001-down")

            file_count_before_up = len(sql_loader.sql_loader._files)
            up_sql = await sql_loader.get_up_sql(migration_file)
            file_count_after_up = len(sql_loader.sql_loader._files)

            assert file_count_before_up == file_count_after_up, "get_up_sql() should not load additional files"
            assert len(up_sql) == 1
            assert "CREATE TABLE test_table" in up_sql[0]

            file_count_before_down = len(sql_loader.sql_loader._files)
            down_sql = await sql_loader.get_down_sql(migration_file)
            file_count_after_down = len(sql_loader.sql_loader._files)

            assert (
                file_count_before_down == file_count_after_down
            ), "get_down_sql() should not load additional files"
            assert len(down_sql) == 1
            assert "DROP TABLE test_table" in down_sql[0]

        asyncio.run(test_workflow())


def test_migration_loader_does_not_reload_on_get_sql_calls() -> None:
    """Test that get_up_sql and get_down_sql do not trigger file reloads.

    Verifies that after validate_migration_file() loads the file,
    subsequent calls to get_up_sql() and get_down_sql() retrieve
    the cached queries without calling load_sql() again.
    """
    import asyncio

    from sqlspec.loader import SQLFileLoader as CoreSQLFileLoader
    from sqlspec.migrations.loaders import SQLFileLoader

    with tempfile.TemporaryDirectory() as temp_dir:
        migrations_path = Path(temp_dir)

        migration_file = migrations_path / "0001_schema.sql"
        migration_content = """
-- name: migrate-0001-up
CREATE TABLE products (id INTEGER, name TEXT);

-- name: migrate-0001-down
DROP TABLE products;
"""
        migration_file.write_text(migration_content)

        sql_loader = SQLFileLoader()

        call_counts = {"load_sql": 0}
        original_load_sql = CoreSQLFileLoader.load_sql

        def counting_load_sql(self: CoreSQLFileLoader, *args: Any, **kwargs: Any) -> None:
            call_counts["load_sql"] += 1
            return original_load_sql(self, *args, **kwargs)

        with patch.object(CoreSQLFileLoader, "load_sql", counting_load_sql):

            async def test_no_reload() -> None:
                sql_loader.validate_migration_file(migration_file)
                assert call_counts["load_sql"] == 1, "validate_migration_file should call load_sql exactly once"

                await sql_loader.get_up_sql(migration_file)
                assert call_counts["load_sql"] == 1, "get_up_sql should NOT call load_sql (should use cache)"

                await sql_loader.get_down_sql(migration_file)
                assert call_counts["load_sql"] == 1, "get_down_sql should NOT call load_sql (should use cache)"

            asyncio.run(test_no_reload())
