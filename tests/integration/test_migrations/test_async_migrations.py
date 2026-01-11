"""Integration tests for async migrations functionality."""

import asyncio
from pathlib import Path
from unittest.mock import Mock

import pytest

import sqlspec.utils.config_tools
from sqlspec.migrations.context import MigrationContext
from sqlspec.migrations.loaders import PythonFileLoader
from sqlspec.migrations.runner import AsyncMigrationRunner, SyncMigrationRunner, create_migration_runner
from sqlspec.utils.config_tools import resolve_config_async
from sqlspec.utils.sync_tools import run_

pytestmark = pytest.mark.xdist_group("migrations")


def test_async_migration_context_properties() -> None:
    """Test async migration context properties."""
    context = MigrationContext(dialect="postgres")

    # Test execution mode detection
    assert context.execution_mode == "sync"

    # Test metadata operations
    context.set_execution_metadata("test_key", "test_value")
    assert context.get_execution_metadata("test_key") == "test_value"


def test_sync_callable_config_resolution() -> None:
    """Test resolving synchronous callable config."""
    mock_config = Mock()
    mock_config.database_url = "sqlite:///test.db"
    mock_config.bind_key = "test"
    mock_config.migration_config = {}

    # Create a config factory function
    def get_test_config() -> Mock:
        return mock_config

    async def _test() -> None:
        # Mock the import_string to return our function

        original_import = sqlspec.utils.config_tools.import_string

        try:
            sqlspec.utils.config_tools.import_string = lambda path: get_test_config
            result = await resolve_config_async("test.config.get_database_config")
            assert result is mock_config
        finally:
            sqlspec.utils.config_tools.import_string = original_import

    run_(_test)()


def test_async_callable_config_resolution() -> None:
    """Test resolving asynchronous callable config."""
    mock_config = Mock()
    mock_config.database_url = "sqlite:///test.db"
    mock_config.bind_key = "test"
    mock_config.migration_config = {}

    # Create an async config factory function
    async def get_test_config() -> Mock:
        return mock_config

    async def _test() -> None:
        # Mock the import_string to return our async function

        original_import = sqlspec.utils.config_tools.import_string

        try:
            sqlspec.utils.config_tools.import_string = lambda path: get_test_config
            result = await resolve_config_async("test.config.async_get_database_config")
            assert result is mock_config
        finally:
            sqlspec.utils.config_tools.import_string = original_import

    run_(_test)()


def test_sync_migration_runner_instantiation(tmp_path: Path) -> None:
    """Test sync migration runner instantiation."""
    migration_dir = tmp_path / "migrations"
    migration_dir.mkdir()

    mock_config = Mock()
    mock_config.database_url = "sqlite:///test.db"
    mock_config.bind_key = "test"
    mock_config.migration_config = {"script_location": "migrations", "version_table_name": "alembic_version"}

    context = MigrationContext.from_config(mock_config)
    runner = SyncMigrationRunner(migration_dir, {}, context, {})

    # Verify it's a sync runner
    assert isinstance(runner, SyncMigrationRunner)
    assert hasattr(runner, "load_migration")
    assert hasattr(runner, "execute_upgrade")


def test_async_migration_runner_instantiation(tmp_path: Path) -> None:
    """Test async migration runner instantiation."""

    migration_dir = tmp_path / "migrations"
    migration_dir.mkdir()

    mock_config = Mock()
    mock_config.database_url = "sqlite:///test.db"
    mock_config.bind_key = "test"
    mock_config.migration_config = {"script_location": "migrations", "version_table_name": "alembic_version"}

    context = MigrationContext.from_config(mock_config)
    runner = AsyncMigrationRunner(migration_dir, {}, context, {})

    # Verify it's an async runner
    assert isinstance(runner, AsyncMigrationRunner)
    assert hasattr(runner, "load_migration")
    assert hasattr(runner, "execute_upgrade")

    # Verify methods are async
    import inspect

    assert inspect.iscoroutinefunction(runner.load_migration)
    assert inspect.iscoroutinefunction(runner.execute_upgrade)


def test_async_python_migration_execution(tmp_path: Path) -> None:
    """Test execution of async Python migration."""
    migration_dir = tmp_path / "migrations"
    migration_dir.mkdir()

    # Create async Python migration file
    migration_file = migration_dir / "0001_create_users_async.py"
    migration_content = '''"""Create users table with async validation."""

async def up(context):
    """Create users table."""
    return [
        """
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    ]

async def down(context):
    """Drop users table."""
    return ["DROP TABLE users;"]
'''
    migration_file.write_text(migration_content)

    # Test loading the migration

    context = MigrationContext(dialect="postgres")
    loader = PythonFileLoader(migration_dir, tmp_path, context)

    # Test async execution
    async def test_async_loading() -> None:
        up_sql = await loader.get_up_sql(migration_file)
        assert len(up_sql) == 1
        assert "CREATE TABLE users" in up_sql[0]

        down_sql = await loader.get_down_sql(migration_file)
        assert len(down_sql) == 1
        assert "DROP TABLE users" in down_sql[0]

    asyncio.run(test_async_loading())


def test_mixed_sync_async_migration_loading(tmp_path: Path) -> None:
    """Test loading both sync and async migrations in the same directory."""
    migration_dir = tmp_path / "migrations"
    migration_dir.mkdir()

    # Create sync migration
    sync_migration = migration_dir / "0001_sync_migration.py"
    sync_migration.write_text("""
def up(context):
    return ["CREATE TABLE sync_test (id INT);"]

def down(context):
    return ["DROP TABLE sync_test;"]
""")

    # Create async migration
    async_migration = migration_dir / "0002_async_migration.py"
    async_migration.write_text("""
async def up(context):
    return ["CREATE TABLE async_test (id INT);"]

async def down(context):
    return ["DROP TABLE async_test;"]
""")

    mock_config = Mock()
    mock_config.database_url = "sqlite:///test.db"
    mock_config.bind_key = "test"
    mock_config.migration_config = {"script_location": "migrations", "version_table_name": "alembic_version"}

    context = MigrationContext(dialect="postgres")
    runner = create_migration_runner(migration_dir, {}, context, {}, is_async=False)

    # Get migration files
    migrations = runner.get_migration_files()
    assert len(migrations) == 2

    # Verify both migrations are loaded
    versions = [version for version, _ in migrations]
    assert "0001" in versions
    assert "0002" in versions


def test_migration_context_validation() -> None:
    """Test migration context async usage validation."""
    context = MigrationContext()

    # Test with sync function
    def sync_migration() -> list[str]:
        return ["CREATE TABLE test (id INT);"]

    # Should not raise any exceptions
    context.validate_async_usage(sync_migration)

    # Test with async function
    async def async_migration() -> list[str]:
        return ["CREATE TABLE test (id INT);"]

    # Should handle async function validation
    context.validate_async_usage(async_migration)


def test_error_handling_in_async_migrations(tmp_path: Path) -> None:
    """Test error handling in async migration execution."""
    migration_dir = tmp_path / "migrations"
    migration_dir.mkdir()

    # Create migration with error
    error_migration = migration_dir / "0001_error_migration.py"
    error_migration.write_text("""
async def up(context):
    raise ValueError("Test error in migration")

def down(context):
    return ["DROP TABLE test;"]
""")

    context = MigrationContext(dialect="postgres")
    loader = PythonFileLoader(migration_dir, tmp_path, context)

    # Test that error is properly raised
    async def test_error_handling() -> None:
        with pytest.raises(Exception):  # Should raise the ValueError from migration
            await loader.get_up_sql(error_migration)

    asyncio.run(test_error_handling())


def test_config_resolver_with_list_configs() -> None:
    """Test config resolver with list of configurations."""
    mock_config1 = Mock()
    mock_config1.database_url = "sqlite:///test1.db"
    mock_config1.bind_key = "test1"
    mock_config1.migration_config = {}

    mock_config2 = Mock()
    mock_config2.database_url = "sqlite:///test2.db"
    mock_config2.bind_key = "test2"
    mock_config2.migration_config = {}

    def get_configs() -> list[Mock]:
        return [mock_config1, mock_config2]

    async def _test() -> None:
        # Mock the import_string to return our function

        original_import = sqlspec.utils.config_tools.import_string

        try:
            sqlspec.utils.config_tools.import_string = lambda path: get_configs
            result = await resolve_config_async("test.config.get_database_configs")
            assert isinstance(result, list)
            assert len(result) == 2
            assert result[0] is mock_config1
            assert result[1] is mock_config2
        finally:
            sqlspec.utils.config_tools.import_string = original_import

    run_(_test)()
