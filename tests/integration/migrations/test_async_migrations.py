"""Integration tests for async migrations functionality."""

import asyncio
from pathlib import Path

import pytest

from sqlspec.migrations.context import MigrationContext
from sqlspec.migrations.loaders import PythonFileLoader
from sqlspec.migrations.runner import create_migration_runner

pytestmark = pytest.mark.xdist_group("migrations")


def test_async_migration_context_properties() -> None:
    """Test async migration context properties."""
    context = MigrationContext(dialect="postgres")

    # Test execution mode detection
    assert context.execution_mode == "sync"

    # Test metadata operations
    context.set_execution_metadata("test_key", "test_value")
    assert context.get_execution_metadata("test_key") == "test_value"


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
