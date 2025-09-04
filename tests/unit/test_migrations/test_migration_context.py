"""Test migration context functionality."""

from pathlib import Path

from sqlspec.adapters.psycopg.config import PsycopgSyncConfig
from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.migrations.context import MigrationContext


def test_migration_context_from_sqlite_config() -> None:
    """Test creating migration context from SQLite config."""
    config = SqliteConfig(pool_config={"database": ":memory:"})
    context = MigrationContext.from_config(config)

    assert context.dialect == "sqlite"
    assert context.config is config
    assert context.driver is None
    assert context.metadata == {}


def test_migration_context_from_postgres_config() -> None:
    """Test creating migration context from PostgreSQL config."""
    config = PsycopgSyncConfig(pool_config={"host": "localhost", "dbname": "test", "user": "test", "password": "test"})
    context = MigrationContext.from_config(config)

    # PostgreSQL config should have postgres dialect
    assert context.dialect in {"postgres", "postgresql"}
    assert context.config is config


def test_migration_context_manual_creation() -> None:
    """Test manually creating migration context."""
    context = MigrationContext(dialect="mysql", metadata={"custom_key": "custom_value"})

    assert context.dialect == "mysql"
    assert context.config is None
    assert context.driver is None
    assert context.metadata == {"custom_key": "custom_value"}


def test_migration_function_with_context() -> None:
    """Test that migration functions can receive context."""
    import importlib.util

    # Load the migration module dynamically
    migration_path = (
        Path(__file__).parent.parent.parent.parent
        / "sqlspec/extensions/litestar/migrations/0001_create_session_table.py"
    )
    spec = importlib.util.spec_from_file_location("migration", migration_path)
    migration_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration_module)

    up = migration_module.up
    down = migration_module.down

    # Test with SQLite context
    sqlite_context = MigrationContext(dialect="sqlite")
    sqlite_up_sql = up(sqlite_context)

    assert isinstance(sqlite_up_sql, list)
    assert len(sqlite_up_sql) == 2  # CREATE TABLE and CREATE INDEX

    # Check that SQLite uses TEXT for data column
    create_table_sql = sqlite_up_sql[0]
    assert "TEXT" in create_table_sql
    assert "DATETIME" in create_table_sql

    # Test with PostgreSQL context
    postgres_context = MigrationContext(dialect="postgres")
    postgres_up_sql = up(postgres_context)

    # Check that PostgreSQL uses JSONB
    create_table_sql = postgres_up_sql[0]
    assert "JSONB" in create_table_sql
    assert "TIMESTAMP WITH TIME ZONE" in create_table_sql

    # Test down migration
    down_sql = down(sqlite_context)
    assert isinstance(down_sql, list)
    assert len(down_sql) == 2  # DROP INDEX and DROP TABLE
    assert "DROP TABLE" in down_sql[1]


def test_migration_function_without_context() -> None:
    """Test that migration functions work without context (fallback)."""
    import importlib.util

    # Load the migration module dynamically
    migration_path = (
        Path(__file__).parent.parent.parent.parent
        / "sqlspec/extensions/litestar/migrations/0001_create_session_table.py"
    )
    spec = importlib.util.spec_from_file_location("migration", migration_path)
    migration_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration_module)

    up = migration_module.up
    down = migration_module.down

    # Should use generic fallback when no context
    up_sql = up()

    assert isinstance(up_sql, list)
    assert len(up_sql) == 2

    # Should use TEXT as fallback
    create_table_sql = up_sql[0]
    assert "TEXT" in create_table_sql

    # Down should also work without context
    down_sql = down()
    assert isinstance(down_sql, list)
    assert len(down_sql) == 2
