"""Test migration context functionality."""

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
