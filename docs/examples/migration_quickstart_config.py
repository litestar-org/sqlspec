"""Declarative SQLite migration configuration used by the CLI quickstart."""

from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("database_config",)

database_config = SqliteConfig(
    bind_key="app",
    connection_config={"database": "app.db"},
    migration_config={"script_location": "migrations", "version_table_name": "schema_versions"},
)
