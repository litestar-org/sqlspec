"""Declarative SQLite migration configuration used by the CLI quickstart."""

# --8<-- [start:example]
from sqlspec.adapters.sqlite import SqliteConfig

database_config = SqliteConfig(
    bind_key="app",
    connection_config={"database": "app.db"},
    migration_config={
        "script_location": "migrations",
        "version_table_name": "schema_versions",
        "auto_dump_schema": True,
        "schema_dump_path": "migrations/schema.sql",
    },
)
# --8<-- [end:example]

__all__ = ("database_config",)
