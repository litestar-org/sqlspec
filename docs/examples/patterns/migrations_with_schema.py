"""Run DuckDB migrations targeting separate application and version-tracking schemas."""

import tempfile
from pathlib import Path

# --8<-- [start:example]
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.migrations.commands import SyncMigrationCommands

MIGRATION_SCRIPT = '''"""Create users."""


def up() -> list[str]:
    """Create an unqualified table in app_schema."""
    return [
        """
        CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL)
        """
    ]


def down() -> list[str]:
    """Drop the unqualified table from app_schema."""
    return ["DROP TABLE IF EXISTS users"]
'''


def run_multi_schema_migration(base_dir: Path) -> DuckDBConfig:
    """Apply a migration across app_schema and admin_schema in DuckDB."""
    migration_dir = base_dir / "migrations"
    db_path = base_dir / "app.duckdb"
    config = DuckDBConfig(
        connection_config={"database": str(db_path)},
        migration_config={
            "script_location": str(migration_dir),
            "version_table_name": "schema_versions",
            "default_schema": "app_schema",
            "version_table_schema": "admin_schema",
        },
    )

    with config.provide_session() as session:
        session.execute("CREATE SCHEMA app_schema")
        session.execute("CREATE SCHEMA admin_schema")

    commands = SyncMigrationCommands(config)
    commands.init(str(migration_dir), package=True)
    revision_file = migration_dir / "0001_create_users.py"
    revision_file.write_text(MIGRATION_SCRIPT, encoding="utf-8")
    commands.upgrade()
    return config


# --8<-- [end:example]

__all__ = ("MIGRATION_SCRIPT", "main", "run_multi_schema_migration")


def main() -> tuple[str, str]:
    """Apply a Python migration script across app_schema and admin_schema in DuckDB."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        config = run_multi_schema_migration(Path(tmp_dir))

        try:
            with config.provide_session() as session:
                users_table = str(
                    session.select_value(
                        """
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = ? AND table_name = ?
                        """,
                        "app_schema",
                        "users",
                    )
                )
                tracker_table = str(
                    session.select_value(
                        """
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = ? AND table_name = ?
                        """,
                        "admin_schema",
                        "schema_versions",
                    )
                )

            assert users_table == "users"
            assert tracker_table == "schema_versions"
            return users_table, tracker_table
        finally:
            if config.connection_instance:
                config.close_pool()


if __name__ == "__main__":
    print(main())
