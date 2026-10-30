"""Run DuckDB migrations targeting separate application and version-tracking schemas."""

import tempfile
from pathlib import Path

from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.migrations.commands import SyncMigrationCommands

__all__ = ("main",)

MIGRATION_SCRIPT = '''"""Create users."""


def up() -> list[str]:
    """Create an unqualified table in app_schema."""
    return ["CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL)"]


def down() -> list[str]:
    """Drop the unqualified table from app_schema."""
    return ["DROP TABLE IF EXISTS users"]
'''


def main() -> tuple[str, str]:
    """Apply a Python migration script across app_schema and admin_schema in DuckDB."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        base_dir = Path(tmp_dir)
        migration_dir = base_dir / "migrations"
        db_path = base_dir / "app.duckdb"

        # --8<-- [start:example]
        config = DuckDBConfig(
            connection_config={"database": str(db_path)},
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "schema_versions",
                "default_schema": "app_schema",
                "version_table_schema": "admin_schema",
            },
        )

        try:
            with config.provide_session() as session:
                session.execute("CREATE SCHEMA app_schema")
                session.execute("CREATE SCHEMA admin_schema")

            commands = SyncMigrationCommands(config)
            commands.init(str(migration_dir), package=True)
            (migration_dir / "0001_create_users.py").write_text(MIGRATION_SCRIPT, encoding="utf-8")
            commands.upgrade()
            # --8<-- [end:example]

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
