"""Squash incremental SQL migrations into canonical DDL and export schema.sql."""

import tempfile
from pathlib import Path

from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.migrations.commands import SyncMigrationCommands

__all__ = ("main",)

FIRST_MIGRATION = """-- name: migrate-0001-up
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    email TEXT NOT NULL UNIQUE
);

-- name: migrate-0001-down
DROP TABLE users;
"""

SECOND_MIGRATION = """-- name: migrate-0002-up
ALTER TABLE users ADD COLUMN display_name TEXT;

-- name: migrate-0002-down
ALTER TABLE users DROP COLUMN display_name;
"""


def main() -> tuple[str, bool, str]:
    """Apply, verify, squash into canonical DDL, and dump the live SQLite schema."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        base_dir = Path(tmp_dir)
        migration_dir = base_dir / "migrations"
        db_path = base_dir / "app.sqlite"

        # --8<-- [start:example]
        config = SqliteConfig(
            connection_config={"database": str(db_path)},
            migration_config={"script_location": str(migration_dir), "echo": False},
        )
        try:
            commands = SyncMigrationCommands(config)
            commands.init(str(migration_dir), package=False)
            (migration_dir / "0001_create_users.sql").write_text(FIRST_MIGRATION, encoding="utf-8")
            (migration_dir / "0002_add_display_name.sql").write_text(SECOND_MIGRATION, encoding="utf-8")

            commands.upgrade(echo=False)
            report = commands.verify(strict=True, echo=False)
            assert report["valid"] is True

            commands.squash("0001", "0002", "initial_baseline", strategy="ddl", yes=True)
            squashed_file = next(migration_dir.glob("0001_*.sql"))
            squashed_sql = squashed_file.read_text(encoding="utf-8")
            assert "-- Squashed from: 0001, 0002" in squashed_sql
            assert "display_name" in squashed_sql

            schema_sql = commands.dump_schema(migration_dir / "schema.sql", echo=False)
            # --8<-- [end:example]
            assert "CREATE TABLE" in schema_sql
            return squashed_file.name, bool(report["valid"]), schema_sql
        finally:
            config.close_pool()


if __name__ == "__main__":
    print(main())
