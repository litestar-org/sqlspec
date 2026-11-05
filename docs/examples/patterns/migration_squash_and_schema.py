"""Squash incremental SQL migrations into canonical DDL and export schema.sql."""

import tempfile
from pathlib import Path

# --8<-- [start:example]
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.migrations.commands import SyncMigrationCommands

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


def run_squash_and_dump(base_dir: Path) -> tuple[str, bool, str, str]:
    """Apply, verify, squash into canonical DDL, and dump the live schema."""
    migration_dir = base_dir / "migrations"
    db_path = base_dir / "app.sqlite"
    conn_cfg = {"database": str(db_path)}
    mig_cfg = {"script_location": str(migration_dir), "echo": False}
    config = SqliteConfig(connection_config=conn_cfg, migration_config=mig_cfg)
    commands = SyncMigrationCommands(config)
    commands.init(str(migration_dir), package=False)
    rev1 = migration_dir / "0001_create_users.sql"
    rev2 = migration_dir / "0002_add_display_name.sql"
    rev1.write_text(FIRST_MIGRATION, encoding="utf-8")
    rev2.write_text(SECOND_MIGRATION, encoding="utf-8")

    try:
        commands.upgrade(echo=False)
        report = commands.verify(strict=True, echo=False)
        commands.squash("0001", "0002", "baseline", strategy="ddl", yes=True)
        squashed_file = next(migration_dir.glob("0001_*.sql"))
        squashed_sql = squashed_file.read_text(encoding="utf-8")
        schema_path = migration_dir / "schema.sql"
        schema_sql = commands.dump_schema(schema_path, echo=False)
        return squashed_file.name, bool(report["valid"]), squashed_sql, schema_sql
    finally:
        config.close_pool()


# --8<-- [end:example]

__all__ = ("FIRST_MIGRATION", "SECOND_MIGRATION", "main", "run_squash_and_dump")


def main() -> tuple[str, bool, str]:
    """Apply, verify, squash into canonical DDL, and dump the live SQLite schema."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        name, is_valid, squashed_sql, schema_sql = run_squash_and_dump(Path(tmp_dir))
        assert is_valid is True
        assert "-- Squashed from: 0001, 0002" in squashed_sql
        assert "display_name" in squashed_sql
        assert "CREATE TABLE" in schema_sql
        return name, is_valid, schema_sql


if __name__ == "__main__":
    print(main())
