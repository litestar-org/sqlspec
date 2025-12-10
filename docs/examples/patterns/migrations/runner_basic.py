"""Minimal migration workflow powered by SyncMigrationCommands."""

from pathlib import Path

from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.migrations.commands import SyncMigrationCommands

__all__ = ("main",)


MIGRATIONS_PATH = Path(__file__).with_name("files")


def _config() -> "SqliteConfig":
    """Return a SQLite config pointing at the bundled migration files."""
    return SqliteConfig(
        connection_config={"database": ":memory:"}, migration_config={"script_location": str(MIGRATIONS_PATH)}
    )


def main() -> None:
    """Apply the demo migration, show row counts, and roll back."""
    config = _config()
    commands = SyncMigrationCommands(config)
    commands.upgrade()
    with config.provide_session() as session:
        rows = session.select("SELECT COUNT(*) AS total FROM articles")
        total = rows[0]["total"] if rows else 0
        print({"articles": total})
    commands.downgrade()


if __name__ == "__main__":
    main()
