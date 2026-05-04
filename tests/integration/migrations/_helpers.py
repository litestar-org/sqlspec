"""Shared helpers for migration integration tests."""

from collections.abc import Iterable, Sequence
from pathlib import Path

from sqlspec.adapters.sqlite import SqliteDriver
from sqlspec.migrations.fix import MigrationFixer
from sqlspec.migrations.runner import SyncMigrationRunner
from sqlspec.migrations.tracker import SyncMigrationTracker
from sqlspec.migrations.version import generate_conversion_map


def write_migration_files(migrations_dir: Path, migrations: Iterable[tuple[str, str]]) -> None:
    """Write migration files into a temporary migration directory."""
    for filename, content in migrations:
        (migrations_dir / filename).write_text(content)


def load_migration_checksums(migrations_dir: Path) -> dict[str, str]:
    """Load checksums keyed by migration version from the current migration files."""
    runner = SyncMigrationRunner(migrations_dir)
    return {
        version: runner.load_migration(file_path, version=version)["checksum"]
        for version, file_path in runner.get_migration_files()
    }


def record_all_migrations(
    migrations_dir: Path, sqlite_session: SqliteDriver
) -> tuple[list[tuple[str, Path]], dict[str, str]]:
    """Execute and record all migrations, returning file entries and original checksums."""
    tracker = SyncMigrationTracker()
    tracker.ensure_tracking_table(sqlite_session)

    runner = SyncMigrationRunner(migrations_dir)
    migration_files = runner.get_migration_files()
    original_checksums: dict[str, str] = {}

    for version, file_path in migration_files:
        migration = runner.load_migration(file_path, version=version)
        runner.execute_upgrade(sqlite_session, migration)
        tracker.record_migration(
            sqlite_session, migration["version"], migration["description"], 100, migration["checksum"]
        )
        original_checksums[version] = migration["checksum"]

    return migration_files, original_checksums


def apply_fix_once(migrations_dir: Path, migration_files: Sequence[tuple[str, Path]] | None = None) -> dict[str, str]:
    """Apply one timestamp-to-sequential fix pass and return the conversion map."""
    files = (
        list(migration_files)
        if migration_files is not None
        else SyncMigrationRunner(migrations_dir).get_migration_files()
    )
    conversion_map = generate_conversion_map(files)
    fixer = MigrationFixer(migrations_dir)
    renames = fixer.plan_renames(conversion_map)
    fixer.apply_renames(renames)
    return conversion_map


def update_version_records(sqlite_session: SqliteDriver, conversion_map: dict[str, str]) -> None:
    """Apply migration version updates for a conversion map."""
    tracker = SyncMigrationTracker()
    for old_version, new_version in conversion_map.items():
        tracker.update_version_record(sqlite_session, old_version, new_version)
