"""Integration tests for checksum stability during fix operations."""

from collections.abc import Generator
from pathlib import Path

import pytest

from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.migrations.fix import MigrationFixer
from sqlspec.migrations.runner import SyncMigrationRunner
from sqlspec.migrations.tracker import SyncMigrationTracker
from sqlspec.migrations.version import generate_conversion_map


@pytest.fixture
def sqlite_config() -> Generator[SqliteConfig, None, None]:
    """Create SQLite config for migration testing."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    yield config
    config.close_pool()


@pytest.fixture
def sqlite_session(sqlite_config: SqliteConfig) -> Generator[SqliteDriver, None, None]:
    """Create SQLite session for migration testing."""
    with sqlite_config.provide_session() as session:
        yield session


@pytest.fixture
def migrations_dir(tmp_path: Path) -> Path:
    """Create temporary migrations directory."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    return migrations_dir


def test_checksum_stable_after_fix_sql_migration(
    migrations_dir: Path, sqlite_session: SqliteDriver, sqlite_config: SqliteConfig
) -> None:
    """Test checksum remains stable when converting SQL migration from timestamp to sequential."""
    sql_content = """-- name: migrate-20251011120000-up
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE
);

-- name: migrate-20251011120000-down
DROP TABLE users;
"""

    sql_file = migrations_dir / "20251011120000_create_users.sql"
    sql_file.write_text(sql_content)

    tracker = SyncMigrationTracker()
    tracker.ensure_tracking_table(sqlite_session)

    runner = SyncMigrationRunner(migrations_dir)
    migration_files = runner.get_migration_files()
    migration = runner.load_migration(migration_files[0][1], version=migration_files[0][0])

    runner.execute_upgrade(sqlite_session, migration)
    tracker.record_migration(sqlite_session, migration["version"], migration["description"], 100, migration["checksum"])

    applied = tracker.get_applied_migrations(sqlite_session)
    original_checksum = applied[0]["checksum"]

    fixer = MigrationFixer(migrations_dir)
    conversion_map = generate_conversion_map(migration_files)
    renames = fixer.plan_renames(conversion_map)
    fixer.apply_renames(renames)

    runner_after = SyncMigrationRunner(migrations_dir)
    migration_files_after = runner_after.get_migration_files()
    migration_after = runner_after.load_migration(migration_files_after[0][1], version=migration_files_after[0][0])

    new_checksum = migration_after["checksum"]

    assert new_checksum == original_checksum


def test_multiple_migrations_checksums_stable(
    migrations_dir: Path, sqlite_session: SqliteDriver, sqlite_config: SqliteConfig
) -> None:
    """Test all migration checksums remain stable during batch conversion."""
    migrations = [
        (
            "20251011120000_create_users.sql",
            """-- name: migrate-20251011120000-up
CREATE TABLE users (id INTEGER PRIMARY KEY);

-- name: migrate-20251011120000-down
DROP TABLE users;
""",
        ),
        (
            "20251012130000_create_products.sql",
            """-- name: migrate-20251012130000-up
CREATE TABLE products (id INTEGER PRIMARY KEY);

-- name: migrate-20251012130000-down
DROP TABLE products;
""",
        ),
        (
            "20251013140000_create_orders.sql",
            """-- name: migrate-20251013140000-up
CREATE TABLE orders (id INTEGER PRIMARY KEY);

-- name: migrate-20251013140000-down
DROP TABLE orders;
""",
        ),
    ]

    for filename, content in migrations:
        (migrations_dir / filename).write_text(content)

    tracker = SyncMigrationTracker()
    tracker.ensure_tracking_table(sqlite_session)

    runner = SyncMigrationRunner(migrations_dir)
    migration_files = runner.get_migration_files()

    original_checksums = {}
    for version, file_path in migration_files:
        migration = runner.load_migration(file_path, version=version)
        runner.execute_upgrade(sqlite_session, migration)
        tracker.record_migration(
            sqlite_session, migration["version"], migration["description"], 100, migration["checksum"]
        )
        original_checksums[version] = migration["checksum"]

    fixer = MigrationFixer(migrations_dir)
    conversion_map = generate_conversion_map(migration_files)
    renames = fixer.plan_renames(conversion_map)
    fixer.apply_renames(renames)

    runner_after = SyncMigrationRunner(migrations_dir)
    migration_files_after = runner_after.get_migration_files()

    for version, file_path in migration_files_after:
        migration = runner_after.load_migration(file_path, version=version)
        new_checksum = migration["checksum"]

        old_version = next(k for k, v in conversion_map.items() if v == version)
        original_checksum = original_checksums[old_version]

        assert new_checksum == original_checksum


def test_checksum_stability_with_complex_sql(
    migrations_dir: Path, sqlite_session: SqliteDriver, sqlite_config: SqliteConfig
) -> None:
    """Test checksum stability with complex SQL containing version references."""
    sql_content = """-- name: migrate-20251011120000-up
-- This migration creates users table
-- Previous migration: migrate-20251010110000-up
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL CHECK (name != 'migrate-20251011120000-up'),
    metadata TEXT DEFAULT '-- name: some-pattern-up'
);

-- Comment about migrate-20251011120000-up
INSERT INTO users (name) VALUES ('test migrate-20251011120000-up reference');

-- name: migrate-20251011120000-down
DROP TABLE users;
"""

    sql_file = migrations_dir / "20251011120000_create_users.sql"
    sql_file.write_text(sql_content)

    tracker = SyncMigrationTracker()
    tracker.ensure_tracking_table(sqlite_session)

    runner = SyncMigrationRunner(migrations_dir)
    migration_files = runner.get_migration_files()
    migration = runner.load_migration(migration_files[0][1], version=migration_files[0][0])

    original_checksum = migration["checksum"]

    fixer = MigrationFixer(migrations_dir)
    conversion_map = generate_conversion_map(migration_files)
    renames = fixer.plan_renames(conversion_map)
    fixer.apply_renames(renames)

    runner_after = SyncMigrationRunner(migrations_dir)
    migration_files_after = runner_after.get_migration_files()
    migration_after = runner_after.load_migration(migration_files_after[0][1], version=migration_files_after[0][0])

    new_checksum = migration_after["checksum"]

    assert new_checksum == original_checksum

    converted_content = (migrations_dir / "0001_create_users.sql").read_text()
    assert "-- name: migrate-0001-up" in converted_content
    assert "-- name: migrate-0001-down" in converted_content
    assert "migrate-20251010110000-up" in converted_content
    assert "CHECK (name != 'migrate-20251011120000-up')" in converted_content
    assert "metadata TEXT DEFAULT '-- name: some-pattern-up'" in converted_content
