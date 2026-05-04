"""Integration tests for migration fix workflow stability and idempotency."""

from pathlib import Path

import pytest

from sqlspec.adapters.sqlite import SqliteDriver
from sqlspec.migrations.runner import SyncMigrationRunner
from sqlspec.migrations.tracker import SyncMigrationTracker
from sqlspec.migrations.version import generate_conversion_map
from tests.integration.migrations._helpers import (
    apply_fix_once,
    load_migration_checksums,
    record_all_migrations,
    update_version_records,
    write_migration_files,
)

SINGLE_TIMESTAMP_MIGRATION = (
    "20251011120000_create_users.sql",
    """-- name: migrate-20251011120000-up
CREATE TABLE users (id INTEGER PRIMARY KEY);

-- name: migrate-20251011120000-down
DROP TABLE users;
""",
)

MULTIPLE_TIMESTAMP_MIGRATIONS = (
    SINGLE_TIMESTAMP_MIGRATION,
    (
        "20251012130000_create_products.sql",
        """-- name: migrate-20251012130000-up
CREATE TABLE products (id INTEGER PRIMARY KEY);

-- name: migrate-20251012130000-down
DROP TABLE products;
""",
    ),
)


@pytest.mark.parametrize(
    ("migrations", "expected_files", "expected_content"),
    [
        pytest.param(
            (
                (
                    "20251011120000_create_users.sql",
                    """-- name: migrate-20251011120000-up
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE
);

-- name: migrate-20251011120000-down
DROP TABLE users;
""",
                ),
            ),
            ("0001_create_users.sql",),
            (),
            id="single-sql",
        ),
        pytest.param(
            (
                SINGLE_TIMESTAMP_MIGRATION,
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
            ),
            ("0001_create_users.sql", "0002_create_products.sql", "0003_create_orders.sql"),
            (),
            id="multiple-sql",
        ),
        pytest.param(
            (
                (
                    "20251011120000_create_users.sql",
                    """-- name: migrate-20251011120000-up
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
""",
                ),
            ),
            ("0001_create_users.sql",),
            (
                "-- name: migrate-0001-up",
                "-- name: migrate-0001-down",
                "migrate-20251010110000-up",
                "CHECK (name != 'migrate-20251011120000-up')",
                "metadata TEXT DEFAULT '-- name: some-pattern-up'",
            ),
            id="complex-sql-version-references",
        ),
    ],
)
def test_checksums_remain_stable_after_fix(
    migrations_dir: Path,
    sqlite_session: SqliteDriver,
    migrations: tuple[tuple[str, str], ...],
    expected_files: tuple[str, ...],
    expected_content: tuple[str, ...],
) -> None:
    """Test migration checksums remain stable during timestamp-to-sequential conversion."""
    write_migration_files(migrations_dir, migrations)
    migration_files, original_checksums = record_all_migrations(migrations_dir, sqlite_session)

    conversion_map = apply_fix_once(migrations_dir, migration_files)
    converted_checksums = load_migration_checksums(migrations_dir)

    assert set(converted_checksums) == set(conversion_map.values())
    for old_version, new_version in conversion_map.items():
        assert converted_checksums[new_version] == original_checksums[old_version]

    for filename in expected_files:
        assert (migrations_dir / filename).exists()

    if expected_content:
        converted_content = (migrations_dir / expected_files[0]).read_text()
        for expected in expected_content:
            assert expected in converted_content


@pytest.mark.parametrize(
    ("migrations", "expected_versions"),
    [
        pytest.param((SINGLE_TIMESTAMP_MIGRATION,), {"0001"}, id="single-sql"),
        pytest.param(MULTIPLE_TIMESTAMP_MIGRATIONS, {"0001", "0002"}, id="multiple-sql"),
        pytest.param(
            (
                (
                    "0001_init.sql",
                    """-- name: migrate-0001-up
CREATE TABLE init (id INTEGER PRIMARY KEY);

-- name: migrate-0001-down
DROP TABLE init;
""",
                ),
                SINGLE_TIMESTAMP_MIGRATION,
            ),
            {"0001", "0002"},
            id="mixed-sequential-and-timestamp",
        ),
    ],
)
def test_fix_command_idempotent_for_applied_migrations(
    migrations_dir: Path,
    sqlite_session: SqliteDriver,
    migrations: tuple[tuple[str, str], ...],
    expected_versions: set[str],
) -> None:
    """Test fix record updates can be repeated without changing applied versions."""
    write_migration_files(migrations_dir, migrations)
    migration_files, _ = record_all_migrations(migrations_dir, sqlite_session)

    conversion_map = apply_fix_once(migrations_dir, migration_files)
    update_version_records(sqlite_session, conversion_map)
    update_version_records(sqlite_session, conversion_map)

    applied = SyncMigrationTracker().get_applied_migrations(sqlite_session)
    assert {row["version_num"] for row in applied} == expected_versions


def test_fix_command_ci_rerun_has_no_pending_conversion(migrations_dir: Path, sqlite_session: SqliteDriver) -> None:
    """Test simulated CI workflow where fix runs on every commit."""
    write_migration_files(migrations_dir, (SINGLE_TIMESTAMP_MIGRATION,))
    migration_files, _ = record_all_migrations(migrations_dir, sqlite_session)

    conversion_map = apply_fix_once(migrations_dir, migration_files)
    update_version_records(sqlite_session, conversion_map)

    files_after_first = SyncMigrationRunner(migrations_dir).get_migration_files()
    assert generate_conversion_map(files_after_first) == {}

    update_version_records(sqlite_session, conversion_map)

    applied = SyncMigrationTracker().get_applied_migrations(sqlite_session)
    assert len(applied) == 1
    assert applied[0]["version_num"] == "0001"


def test_fix_command_noop_for_already_converted_migration(migrations_dir: Path, sqlite_session: SqliteDriver) -> None:
    """Test developer pulls changes and runs fix on already-converted files."""
    write_migration_files(
        migrations_dir,
        (
            (
                "0001_create_users.sql",
                """-- name: migrate-0001-up
CREATE TABLE users (id INTEGER PRIMARY KEY);

-- name: migrate-0001-down
DROP TABLE users;
""",
            ),
        ),
    )
    record_all_migrations(migrations_dir, sqlite_session)

    migration_files = SyncMigrationRunner(migrations_dir).get_migration_files()
    assert generate_conversion_map(migration_files) == {}

    applied = SyncMigrationTracker().get_applied_migrations(sqlite_session)
    assert len(applied) == 1
    assert applied[0]["version_num"] == "0001"
    assert applied[0]["version_type"] == "sequential"


def test_fix_command_partial_conversion_recovery(migrations_dir: Path, sqlite_session: SqliteDriver) -> None:
    """Test recovery when fix partially completes."""
    write_migration_files(migrations_dir, MULTIPLE_TIMESTAMP_MIGRATIONS)
    migration_files, _ = record_all_migrations(migrations_dir, sqlite_session)

    apply_fix_once(migrations_dir, migration_files)

    tracker = SyncMigrationTracker()
    tracker.update_version_record(sqlite_session, "20251011120000", "0001")

    applied_partial = tracker.get_applied_migrations(sqlite_session)
    versions_in_db = {row["version_num"] for row in applied_partial}
    assert "0001" in versions_in_db
    assert "20251012130000" in versions_in_db

    files_partial = SyncMigrationRunner(migrations_dir).get_migration_files()
    generate_conversion_map(files_partial)

    tracker.update_version_record(sqlite_session, "20251012130000", "0002")

    applied_complete = tracker.get_applied_migrations(sqlite_session)
    assert len(applied_complete) == 2
    assert all(row["version_num"] in ["0001", "0002"] for row in applied_complete)
