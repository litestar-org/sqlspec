"""Integration tests for idempotent fix command workflow."""

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


def test_fix_command_idempotent_single_migration(
    migrations_dir: Path, sqlite_session: SqliteDriver, sqlite_config: SqliteConfig
) -> None:
    """Test fix command can be run multiple times without error on single migration."""
    sql_content = """-- name: migrate-20251011120000-up
CREATE TABLE users (id INTEGER PRIMARY KEY);

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

    fixer = MigrationFixer(migrations_dir)
    conversion_map = generate_conversion_map(migration_files)
    renames = fixer.plan_renames(conversion_map)
    fixer.apply_renames(renames)

    tracker.update_version_record(sqlite_session, "20251011120000", "0001")

    applied = tracker.get_applied_migrations(sqlite_session)
    assert len(applied) == 1
    assert applied[0]["version_num"] == "0001"

    new_migration_files = runner.get_migration_files()
    conversion_map_second = generate_conversion_map(new_migration_files)

    assert conversion_map_second == {}

    tracker.update_version_record(sqlite_session, "20251011120000", "0001")

    applied_after = tracker.get_applied_migrations(sqlite_session)
    assert len(applied_after) == 1
    assert applied_after[0]["version_num"] == "0001"


def test_fix_command_idempotent_multiple_migrations(
    migrations_dir: Path, sqlite_session: SqliteDriver, sqlite_config: SqliteConfig
) -> None:
    """Test fix command is idempotent with multiple migrations."""
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
    ]

    for filename, content in migrations:
        (migrations_dir / filename).write_text(content)

    tracker = SyncMigrationTracker()
    tracker.ensure_tracking_table(sqlite_session)

    runner = SyncMigrationRunner(migrations_dir)
    migration_files = runner.get_migration_files()

    for version, file_path in migration_files:
        migration = runner.load_migration(file_path, version=version)
        runner.execute_upgrade(sqlite_session, migration)
        tracker.record_migration(
            sqlite_session, migration["version"], migration["description"], 100, migration["checksum"]
        )

    fixer = MigrationFixer(migrations_dir)
    conversion_map = generate_conversion_map(migration_files)
    renames = fixer.plan_renames(conversion_map)
    fixer.apply_renames(renames)

    for old_version, new_version in conversion_map.items():
        tracker.update_version_record(sqlite_session, old_version, new_version)

    applied_first = tracker.get_applied_migrations(sqlite_session)
    assert len(applied_first) == 2
    assert applied_first[0]["version_num"] == "0001"
    assert applied_first[1]["version_num"] == "0002"

    for old_version, new_version in conversion_map.items():
        tracker.update_version_record(sqlite_session, old_version, new_version)

    applied_second = tracker.get_applied_migrations(sqlite_session)
    assert len(applied_second) == 2
    assert applied_second[0]["version_num"] == "0001"
    assert applied_second[1]["version_num"] == "0002"


def test_ci_workflow_simulation(
    migrations_dir: Path, sqlite_session: SqliteDriver, sqlite_config: SqliteConfig
) -> None:
    """Test simulated CI workflow where fix runs on every commit."""
    sql_content = """-- name: migrate-20251011120000-up
CREATE TABLE users (id INTEGER PRIMARY KEY);

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

    fixer = MigrationFixer(migrations_dir)
    conversion_map = generate_conversion_map(migration_files)
    renames = fixer.plan_renames(conversion_map)
    fixer.apply_renames(renames)

    for old_version, new_version in conversion_map.items():
        tracker.update_version_record(sqlite_session, old_version, new_version)

    runner_after_first_fix = SyncMigrationRunner(migrations_dir)
    files_after_first = runner_after_first_fix.get_migration_files()
    conversion_map_second = generate_conversion_map(files_after_first)

    assert conversion_map_second == {}

    for old_version, new_version in conversion_map.items():
        tracker.update_version_record(sqlite_session, old_version, new_version)

    applied = tracker.get_applied_migrations(sqlite_session)
    assert len(applied) == 1
    assert applied[0]["version_num"] == "0001"


def test_developer_pull_workflow(
    migrations_dir: Path, sqlite_session: SqliteDriver, sqlite_config: SqliteConfig
) -> None:
    """Test developer pulls changes and runs fix on already-converted files."""
    sql_content = """-- name: migrate-0001-up
CREATE TABLE users (id INTEGER PRIMARY KEY);

-- name: migrate-0001-down
DROP TABLE users;
"""

    sql_file = migrations_dir / "0001_create_users.sql"
    sql_file.write_text(sql_content)

    tracker = SyncMigrationTracker()
    tracker.ensure_tracking_table(sqlite_session)

    runner = SyncMigrationRunner(migrations_dir)
    migration_files = runner.get_migration_files()
    migration = runner.load_migration(migration_files[0][1], version=migration_files[0][0])

    runner.execute_upgrade(sqlite_session, migration)
    tracker.record_migration(sqlite_session, migration["version"], migration["description"], 100, migration["checksum"])

    MigrationFixer(migrations_dir)
    conversion_map = generate_conversion_map(migration_files)

    assert conversion_map == {}

    applied = tracker.get_applied_migrations(sqlite_session)
    assert len(applied) == 1
    assert applied[0]["version_num"] == "0001"
    assert applied[0]["version_type"] == "sequential"


def test_partial_conversion_recovery(
    migrations_dir: Path, sqlite_session: SqliteDriver, sqlite_config: SqliteConfig
) -> None:
    """Test recovery when fix partially completes."""
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
    ]

    for filename, content in migrations:
        (migrations_dir / filename).write_text(content)

    tracker = SyncMigrationTracker()
    tracker.ensure_tracking_table(sqlite_session)

    runner = SyncMigrationRunner(migrations_dir)
    migration_files = runner.get_migration_files()

    for version, file_path in migration_files:
        migration = runner.load_migration(file_path, version=version)
        runner.execute_upgrade(sqlite_session, migration)
        tracker.record_migration(
            sqlite_session, migration["version"], migration["description"], 100, migration["checksum"]
        )

    fixer = MigrationFixer(migrations_dir)
    conversion_map = generate_conversion_map(migration_files)
    renames = fixer.plan_renames(conversion_map)
    fixer.apply_renames(renames)

    tracker.update_version_record(sqlite_session, "20251011120000", "0001")

    applied_partial = tracker.get_applied_migrations(sqlite_session)
    versions_in_db = {row["version_num"] for row in applied_partial}
    assert "0001" in versions_in_db
    assert "20251012130000" in versions_in_db

    runner_partial = SyncMigrationRunner(migrations_dir)
    files_partial = runner_partial.get_migration_files()
    generate_conversion_map(files_partial)

    tracker.update_version_record(sqlite_session, "20251012130000", "0002")

    applied_complete = tracker.get_applied_migrations(sqlite_session)
    assert len(applied_complete) == 2
    assert all(row["version_num"] in ["0001", "0002"] for row in applied_complete)


def test_mixed_sequential_and_timestamp_idempotent(
    migrations_dir: Path, sqlite_session: SqliteDriver, sqlite_config: SqliteConfig
) -> None:
    """Test fix is idempotent with mixed sequential and timestamp migrations."""
    migrations = [
        (
            "0001_init.sql",
            """-- name: migrate-0001-up
CREATE TABLE init (id INTEGER PRIMARY KEY);

-- name: migrate-0001-down
DROP TABLE init;
""",
        ),
        (
            "20251011120000_create_users.sql",
            """-- name: migrate-20251011120000-up
CREATE TABLE users (id INTEGER PRIMARY KEY);

-- name: migrate-20251011120000-down
DROP TABLE users;
""",
        ),
    ]

    for filename, content in migrations:
        (migrations_dir / filename).write_text(content)

    tracker = SyncMigrationTracker()
    tracker.ensure_tracking_table(sqlite_session)

    runner = SyncMigrationRunner(migrations_dir)
    migration_files = runner.get_migration_files()

    for version, file_path in migration_files:
        migration = runner.load_migration(file_path, version=version)
        runner.execute_upgrade(sqlite_session, migration)
        tracker.record_migration(
            sqlite_session, migration["version"], migration["description"], 100, migration["checksum"]
        )

    fixer = MigrationFixer(migrations_dir)
    conversion_map = generate_conversion_map(migration_files)
    renames = fixer.plan_renames(conversion_map)
    fixer.apply_renames(renames)

    for old_version, new_version in conversion_map.items():
        tracker.update_version_record(sqlite_session, old_version, new_version)

    applied_first = tracker.get_applied_migrations(sqlite_session)
    assert len(applied_first) == 2
    versions_first = {row["version_num"] for row in applied_first}
    assert versions_first == {"0001", "0002"}

    for old_version, new_version in conversion_map.items():
        tracker.update_version_record(sqlite_session, old_version, new_version)

    applied_second = tracker.get_applied_migrations(sqlite_session)
    assert len(applied_second) == 2
    versions_second = {row["version_num"] for row in applied_second}
    assert versions_second == {"0001", "0002"}
