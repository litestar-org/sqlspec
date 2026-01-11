"""Integration tests for auto-sync functionality in upgrade command."""

from collections.abc import Generator
from pathlib import Path

import pytest

from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.migrations.commands import SyncMigrationCommands
from sqlspec.migrations.fix import MigrationFixer
from sqlspec.migrations.version import generate_conversion_map


@pytest.fixture
def sqlite_config(tmp_path: Path) -> Generator[SqliteConfig, None, None]:
    """Create SQLite config with migrations directory."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        migration_config={
            "script_location": str(migrations_dir),
            "version_table_name": "ddl_migrations",
            "auto_sync": True,
        },
    )
    yield config
    config.close_pool()


@pytest.fixture
def migrations_dir(tmp_path: Path) -> Path:
    """Get migrations directory."""
    return tmp_path / "migrations"


def test_auto_sync_reconciles_renamed_migrations(sqlite_config: SqliteConfig, migrations_dir: Path) -> None:
    """Test auto-sync automatically reconciles renamed migrations during upgrade."""
    migrations = [
        ("20251011120000_create_users.sql", "20251011120000", "CREATE TABLE users (id INTEGER PRIMARY KEY);"),
        ("20251012130000_create_products.sql", "20251012130000", "CREATE TABLE products (id INTEGER PRIMARY KEY);"),
    ]

    for filename, version, sql in migrations:
        content = f"""-- name: migrate-{version}-up
{sql}

-- name: migrate-{version}-down
DROP TABLE {filename.split("_")[1]};
"""
        (migrations_dir / filename).write_text(content)

    commands = SyncMigrationCommands(sqlite_config)

    commands.upgrade()

    with sqlite_config.provide_session() as session:
        applied_before = commands.tracker.get_applied_migrations(session)

    assert len(applied_before) == 2
    assert applied_before[0]["version_num"] == "20251011120000"
    assert applied_before[1]["version_num"] == "20251012130000"

    fixer = MigrationFixer(migrations_dir)
    all_files = [(v, p) for v, p in commands.runner.get_migration_files()]
    conversion_map = generate_conversion_map(all_files)
    renames = fixer.plan_renames(conversion_map)
    fixer.apply_renames(renames)

    commands_after_rename = SyncMigrationCommands(sqlite_config)

    commands_after_rename.upgrade()

    with sqlite_config.provide_session() as session:
        applied_after = commands_after_rename.tracker.get_applied_migrations(session)

    assert len(applied_after) == 2
    assert applied_after[0]["version_num"] == "0001"
    assert applied_after[1]["version_num"] == "0002"


def test_auto_sync_validates_checksums(sqlite_config: SqliteConfig, migrations_dir: Path) -> None:
    """Test auto-sync validates checksums before reconciling."""
    content = """-- name: migrate-20251011120000-up
CREATE TABLE users (id INTEGER PRIMARY KEY);

-- name: migrate-20251011120000-down
DROP TABLE users;
"""
    (migrations_dir / "20251011120000_create_users.sql").write_text(content)

    commands = SyncMigrationCommands(sqlite_config)
    commands.upgrade()

    with sqlite_config.provide_session() as session:
        applied = commands.tracker.get_applied_migrations(session)
        original_checksum = applied[0]["checksum"]

    (migrations_dir / "20251011120000_create_users.sql").unlink()

    modified_content = """-- name: migrate-0001-up
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);

-- name: migrate-0001-down
DROP TABLE users;
"""
    (migrations_dir / "0001_create_users.sql").write_text(modified_content)

    commands_after = SyncMigrationCommands(sqlite_config)

    commands_after.upgrade()

    with sqlite_config.provide_session() as session:
        applied_after = commands_after.tracker.get_applied_migrations(session)

    assert applied_after[0]["version_num"] == "20251011120000"
    assert applied_after[0]["checksum"] == original_checksum


def test_auto_sync_disabled_via_config(sqlite_config: SqliteConfig, migrations_dir: Path) -> None:
    """Test auto-sync can be disabled via migration config."""
    sqlite_config.migration_config["auto_sync"] = False

    migrations = [("20251011120000_create_users.sql", "20251011120000", "CREATE TABLE users (id INTEGER PRIMARY KEY);")]

    for filename, version, sql in migrations:
        content = f"""-- name: migrate-{version}-up
{sql}

-- name: migrate-{version}-down
DROP TABLE users;
"""
        (migrations_dir / filename).write_text(content)

    commands = SyncMigrationCommands(sqlite_config)
    commands.upgrade()

    with sqlite_config.provide_session() as session:
        applied_before = commands.tracker.get_applied_migrations(session)

    assert applied_before[0]["version_num"] == "20251011120000"

    fixer = MigrationFixer(migrations_dir)
    all_files = [(v, p) for v, p in commands.runner.get_migration_files()]
    conversion_map = generate_conversion_map(all_files)
    renames = fixer.plan_renames(conversion_map)
    fixer.apply_renames(renames)

    commands_after = SyncMigrationCommands(sqlite_config)

    commands_after.upgrade()

    with sqlite_config.provide_session() as session:
        applied_after = commands_after.tracker.get_applied_migrations(session)

    assert applied_after[0]["version_num"] == "20251011120000"


def test_auto_sync_disabled_via_flag(sqlite_config: SqliteConfig, migrations_dir: Path) -> None:
    """Test auto-sync can be disabled via upgrade flag."""
    migrations = [("20251011120000_create_users.sql", "20251011120000", "CREATE TABLE users (id INTEGER PRIMARY KEY);")]

    for filename, version, sql in migrations:
        content = f"""-- name: migrate-{version}-up
{sql}

-- name: migrate-{version}-down
DROP TABLE users;
"""
        (migrations_dir / filename).write_text(content)

    commands = SyncMigrationCommands(sqlite_config)
    commands.upgrade()

    with sqlite_config.provide_session() as session:
        applied_before = commands.tracker.get_applied_migrations(session)

    assert applied_before[0]["version_num"] == "20251011120000"

    fixer = MigrationFixer(migrations_dir)
    all_files = [(v, p) for v, p in commands.runner.get_migration_files()]
    conversion_map = generate_conversion_map(all_files)
    renames = fixer.plan_renames(conversion_map)
    fixer.apply_renames(renames)

    commands_after = SyncMigrationCommands(sqlite_config)

    commands_after.upgrade(auto_sync=False)

    with sqlite_config.provide_session() as session:
        applied_after = commands_after.tracker.get_applied_migrations(session)

    assert applied_after[0]["version_num"] == "20251011120000"


def test_auto_sync_handles_multiple_migrations(sqlite_config: SqliteConfig, migrations_dir: Path) -> None:
    """Test auto-sync handles multiple migrations being renamed."""
    migrations = [
        ("0001_init.sql", "0001", "CREATE TABLE init (id INTEGER PRIMARY KEY);"),
        ("20251011120000_create_users.sql", "20251011120000", "CREATE TABLE users (id INTEGER PRIMARY KEY);"),
        ("20251012130000_create_products.sql", "20251012130000", "CREATE TABLE products (id INTEGER PRIMARY KEY);"),
        ("20251013140000_create_orders.sql", "20251013140000", "CREATE TABLE orders (id INTEGER PRIMARY KEY);"),
    ]

    for filename, version, sql in migrations:
        name_without_ext = filename.rsplit(".", 1)[0]
        parts = name_without_ext.split("_", 1)
        table_name = parts[1].replace("create_", "") if len(parts) > 1 else name_without_ext
        content = f"""-- name: migrate-{version}-up
{sql}

-- name: migrate-{version}-down
DROP TABLE {table_name};
"""
        (migrations_dir / filename).write_text(content)

    commands = SyncMigrationCommands(sqlite_config)
    commands.upgrade()

    with sqlite_config.provide_session() as session:
        applied_before = commands.tracker.get_applied_migrations(session)

    assert len(applied_before) == 4
    timestamp_versions = [m["version_num"] for m in applied_before if m["version_type"] == "timestamp"]
    assert len(timestamp_versions) == 3

    fixer = MigrationFixer(migrations_dir)
    all_files = [(v, p) for v, p in commands.runner.get_migration_files()]
    conversion_map = generate_conversion_map(all_files)
    renames = fixer.plan_renames(conversion_map)
    fixer.apply_renames(renames)

    commands_after = SyncMigrationCommands(sqlite_config)

    commands_after.upgrade()

    with sqlite_config.provide_session() as session:
        applied_after = commands_after.tracker.get_applied_migrations(session)

    assert len(applied_after) == 4

    expected_versions = {"0001", "0002", "0003", "0004"}
    actual_versions = {m["version_num"] for m in applied_after}
    assert actual_versions == expected_versions


def test_auto_sync_preserves_execution_sequence(sqlite_config: SqliteConfig, migrations_dir: Path) -> None:
    """Test auto-sync preserves original execution sequence."""
    migrations = [
        ("0001_init.sql", "0001", "CREATE TABLE init (id INTEGER PRIMARY KEY);"),
        ("20251011120000_create_users.sql", "20251011120000", "CREATE TABLE users (id INTEGER PRIMARY KEY);"),
        ("20251012130000_create_products.sql", "20251012130000", "CREATE TABLE products (id INTEGER PRIMARY KEY);"),
    ]

    for filename, version, sql in migrations:
        name_without_ext = filename.rsplit(".", 1)[0]
        parts = name_without_ext.split("_", 1)
        table_name = parts[1].replace("create_", "") if len(parts) > 1 else name_without_ext
        content = f"""-- name: migrate-{version}-up
{sql}

-- name: migrate-{version}-down
DROP TABLE {table_name};
"""
        (migrations_dir / filename).write_text(content)

    commands = SyncMigrationCommands(sqlite_config)
    commands.upgrade()

    with sqlite_config.provide_session() as session:
        applied_before = commands.tracker.get_applied_migrations(session)

    original_sequences = {m["version_num"]: m["execution_sequence"] for m in applied_before}

    fixer = MigrationFixer(migrations_dir)
    all_files = [(v, p) for v, p in commands.runner.get_migration_files()]
    conversion_map = generate_conversion_map(all_files)
    renames = fixer.plan_renames(conversion_map)
    fixer.apply_renames(renames)

    commands_after = SyncMigrationCommands(sqlite_config)

    commands_after.upgrade()

    with sqlite_config.provide_session() as session:
        applied_after = commands_after.tracker.get_applied_migrations(session)

    assert applied_after[0]["execution_sequence"] == original_sequences["0001"]
    assert applied_after[1]["execution_sequence"] == original_sequences["20251011120000"]
    assert applied_after[2]["execution_sequence"] == original_sequences["20251012130000"]


def test_auto_sync_with_new_migrations_after_rename(sqlite_config: SqliteConfig, migrations_dir: Path) -> None:
    """Test auto-sync works when adding new migrations after rename."""
    migrations = [("20251011120000_create_users.sql", "20251011120000", "CREATE TABLE users (id INTEGER PRIMARY KEY);")]

    for filename, version, sql in migrations:
        content = f"""-- name: migrate-{version}-up
{sql}

-- name: migrate-{version}-down
DROP TABLE users;
"""
        (migrations_dir / filename).write_text(content)

    commands = SyncMigrationCommands(sqlite_config)
    commands.upgrade()

    fixer = MigrationFixer(migrations_dir)
    all_files = [(v, p) for v, p in commands.runner.get_migration_files()]
    conversion_map = generate_conversion_map(all_files)
    renames = fixer.plan_renames(conversion_map)
    fixer.apply_renames(renames)

    new_migration = """-- name: migrate-0002-up
CREATE TABLE products (id INTEGER PRIMARY KEY);

-- name: migrate-0002-down
DROP TABLE products;
"""
    (migrations_dir / "0002_create_products.sql").write_text(new_migration)

    commands_after = SyncMigrationCommands(sqlite_config)

    commands_after.upgrade()

    with sqlite_config.provide_session() as session:
        applied = commands_after.tracker.get_applied_migrations(session)

    assert len(applied) == 2
    assert applied[0]["version_num"] == "0001"
    assert applied[1]["version_num"] == "0002"
