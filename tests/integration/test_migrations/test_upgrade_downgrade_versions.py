"""Integration tests for upgrade/downgrade commands with hybrid versioning."""

from collections.abc import Generator
from pathlib import Path

import pytest

from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.migrations.commands import SyncMigrationCommands

pytestmark = pytest.mark.xdist_group("migrations")


@pytest.fixture
def sqlite_config(tmp_path: Path) -> Generator[SqliteConfig, None, None]:
    """Create SQLite config with migrations directory."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        migration_config={"script_location": str(migrations_dir), "version_table_name": "ddl_migrations"},
    )
    yield config
    config.close_pool()


@pytest.fixture
def migrations_dir(tmp_path: Path) -> Path:
    """Get migrations directory."""
    return tmp_path / "migrations"


def test_upgrade_with_sequential_versions(sqlite_config: SqliteConfig, migrations_dir: Path) -> None:
    """Test upgrade works with sequential version numbers."""
    migrations = [
        ("0001_create_users.sql", "0001", "CREATE TABLE users (id INTEGER PRIMARY KEY);"),
        ("0002_create_products.sql", "0002", "CREATE TABLE products (id INTEGER PRIMARY KEY);"),
        ("0003_create_orders.sql", "0003", "CREATE TABLE orders (id INTEGER PRIMARY KEY);"),
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

    current = commands.current()
    assert current == "0003"


def test_upgrade_with_timestamp_versions(sqlite_config: SqliteConfig, migrations_dir: Path) -> None:
    """Test upgrade works with timestamp version numbers."""
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

    current = commands.current()
    assert current == "20251012130000"


def test_upgrade_with_mixed_versions(sqlite_config: SqliteConfig, migrations_dir: Path) -> None:
    """Test upgrade works with mixed sequential and timestamp versions."""
    migrations = [
        ("0001_create_users.sql", "0001", "CREATE TABLE users (id INTEGER PRIMARY KEY);"),
        ("0002_create_products.sql", "0002", "CREATE TABLE products (id INTEGER PRIMARY KEY);"),
        ("20251011120000_create_orders.sql", "20251011120000", "CREATE TABLE orders (id INTEGER PRIMARY KEY);"),
        ("20251012130000_create_payments.sql", "20251012130000", "CREATE TABLE payments (id INTEGER PRIMARY KEY);"),
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

    current = commands.current()
    assert current == "20251012130000"

    with sqlite_config.provide_session() as session:
        applied = commands.tracker.get_applied_migrations(session)

    assert len(applied) == 4
    assert applied[0]["version_num"] == "0001"
    assert applied[1]["version_num"] == "0002"
    assert applied[2]["version_num"] == "20251011120000"
    assert applied[3]["version_num"] == "20251012130000"


def test_upgrade_to_specific_sequential_version(sqlite_config: SqliteConfig, migrations_dir: Path) -> None:
    """Test upgrade to specific sequential version."""
    migrations = [
        ("0001_create_users.sql", "0001", "CREATE TABLE users (id INTEGER PRIMARY KEY);"),
        ("0002_create_products.sql", "0002", "CREATE TABLE products (id INTEGER PRIMARY KEY);"),
        ("0003_create_orders.sql", "0003", "CREATE TABLE orders (id INTEGER PRIMARY KEY);"),
    ]

    for filename, version, sql in migrations:
        content = f"""-- name: migrate-{version}-up
{sql}

-- name: migrate-{version}-down
DROP TABLE {filename.split("_")[1]};
"""
        (migrations_dir / filename).write_text(content)

    commands = SyncMigrationCommands(sqlite_config)

    commands.upgrade(revision="0002")

    current = commands.current()
    assert current == "0002"


def test_upgrade_to_specific_timestamp_version(sqlite_config: SqliteConfig, migrations_dir: Path) -> None:
    """Test upgrade to specific timestamp version."""
    migrations = [
        ("20251011120000_create_users.sql", "20251011120000", "CREATE TABLE users (id INTEGER PRIMARY KEY);"),
        ("20251012130000_create_products.sql", "20251012130000", "CREATE TABLE products (id INTEGER PRIMARY KEY);"),
        ("20251013140000_create_orders.sql", "20251013140000", "CREATE TABLE orders (id INTEGER PRIMARY KEY);"),
    ]

    for filename, version, sql in migrations:
        content = f"""-- name: migrate-{version}-up
{sql}

-- name: migrate-{version}-down
DROP TABLE {filename.split("_")[1]};
"""
        (migrations_dir / filename).write_text(content)

    commands = SyncMigrationCommands(sqlite_config)

    commands.upgrade(revision="20251012130000")

    current = commands.current()
    assert current == "20251012130000"


def test_downgrade_with_sequential_versions(sqlite_config: SqliteConfig, migrations_dir: Path) -> None:
    """Test downgrade works with sequential versions."""
    migrations = [
        ("0001_create_users.sql", "0001", "CREATE TABLE users (id INTEGER PRIMARY KEY);"),
        ("0002_create_products.sql", "0002", "CREATE TABLE products (id INTEGER PRIMARY KEY);"),
        ("0003_create_orders.sql", "0003", "CREATE TABLE orders (id INTEGER PRIMARY KEY);"),
    ]

    for filename, version, sql in migrations:
        table_name = filename.split("_", 2)[2].rsplit(".", 1)[0]
        content = f"""-- name: migrate-{version}-up
{sql}

-- name: migrate-{version}-down
DROP TABLE {table_name};
"""
        (migrations_dir / filename).write_text(content)

    commands = SyncMigrationCommands(sqlite_config)

    commands.upgrade()
    assert commands.current() == "0003"

    commands.downgrade()
    assert commands.current() == "0002"

    commands.downgrade()
    assert commands.current() == "0001"


def test_downgrade_with_timestamp_versions(sqlite_config: SqliteConfig, migrations_dir: Path) -> None:
    """Test downgrade works with timestamp versions."""
    migrations = [
        ("20251011120000_create_users.sql", "20251011120000", "CREATE TABLE users (id INTEGER PRIMARY KEY);"),
        ("20251012130000_create_products.sql", "20251012130000", "CREATE TABLE products (id INTEGER PRIMARY KEY);"),
    ]

    for filename, version, sql in migrations:
        table_name = filename.split("_", 2)[2].rsplit(".", 1)[0]
        content = f"""-- name: migrate-{version}-up
{sql}

-- name: migrate-{version}-down
DROP TABLE {table_name};
"""
        (migrations_dir / filename).write_text(content)

    commands = SyncMigrationCommands(sqlite_config)

    commands.upgrade()
    assert commands.current() == "20251012130000"

    commands.downgrade()
    assert commands.current() == "20251011120000"


def test_downgrade_to_specific_version(sqlite_config: SqliteConfig, migrations_dir: Path) -> None:
    """Test downgrade to specific version."""
    migrations = [
        ("0001_create_users.sql", "0001", "CREATE TABLE users (id INTEGER PRIMARY KEY);"),
        ("0002_create_products.sql", "0002", "CREATE TABLE products (id INTEGER PRIMARY KEY);"),
        ("0003_create_orders.sql", "0003", "CREATE TABLE orders (id INTEGER PRIMARY KEY);"),
        ("0004_create_payments.sql", "0004", "CREATE TABLE payments (id INTEGER PRIMARY KEY);"),
    ]

    for filename, version, sql in migrations:
        table_name = filename.split("_", 2)[2].rsplit(".", 1)[0]
        content = f"""-- name: migrate-{version}-up
{sql}

-- name: migrate-{version}-down
DROP TABLE {table_name};
"""
        (migrations_dir / filename).write_text(content)

    commands = SyncMigrationCommands(sqlite_config)

    commands.upgrade()
    assert commands.current() == "0004"

    commands.downgrade(revision="0002")
    assert commands.current() == "0002"

    with sqlite_config.provide_session() as session:
        applied = commands.tracker.get_applied_migrations(session)

    assert len(applied) == 2
    assert applied[0]["version_num"] == "0001"
    assert applied[1]["version_num"] == "0002"


def test_downgrade_to_base(sqlite_config: SqliteConfig, migrations_dir: Path) -> None:
    """Test downgrade to base (removes all migrations)."""
    migrations = [
        ("0001_create_users.sql", "0001", "CREATE TABLE users (id INTEGER PRIMARY KEY);"),
        ("0002_create_products.sql", "0002", "CREATE TABLE products (id INTEGER PRIMARY KEY);"),
    ]

    for filename, version, sql in migrations:
        table_name = filename.split("_", 2)[2].rsplit(".", 1)[0]
        content = f"""-- name: migrate-{version}-up
{sql}

-- name: migrate-{version}-down
DROP TABLE {table_name};
"""
        (migrations_dir / filename).write_text(content)

    commands = SyncMigrationCommands(sqlite_config)

    commands.upgrade()
    assert commands.current() == "0002"

    commands.downgrade(revision="base")
    assert commands.current() is None


def test_upgrade_with_extension_migrations(sqlite_config: SqliteConfig, migrations_dir: Path) -> None:
    """Test upgrade works with extension-prefixed versions."""
    migrations = [
        ("0001_core_init.sql", "0001", "CREATE TABLE core (id INTEGER PRIMARY KEY);"),
        ("ext_litestar_0001_init.sql", "ext_litestar_0001", "CREATE TABLE litestar_ext (id INTEGER PRIMARY KEY);"),
        ("0002_core_users.sql", "0002", "CREATE TABLE users (id INTEGER PRIMARY KEY);"),
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
        applied = commands.tracker.get_applied_migrations(session)

    assert len(applied) == 3
    assert applied[0]["version_num"] == "0001"
    assert applied[1]["version_num"] == "0002"
    assert applied[2]["version_num"] == "ext_litestar_0001"


def test_upgrade_respects_version_comparison_order(sqlite_config: SqliteConfig, migrations_dir: Path) -> None:
    """Test upgrade applies migrations in correct version comparison order."""
    migrations = [
        ("0001_init.sql", "0001", "CREATE TABLE init (id INTEGER PRIMARY KEY);"),
        ("9999_large_seq.sql", "9999", "CREATE TABLE large_seq (id INTEGER PRIMARY KEY);"),
        ("20200101000000_early_timestamp.sql", "20200101000000", "CREATE TABLE early (id INTEGER PRIMARY KEY);"),
        ("20251011120000_late_timestamp.sql", "20251011120000", "CREATE TABLE late (id INTEGER PRIMARY KEY);"),
        ("ext_aaa_0001_ext_a.sql", "ext_aaa_0001", "CREATE TABLE ext_a (id INTEGER PRIMARY KEY);"),
        ("ext_zzz_0001_ext_z.sql", "ext_zzz_0001", "CREATE TABLE ext_z (id INTEGER PRIMARY KEY);"),
    ]

    for filename, version, sql in migrations:
        content = f"""-- name: migrate-{version}-up
{sql}

-- name: migrate-{version}-down
DROP TABLE {filename.split("_")[0]};
"""
        (migrations_dir / filename).write_text(content)

    commands = SyncMigrationCommands(sqlite_config)

    commands.upgrade()

    with sqlite_config.provide_session() as session:
        applied = commands.tracker.get_applied_migrations(session)

    applied_versions = [m["version_num"] for m in applied]

    expected_order = ["0001", "9999", "20200101000000", "20251011120000", "ext_aaa_0001", "ext_zzz_0001"]

    assert applied_versions == expected_order


def test_upgrade_dry_run_shows_pending_migrations(sqlite_config: SqliteConfig, migrations_dir: Path) -> None:
    """Test dry run mode shows what would be applied without making changes."""
    migrations = [
        ("0001_create_users.sql", "0001", "CREATE TABLE users (id INTEGER PRIMARY KEY);"),
        ("0002_create_products.sql", "0002", "CREATE TABLE products (id INTEGER PRIMARY KEY);"),
    ]

    for filename, version, sql in migrations:
        content = f"""-- name: migrate-{version}-up
{sql}

-- name: migrate-{version}-down
DROP TABLE {filename.split("_")[1]};
"""
        (migrations_dir / filename).write_text(content)

    commands = SyncMigrationCommands(sqlite_config)

    commands.upgrade(dry_run=True)

    current = commands.current()
    assert current is None

    with sqlite_config.provide_session() as session:
        result = session.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('users', 'products')")
        tables = [row["name"] for row in result.data or []]

    assert len(tables) == 0


def test_downgrade_dry_run_shows_pending_downgrades(sqlite_config: SqliteConfig, migrations_dir: Path) -> None:
    """Test downgrade dry run shows what would be reverted without making changes."""
    migrations = [
        ("0001_create_users.sql", "0001", "CREATE TABLE users (id INTEGER PRIMARY KEY);"),
        ("0002_create_products.sql", "0002", "CREATE TABLE products (id INTEGER PRIMARY KEY);"),
    ]

    for filename, version, sql in migrations:
        table_name = filename.split("_", 2)[2].rsplit(".", 1)[0]
        content = f"""-- name: migrate-{version}-up
{sql}

-- name: migrate-{version}-down
DROP TABLE {table_name};
"""
        (migrations_dir / filename).write_text(content)

    commands = SyncMigrationCommands(sqlite_config)

    commands.upgrade()
    assert commands.current() == "0002"

    commands.downgrade(dry_run=True)

    current = commands.current()
    assert current == "0002"

    with sqlite_config.provide_session() as session:
        result = session.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('users', 'products')")
        tables = [row["name"] for row in result.data or []]

    assert "users" in tables
    assert "products" in tables
