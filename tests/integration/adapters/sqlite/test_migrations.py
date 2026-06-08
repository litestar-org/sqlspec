"""Integration tests for SqliteConfig migration helper methods.

The shared init/upgrade/downgrade/current/error migration lifecycle is covered by
tests/integration/adapters/contracts/test_migrations_contract.py. This module keeps the
SqliteConfig convenience-method surface (migrate_up, migrate_down, get_current_migration,
create_migration, stamp_migration, fix_migrations).
"""

from pathlib import Path

import pytest

from sqlspec.adapters.sqlite.config import SqliteConfig

pytestmark = pytest.mark.xdist_group("sqlite")


def test_sqlite_config_migrate_up_method(tmp_path: Path) -> None:
    """Test SqliteConfig.migrate_up() method works correctly."""
    migration_dir = tmp_path / "migrations"
    temp_db = str(tmp_path / "test.db")

    config = SqliteConfig(
        connection_config={"database": temp_db},
        migration_config={"script_location": str(migration_dir), "version_table_name": "sqlspec_migrations"},
    )

    config.init_migrations()

    migration_content = '''"""Create products table."""


def up():
    """Create products table."""
    return ["""
        CREATE TABLE products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            price REAL
        )
    """]


def down():
    """Drop products table."""
    return ["DROP TABLE IF EXISTS products"]
'''

    (migration_dir / "0001_create_products.py").write_text(migration_content)

    config.migrate_up()

    with config.provide_session() as driver:
        result = driver.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='products'")
        assert len(result.data) == 1


def test_sqlite_config_migrate_down_method(tmp_path: Path) -> None:
    """Test SqliteConfig.migrate_down() method works correctly."""
    migration_dir = tmp_path / "migrations"
    temp_db = str(tmp_path / "test.db")

    config = SqliteConfig(
        connection_config={"database": temp_db},
        migration_config={"script_location": str(migration_dir), "version_table_name": "sqlspec_migrations"},
    )

    config.init_migrations()

    migration_content = '''"""Create inventory table."""


def up():
    """Create inventory table."""
    return ["""
        CREATE TABLE inventory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item TEXT NOT NULL
        )
    """]


def down():
    """Drop inventory table."""
    return ["DROP TABLE IF EXISTS inventory"]
'''

    (migration_dir / "0001_create_inventory.py").write_text(migration_content)

    config.migrate_up()

    with config.provide_session() as driver:
        result = driver.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='inventory'")
        assert len(result.data) == 1

    config.migrate_down()

    with config.provide_session() as driver:
        result = driver.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='inventory'")
        assert len(result.data) == 0


def test_sqlite_config_get_current_migration_method(tmp_path: Path) -> None:
    """Test SqliteConfig.get_current_migration() method returns correct version."""
    migration_dir = tmp_path / "migrations"
    temp_db = str(tmp_path / "test.db")

    config = SqliteConfig(
        connection_config={"database": temp_db},
        migration_config={"script_location": str(migration_dir), "version_table_name": "sqlspec_migrations"},
    )

    config.init_migrations()

    current_version = config.get_current_migration()
    assert current_version is None

    migration_content = '''"""First migration."""


def up():
    """Create test table."""
    return ["CREATE TABLE test_version (id INTEGER PRIMARY KEY)"]


def down():
    """Drop test table."""
    return ["DROP TABLE IF EXISTS test_version"]
'''

    (migration_dir / "0001_first.py").write_text(migration_content)

    config.migrate_up()

    current_version = config.get_current_migration()
    assert current_version == "0001"


def test_sqlite_config_create_migration_method(tmp_path: Path) -> None:
    """Test SqliteConfig.create_migration() method generates migration file."""
    migration_dir = tmp_path / "migrations"
    temp_db = str(tmp_path / "test.db")

    config = SqliteConfig(
        connection_config={"database": temp_db},
        migration_config={"script_location": str(migration_dir), "version_table_name": "sqlspec_migrations"},
    )

    config.init_migrations()

    config.create_migration("add users table", file_type="py")

    migration_files = list(migration_dir.glob("*.py"))
    migration_files = [f for f in migration_files if f.name != "__init__.py"]

    assert len(migration_files) == 1
    assert "add_users_table" in migration_files[0].name


def test_sqlite_config_stamp_migration_method(tmp_path: Path) -> None:
    """Test SqliteConfig.stamp_migration() method marks database at revision."""
    migration_dir = tmp_path / "migrations"
    temp_db = str(tmp_path / "test.db")

    config = SqliteConfig(
        connection_config={"database": temp_db},
        migration_config={"script_location": str(migration_dir), "version_table_name": "sqlspec_migrations"},
    )

    config.init_migrations()

    migration_content = '''"""Stamped migration."""


def up():
    """Create stamped table."""
    return ["CREATE TABLE stamped (id INTEGER PRIMARY KEY)"]


def down():
    """Drop stamped table."""
    return ["DROP TABLE IF EXISTS stamped"]
'''

    (migration_dir / "0001_stamped.py").write_text(migration_content)

    config.stamp_migration("0001")

    current_version = config.get_current_migration()
    assert current_version == "0001"

    with config.provide_session() as driver:
        result = driver.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='stamped'")
        assert len(result.data) == 0


def test_sqlite_config_fix_migrations_dry_run(tmp_path: Path) -> None:
    """Test SqliteConfig.fix_migrations() dry run shows what would change."""
    migration_dir = tmp_path / "migrations"
    temp_db = str(tmp_path / "test.db")

    config = SqliteConfig(
        connection_config={"database": temp_db},
        migration_config={"script_location": str(migration_dir), "version_table_name": "sqlspec_migrations"},
    )

    config.init_migrations()

    timestamp_migration = '''"""Timestamp migration."""


def up():
    """Create timestamp table."""
    return ["CREATE TABLE timestamp_test (id INTEGER PRIMARY KEY)"]


def down():
    """Drop timestamp table."""
    return ["DROP TABLE IF EXISTS timestamp_test"]
'''

    (migration_dir / "20251030120000_timestamp_migration.py").write_text(timestamp_migration)

    config.fix_migrations(dry_run=True, yes=True)

    timestamp_file = migration_dir / "20251030120000_timestamp_migration.py"
    assert timestamp_file.exists()

    sequential_file = migration_dir / "0001_timestamp_migration.py"
    assert not sequential_file.exists()
