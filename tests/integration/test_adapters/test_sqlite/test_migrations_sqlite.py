"""Integration tests for SQLite migration workflow."""

from pathlib import Path
from typing import Any

import pytest

from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands, create_migration_commands

pytestmark = pytest.mark.xdist_group("sqlite")


def test_sqlite_migration_full_workflow(tmp_path: Path) -> None:
    """Test full SQLite migration workflow: init -> create -> upgrade -> downgrade."""
    migration_dir = tmp_path / "migrations"
    temp_db = str(tmp_path / "test.db")
    config = SqliteConfig(
        connection_config={"database": temp_db},
        migration_config={"script_location": str(migration_dir), "version_table_name": "sqlspec_migrations"},
    )
    commands: SyncMigrationCommands[Any] | AsyncMigrationCommands[Any] = create_migration_commands(config)

    commands.init(str(migration_dir), package=True)

    assert migration_dir.exists()
    assert (migration_dir / "__init__.py").exists()

    migration_content = '''"""Initial schema migration."""


def up():
    """Create users table."""
    return ["""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """]


def down():
    """Drop users table."""
    return ["DROP TABLE IF EXISTS users"]
'''

    migration_file = migration_dir / "001_create_users.py"
    migration_file.write_text(migration_content)

    commands.upgrade()

    with config.provide_session() as driver:
        result = driver.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
        assert len(result.data) == 1

        driver.execute("INSERT INTO users (name, email) VALUES (?, ?)", ("John Doe", "john@example.com"))

        users_result = driver.execute("SELECT * FROM users")
        assert len(users_result.data) == 1
        assert users_result.data[0]["name"] == "John Doe"
        assert users_result.data[0]["email"] == "john@example.com"

    commands.downgrade("base")

    with config.provide_session() as driver:
        result = driver.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
        assert len(result.data) == 0


def test_sqlite_multiple_migrations_workflow(tmp_path: Path) -> None:
    """Test SQLite workflow with multiple migrations: create -> apply both -> downgrade one -> downgrade all."""
    migration_dir = tmp_path / "migrations"
    temp_db = str(tmp_path / "test.db")
    config = SqliteConfig(
        connection_config={"database": temp_db},
        migration_config={"script_location": str(migration_dir), "version_table_name": "sqlspec_migrations"},
    )
    commands: SyncMigrationCommands[Any] | AsyncMigrationCommands[Any] = create_migration_commands(config)

    commands.init(str(migration_dir), package=True)

    migration1_content = '''"""Create users table."""


def up():
    """Create users table."""
    return ["""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL
        )
    """]


def down():
    """Drop users table."""
    return ["DROP TABLE IF EXISTS users"]
'''

    migration2_content = '''"""Create posts table."""


def up():
    """Create posts table."""
    return ["""
        CREATE TABLE posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            content TEXT,
            user_id INTEGER,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
    """]


def down():
    """Drop posts table."""
    return ["DROP TABLE IF EXISTS posts"]
'''

    (migration_dir / "0001_create_users.py").write_text(migration1_content)
    (migration_dir / "0002_create_posts.py").write_text(migration2_content)

    commands.upgrade()

    with config.provide_session() as driver:
        tables_result = driver.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        table_names = [t["name"] for t in tables_result.data]
        assert "users" in table_names
        assert "posts" in table_names

        driver.execute("INSERT INTO users (name, email) VALUES (?, ?)", ("Author", "author@example.com"))
        driver.execute("INSERT INTO posts (title, content, user_id) VALUES (?, ?, ?)", ("My Post", "Post content", 1))

        posts_result = driver.execute("SELECT * FROM posts")
        assert len(posts_result.data) == 1
        assert posts_result.data[0]["title"] == "My Post"

    commands.downgrade("0001")

    with config.provide_session() as driver:
        tables_result = driver.execute("SELECT name FROM sqlite_master WHERE type='table'")
        table_names = [t["name"] for t in tables_result.data]
        assert "users" in table_names
        assert "posts" not in table_names

    commands.downgrade("base")

    with config.provide_session() as driver:
        tables_result = driver.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")

        table_names = [t["name"] for t in tables_result.data if not t["name"].startswith("sqlspec_")]
        assert len(table_names) == 0


def test_sqlite_migration_current_command(tmp_path: Path) -> None:
    """Test the current migration command shows correct version for SQLite."""
    migration_dir = tmp_path / "migrations"
    temp_db = str(tmp_path / "test.db")
    config = SqliteConfig(
        connection_config={"database": temp_db},
        migration_config={"script_location": str(migration_dir), "version_table_name": "sqlspec_migrations"},
    )
    commands: SyncMigrationCommands[Any] | AsyncMigrationCommands[Any] = create_migration_commands(config)

    commands.init(str(migration_dir), package=True)

    commands.current(verbose=False)

    migration_content = '''"""Test migration."""


def up():
    """Create test table."""
    return ["CREATE TABLE test_table (id INTEGER PRIMARY KEY)"]


def down():
    """Drop test table."""
    return ["DROP TABLE IF EXISTS test_table"]
'''

    (migration_dir / "001_test.py").write_text(migration_content)

    commands.upgrade()

    commands.current(verbose=True)


def test_sqlite_migration_error_handling(tmp_path: Path) -> None:
    """Test SQLite migration error handling."""
    migration_dir = tmp_path / "migrations"
    temp_db = str(tmp_path / "test.db")
    config = SqliteConfig(
        connection_config={"database": temp_db},
        migration_config={"script_location": str(migration_dir), "version_table_name": "sqlspec_migrations"},
    )
    commands: SyncMigrationCommands[Any] | AsyncMigrationCommands[Any] = create_migration_commands(config)

    commands.init(str(migration_dir), package=True)

    migration_content = '''"""Bad migration."""


def up():
    """Invalid SQL - should cause error."""
    return ["CREATE THAT TABLE invalid_sql"]


def down():
    """No downgrade needed."""
    return []
'''

    (migration_dir / "001_bad.py").write_text(migration_content)

    commands.upgrade()

    with config.provide_session() as driver:
        try:
            driver.execute("SELECT version FROM sqlspec_migrations ORDER BY version")
            msg = "Expected migration table to not exist, but it does"
            raise AssertionError(msg)
        except Exception as e:
            assert "no such" in str(e).lower() or "does not exist" in str(e).lower()


def test_sqlite_migration_with_transactions(tmp_path: Path) -> None:
    """Test SQLite migrations work properly with transactions."""
    migration_dir = tmp_path / "migrations"
    temp_db = str(tmp_path / "test.db")
    config = SqliteConfig(
        connection_config={"database": temp_db},
        migration_config={"script_location": str(migration_dir), "version_table_name": "sqlspec_migrations"},
    )
    commands: SyncMigrationCommands[Any] | AsyncMigrationCommands[Any] = create_migration_commands(config)

    commands.init(str(migration_dir), package=True)

    migration_content = '''"""Migration with multiple operations."""


def up():
    """Create customers table with data."""
    return [
        """CREATE TABLE customers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL
        )""",
        "INSERT INTO customers (name) VALUES ('Customer 1')",
        "INSERT INTO customers (name) VALUES ('Customer 2')"
    ]


def down():
    """Drop customers table."""
    return ["DROP TABLE IF EXISTS customers"]
'''

    (migration_dir / "0001_transaction_test.py").write_text(migration_content)

    commands.upgrade()

    with config.provide_session() as driver:
        customers_result = driver.execute("SELECT * FROM customers ORDER BY name")
        assert len(customers_result.data) == 2
        assert customers_result.data[0]["name"] == "Customer 1"
        assert customers_result.data[1]["name"] == "Customer 2"

    commands.downgrade("base")

    with config.provide_session() as driver:
        result = driver.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='customers'")
        assert len(result.data) == 0


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
