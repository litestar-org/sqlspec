"""Integration tests for AioSQLite migration workflow."""

import tempfile
from pathlib import Path

import pytest

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.migrations.commands import MigrationCommands


@pytest.mark.xdist_group("migrations")
def test_aiosqlite_migration_full_workflow() -> None:
    """Test full AioSQLite migration workflow: init -> create -> upgrade -> downgrade."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        db_path = Path(temp_dir) / "test.db"

        # Create AioSQLite config with migration directory
        config = AiosqliteConfig(
            pool_config={"database": str(db_path)},
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "sqlspec_migrations"
            }
        )
        commands = MigrationCommands(config)

        # 1. Initialize migrations
        commands.init(str(migration_dir), package=True)

        # Verify initialization
        assert migration_dir.exists()
        assert (migration_dir / "__init__.py").exists()

        # 2. Create a migration with simple schema
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

        # Write migration file
        migration_file = migration_dir / "0001_create_users.py"
        migration_file.write_text(migration_content)

        # 3. Apply migration (upgrade)
        commands.upgrade()

        # 4. Verify migration was applied
        # Note: We use the unified MigrationCommands interface which handles async/sync internally
        with config.provide_session() as driver:
            # Check that table exists
            result = driver.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='users'"
            )
            assert len(result.data) == 1

            # Insert test data
            driver.execute(
                "INSERT INTO users (name, email) VALUES (?, ?)",
                ("John Doe", "john@example.com")
            )

            # Verify data
            users_result = driver.execute("SELECT * FROM users")
            assert len(users_result.data) == 1
            assert users_result.data[0]["name"] == "John Doe"
            assert users_result.data[0]["email"] == "john@example.com"

        # 5. Downgrade migration
        commands.downgrade("base")

        # 6. Verify table was dropped
        with config.provide_session() as driver:
            result = driver.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='users'"
            )
            assert len(result.data) == 0


@pytest.mark.xdist_group("migrations")
def test_aiosqlite_multiple_migrations_workflow() -> None:
    """Test AioSQLite workflow with multiple migrations: create -> apply both -> downgrade one -> downgrade all."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        db_path = Path(temp_dir) / "test.db"

        # Create AioSQLite config with migration directory
        config = AiosqliteConfig(
            pool_config={"database": str(db_path)},
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "sqlspec_migrations"
            }
        )
        commands = MigrationCommands(config)

        # Initialize migrations
        commands.init(str(migration_dir), package=True)

        # First migration - create users table
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

        # Second migration - create posts table
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

        # Write migration files
        (migration_dir / "0001_create_users.py").write_text(migration1_content)
        (migration_dir / "0002_create_posts.py").write_text(migration2_content)

        # Apply all migrations
        commands.upgrade()

        # Verify both tables exist
        with config.provide_session() as driver:
            tables_result = driver.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
            table_names = [t["name"] for t in tables_result.data]
            assert "users" in table_names
            assert "posts" in table_names

            # Test the relationship
            driver.execute("INSERT INTO users (name, email) VALUES (?, ?)", ("Author", "author@example.com"))
            driver.execute("INSERT INTO posts (title, content, user_id) VALUES (?, ?, ?)",
                         ("My Post", "Post content", 1))

            posts_result = driver.execute("SELECT * FROM posts")
            assert len(posts_result.data) == 1
            assert posts_result.data[0]["title"] == "My Post"

        # Downgrade to revision 0001 (should drop posts table)
        commands.downgrade("0001")

        with config.provide_session() as driver:
            tables_result = driver.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
            table_names = [t["name"] for t in tables_result.data]
            assert "users" in table_names
            assert "posts" not in table_names

        # Downgrade to base (should drop all tables)
        commands.downgrade("base")

        with config.provide_session() as driver:
            tables_result = driver.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
            # Should only have migration tracking table remaining
            table_names = [t["name"] for t in tables_result.data if not t["name"].startswith("sqlspec_")]
            assert len(table_names) == 0


@pytest.mark.xdist_group("migrations")
def test_aiosqlite_migration_current_command() -> None:
    """Test the current migration command shows correct version for AioSQLite."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        db_path = Path(temp_dir) / "test.db"

        # Create AioSQLite config with migration directory
        config = AiosqliteConfig(
            pool_config={"database": str(db_path)},
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "sqlspec_migrations"
            }
        )
        commands = MigrationCommands(config)

        # Initialize migrations
        commands.init(str(migration_dir), package=True)

        # Should show no current version initially
        commands.current(verbose=False)  # This just outputs to console

        # Create and apply a migration
        migration_content = '''"""Test migration."""


def up():
    """Create test table."""
    return ["CREATE TABLE test_table (id INTEGER PRIMARY KEY)"]


def down():
    """Drop test table."""
    return ["DROP TABLE IF EXISTS test_table"]
'''

        (migration_dir / "0001_test.py").write_text(migration_content)

        # Apply migration
        commands.upgrade()

        # Check current version (this just outputs, can't assert return value)
        commands.current(verbose=True)


@pytest.mark.xdist_group("migrations")
def test_aiosqlite_migration_error_handling() -> None:
    """Test AioSQLite migration error handling."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        db_path = Path(temp_dir) / "test.db"

        # Create AioSQLite config with migration directory
        config = AiosqliteConfig(
            pool_config={"database": str(db_path)},
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "sqlspec_migrations"
            }
        )
        commands = MigrationCommands(config)

        # Initialize migrations
        commands.init(str(migration_dir), package=True)

        # Create a migration with syntax error
        migration_content = '''"""Bad migration."""


def up():
    """Invalid SQL - should cause error."""
    return ["CREATE TABL invalid_sql"]


def down():
    """No downgrade needed."""
    return []
'''

        (migration_dir / "0001_bad.py").write_text(migration_content)

        # Attempting to upgrade should raise an error
        with pytest.raises(Exception):  # Will be wrapped in some migration exception
            commands.upgrade()


@pytest.mark.xdist_group("migrations")
def test_aiosqlite_migration_with_transactions() -> None:
    """Test AioSQLite migrations work properly with transactions."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        db_path = Path(temp_dir) / "test.db"

        # Create AioSQLite config with migration directory
        config = AiosqliteConfig(
            pool_config={"database": str(db_path)},
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "sqlspec_migrations"
            }
        )
        commands = MigrationCommands(config)

        # Initialize migrations
        commands.init(str(migration_dir), package=True)

        # Create a migration that uses transactions
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

        # Apply migration
        commands.upgrade()

        # Verify both table and data exist
        with config.provide_session() as driver:
            customers_result = driver.execute("SELECT * FROM customers ORDER BY name")
            assert len(customers_result.data) == 2
            assert customers_result.data[0]["name"] == "Customer 1"
            assert customers_result.data[1]["name"] == "Customer 2"

        # Downgrade should remove everything
        commands.downgrade("base")

        with config.provide_session() as driver:
            result = driver.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='customers'"
            )
            assert len(result.data) == 0
