"""Integration tests for ADBC migration workflow."""

import tempfile
from pathlib import Path

import pytest

from sqlspec.adapters.adbc.config import AdbcConfig
from sqlspec.migrations.commands import MigrationCommands

# xdist_group is assigned per test based on database backend to enable parallel execution


@pytest.mark.xdist_group("sqlite")
def test_adbc_sqlite_migration_full_workflow() -> None:
    """Test full ADBC SQLite migration workflow: init -> create -> upgrade -> downgrade."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        db_path = Path(temp_dir) / "test.db"

        config = AdbcConfig(
            connection_config={"driver_name": "adbc_driver_sqlite", "uri": f"file:{db_path}", "autocommit": True},
            migration_config={"script_location": str(migration_dir), "version_table_name": "sqlspec_migrations"},
        )
        commands = MigrationCommands(config)

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

        migration_file = migration_dir / "0001_create_users.py"
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


@pytest.mark.xdist_group("postgres")
def test_adbc_postgresql_migration_workflow() -> None:
    """Test ADBC PostgreSQL migration workflow with test database."""
    pytest.skip("PostgreSQL ADBC driver tests require running PostgreSQL instance")


@pytest.mark.xdist_group("sqlite")
def test_adbc_multiple_migrations_workflow() -> None:
    """Test ADBC workflow with multiple migrations: create -> apply both -> downgrade one -> downgrade all."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        db_path = Path(temp_dir) / "test.db"

        config = AdbcConfig(
            connection_config={"driver_name": "adbc_driver_sqlite", "uri": f"file:{db_path}", "autocommit": True},
            migration_config={"script_location": str(migration_dir), "version_table_name": "sqlspec_migrations"},
        )
        commands = MigrationCommands(config)

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
            driver.execute(
                "INSERT INTO posts (title, content, user_id) VALUES (?, ?, ?)", ("My Post", "Post content", 1)
            )

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
            tables_result = driver.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )

            table_names = [t["name"] for t in tables_result.data if not t["name"].startswith("sqlspec_")]
            assert len(table_names) == 0


@pytest.mark.xdist_group("sqlite")
def test_adbc_migration_current_command() -> None:
    """Test the current migration command shows correct version for ADBC."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        db_path = Path(temp_dir) / "test.db"

        config = AdbcConfig(
            connection_config={"driver_name": "adbc_driver_sqlite", "uri": f"file:{db_path}", "autocommit": True},
            migration_config={"script_location": str(migration_dir), "version_table_name": "sqlspec_migrations"},
        )
        commands = MigrationCommands(config)

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

        (migration_dir / "0001_test.py").write_text(migration_content)

        commands.upgrade()

        commands.current(verbose=True)


@pytest.mark.xdist_group("sqlite")
def test_adbc_migration_error_handling() -> None:
    """Test ADBC migration error handling."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        db_path = Path(temp_dir) / "test.db"

        config = AdbcConfig(
            connection_config={"driver_name": "adbc_driver_sqlite", "uri": f"file:{db_path}", "autocommit": True},
            migration_config={"script_location": str(migration_dir), "version_table_name": "sqlspec_migrations"},
        )
        commands = MigrationCommands(config)

        commands.init(str(migration_dir), package=True)

        migration_content = '''"""Bad migration."""


def up():
    """Invalid SQL - should cause error."""
    return ["CREATE SOME TABLE invalid_sql"]


def down():
    """No downgrade needed."""
    return []
'''

        (migration_dir / "0001_bad.py").write_text(migration_content)

        with pytest.raises(Exception):
            commands.upgrade()


@pytest.mark.xdist_group("sqlite")
def test_adbc_migration_with_transactions() -> None:
    """Test ADBC migrations work properly with transactions."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        db_path = Path(temp_dir) / "test.db"

        config = AdbcConfig(
            connection_config={"driver_name": "adbc_driver_sqlite", "uri": f"file:{db_path}", "autocommit": True},
            migration_config={"script_location": str(migration_dir), "version_table_name": "sqlspec_migrations"},
        )
        commands = MigrationCommands(config)

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
