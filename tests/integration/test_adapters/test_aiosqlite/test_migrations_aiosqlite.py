"""Integration tests for AioSQLite migration workflow."""

from pathlib import Path

import pytest

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.migrations.commands import AsyncMigrationCommands

pytestmark = pytest.mark.xdist_group("sqlite")


async def test_aiosqlite_migration_full_workflow(tmp_path: Path) -> None:
    """Test full AioSQLite migration workflow: init -> create -> upgrade -> downgrade."""

    test_id = "aiosqlite_full_workflow"
    migration_table = f"sqlspec_migrations_{test_id}"
    users_table = f"users_{test_id}"

    migration_dir = tmp_path / "migrations"
    db_path = tmp_path / "test.db"

    config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migration_dir), "version_table_name": migration_table},
    )
    commands = AsyncMigrationCommands(config)

    await commands.init(str(migration_dir), package=True)

    assert migration_dir.exists()
    assert (migration_dir / "__init__.py").exists()

    migration_content = f'''"""Initial schema migration."""


def up():
    """Create users table."""
    return ["""
        CREATE TABLE {users_table} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """]


def down():
    """Drop users table."""
    return ["DROP TABLE IF EXISTS {users_table}"]
'''

    migration_file = migration_dir / "0001_create_users.py"
    migration_file.write_text(migration_content)

    await commands.upgrade()

    async with config.provide_session() as driver:
        result = await driver.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{users_table}'")
        assert len(result.data) == 1

        await driver.execute(f"INSERT INTO {users_table} (name, email) VALUES (?, ?)", ("John Doe", "john@example.com"))

        users_result = await driver.execute(f"SELECT * FROM {users_table}")
        assert len(users_result.data) == 1
        assert users_result.data[0]["name"] == "John Doe"
        assert users_result.data[0]["email"] == "john@example.com"

    try:
        await commands.downgrade("base")

        async with config.provide_session() as driver:
            result = await driver.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{users_table}'")
            assert len(result.data) == 0
    finally:
        if config.connection_instance:
            await config.close_pool()


async def test_aiosqlite_multiple_migrations_workflow(tmp_path: Path) -> None:
    """Test AioSQLite workflow with multiple migrations: create -> apply both -> downgrade one -> downgrade all."""

    test_id = "aiosqlite_multiple_workflow"
    migration_table = f"sqlspec_migrations_{test_id}"
    users_table = f"users_{test_id}"
    posts_table = f"posts_{test_id}"

    migration_dir = tmp_path / "migrations"
    db_path = tmp_path / "test.db"

    config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migration_dir), "version_table_name": migration_table},
    )
    commands = AsyncMigrationCommands(config)

    await commands.init(str(migration_dir), package=True)

    migration1_content = f'''"""Create users table."""


def up():
    """Create users table."""
    return ["""
        CREATE TABLE {users_table} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL
        )
    """]


def down():
    """Drop users table."""
    return ["DROP TABLE IF EXISTS {users_table}"]
'''

    migration2_content = f'''"""Create posts table."""


def up():
    """Create posts table."""
    return ["""
        CREATE TABLE {posts_table} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            content TEXT,
            user_id INTEGER,
            FOREIGN KEY (user_id) REFERENCES {users_table} (id)
        )
    """]


def down():
    """Drop posts table."""
    return ["DROP TABLE IF EXISTS {posts_table}"]
'''

    (migration_dir / "0001_create_users.py").write_text(migration1_content)
    (migration_dir / "0002_create_posts.py").write_text(migration2_content)

    try:
        await commands.upgrade()

        async with config.provide_session() as driver:
            tables_result = await driver.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            table_names = [t["name"] for t in tables_result.data]
            assert users_table in table_names
            assert posts_table in table_names

            await driver.execute(
                f"INSERT INTO {users_table} (name, email) VALUES (?, ?)", ("Author", "author@example.com")
            )
            await driver.execute(
                f"INSERT INTO {posts_table} (title, content, user_id) VALUES (?, ?, ?)", ("My Post", "Post content", 1)
            )

            posts_result = await driver.execute(f"SELECT * FROM {posts_table}")
            assert len(posts_result.data) == 1
            assert posts_result.data[0]["title"] == "My Post"

        await commands.downgrade("0001")

        async with config.provide_session() as driver:
            tables_result = await driver.execute("SELECT name FROM sqlite_master WHERE type='table'")
            table_names = [t["name"] for t in tables_result.data]
            assert users_table in table_names
            assert posts_table not in table_names

        await commands.downgrade("base")

        async with config.provide_session() as driver:
            tables_result = await driver.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )

            table_names = [t["name"] for t in tables_result.data if not t["name"].startswith("sqlspec_")]
            assert len(table_names) == 0
    finally:
        if config.connection_instance:
            await config.close_pool()


async def test_aiosqlite_migration_current_command(tmp_path: Path) -> None:
    """Test the current migration command shows correct version for AioSQLite."""

    test_id = "aiosqlite_current_cmd"
    migration_table = f"sqlspec_migrations_{test_id}"
    test_table = f"test_table_{test_id}"

    migration_dir = tmp_path / "migrations"
    db_path = tmp_path / "test.db"

    config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migration_dir), "version_table_name": migration_table},
    )
    commands = AsyncMigrationCommands(config)

    try:
        await commands.init(str(migration_dir), package=True)

        await commands.current(verbose=False)

        migration_content = f'''"""Test migration."""


def up():
    """Create test table."""
    return ["CREATE TABLE {test_table} (id INTEGER PRIMARY KEY)"]


def down():
    """Drop test table."""
    return ["DROP TABLE IF EXISTS {test_table}"]
'''

        (migration_dir / "0001_test.py").write_text(migration_content)

        await commands.upgrade()

        await commands.current(verbose=True)
    finally:
        if config.connection_instance:
            await config.close_pool()


async def test_aiosqlite_migration_error_handling(tmp_path: Path) -> None:
    """Test AioSQLite migration error handling."""

    test_id = "aiosqlite_error_handling"
    migration_table = f"sqlspec_migrations_{test_id}"

    migration_dir = tmp_path / "migrations"
    db_path = tmp_path / "test.db"

    config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migration_dir), "version_table_name": migration_table},
    )
    commands = AsyncMigrationCommands(config)

    try:
        await commands.init(str(migration_dir), package=True)

        migration_content = '''"""Bad migration."""


def up():
    """Invalid SQL - should cause error."""
    return ["CREATE A TABLE invalid_sql"]


def down():
    """No downgrade needed."""
    return []
'''

        (migration_dir / "0001_bad.py").write_text(migration_content)

        await commands.upgrade()

        async with config.provide_session() as driver:
            try:
                await driver.execute(f"SELECT version FROM {migration_table} ORDER BY version")
                msg = "Expected migration table to not exist, but it does"
                raise AssertionError(msg)
            except Exception as e:
                assert "no such" in str(e).lower() or "does not exist" in str(e).lower()
    finally:
        if config.connection_instance:
            await config.close_pool()


async def test_aiosqlite_migration_with_transactions(tmp_path: Path) -> None:
    """Test AioSQLite migrations work properly with transactions."""

    test_id = "aiosqlite_transactions"
    migration_table = f"sqlspec_migrations_{test_id}"
    customers_table = f"customers_{test_id}"

    migration_dir = tmp_path / "migrations"
    db_path = tmp_path / "test.db"

    config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migration_dir), "version_table_name": migration_table},
    )
    commands = AsyncMigrationCommands(config)

    try:
        await commands.init(str(migration_dir), package=True)

        migration_content = f'''"""Migration with multiple operations."""


def up():
    """Create customers table with data."""
    return [
        """CREATE TABLE {customers_table} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL
        )""",
        "INSERT INTO {customers_table} (name) VALUES ('Customer 1')",
        "INSERT INTO {customers_table} (name) VALUES ('Customer 2')"
    ]


def down():
    """Drop customers table."""
    return ["DROP TABLE IF EXISTS {customers_table}"]
'''

        (migration_dir / "0001_transaction_test.py").write_text(migration_content)

        await commands.upgrade()

        async with config.provide_session() as driver:
            customers_result = await driver.execute(f"SELECT * FROM {customers_table} ORDER BY name")
            assert len(customers_result.data) == 2
            assert customers_result.data[0]["name"] == "Customer 1"
            assert customers_result.data[1]["name"] == "Customer 2"

        await commands.downgrade("base")

        async with config.provide_session() as driver:
            result = await driver.execute(
                f"SELECT name FROM sqlite_master WHERE type='table' AND name='{customers_table}'"
            )
            assert len(result.data) == 0
    finally:
        if config.connection_instance:
            await config.close_pool()
