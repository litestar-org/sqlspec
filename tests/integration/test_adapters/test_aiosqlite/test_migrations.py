"""Integration tests for AioSQLite migration workflow."""

import tempfile
from pathlib import Path

import pytest

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.migrations.commands import AsyncMigrationCommands


@pytest.mark.xdist_group("migrations")
async def test_aiosqlite_migration_full_workflow() -> None:
    """Test full AioSQLite migration workflow: init -> create -> upgrade -> downgrade."""
    # Generate unique table names for this test
    test_id = "aiosqlite_full_workflow"
    migration_table = f"sqlspec_migrations_{test_id}"
    users_table = f"users_{test_id}"

    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        db_path = Path(temp_dir) / "test.db"

        # Create AioSQLite config with migration directory
        config = AiosqliteConfig(
            pool_config={"database": str(db_path)},
            migration_config={"script_location": str(migration_dir), "version_table_name": migration_table},
        )
        commands = AsyncMigrationCommands(config)

        # 1. Initialize migrations
        await commands.init(str(migration_dir), package=True)

        # Verify initialization
        assert migration_dir.exists()
        assert (migration_dir / "__init__.py").exists()

        # 2. Create a migration with simple schema
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

        # Write migration file
        migration_file = migration_dir / "0001_create_users.py"
        migration_file.write_text(migration_content)

        # 3. Apply migration (upgrade)
        await commands.upgrade()

        # 4. Verify migration was applied
        # Note: We use the unified MigrationCommands interface which handles async/sync internally
        async with config.provide_session() as driver:
            # Check that table exists
            result = await driver.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{users_table}'")
            assert len(result.data) == 1

            # Insert test data
            await driver.execute(
                f"INSERT INTO {users_table} (name, email) VALUES (?, ?)", ("John Doe", "john@example.com")
            )

            # Verify data
            users_result = await driver.execute(f"SELECT * FROM {users_table}")
            assert len(users_result.data) == 1
            assert users_result.data[0]["name"] == "John Doe"
            assert users_result.data[0]["email"] == "john@example.com"

        try:
            # 5. Downgrade migration
            await commands.downgrade("base")

            # 6. Verify table was dropped
            async with config.provide_session() as driver:
                result = await driver.execute(
                    f"SELECT name FROM sqlite_master WHERE type='table' AND name='{users_table}'"
                )
                assert len(result.data) == 0
        finally:
            # Ensure pool is closed
            if config.pool_instance:
                await config.close_pool()


@pytest.mark.xdist_group("migrations")
async def test_aiosqlite_multiple_migrations_workflow() -> None:
    """Test AioSQLite workflow with multiple migrations: create -> apply both -> downgrade one -> downgrade all."""
    # Generate unique table names for this test
    test_id = "aiosqlite_multiple_workflow"
    migration_table = f"sqlspec_migrations_{test_id}"
    users_table = f"users_{test_id}"
    posts_table = f"posts_{test_id}"

    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        db_path = Path(temp_dir) / "test.db"

        # Create AioSQLite config with migration directory
        config = AiosqliteConfig(
            pool_config={"database": str(db_path)},
            migration_config={"script_location": str(migration_dir), "version_table_name": migration_table},
        )
        commands = AsyncMigrationCommands(config)

        # Initialize migrations
        await commands.init(str(migration_dir), package=True)

        # First migration - create users table
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

        # Second migration - create posts table
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

        # Write migration files
        (migration_dir / "0001_create_users.py").write_text(migration1_content)
        (migration_dir / "0002_create_posts.py").write_text(migration2_content)

        try:
            # Apply all migrations
            await commands.upgrade()

            # Verify both tables exist
            async with config.provide_session() as driver:
                tables_result = await driver.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
                table_names = [t["name"] for t in tables_result.data]
                assert users_table in table_names
                assert posts_table in table_names

                # Test the relationship
                await driver.execute(
                    f"INSERT INTO {users_table} (name, email) VALUES (?, ?)", ("Author", "author@example.com")
                )
                await driver.execute(
                    f"INSERT INTO {posts_table} (title, content, user_id) VALUES (?, ?, ?)",
                    ("My Post", "Post content", 1),
                )

                posts_result = await driver.execute(f"SELECT * FROM {posts_table}")
                assert len(posts_result.data) == 1
                assert posts_result.data[0]["title"] == "My Post"

            # Downgrade to revision 0001 (should drop posts table)
            await commands.downgrade("0001")

            async with config.provide_session() as driver:
                tables_result = await driver.execute("SELECT name FROM sqlite_master WHERE type='table'")
                table_names = [t["name"] for t in tables_result.data]
                assert users_table in table_names
                assert posts_table not in table_names

            # Downgrade to base (should drop all tables)
            await commands.downgrade("base")

            async with config.provide_session() as driver:
                tables_result = await driver.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                )
                # Should only have migration tracking table remaining
                table_names = [t["name"] for t in tables_result.data if not t["name"].startswith("sqlspec_")]
                assert len(table_names) == 0
        finally:
            # Ensure pool is closed
            if config.pool_instance:
                await config.close_pool()


@pytest.mark.xdist_group("migrations")
async def test_aiosqlite_migration_current_command() -> None:
    """Test the current migration command shows correct version for AioSQLite."""
    # Generate unique table names for this test
    test_id = "aiosqlite_current_cmd"
    migration_table = f"sqlspec_migrations_{test_id}"
    test_table = f"test_table_{test_id}"

    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        db_path = Path(temp_dir) / "test.db"

        # Create AioSQLite config with migration directory
        config = AiosqliteConfig(
            pool_config={"database": str(db_path)},
            migration_config={"script_location": str(migration_dir), "version_table_name": migration_table},
        )
        commands = AsyncMigrationCommands(config)

        try:
            # Initialize migrations
            await commands.init(str(migration_dir), package=True)

            # Should show no current version initially
            await commands.current(verbose=False)  # This just outputs to console

            # Create and apply a migration
            migration_content = f'''"""Test migration."""


def up():
    """Create test table."""
    return ["CREATE TABLE {test_table} (id INTEGER PRIMARY KEY)"]


def down():
    """Drop test table."""
    return ["DROP TABLE IF EXISTS {test_table}"]
'''

            (migration_dir / "0001_test.py").write_text(migration_content)

            # Apply migration
            await commands.upgrade()

            # Check current version (this just outputs, can't assert return value)
            await commands.current(verbose=True)
        finally:
            # Ensure pool is closed
            if config.pool_instance:
                await config.close_pool()


@pytest.mark.xdist_group("migrations")
async def test_aiosqlite_migration_error_handling() -> None:
    """Test AioSQLite migration error handling."""
    # Generate unique table names for this test
    test_id = "aiosqlite_error_handling"
    migration_table = f"sqlspec_migrations_{test_id}"

    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        db_path = Path(temp_dir) / "test.db"

        # Create AioSQLite config with migration directory
        config = AiosqliteConfig(
            pool_config={"database": str(db_path)},
            migration_config={"script_location": str(migration_dir), "version_table_name": migration_table},
        )
        commands = AsyncMigrationCommands(config)

        try:
            # Initialize migrations
            await commands.init(str(migration_dir), package=True)

            # Create a migration with syntax error
            migration_content = '''"""Bad migration."""


def up():
    """Invalid SQL - should cause error."""
    return ["CREATE A TABLE invalid_sql"]


def down():
    """No downgrade needed."""
    return []
'''

            (migration_dir / "0001_bad.py").write_text(migration_content)

            # Attempting to upgrade should raise an error
            with pytest.raises(Exception):  # Will be wrapped in some migration exception
                await commands.upgrade()
        finally:
            # Ensure pool is closed
            if config.pool_instance:
                await config.close_pool()


@pytest.mark.xdist_group("migrations")
async def test_aiosqlite_migration_with_transactions() -> None:
    """Test AioSQLite migrations work properly with transactions."""
    # Generate unique table names for this test
    test_id = "aiosqlite_transactions"
    migration_table = f"sqlspec_migrations_{test_id}"
    customers_table = f"customers_{test_id}"

    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        db_path = Path(temp_dir) / "test.db"

        # Create AioSQLite config with migration directory
        config = AiosqliteConfig(
            pool_config={"database": str(db_path)},
            migration_config={"script_location": str(migration_dir), "version_table_name": migration_table},
        )
        commands = AsyncMigrationCommands(config)

        try:
            # Initialize migrations
            await commands.init(str(migration_dir), package=True)

            # Create a migration that uses transactions
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

            # Apply migration
            await commands.upgrade()

            # Verify both table and data exist
            async with config.provide_session() as driver:
                customers_result = await driver.execute(f"SELECT * FROM {customers_table} ORDER BY name")
                assert len(customers_result.data) == 2
                assert customers_result.data[0]["name"] == "Customer 1"
                assert customers_result.data[1]["name"] == "Customer 2"

            # Downgrade should remove everything
            await commands.downgrade("base")

            async with config.provide_session() as driver:
                result = await driver.execute(
                    f"SELECT name FROM sqlite_master WHERE type='table' AND name='{customers_table}'"
                )
                assert len(result.data) == 0
        finally:
            # Ensure pool is closed
            if config.pool_instance:
                await config.close_pool()
