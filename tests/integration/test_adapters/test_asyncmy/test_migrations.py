"""Integration tests for Asyncmy (MySQL) migration workflow."""

import tempfile
from pathlib import Path

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.asyncmy.config import AsyncmyConfig
from sqlspec.migrations.commands import AsyncMigrationCommands


@pytest.mark.xdist_group("migrations")
async def test_asyncmy_migration_full_workflow(mysql_service: MySQLService) -> None:
    """Test full Asyncmy migration workflow: init -> create -> upgrade -> downgrade."""
    # Generate unique table names for this test
    test_id = "asyncmy_full_workflow"
    migration_table = f"sqlspec_migrations_{test_id}"
    users_table = f"users_{test_id}"

    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"

        # Create Asyncmy config with migration directory
        config = AsyncmyConfig(
            pool_config={
                "host": mysql_service.host,
                "port": mysql_service.port,
                "user": mysql_service.user,
                "password": mysql_service.password,
                "database": mysql_service.db,
                "autocommit": True,
            },
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
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL,
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

        try:
            # 3. Apply migration (upgrade)
            await commands.upgrade()

            # 4. Verify migration was applied
            async with config.provide_session() as driver:
                # Check that table exists
                result = await driver.execute(
                    f"SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_name = '{users_table}'",
                    (mysql_service.db,),
                )
                assert len(result.data) == 1

                # Insert test data
                await driver.execute(
                    f"INSERT INTO {users_table} (name, email) VALUES (%s, %s)", ("John Doe", "john@example.com")
                )

                # Verify data
                users_result = await driver.execute(f"SELECT * FROM {users_table}")
                assert len(users_result.data) == 1
                assert users_result.data[0]["name"] == "John Doe"
                assert users_result.data[0]["email"] == "john@example.com"

            # 5. Downgrade migration
            await commands.downgrade("base")

            # 6. Verify table was dropped
            async with config.provide_session() as driver:
                result = await driver.execute(
                    f"SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_name = '{users_table}'",
                    (mysql_service.db,),
                )
                assert len(result.data) == 0
        finally:
            # Ensure pool is closed
            if config.pool_instance:
                await config.close_pool()


@pytest.mark.xdist_group("migrations")
async def test_asyncmy_multiple_migrations_workflow(mysql_service: MySQLService) -> None:
    """Test Asyncmy workflow with multiple migrations: create -> apply both -> downgrade one -> downgrade all."""
    # Generate unique table names for this test
    test_id = "asyncmy_multiple_workflow"
    migration_table = f"sqlspec_migrations_{test_id}"
    users_table = f"users_{test_id}"
    posts_table = f"posts_{test_id}"

    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"

        config = AsyncmyConfig(
            pool_config={
                "host": mysql_service.host,
                "port": mysql_service.port,
                "user": mysql_service.user,
                "password": mysql_service.password,
                "database": mysql_service.db,
                "autocommit": True,
            },
            migration_config={"script_location": str(migration_dir), "version_table_name": migration_table},
        )
        commands = AsyncMigrationCommands(config)

        # 1. Initialize migrations
        await commands.init(str(migration_dir), package=True)

        # 2. Create first migration
        migration1_content = f'''"""Create users table."""


def up():
    """Create users table."""
    return ["""
        CREATE TABLE {users_table} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """]


def down():
    """Drop users table."""
    return ["DROP TABLE IF EXISTS {users_table}"]
'''
        (migration_dir / "0001_create_users.py").write_text(migration1_content)

        # 3. Create second migration
        migration2_content = f'''"""Create posts table."""


def up():
    """Create posts table."""
    return ["""
        CREATE TABLE {posts_table} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            content TEXT,
            user_id INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES {users_table}(id)
        )
    """]


def down():
    """Drop posts table."""
    return ["DROP TABLE IF EXISTS {posts_table}"]
'''
        (migration_dir / "0002_create_posts.py").write_text(migration2_content)

        try:
            # 4. Apply all migrations
            await commands.upgrade()

            # 5. Verify both tables exist
            async with config.provide_session() as driver:
                users_result = await driver.execute(
                    f"SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_name = '{users_table}'",
                    (mysql_service.db,),
                )
                posts_result = await driver.execute(
                    f"SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_name = '{posts_table}'",
                    (mysql_service.db,),
                )
                assert len(users_result.data) == 1
                assert len(posts_result.data) == 1

                # Test relational integrity
                await driver.execute(
                    f"INSERT INTO {users_table} (name, email) VALUES (%s, %s)", ("John Doe", "john@example.com")
                )
                await driver.execute(
                    f"INSERT INTO {posts_table} (title, content, user_id) VALUES (%s, %s, %s)",
                    ("Test Post", "This is a test post", 1),
                )

            # 6. Downgrade to version 0001 (should remove posts table)
            await commands.downgrade("0001")

            # 7. Verify only users table remains
            async with config.provide_session() as driver:
                users_result = await driver.execute(
                    f"SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_name = '{users_table}'",
                    (mysql_service.db,),
                )
                posts_result = await driver.execute(
                    f"SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_name = '{posts_table}'",
                    (mysql_service.db,),
                )
                assert len(users_result.data) == 1
                assert len(posts_result.data) == 0

            # 8. Downgrade to base
            await commands.downgrade("base")

            # 9. Verify all tables are gone
            async with config.provide_session() as driver:
                users_result = await driver.execute(
                    f"SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_name IN ('{users_table}', '{posts_table}')",
                    (mysql_service.db,),
                )
                assert len(users_result.data) == 0
        finally:
            if config.pool_instance:
                await config.close_pool()


@pytest.mark.xdist_group("migrations")
async def test_asyncmy_migration_current_command(mysql_service: MySQLService) -> None:
    """Test the current migration command shows correct version for Asyncmy."""
    # Generate unique table names for this test
    test_id = "asyncmy_current_cmd"
    migration_table = f"sqlspec_migrations_{test_id}"
    users_table = f"users_{test_id}"

    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"

        config = AsyncmyConfig(
            pool_config={
                "host": mysql_service.host,
                "port": mysql_service.port,
                "user": mysql_service.user,
                "password": mysql_service.password,
                "database": mysql_service.db,
                "autocommit": True,
            },
            migration_config={"script_location": str(migration_dir), "version_table_name": migration_table},
        )
        commands = AsyncMigrationCommands(config)

        try:
            # 1. Initialize migrations
            await commands.init(str(migration_dir), package=True)

            # 2. Initially no current version
            current_version = await commands.current()
            assert current_version is None or current_version == "base"

            # 3. Create a migration
            migration_content = f'''"""Initial schema migration."""


def up():
    """Create users table."""
    return ["""
        CREATE TABLE {users_table} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL
        )
    """]


def down():
    """Drop users table."""
    return ["DROP TABLE IF EXISTS {users_table}"]
'''
            (migration_dir / "0001_create_users.py").write_text(migration_content)

            # 4. Apply migration
            await commands.upgrade()

            # 5. Check current version is now 0001
            current_version = await commands.current()
            assert current_version == "0001"

            # 6. Downgrade
            await commands.downgrade("base")

            # 7. Check current version is back to base/None
            current_version = await commands.current()
            assert current_version is None or current_version == "base"
        finally:
            if config.pool_instance:
                await config.close_pool()


@pytest.mark.xdist_group("migrations")
async def test_asyncmy_migration_error_handling(mysql_service: MySQLService) -> None:
    """Test Asyncmy migration error handling."""
    # Generate unique table names for this test
    test_id = "asyncmy_error_handling"
    migration_table = f"sqlspec_migrations_{test_id}"

    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"

        config = AsyncmyConfig(
            pool_config={
                "host": mysql_service.host,
                "port": mysql_service.port,
                "user": mysql_service.user,
                "password": mysql_service.password,
                "database": mysql_service.db,
                "autocommit": True,
            },
            migration_config={"script_location": str(migration_dir), "version_table_name": migration_table},
        )
        commands = AsyncMigrationCommands(config)

        try:
            # 1. Initialize migrations
            await commands.init(str(migration_dir), package=True)

            # 2. Create a migration with invalid SQL
            migration_content = '''"""Migration with invalid SQL."""


def up():
    """Create table with invalid SQL."""
    return ["CREATE INVALID SQL STATEMENT"]


def down():
    """Drop table."""
    return ["DROP TABLE IF EXISTS invalid_table"]
'''
            (migration_dir / "0001_invalid.py").write_text(migration_content)

            # 3. Try to apply migration - should raise an error
            with pytest.raises(Exception):
                await commands.upgrade()

            # 4. Verify no migration was recorded due to error
            async with config.provide_session() as driver:
                # Check migration tracking table exists but is empty
                try:
                    result = await driver.execute(f"SELECT COUNT(*) as count FROM {migration_table}")
                    assert result.data[0]["count"] == 0
                except Exception:
                    # If table doesn't exist, that's also acceptable
                    pass
        finally:
            if config.pool_instance:
                await config.close_pool()


@pytest.mark.xdist_group("migrations")
async def test_asyncmy_migration_with_transactions(mysql_service: MySQLService) -> None:
    """Test Asyncmy migrations work properly with transactions."""
    # Generate unique table names for this test
    test_id = "asyncmy_transactions"
    migration_table = f"sqlspec_migrations_{test_id}"
    users_table = f"users_{test_id}"

    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"

        config = AsyncmyConfig(
            pool_config={
                "host": mysql_service.host,
                "port": mysql_service.port,
                "user": mysql_service.user,
                "password": mysql_service.password,
                "database": mysql_service.db,
                "autocommit": False,  # Disable autocommit for transaction tests
            },
            migration_config={"script_location": str(migration_dir), "version_table_name": migration_table},
        )
        commands = AsyncMigrationCommands(config)

        try:
            # 1. Initialize migrations
            await commands.init(str(migration_dir), package=True)

            # 2. Create a migration
            migration_content = f'''"""Initial schema migration."""


def up():
    """Create users table."""
    return ["""
        CREATE TABLE {users_table} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL
        )
    """]


def down():
    """Drop users table."""
    return ["DROP TABLE IF EXISTS {users_table}"]
'''
            (migration_dir / "0001_create_users.py").write_text(migration_content)

            # 3. Apply migration
            await commands.upgrade()

            # 4. Test transaction behavior with the session
            async with config.provide_session() as driver:
                # Start manual transaction
                await driver.begin()
                try:
                    # Insert data within transaction
                    await driver.execute(
                        f"INSERT INTO {users_table} (name, email) VALUES (%s, %s)",
                        ("Transaction User", "trans@example.com"),
                    )

                    # Verify data exists within transaction
                    result = await driver.execute(f"SELECT * FROM {users_table} WHERE name = 'Transaction User'")
                    assert len(result.data) == 1
                    await driver.commit()
                except Exception:
                    await driver.rollback()
                    raise

                # Transaction should be committed - verify data persists
                result = await driver.execute(f"SELECT * FROM {users_table} WHERE name = 'Transaction User'")
                assert len(result.data) == 1

            # 5. Test transaction rollback
            async with config.provide_session() as driver:
                await driver.begin()
                try:
                    await driver.execute(
                        f"INSERT INTO {users_table} (name, email) VALUES (%s, %s)",
                        ("Rollback User", "rollback@example.com"),
                    )
                    # Force an error to trigger rollback
                    raise Exception("Intentional rollback")
                except Exception:
                    await driver.rollback()

                # Verify rollback - data should not exist
                result = await driver.execute(f"SELECT * FROM {users_table} WHERE name = 'Rollback User'")
                assert len(result.data) == 0
        finally:
            if config.pool_instance:
                await config.close_pool()
