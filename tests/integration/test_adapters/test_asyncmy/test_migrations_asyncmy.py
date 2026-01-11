"""Integration tests for Asyncmy (MySQL) migration workflow."""

from pathlib import Path

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.asyncmy.config import AsyncmyConfig
from sqlspec.migrations.commands import AsyncMigrationCommands

pytestmark = pytest.mark.xdist_group("mysql")


async def test_asyncmy_migration_full_workflow(tmp_path: Path, mysql_service: MySQLService) -> None:
    """Test full Asyncmy migration workflow: init -> create -> upgrade -> downgrade."""

    test_id = "asyncmy_full_workflow"
    migration_table = f"sqlspec_migrations_{test_id}"
    users_table = f"users_{test_id}"

    migration_dir = tmp_path / "migrations"

    config = AsyncmyConfig(
        connection_config={
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

    await commands.init(str(migration_dir), package=True)

    assert migration_dir.exists()
    assert (migration_dir / "__init__.py").exists()

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

    migration_file = migration_dir / "0001_create_users.py"
    migration_file.write_text(migration_content)

    try:
        await commands.upgrade()

        async with config.provide_session() as driver:
            result = await driver.execute(
                f"SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_name = '{users_table}'",
                (mysql_service.db,),
            )
            assert len(result.data) == 1

            await driver.execute(
                f"INSERT INTO {users_table} (name, email) VALUES (%s, %s)", ("John Doe", "john@example.com")
            )

            users_result = await driver.execute(f"SELECT * FROM {users_table}")
            assert len(users_result.data) == 1
            assert users_result.data[0]["name"] == "John Doe"
            assert users_result.data[0]["email"] == "john@example.com"

        await commands.downgrade("base")

        async with config.provide_session() as driver:
            result = await driver.execute(
                f"SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_name = '{users_table}'",
                (mysql_service.db,),
            )
            assert len(result.data) == 0
    finally:
        if config.connection_instance:
            await config.close_pool()


async def test_asyncmy_multiple_migrations_workflow(tmp_path: Path, mysql_service: MySQLService) -> None:
    """Test Asyncmy workflow with multiple migrations: create -> apply both -> downgrade one -> downgrade all."""

    test_id = "asyncmy_multiple_workflow"
    migration_table = f"sqlspec_migrations_{test_id}"
    users_table = f"users_{test_id}"
    posts_table = f"posts_{test_id}"

    migration_dir = tmp_path / "migrations"

    config = AsyncmyConfig(
        connection_config={
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

    await commands.init(str(migration_dir), package=True)

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
        await commands.upgrade()

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

            await driver.execute(
                f"INSERT INTO {users_table} (name, email) VALUES (%s, %s)", ("John Doe", "john@example.com")
            )
            await driver.execute(
                f"INSERT INTO {posts_table} (title, content, user_id) VALUES (%s, %s, %s)",
                ("Test Post", "This is a test post", 1),
            )

        await commands.downgrade("0001")

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

        await commands.downgrade("base")

        async with config.provide_session() as driver:
            users_result = await driver.execute(
                f"SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_name IN ('{users_table}', '{posts_table}')",
                (mysql_service.db,),
            )
            assert len(users_result.data) == 0
    finally:
        if config.connection_instance:
            await config.close_pool()


async def test_asyncmy_migration_current_command(tmp_path: Path, mysql_service: MySQLService) -> None:
    """Test the current migration command shows correct version for Asyncmy."""

    test_id = "asyncmy_current_cmd"
    migration_table = f"sqlspec_migrations_{test_id}"
    users_table = f"users_{test_id}"

    migration_dir = tmp_path / "migrations"

    config = AsyncmyConfig(
        connection_config={
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
        await commands.init(str(migration_dir), package=True)

        current_version = await commands.current()
        assert current_version is None or current_version == "base"

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

        await commands.upgrade()

        current_version = await commands.current()
        assert current_version == "0001"

        await commands.downgrade("base")

        current_version = await commands.current()
        assert current_version is None or current_version == "base"
    finally:
        if config.connection_instance:
            await config.close_pool()


async def test_asyncmy_migration_error_handling(tmp_path: Path, mysql_service: MySQLService) -> None:
    """Test Asyncmy migration error handling."""

    test_id = "asyncmy_error_handling"
    migration_table = f"sqlspec_migrations_{test_id}"

    migration_dir = tmp_path / "migrations"

    config = AsyncmyConfig(
        connection_config={
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
        await commands.init(str(migration_dir), package=True)

        migration_content = '''"""Migration with invalid SQL."""


def up():
    """Create table with invalid SQL."""
    return ["CREATE INVALID SQL STATEMENT"]


def down():
    """Drop table."""
    return ["DROP TABLE IF EXISTS invalid_table"]
'''
        (migration_dir / "0001_invalid.py").write_text(migration_content)

        await commands.upgrade()

        async with config.provide_session() as driver:
            count = await driver.select_value(f"SELECT COUNT(*) FROM {migration_table}")
            assert count == 0, f"Expected empty migration table after failed migration, but found {count} records"
    finally:
        if config.connection_instance:
            await config.close_pool()


async def test_asyncmy_migration_with_transactions(tmp_path: Path, mysql_service: MySQLService) -> None:
    """Test Asyncmy migrations work properly with transactions."""

    test_id = "asyncmy_transactions"
    migration_table = f"sqlspec_migrations_{test_id}"
    users_table = f"users_{test_id}"

    migration_dir = tmp_path / "migrations"

    config = AsyncmyConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "autocommit": False,
        },
        migration_config={"script_location": str(migration_dir), "version_table_name": migration_table},
    )
    commands = AsyncMigrationCommands(config)

    try:
        await commands.init(str(migration_dir), package=True)

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

        await commands.upgrade()

        async with config.provide_session() as driver:
            await driver.begin()
            try:
                await driver.execute(
                    f"INSERT INTO {users_table} (name, email) VALUES (%s, %s)",
                    ("Transaction User", "trans@example.com"),
                )

                result = await driver.execute(f"SELECT * FROM {users_table} WHERE name = 'Transaction User'")
                assert len(result.data) == 1
                await driver.commit()
            except Exception:
                await driver.rollback()
                raise

            result = await driver.execute(f"SELECT * FROM {users_table} WHERE name = 'Transaction User'")
            assert len(result.data) == 1

        async with config.provide_session() as driver:
            await driver.begin()
            try:
                await driver.execute(
                    f"INSERT INTO {users_table} (name, email) VALUES (%s, %s)",
                    ("Rollback User", "rollback@example.com"),
                )

                raise Exception("Intentional rollback")
            except Exception:
                await driver.rollback()

            result = await driver.execute(f"SELECT * FROM {users_table} WHERE name = 'Rollback User'")
            assert len(result.data) == 0
    finally:
        if config.connection_instance:
            await config.close_pool()
