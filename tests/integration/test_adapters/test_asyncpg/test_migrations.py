"""Integration tests for AsyncPG (PostgreSQL) migration workflow."""

import tempfile
from pathlib import Path

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.asyncpg.config import AsyncpgConfig
from sqlspec.migrations.commands import AsyncMigrationCommands

pytestmark = pytest.mark.xdist_group("postgres")


async def test_asyncpg_migration_full_workflow(postgres_service: PostgresService) -> None:
    """Test full AsyncPG migration workflow: init -> create -> upgrade -> downgrade."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"

        config = AsyncpgConfig(
            pool_config={
                "host": postgres_service.host,
                "port": postgres_service.port,
                "user": postgres_service.user,
                "password": postgres_service.password,
                "database": postgres_service.database,
            },
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "sqlspec_migrations_asyncpg",
            },
        )
        commands = AsyncMigrationCommands(config)

        await commands.init(str(migration_dir), package=True)

        assert migration_dir.exists()
        assert (migration_dir / "__init__.py").exists()

        migration_content = '''"""Initial schema migration."""


def up():
    """Create users table."""
    return ["""
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """]


def down():
    """Drop users table."""
    return ["DROP TABLE IF EXISTS users"]
'''

        migration_file = migration_dir / "0001_create_users.py"
        migration_file.write_text(migration_content)

        try:
            await commands.upgrade()

            async with config.provide_session() as driver:
                result = await driver.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'users'"
                )
                assert len(result.data) == 1

                await driver.execute(
                    "INSERT INTO users (name, email) VALUES ($1, $2)", ("John Doe", "john@example.com")
                )

                users_result = await driver.execute("SELECT * FROM users")
                assert len(users_result.data) == 1
                assert users_result.data[0]["name"] == "John Doe"
                assert users_result.data[0]["email"] == "john@example.com"

            await commands.downgrade("base")

            async with config.provide_session() as driver:
                result = await driver.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'users'"
                )
                assert len(result.data) == 0
        finally:
            if config.pool_instance:
                await config.close_pool()


async def test_asyncpg_multiple_migrations_workflow(postgres_service: PostgresService) -> None:
    """Test AsyncPG workflow with multiple migrations: create -> apply both -> downgrade one -> downgrade all."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"

        config = AsyncpgConfig(
            pool_config={
                "host": postgres_service.host,
                "port": postgres_service.port,
                "user": postgres_service.user,
                "password": postgres_service.password,
                "database": postgres_service.database,
            },
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "sqlspec_migrations_asyncpg",
            },
        )
        commands = AsyncMigrationCommands(config)

        await commands.init(str(migration_dir), package=True)

        migration1_content = '''"""Create users table."""


def up():
    """Create users table."""
    return ["""
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """]


def down():
    """Drop users table."""
    return ["DROP TABLE IF EXISTS users"]
'''
        (migration_dir / "0001_create_users.py").write_text(migration1_content)

        migration2_content = '''"""Create posts table."""


def up():
    """Create posts table."""
    return ["""
        CREATE TABLE posts (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            content TEXT,
            user_id INTEGER REFERENCES users(id),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """]


def down():
    """Drop posts table."""
    return ["DROP TABLE IF EXISTS posts"]
'''
        (migration_dir / "0002_create_posts.py").write_text(migration2_content)

        try:
            await commands.upgrade()

            async with config.provide_session() as driver:
                users_result = await driver.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'users'"
                )
                posts_result = await driver.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'posts'"
                )
                assert len(users_result.data) == 1
                assert len(posts_result.data) == 1

                await driver.execute(
                    "INSERT INTO users (name, email) VALUES ($1, $2)", ("John Doe", "john@example.com")
                )
                await driver.execute(
                    "INSERT INTO posts (title, content, user_id) VALUES ($1, $2, $3)",
                    ("Test Post", "This is a test post", 1),
                )

            await commands.downgrade("0001")

            async with config.provide_session() as driver:
                users_result = await driver.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'users'"
                )
                posts_result = await driver.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'posts'"
                )
                assert len(users_result.data) == 1
                assert len(posts_result.data) == 0

            await commands.downgrade("base")

            async with config.provide_session() as driver:
                users_result = await driver.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('users', 'posts')"
                )
                assert len(users_result.data) == 0
        finally:
            if config.pool_instance:
                await config.close_pool()


async def test_asyncpg_migration_current_command(postgres_service: PostgresService) -> None:
    """Test the current migration command shows correct version for AsyncPG."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"

        config = AsyncpgConfig(
            pool_config={
                "host": postgres_service.host,
                "port": postgres_service.port,
                "user": postgres_service.user,
                "password": postgres_service.password,
                "database": postgres_service.database,
            },
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "sqlspec_migrations_asyncpg",
            },
        )
        commands = AsyncMigrationCommands(config)

        try:
            await commands.init(str(migration_dir), package=True)

            current_version = await commands.current()
            assert current_version is None or current_version == "base"

            migration_content = '''"""Initial schema migration."""


def up():
    """Create users table."""
    return ["""
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL
        )
    """]


def down():
    """Drop users table."""
    return ["DROP TABLE IF EXISTS users"]
'''
            (migration_dir / "0001_create_users.py").write_text(migration_content)

            await commands.upgrade()

            current_version = await commands.current()
            assert current_version == "0001"

            await commands.downgrade("base")

            current_version = await commands.current()
            assert current_version is None or current_version == "base"
        finally:
            if config.pool_instance:
                await config.close_pool()


async def test_asyncpg_migration_error_handling(postgres_service: PostgresService) -> None:
    """Test AsyncPG migration error handling."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"

        config = AsyncpgConfig(
            pool_config={
                "host": postgres_service.host,
                "port": postgres_service.port,
                "user": postgres_service.user,
                "password": postgres_service.password,
                "database": postgres_service.database,
            },
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "sqlspec_migrations_asyncpg",
            },
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

            with pytest.raises(Exception):
                await commands.upgrade()

            async with config.provide_session() as driver:
                try:
                    result = await driver.execute("SELECT COUNT(*) as count FROM sqlspec_migrations")
                    assert result.data[0]["count"] == 0
                except Exception:
                    pass
        finally:
            if config.pool_instance:
                await config.close_pool()


async def test_asyncpg_migration_with_transactions(postgres_service: PostgresService) -> None:
    """Test AsyncPG migrations work properly with transactions."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"

        config = AsyncpgConfig(
            pool_config={
                "host": postgres_service.host,
                "port": postgres_service.port,
                "user": postgres_service.user,
                "password": postgres_service.password,
                "database": postgres_service.database,
            },
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "sqlspec_migrations_asyncpg",
            },
        )
        commands = AsyncMigrationCommands(config)

        try:
            await commands.init(str(migration_dir), package=True)

            migration_content = '''"""Initial schema migration."""


def up():
    """Create users table."""
    return ["""
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL
        )
    """]


def down():
    """Drop users table."""
    return ["DROP TABLE IF EXISTS users"]
'''
            (migration_dir / "0001_create_users.py").write_text(migration_content)

            await commands.upgrade()

            async with config.provide_session() as driver:
                await driver.begin()
                try:
                    await driver.execute(
                        "INSERT INTO users (name, email) VALUES ($1, $2)", ("Transaction User", "trans@example.com")
                    )

                    result = await driver.execute("SELECT * FROM users WHERE name = 'Transaction User'")
                    assert len(result.data) == 1
                    await driver.commit()
                except Exception:
                    await driver.rollback()
                    raise

                result = await driver.execute("SELECT * FROM users WHERE name = 'Transaction User'")
                assert len(result.data) == 1

            async with config.provide_session() as driver:
                await driver.begin()
                try:
                    await driver.execute(
                        "INSERT INTO users (name, email) VALUES ($1, $2)", ("Rollback User", "rollback@example.com")
                    )

                    raise Exception("Intentional rollback")
                except Exception:
                    await driver.rollback()

                result = await driver.execute("SELECT * FROM users WHERE name = 'Rollback User'")
                assert len(result.data) == 0
        finally:
            if config.pool_instance:
                await config.close_pool()
