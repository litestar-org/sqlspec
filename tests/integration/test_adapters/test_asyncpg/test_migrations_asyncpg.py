"""Integration tests for AsyncPG (PostgreSQL) migration workflow."""

from pathlib import Path

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.asyncpg.config import AsyncpgConfig
from sqlspec.migrations.commands import AsyncMigrationCommands

pytestmark = pytest.mark.xdist_group("postgres")


async def test_asyncpg_migration_full_workflow(tmp_path: Path, postgres_service: "PostgresService") -> None:
    """Test full AsyncPG migration workflow: init -> create -> upgrade -> downgrade."""
    migration_dir = tmp_path / "migrations"

    config = AsyncpgConfig(
        connection_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "database": postgres_service.database,
        },
        migration_config={"script_location": str(migration_dir), "version_table_name": "sqlspec_migrations_asyncpg"},
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

            await driver.execute("INSERT INTO users (name, email) VALUES ($1, $2)", ("John Doe", "john@example.com"))

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
        if config.connection_instance:
            await config.close_pool()


async def test_asyncpg_multiple_migrations_workflow(tmp_path: Path, postgres_service: "PostgresService") -> None:
    """Test AsyncPG workflow with multiple migrations: create -> apply both -> downgrade one -> downgrade all."""
    migration_dir = tmp_path / "migrations"

    config = AsyncpgConfig(
        connection_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "database": postgres_service.database,
        },
        migration_config={"script_location": str(migration_dir), "version_table_name": "sqlspec_migrations_asyncpg"},
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

            await driver.execute("INSERT INTO users (name, email) VALUES ($1, $2)", ("John Doe", "john@example.com"))
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
        if config.connection_instance:
            await config.close_pool()


async def test_asyncpg_migration_current_command(tmp_path: Path, postgres_service: "PostgresService") -> None:
    """Test the current migration command shows correct version for AsyncPG."""
    migration_dir = tmp_path / "migrations"

    config = AsyncpgConfig(
        connection_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "database": postgres_service.database,
        },
        migration_config={"script_location": str(migration_dir), "version_table_name": "sqlspec_migrations_asyncpg"},
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
        if config.connection_instance:
            await config.close_pool()


async def test_asyncpg_migration_error_handling(tmp_path: Path, postgres_service: "PostgresService") -> None:
    """Test AsyncPG migration error handling."""
    migration_dir = tmp_path / "migrations"

    config = AsyncpgConfig(
        connection_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "database": postgres_service.database,
        },
        migration_config={"script_location": str(migration_dir), "version_table_name": "sqlspec_migrations_asyncpg"},
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
            try:
                await driver.execute("SELECT version FROM sqlspec_migrations_asyncpg ORDER BY version")
                msg = "Expected migration table to not exist, but it does"
                raise AssertionError(msg)
            except Exception as e:
                assert "no such" in str(e).lower() or "does not exist" in str(e).lower()
    finally:
        if config.connection_instance:
            await config.close_pool()


async def test_asyncpg_migration_with_transactions(tmp_path: Path, postgres_service: "PostgresService") -> None:
    """Test AsyncPG migrations work properly with transactions."""
    migration_dir = tmp_path / "migrations"

    config = AsyncpgConfig(
        connection_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "database": postgres_service.database,
        },
        migration_config={"script_location": str(migration_dir), "version_table_name": "sqlspec_migrations_asyncpg"},
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
        if config.connection_instance:
            await config.close_pool()


async def test_asyncpg_config_migrate_up_method(tmp_path: Path, postgres_service: "PostgresService") -> None:
    """Test AsyncpgConfig.migrate_up() method works correctly."""
    migration_dir = tmp_path / "migrations"

    config = AsyncpgConfig(
        connection_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "database": postgres_service.database,
        },
        migration_config={
            "script_location": str(migration_dir),
            "version_table_name": "sqlspec_migrations_asyncpg_config",
        },
    )

    try:
        await config.init_migrations()

        migration_content = '''"""Create products table."""


def up():
    """Create products table."""
    return ["""
        CREATE TABLE products (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            price DECIMAL(10, 2)
        )
    """]


def down():
    """Drop products table."""
    return ["DROP TABLE IF EXISTS products"]
'''

        (migration_dir / "0001_create_products.py").write_text(migration_content)

        await config.migrate_up()

        async with config.provide_session() as driver:
            result = await driver.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'products'"
            )
            assert len(result.data) == 1
    finally:
        if config.connection_instance:
            await config.close_pool()


async def test_asyncpg_config_migrate_down_method(tmp_path: Path, postgres_service: "PostgresService") -> None:
    """Test AsyncpgConfig.migrate_down() method works correctly."""
    migration_dir = tmp_path / "migrations"

    config = AsyncpgConfig(
        connection_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "database": postgres_service.database,
        },
        migration_config={
            "script_location": str(migration_dir),
            "version_table_name": "sqlspec_migrations_asyncpg_down",
        },
    )

    try:
        await config.init_migrations()

        migration_content = '''"""Create inventory table."""


def up():
    """Create inventory table."""
    return ["""
        CREATE TABLE inventory (
            id SERIAL PRIMARY KEY,
            item VARCHAR(255) NOT NULL
        )
    """]


def down():
    """Drop inventory table."""
    return ["DROP TABLE IF EXISTS inventory"]
'''

        (migration_dir / "0001_create_inventory.py").write_text(migration_content)

        await config.migrate_up()

        async with config.provide_session() as driver:
            result = await driver.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'inventory'"
            )
            assert len(result.data) == 1

        await config.migrate_down()

        async with config.provide_session() as driver:
            result = await driver.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'inventory'"
            )
            assert len(result.data) == 0
    finally:
        if config.connection_instance:
            await config.close_pool()


async def test_asyncpg_config_get_current_migration_method(tmp_path: Path, postgres_service: "PostgresService") -> None:
    """Test AsyncpgConfig.get_current_migration() method returns correct version."""
    migration_dir = tmp_path / "migrations"

    config = AsyncpgConfig(
        connection_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "database": postgres_service.database,
        },
        migration_config={"script_location": str(migration_dir), "version_table_name": "sqlspec_migrations_current"},
    )

    try:
        await config.init_migrations()

        current_version = await config.get_current_migration()
        assert current_version is None or current_version == "base"

        migration_content = '''"""First migration."""


def up():
    """Create test table."""
    return ["CREATE TABLE test_version (id SERIAL PRIMARY KEY)"]


def down():
    """Drop test table."""
    return ["DROP TABLE IF EXISTS test_version"]
'''

        (migration_dir / "0001_first.py").write_text(migration_content)

        await config.migrate_up()

        current_version = await config.get_current_migration()
        assert current_version == "0001"
    finally:
        if config.connection_instance:
            await config.close_pool()


async def test_asyncpg_config_create_migration_method(tmp_path: Path, postgres_service: "PostgresService") -> None:
    """Test AsyncpgConfig.create_migration() method generates migration file."""
    migration_dir = tmp_path / "migrations"

    config = AsyncpgConfig(
        connection_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "database": postgres_service.database,
        },
        migration_config={"script_location": str(migration_dir), "version_table_name": "sqlspec_migrations_create"},
    )

    try:
        await config.init_migrations()

        await config.create_migration("add users table", file_type="py")

        migration_files = list(migration_dir.glob("*.py"))
        migration_files = [f for f in migration_files if f.name != "__init__.py"]

        assert len(migration_files) == 1
        assert "add_users_table" in migration_files[0].name
    finally:
        if config.connection_instance:
            await config.close_pool()


async def test_asyncpg_config_stamp_migration_method(tmp_path: Path, postgres_service: "PostgresService") -> None:
    """Test AsyncpgConfig.stamp_migration() method marks database at revision."""
    migration_dir = tmp_path / "migrations"

    config = AsyncpgConfig(
        connection_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "database": postgres_service.database,
        },
        migration_config={"script_location": str(migration_dir), "version_table_name": "sqlspec_migrations_stamp"},
    )

    try:
        await config.init_migrations()

        migration_content = '''"""Stamped migration."""


def up():
    """Create stamped table."""
    return ["CREATE TABLE stamped (id SERIAL PRIMARY KEY)"]


def down():
    """Drop stamped table."""
    return ["DROP TABLE IF EXISTS stamped"]
'''

        (migration_dir / "0001_stamped.py").write_text(migration_content)

        await config.stamp_migration("0001")

        current_version = await config.get_current_migration()
        assert current_version == "0001"

        async with config.provide_session() as driver:
            result = await driver.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'stamped'"
            )
            assert len(result.data) == 0
    finally:
        if config.connection_instance:
            await config.close_pool()


async def test_asyncpg_config_fix_migrations_dry_run(tmp_path: Path, postgres_service: "PostgresService") -> None:
    """Test AsyncpgConfig.fix_migrations() dry run shows what would change."""
    migration_dir = tmp_path / "migrations"

    config = AsyncpgConfig(
        connection_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "database": postgres_service.database,
        },
        migration_config={"script_location": str(migration_dir), "version_table_name": "sqlspec_migrations_fix"},
    )

    try:
        await config.init_migrations()

        timestamp_migration = '''"""Timestamp migration."""


def up():
    """Create timestamp table."""
    return ["CREATE TABLE timestamp_test (id SERIAL PRIMARY KEY)"]


def down():
    """Drop timestamp table."""
    return ["DROP TABLE IF EXISTS timestamp_test"]
'''

        (migration_dir / "20251030120000_timestamp_migration.py").write_text(timestamp_migration)

        await config.fix_migrations(dry_run=True, yes=True)

        timestamp_file = migration_dir / "20251030120000_timestamp_migration.py"
        assert timestamp_file.exists()

        sequential_file = migration_dir / "0001_timestamp_migration.py"
        assert not sequential_file.exists()
    finally:
        if config.connection_instance:
            await config.close_pool()
