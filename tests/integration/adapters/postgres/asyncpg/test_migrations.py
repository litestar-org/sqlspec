"""Integration tests for AsyncpgConfig migration helper methods.

The shared init/upgrade/downgrade/current/error migration lifecycle is covered by
tests/integration/adapters/_shared/suite_migrations_contract.py. This module keeps the
AsyncpgConfig convenience-method surface (migrate_up, migrate_down, get_current_migration,
create_migration, stamp_migration, fix_migrations).
"""

from pathlib import Path

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.asyncpg.config import AsyncpgConfig

pytestmark = pytest.mark.xdist_group("postgres")


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
