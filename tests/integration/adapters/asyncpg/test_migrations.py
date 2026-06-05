"""Integration tests for AsyncpgConfig migration helper methods and default-schema behavior.

The shared init/upgrade/downgrade/current/error migration lifecycle is covered by
tests/integration/adapters/contracts/test_migrations_contract.py. This module keeps the
AsyncpgConfig convenience-method surface (migrate_up, migrate_down, get_current_migration,
create_migration, stamp_migration, fix_migrations) and the PostgreSQL default_schema behavior.
"""

from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.asyncpg.config import AsyncpgConfig
from sqlspec.exceptions import MigrationError
from sqlspec.migrations.commands import AsyncMigrationCommands
from sqlspec.utils.text import quote_identifier

pytestmark = pytest.mark.xdist_group("postgres")


def unique_identifier(prefix: str) -> str:
    """Return a short PostgreSQL-safe identifier for integration tests."""
    return f"{prefix}_{uuid4().hex[:10]}"


def create_schema_sql(schema: str) -> str:
    """Return PostgreSQL CREATE SCHEMA SQL for a trusted test identifier."""
    return f"CREATE SCHEMA {quote_identifier(schema)}"


def drop_schema_sql(schema: str) -> str:
    """Return PostgreSQL DROP SCHEMA SQL for a trusted test identifier."""
    return f"DROP SCHEMA IF EXISTS {quote_identifier(schema)} CASCADE"


async def async_table_exists(driver: Any, schema: str, table_name: str, *, style: str = "numeric") -> bool:
    """Return whether the table exists using an async SQLSpec driver."""
    placeholders = "%s AND table_name = %s" if style == "pyformat" else "$1 AND table_name = $2"
    result = await driver.execute(
        f"SELECT 1 FROM information_schema.tables WHERE table_schema = {placeholders}", (schema, table_name)
    )
    return bool(result.data)


def write_unqualified_table_migration(migration_dir: Path, table_name: str) -> None:
    """Write a Python migration that creates an unqualified table."""
    (migration_dir / "0001_create_unqualified_table.py").write_text(
        f'''"""Create an unqualified table."""


def up():
    """Create an unqualified table."""
    return ["""
        CREATE TABLE {table_name} (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL
        )
    """]


def down():
    """Drop the unqualified table."""
    return ["DROP TABLE IF EXISTS {table_name}"]
'''
    )


def write_non_transactional_unqualified_table_migration(migration_dir: Path, table_name: str) -> None:
    """Write a SQL migration that creates an unqualified table without a transaction."""
    (migration_dir / "0001_create_unqualified_table.sql").write_text(
        f"""-- transactional: false
-- name: migrate-0001-up
CREATE TABLE {table_name} (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

-- name: migrate-0001-down
DROP TABLE IF EXISTS {table_name};"""
    )


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


async def test_asyncpg_migration_default_schema_applies_to_ddl(
    tmp_path: Path, postgres_service: "PostgresService"
) -> None:
    """AsyncPG migrations run unqualified DDL in the configured default schema."""
    schema = unique_identifier("asyncpg_default")
    table_name = unique_identifier("asyncpg_table")
    version_table = unique_identifier("asyncpg_versions")
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
            "version_table_name": version_table,
            "default_schema": schema,
        },
    )
    commands = AsyncMigrationCommands(config)

    try:
        async with config.provide_session() as driver:
            await driver.execute_script(create_schema_sql(schema))

        await commands.init(str(migration_dir), package=True)
        write_unqualified_table_migration(migration_dir, table_name)
        await commands.upgrade()

        async with config.provide_session() as driver:
            assert await async_table_exists(driver, schema, table_name, style="numeric")
            assert not await async_table_exists(driver, "public", table_name, style="numeric")
            assert await async_table_exists(driver, schema, version_table, style="numeric")
    finally:
        async with config.provide_session() as driver:
            await driver.execute_script(drop_schema_sql(schema))
        if config.connection_instance:
            await config.close_pool()


async def test_asyncpg_non_transactional_migration_default_schema_applies_to_ddl(
    tmp_path: Path, postgres_service: "PostgresService"
) -> None:
    """AsyncPG non-transactional migrations run unqualified DDL in the configured default schema."""
    schema = unique_identifier("asyncpg_default")
    table_name = unique_identifier("asyncpg_table")
    version_table = unique_identifier("asyncpg_versions")
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
            "version_table_name": version_table,
            "default_schema": schema,
        },
    )
    commands = AsyncMigrationCommands(config)

    try:
        async with config.provide_session() as driver:
            await driver.execute_script(create_schema_sql(schema))

        await commands.init(str(migration_dir), package=True)
        write_non_transactional_unqualified_table_migration(migration_dir, table_name)
        await commands.upgrade()

        async with config.provide_session() as driver:
            assert await async_table_exists(driver, schema, table_name, style="numeric")
            assert not await async_table_exists(driver, "public", table_name, style="numeric")
    finally:
        async with config.provide_session() as driver:
            await driver.execute_script(drop_schema_sql(schema))
        if config.connection_instance:
            await config.close_pool()


async def test_asyncpg_migration_separable_tracker_and_default_schema(
    tmp_path: Path, postgres_service: "PostgresService"
) -> None:
    """AsyncPG supports separate schemas for migrated DDL and the tracker table."""
    default_schema = unique_identifier("asyncpg_default")
    tracker_schema = unique_identifier("asyncpg_tracker")
    table_name = unique_identifier("asyncpg_table")
    version_table = unique_identifier("asyncpg_versions")
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
            "version_table_name": version_table,
            "default_schema": default_schema,
            "version_table_schema": tracker_schema,
        },
    )
    commands = AsyncMigrationCommands(config)

    try:
        async with config.provide_session() as driver:
            await driver.execute_script(create_schema_sql(default_schema))
            await driver.execute_script(create_schema_sql(tracker_schema))

        await commands.init(str(migration_dir), package=True)
        write_unqualified_table_migration(migration_dir, table_name)
        await commands.upgrade()

        async with config.provide_session() as driver:
            assert await async_table_exists(driver, default_schema, table_name, style="numeric")
            assert await async_table_exists(driver, tracker_schema, version_table, style="numeric")
            assert not await async_table_exists(driver, default_schema, version_table, style="numeric")
    finally:
        async with config.provide_session() as driver:
            await driver.execute_script(drop_schema_sql(default_schema))
            await driver.execute_script(drop_schema_sql(tracker_schema))
        if config.connection_instance:
            await config.close_pool()


async def test_asyncpg_migration_missing_schema_fails_fast(tmp_path: Path, postgres_service: "PostgresService") -> None:
    """AsyncPG validates the default schema before creating tracker tables or applying DDL."""
    schema = unique_identifier("asyncpg_missing")
    table_name = unique_identifier("asyncpg_table")
    version_table = unique_identifier("asyncpg_versions")
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
            "version_table_name": version_table,
            "default_schema": schema,
        },
    )
    commands = AsyncMigrationCommands(config)

    try:
        await commands.init(str(migration_dir), package=True)
        write_unqualified_table_migration(migration_dir, table_name)

        with pytest.raises(MigrationError, match=f"Configured schema '{schema}' does not exist"):
            await commands.upgrade()

        async with config.provide_session() as driver:
            assert not await async_table_exists(driver, "public", version_table, style="numeric")
            assert not await async_table_exists(driver, "public", table_name, style="numeric")
    finally:
        if config.connection_instance:
            await config.close_pool()
