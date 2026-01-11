"""Integration tests for Psycopg (PostgreSQL) migration workflow."""

from pathlib import Path
from typing import Any

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psycopg import PsycopgAsyncConfig
from sqlspec.adapters.psycopg.config import PsycopgSyncConfig
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands, create_migration_commands

pytestmark = pytest.mark.xdist_group("postgres")


def test_psycopg_sync_migration_full_workflow(tmp_path: Path, postgres_service: "PostgresService") -> None:
    """Test full Psycopg sync migration workflow: init -> create -> upgrade -> downgrade."""

    test_id = "psycopg_sync_full"
    migration_table = f"sqlspec_migrations_{test_id}"
    users_table = f"users_{test_id}"

    migration_dir = tmp_path / "migrations"

    config = PsycopgSyncConfig(
        connection_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        },
        migration_config={"script_location": str(migration_dir), "version_table_name": migration_table},
    )
    commands: SyncMigrationCommands[Any] | AsyncMigrationCommands[Any] = create_migration_commands(config)

    commands.init(str(migration_dir), package=True)

    assert migration_dir.exists()
    assert (migration_dir / "__init__.py").exists()

    migration_content = f'''"""Initial schema migration."""


def up():
    """Create users table."""
    return ["""
        CREATE TABLE {users_table} (
            id SERIAL PRIMARY KEY,
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
        commands.upgrade()

        with config.provide_session() as driver:
            result = driver.execute(
                f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{users_table}'"
            )
            assert len(result.data) == 1

            driver.execute(f"INSERT INTO {users_table} (name, email) VALUES (%s, %s)", ("John Doe", "john@example.com"))

            users_result = driver.execute(f"SELECT * FROM {users_table}")
            assert len(users_result.data) == 1
            assert users_result.data[0]["name"] == "John Doe"
            assert users_result.data[0]["email"] == "john@example.com"

        commands.downgrade("base")

        with config.provide_session() as driver:
            result = driver.execute(
                f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{users_table}'"
            )
            assert len(result.data) == 0
    finally:
        if config.connection_instance:
            config.close_pool()


async def test_psycopg_async_migration_full_workflow(tmp_path: Path, postgres_service: "PostgresService") -> None:
    """Test full Psycopg async migration workflow: init -> create -> upgrade -> downgrade."""

    test_id = "psycopg_async_full"
    migration_table = f"sqlspec_migrations_{test_id}"
    users_table = f"users_{test_id}"

    migration_dir = tmp_path / "migrations"

    config = PsycopgAsyncConfig(
        connection_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
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
            id SERIAL PRIMARY KEY,
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
                f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{users_table}'"
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
                f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{users_table}'"
            )
            assert len(result.data) == 0
    finally:
        if config.connection_instance:
            await config.close_pool()


def test_psycopg_sync_multiple_migrations_workflow(tmp_path: Path, postgres_service: "PostgresService") -> None:
    """Test Psycopg sync workflow with multiple migrations: create -> apply both -> downgrade one -> downgrade all."""

    test_id = "psycopg_sync_multi"
    migration_table = f"sqlspec_migrations_{test_id}"
    users_table = f"users_{test_id}"
    posts_table = f"posts_{test_id}"

    migration_dir = tmp_path / "migrations"

    config = PsycopgSyncConfig(
        connection_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        },
        migration_config={"script_location": str(migration_dir), "version_table_name": migration_table},
    )
    commands: SyncMigrationCommands[Any] | AsyncMigrationCommands[Any] = create_migration_commands(config)

    commands.init(str(migration_dir), package=True)

    migration1_content = f'''"""Create users table."""


def up():
    """Create users table."""
    return ["""
        CREATE TABLE {users_table} (
            id SERIAL PRIMARY KEY,
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
            id SERIAL PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            content TEXT,
            user_id INTEGER REFERENCES {users_table}(id),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """]


def down():
    """Drop posts table."""
    return ["DROP TABLE IF EXISTS {posts_table}"]
'''
    (migration_dir / "0002_create_posts.py").write_text(migration2_content)

    try:
        commands.upgrade()

        with config.provide_session() as driver:
            users_result = driver.execute(
                f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{users_table}'"
            )
            posts_result = driver.execute(
                f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{posts_table}'"
            )
            assert len(users_result.data) == 1
            assert len(posts_result.data) == 1

        commands.downgrade("0001")

        with config.provide_session() as driver:
            users_result = driver.execute(
                f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{users_table}'"
            )
            posts_result = driver.execute(
                f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{posts_table}'"
            )
            assert len(users_result.data) == 1
            assert len(posts_result.data) == 0

        commands.downgrade("base")

        with config.provide_session() as driver:
            users_result = driver.execute(
                f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('{users_table}', '{posts_table}')"
            )
            assert len(users_result.data) == 0
    finally:
        if config.connection_instance:
            config.close_pool()


async def test_psycopg_async_multiple_migrations_workflow(tmp_path: Path, postgres_service: "PostgresService") -> None:
    """Test Psycopg async workflow with multiple migrations: create -> apply both -> downgrade one -> downgrade all."""

    test_id = "psycopg_async_multi"
    migration_table = f"sqlspec_migrations_{test_id}"
    users_table = f"users_{test_id}"
    posts_table = f"posts_{test_id}"

    migration_dir = tmp_path / "migrations"

    try:
        from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig
    except ImportError:
        pytest.skip("PsycopgAsyncConfig not available")

    config = PsycopgAsyncConfig(
        connection_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
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
            id SERIAL PRIMARY KEY,
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
            id SERIAL PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            content TEXT,
            user_id INTEGER REFERENCES {users_table}(id),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
                f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{users_table}'"
            )
            posts_result = await driver.execute(
                f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{posts_table}'"
            )
            assert len(users_result.data) == 1
            assert len(posts_result.data) == 1

        await commands.downgrade("0001")

        async with config.provide_session() as driver:
            users_result = await driver.execute(
                f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{users_table}'"
            )
            posts_result = await driver.execute(
                f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{posts_table}'"
            )
            assert len(users_result.data) == 1
            assert len(posts_result.data) == 0

        await commands.downgrade("base")

        async with config.provide_session() as driver:
            users_result = await driver.execute(
                f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('{users_table}', '{posts_table}')"
            )
            assert len(users_result.data) == 0
    finally:
        if config.connection_instance:
            await config.close_pool()


def test_psycopg_sync_migration_current_command(tmp_path: Path, postgres_service: "PostgresService") -> None:
    """Test the current migration command shows correct version for Psycopg sync."""

    test_id = "psycopg_sync_current"
    migration_table = f"sqlspec_migrations_{test_id}"
    users_table = f"users_{test_id}"

    migration_dir = tmp_path / "migrations"

    config = PsycopgSyncConfig(
        connection_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        },
        migration_config={"script_location": str(migration_dir), "version_table_name": migration_table},
    )
    commands: SyncMigrationCommands[Any] | AsyncMigrationCommands[Any] = create_migration_commands(config)

    try:
        commands.init(str(migration_dir), package=True)

        current_version = commands.current()
        assert current_version is None or current_version == "base"

        migration_content = f'''"""Initial schema migration."""


def up():
    """Create users table."""
    return ["""
        CREATE TABLE {users_table} (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL
        )
    """]


def down():
    """Drop users table."""
    return ["DROP TABLE IF EXISTS {users_table}"]
'''
        (migration_dir / "0001_create_users.py").write_text(migration_content)

        commands.upgrade()

        current_version = commands.current()
        assert current_version == "0001"

        commands.downgrade("base")

        current_version = commands.current()
        assert current_version is None or current_version == "base"
    finally:
        if config.connection_instance:
            config.close_pool()


async def test_psycopg_async_migration_current_command(tmp_path: Path, postgres_service: "PostgresService") -> None:
    """Test the current migration command shows correct version for Psycopg async."""

    test_id = "psycopg_async_current"
    migration_table = f"sqlspec_migrations_{test_id}"
    users_table = f"users_{test_id}"

    migration_dir = tmp_path / "migrations"

    try:
        from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig
    except ImportError:
        pytest.skip("PsycopgAsyncConfig not available")

    config = PsycopgAsyncConfig(
        connection_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
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
            id SERIAL PRIMARY KEY,
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


def test_psycopg_sync_migration_error_handling(tmp_path: Path, postgres_service: "PostgresService") -> None:
    """Test Psycopg sync migration error handling."""
    migration_dir = tmp_path / "migrations"

    config = PsycopgSyncConfig(
        connection_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        },
        migration_config={
            "script_location": str(migration_dir),
            "version_table_name": "sqlspec_migrations_psycopg_sync_error",
        },
    )
    commands: SyncMigrationCommands[Any] | AsyncMigrationCommands[Any] = create_migration_commands(config)

    try:
        commands.init(str(migration_dir), package=True)

        migration_content = '''"""Migration with invalid SQL."""


def up():
    """Create table with invalid SQL."""
    return ["CREATE INVALID SQL STATEMENT"]


def down():
    """Drop table."""
    return ["DROP TABLE IF EXISTS invalid_table"]
'''
        (migration_dir / "0001_invalid.py").write_text(migration_content)

        commands.upgrade()

        with config.provide_session() as driver:
            try:
                driver.execute("SELECT version FROM sqlspec_migrations_psycopg_sync_error ORDER BY version")
                msg = "Expected migration table to not exist, but it does"
                raise AssertionError(msg)
            except Exception as e:
                assert "no such" in str(e).lower() or "does not exist" in str(e).lower()
    finally:
        if config.connection_instance:
            config.close_pool()


async def test_psycopg_async_migration_error_handling(tmp_path: Path, postgres_service: "PostgresService") -> None:
    """Test Psycopg async migration error handling."""
    migration_dir = tmp_path / "migrations"

    try:
        from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig
    except ImportError:
        pytest.skip("PsycopgAsyncConfig not available")

    config = PsycopgAsyncConfig(
        connection_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        },
        migration_config={
            "script_location": str(migration_dir),
            "version_table_name": "sqlspec_migrations_psycopg_async_error",
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

        await commands.upgrade()

        async with config.provide_session() as driver:
            try:
                await driver.execute("SELECT version FROM sqlspec_migrations_psycopg_async_error ORDER BY version")
                msg = "Expected migration table to not exist, but it does"
                raise AssertionError(msg)
            except Exception as e:
                assert "no such" in str(e).lower() or "does not exist" in str(e).lower()
    finally:
        if config.connection_instance:
            await config.close_pool()


def test_psycopg_sync_migration_with_transactions(tmp_path: Path, postgres_service: "PostgresService") -> None:
    """Test Psycopg sync migrations work properly with transactions."""

    test_id = "psycopg_sync_trans"
    migration_table = f"sqlspec_migrations_{test_id}"
    users_table = f"users_{test_id}"

    migration_dir = tmp_path / "migrations"

    config = PsycopgSyncConfig(
        connection_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        },
        migration_config={"script_location": str(migration_dir), "version_table_name": migration_table},
    )
    commands: SyncMigrationCommands[Any] | AsyncMigrationCommands[Any] = create_migration_commands(config)

    try:
        commands.init(str(migration_dir), package=True)

        migration_content = f'''"""Initial schema migration."""


def up():
    """Create users table."""
    return ["""
        CREATE TABLE {users_table} (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL
        )
    """]


def down():
    """Drop users table."""
    return ["DROP TABLE IF EXISTS {users_table}"]
'''
        (migration_dir / "0001_create_users.py").write_text(migration_content)

        commands.upgrade()

        with config.provide_session() as driver:
            driver.begin()
            try:
                driver.execute(
                    f"INSERT INTO {users_table} (name, email) VALUES (%s, %s)",
                    ("Transaction User", "trans@example.com"),
                )

                result = driver.execute(f"SELECT * FROM {users_table} WHERE name = 'Transaction User'")
                assert len(result.data) == 1
                driver.commit()
            except Exception:
                driver.rollback()
                raise

            result = driver.execute(f"SELECT * FROM {users_table} WHERE name = 'Transaction User'")
            assert len(result.data) == 1

        with config.provide_session() as driver:
            driver.begin()
            try:
                driver.execute(
                    f"INSERT INTO {users_table} (name, email) VALUES (%s, %s)",
                    ("Rollback User", "rollback@example.com"),
                )

                raise Exception("Intentional rollback")
            except Exception:
                driver.rollback()

            result = driver.execute(f"SELECT * FROM {users_table} WHERE name = 'Rollback User'")
            assert len(result.data) == 0
    finally:
        if config.connection_instance:
            config.close_pool()


async def test_psycopg_async_migration_with_transactions(tmp_path: Path, postgres_service: "PostgresService") -> None:
    """Test Psycopg async migrations work properly with transactions."""

    test_id = "psycopg_async_trans"
    migration_table = f"sqlspec_migrations_{test_id}"
    users_table = f"users_{test_id}"

    migration_dir = tmp_path / "migrations"

    try:
        from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig
    except ImportError:
        pytest.skip("PsycopgAsyncConfig not available")

    config = PsycopgAsyncConfig(
        connection_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
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
            id SERIAL PRIMARY KEY,
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
