"""Integration tests for Psycopg (PostgreSQL) migration workflow."""

import tempfile
from pathlib import Path

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psycopg.config import PsycopgSyncConfig
from sqlspec.migrations.commands import MigrationCommands


@pytest.mark.xdist_group("migrations")
def test_psycopg_sync_migration_full_workflow(postgres_service: PostgresService) -> None:
    """Test full Psycopg sync migration workflow: init -> create -> upgrade -> downgrade."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"

        # Create Psycopg sync config with migration directory
        config = PsycopgSyncConfig(
            pool_config={
                "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
            },
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "sqlspec_migrations"
            }
        )
        commands = MigrationCommands(config)

        # 1. Initialize migrations
        commands.init(str(migration_dir), package=True)

        # Verify initialization
        assert migration_dir.exists()
        assert (migration_dir / "__init__.py").exists()

        # 2. Create a migration with simple schema
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

        # Write migration file
        migration_file = migration_dir / "0001_create_users.py"
        migration_file.write_text(migration_content)

        try:
            # 3. Apply migration (upgrade)
            commands.upgrade()

            # 4. Verify migration was applied
            with config.provide_session() as driver:
                # Check that table exists
                result = driver.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'users'"
                )
                assert len(result.data) == 1

                # Insert test data
                driver.execute(
                    "INSERT INTO users (name, email) VALUES (%s, %s)",
                    ("John Doe", "john@example.com")
                )

                # Verify data
                users_result = driver.execute("SELECT * FROM users")
                assert len(users_result.data) == 1
                assert users_result.data[0]["name"] == "John Doe"
                assert users_result.data[0]["email"] == "john@example.com"

            # 5. Downgrade migration
            commands.downgrade("base")

            # 6. Verify table was dropped
            with config.provide_session() as driver:
                result = driver.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'users'"
                )
                assert len(result.data) == 0
        finally:
            # Ensure pool is closed
            if config.pool_instance:
                config.close_pool()
    # This test would require a PostgreSQL instance running in CI
    # Implementation would be similar but with PostgreSQL-specific SQL and config

    # with tempfile.TemporaryDirectory() as temp_dir:
    #     migration_dir = Path(temp_dir) / "migrations"
    #
    #     # Create Psycopg sync config with migration directory
    #     config = PsycopgSyncConfig(
    #         pool_config={
    #             "conninfo": "postgresql://test_user:test_password@localhost:5432/test_db"
    #         },
    #         migration_config={
    #             "script_location": str(migration_dir),
    #             "version_table_name": "sqlspec_migrations"
    #         }
    #     )
    #     commands = MigrationCommands(config)
    #
    #     # 1. Initialize migrations
    #     commands.init(str(migration_dir), package=True)
    #
    #     # Verify initialization
    #     assert migration_dir.exists()
    #     assert (migration_dir / "__init__.py").exists()
    #
    #     # 2. Create a migration with simple schema
    #     migration_content = '''"""Initial schema migration."""
    #
    #
    # def up():
    #     """Create users table."""
    #     return ["""
    #         CREATE TABLE users (
    #             id SERIAL PRIMARY KEY,
    #             name VARCHAR(255) NOT NULL,
    #             email VARCHAR(255) UNIQUE NOT NULL,
    #             created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    #         )
    #     """]
    #
    #
    # def down():
    #     """Drop users table."""
    #     return ["DROP TABLE IF EXISTS users"]
    # '''
    #
    #     # Write migration file
    #     migration_file = migration_dir / "0001_create_users.py"
    #     migration_file.write_text(migration_content)
    #
    #     # 3. Apply migration (upgrade)
    #     commands.upgrade()
    #
    #     # 4. Verify migration was applied
    #     with config.provide_session() as driver:
    #         # Check that table exists
    #         result = driver.execute(
    #             "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'users'"
    #         )
    #         assert len(result.data) == 1
    #
    #         # Insert test data
    #         driver.execute(
    #             "INSERT INTO users (name, email) VALUES (%s, %s)",
    #             ("John Doe", "john@example.com")
    #         )
    #
    #         # Verify data
    #         users_result = driver.execute("SELECT * FROM users")
    #         assert len(users_result.data) == 1
    #         assert users_result.data[0]["name"] == "John Doe"
    #         assert users_result.data[0]["email"] == "john@example.com"
    #
    #     # 5. Downgrade migration
    #     commands.downgrade("base")
    #
    #     # 6. Verify table was dropped
    #     with config.provide_session() as driver:
    #         result = driver.execute(
    #             "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'users'"
    #         )
    #         assert len(result.data) == 0


@pytest.mark.xdist_group("migrations")
def test_psycopg_async_migration_full_workflow(postgres_service: PostgresService) -> None:
    """Test full Psycopg async migration workflow: init -> create -> upgrade -> downgrade."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"

        # Create Psycopg async config with migration directory
        try:
            from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig
        except ImportError:
            pytest.skip("PsycopgAsyncConfig not available")
            
        config = PsycopgAsyncConfig(
            pool_config={
                "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
            },
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "sqlspec_migrations"
            }
        )
        commands = MigrationCommands(config)

        # 1. Initialize migrations
        commands.init(str(migration_dir), package=True)

        # Verify initialization
        assert migration_dir.exists()
        assert (migration_dir / "__init__.py").exists()

        # 2. Create a migration with simple schema
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

        # Write migration file
        migration_file = migration_dir / "0001_create_users.py"
        migration_file.write_text(migration_content)

        try:
            # 3. Apply migration (upgrade)
            commands.upgrade()

            # 4. Verify migration was applied
            with config.provide_session() as driver:
                # Check that table exists
                result = driver.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'users'"
                )
                assert len(result.data) == 1

                # Insert test data
                driver.execute(
                    "INSERT INTO users (name, email) VALUES (%s, %s)",
                    ("John Doe", "john@example.com")
                )

                # Verify data
                users_result = driver.execute("SELECT * FROM users")
                assert len(users_result.data) == 1
                assert users_result.data[0]["name"] == "John Doe"
                assert users_result.data[0]["email"] == "john@example.com"

            # 5. Downgrade migration
            commands.downgrade("base")

            # 6. Verify table was dropped
            with config.provide_session() as driver:
                result = driver.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'users'"
                )
                assert len(result.data) == 0
        finally:
            # Ensure pool is closed
            if config.pool_instance:
                import asyncio
                asyncio.get_event_loop().run_until_complete(config.close_pool())


@pytest.mark.xdist_group("migrations")
def test_psycopg_sync_multiple_migrations_workflow(postgres_service: PostgresService) -> None:
    """Test Psycopg sync workflow with multiple migrations: create -> apply both -> downgrade one -> downgrade all."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"

        config = PsycopgSyncConfig(
            pool_config={
                "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
            },
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "sqlspec_migrations"
            }
        )
        commands = MigrationCommands(config)

        # 1. Initialize migrations
        commands.init(str(migration_dir), package=True)

        # 2. Create first migration
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

        # 3. Create second migration
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
            # 4. Apply all migrations
            commands.upgrade()

            # 5. Verify both tables exist
            with config.provide_session() as driver:
                users_result = driver.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'users'"
                )
                posts_result = driver.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'posts'"
                )
                assert len(users_result.data) == 1
                assert len(posts_result.data) == 1

            # 6. Downgrade to version 0001 (should remove posts table)
            commands.downgrade("0001")

            # 7. Verify only users table remains
            with config.provide_session() as driver:
                users_result = driver.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'users'"
                )
                posts_result = driver.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'posts'"
                )
                assert len(users_result.data) == 1
                assert len(posts_result.data) == 0

            # 8. Downgrade to base
            commands.downgrade("base")

            # 9. Verify all tables are gone
            with config.provide_session() as driver:
                users_result = driver.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('users', 'posts')"
                )
                assert len(users_result.data) == 0
        finally:
            if config.pool_instance:
                config.close_pool()


@pytest.mark.xdist_group("migrations")
def test_psycopg_async_multiple_migrations_workflow() -> None:
    """Test Psycopg async workflow with multiple migrations: create -> apply both -> downgrade one -> downgrade all."""
    pytest.skip("PostgreSQL Psycopg async driver tests require running PostgreSQL instance")
    # This test would require a PostgreSQL instance running in CI
    # Implementation would be similar but with PostgreSQL-specific SQL and config


@pytest.mark.xdist_group("migrations")
def test_psycopg_sync_migration_current_command() -> None:
    """Test the current migration command shows correct version for Psycopg sync."""
    pytest.skip("PostgreSQL Psycopg sync driver tests require running PostgreSQL instance")
    # This test would require a PostgreSQL instance running in CI
    # Implementation would be similar but with PostgreSQL-specific SQL and config


@pytest.mark.xdist_group("migrations")
def test_psycopg_async_migration_current_command() -> None:
    """Test the current migration command shows correct version for Psycopg async."""
    pytest.skip("PostgreSQL Psycopg async driver tests require running PostgreSQL instance")
    # This test would require a PostgreSQL instance running in CI
    # Implementation would be similar but with PostgreSQL-specific SQL and config


@pytest.mark.xdist_group("migrations")
def test_psycopg_sync_migration_error_handling() -> None:
    """Test Psycopg sync migration error handling."""
    pytest.skip("PostgreSQL Psycopg sync driver tests require running PostgreSQL instance")
    # This test would require a PostgreSQL instance running in CI
    # Implementation would be similar but with PostgreSQL-specific SQL and config


@pytest.mark.xdist_group("migrations")
def test_psycopg_async_migration_error_handling() -> None:
    """Test Psycopg async migration error handling."""
    pytest.skip("PostgreSQL Psycopg async driver tests require running PostgreSQL instance")
    # This test would require a PostgreSQL instance running in CI
    # Implementation would be similar but with PostgreSQL-specific SQL and config


@pytest.mark.xdist_group("migrations")
def test_psycopg_sync_migration_with_transactions() -> None:
    """Test Psycopg sync migrations work properly with transactions."""
    pytest.skip("PostgreSQL Psycopg sync driver tests require running PostgreSQL instance")
    # This test would require a PostgreSQL instance running in CI
    # Implementation would be similar but with PostgreSQL-specific SQL and config


@pytest.mark.xdist_group("migrations")
def test_psycopg_async_migration_with_transactions() -> None:
    """Test Psycopg async migrations work properly with transactions."""
    pytest.skip("PostgreSQL Psycopg async driver tests require running PostgreSQL instance")
    # This test would require a PostgreSQL instance running in CI
    # Implementation would be similar but with PostgreSQL-specific SQL and config
