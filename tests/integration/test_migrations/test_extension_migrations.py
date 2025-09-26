"""Integration test for extension migrations with context."""

import tempfile
from pathlib import Path

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psycopg.config import PsycopgSyncConfig
from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.migrations.commands import SyncMigrationCommands


def test_litestar_extension_migration_with_sqlite() -> None:
    """Test that Litestar extension migrations work with SQLite context."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test.db"

        # Create config with Litestar extension enabled
        config = SqliteConfig(
            pool_config={"database": str(db_path)},
            migration_config={
                "script_location": str(temp_dir),
                "version_table_name": "test_migrations",
                "include_extensions": ["litestar"],
            },
        )

        # Create commands and init
        commands = SyncMigrationCommands(config)
        commands.init(str(temp_dir), package=False)

        # Get migration files - should include extension migrations
        migration_files = commands.runner.get_migration_files()
        versions = [version for version, _ in migration_files]

        # Should have Litestar migration
        litestar_migrations = [v for v in versions if "ext_litestar" in v]
        assert len(litestar_migrations) > 0, "No Litestar migrations found"

        # Check that context is passed correctly
        assert commands.runner.context is not None
        assert commands.runner.context.dialect == "sqlite"

        # Apply migrations
        with config.provide_session() as driver:
            commands.tracker.ensure_tracking_table(driver)

            # Apply the Litestar migration
            for version, file_path in migration_files:
                if "ext_litestar" in version and "0001" in version:
                    migration = commands.runner.load_migration(file_path)

                    # Execute upgrade
                    _, execution_time = commands.runner.execute_upgrade(driver, migration)
                    commands.tracker.record_migration(
                        driver, migration["version"], migration["description"], execution_time, migration["checksum"]
                    )

                    # Check that table was created with correct schema
                    result = driver.execute(
                        "SELECT sql FROM sqlite_master WHERE type='table' AND name='litestar_sessions'"
                    )
                    assert len(result.data) == 1
                    create_sql = result.data[0]["sql"]

                    # SQLite should use TEXT for data column
                    assert "TEXT" in create_sql
                    assert "DATETIME" in create_sql or "TIMESTAMP" in create_sql

                    # Revert the migration
                    _, execution_time = commands.runner.execute_downgrade(driver, migration)
                    commands.tracker.remove_migration(driver, version)

                    # Check that table was dropped
                    result = driver.execute(
                        "SELECT sql FROM sqlite_master WHERE type='table' AND name='litestar_sessions'"
                    )
                    assert len(result.data) == 0


@pytest.mark.postgres
def test_litestar_extension_migration_with_postgres(postgres_service: PostgresService) -> None:
    """Test that Litestar extension migrations work with PostgreSQL context."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create config with Litestar extension enabled
        config = PsycopgSyncConfig(
            pool_config={
                "host": postgres_service.host,
                "port": postgres_service.port,
                "user": postgres_service.user,
                "password": postgres_service.password,
                "dbname": postgres_service.database,
            },
            migration_config={
                "script_location": str(temp_dir),
                "version_table_name": "test_migrations",
                "include_extensions": ["litestar"],
            },
        )

        # Create commands and init
        commands = SyncMigrationCommands(config)
        commands.init(str(temp_dir), package=False)

        # Check that context has correct dialect
        assert commands.runner.context is not None
        assert commands.runner.context.dialect in {"postgres", "postgresql"}

        # Get migration files
        migration_files = commands.runner.get_migration_files()

        # Apply migrations
        with config.provide_session() as driver:
            commands.tracker.ensure_tracking_table(driver)

            # Apply the Litestar migration
            for version, file_path in migration_files:
                if "ext_litestar" in version and "0001" in version:
                    migration = commands.runner.load_migration(file_path)

                    # Execute upgrade
                    _, execution_time = commands.runner.execute_upgrade(driver, migration)
                    commands.tracker.record_migration(
                        driver, migration["version"], migration["description"], execution_time, migration["checksum"]
                    )

                    # Check that table was created with correct schema
                    result = driver.execute("""
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_name = 'litestar_sessions'
                        AND column_name IN ('data', 'expires_at')
                    """)

                    columns = {row["column_name"]: row["data_type"] for row in result.data}

                    # PostgreSQL should use JSONB for data column
                    assert columns.get("data") == "jsonb"
                    assert "timestamp" in columns.get("expires_at", "").lower()

                    # Revert the migration
                    _, execution_time = commands.runner.execute_downgrade(driver, migration)
                    commands.tracker.remove_migration(driver, version)

                    # Check that table was dropped
                    result = driver.execute("""
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_name = 'litestar_sessions'
                    """)
                    assert len(result.data) == 0
