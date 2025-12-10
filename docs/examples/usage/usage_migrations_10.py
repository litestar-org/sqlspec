"""Using AsyncMigrationTracker for version management."""

import tempfile
from pathlib import Path

import pytest
from pytest_databases.docker.postgres import PostgresService

pytestmark = pytest.mark.xdist_group("postgres")

__all__ = ("test_tracker_instance",)


async def test_tracker_instance(postgres_service: PostgresService) -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir()

        # start-example
        from sqlspec.adapters.asyncpg import AsyncpgConfig
        from sqlspec.migrations.tracker import AsyncMigrationTracker

        # Create tracker with custom table name
        tracker = AsyncMigrationTracker(version_table_name="ddl_migrations")

        dsn = (
            f"postgresql://{postgres_service.user}:{postgres_service.password}"
            f"@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        )
        config = AsyncpgConfig(
            connection_config={"dsn": dsn},
            migration_config={
                "enabled": True,
                "script_location": str(migration_dir),
                "version_table_name": "ddl_migrations",
                "auto_sync": True,  # Enable automatic version reconciliation
            },
        )

        # Use the session to work with migrations
        async with config.provide_session() as session:
            # Ensure the tracking table exists
            await tracker.ensure_tracking_table(session)

            # Get current version (None if no migrations applied)
            current = await tracker.get_current_version(session)
            print(f"Current version: {current}")
        # end-example

        assert isinstance(tracker, AsyncMigrationTracker)
        assert config.migration_config["version_table_name"] == "ddl_migrations"
