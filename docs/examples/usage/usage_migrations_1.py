"""Async migration commands via config methods."""

import tempfile
from pathlib import Path

import pytest
from pytest_databases.docker.postgres import PostgresService

pytestmark = pytest.mark.xdist_group("postgres")

__all__ = ("test_async_methods",)


async def test_async_methods(postgres_service: PostgresService) -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir()

        # start-example
        from sqlspec.adapters.asyncpg import AsyncpgConfig

        dsn = (
            f"postgresql://{postgres_service.user}:{postgres_service.password}"
            f"@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        )
        config = AsyncpgConfig(
            pool_config={"dsn": dsn}, migration_config={"enabled": True, "script_location": str(migration_dir)}
        )

        # Initialize migrations directory (creates __init__.py if package=True)
        await config.init_migrations()

        # Create new migration file
        await config.create_migration("add users table", file_type="sql")

        # Apply migrations to head
        await config.migrate_up("head")

        # Rollback one revision
        await config.migrate_down("-1")

        # Check current version
        await config.get_current_migration(verbose=True)

        # Stamp database to specific revision
        await config.stamp_migration("0001")

        # Convert timestamp to sequential migrations
        await config.fix_migrations(dry_run=True, update_database=False, yes=True)
        # end-example
