__all__ = ("test_tracker_instance",)


from pytest_databases.docker.postgres import PostgresService


async def test_tracker_instance(postgres_service: PostgresService) -> None:

    # start-example
    from sqlspec.adapters.asyncpg import AsyncpgConfig
    from sqlspec.migrations.tracker import AsyncMigrationTracker

    tracker = AsyncMigrationTracker()

    config = AsyncpgConfig(
        pool_config={
            "dsn": f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        },
        migration_config={
            "enabled": True,
            "script_location": "migrations",
            "version_table_name": "ddl_migrations",
            "auto_sync": True,  # Enable automatic version reconciliation
        },
    )
    async with config.provide_session() as session:
        driver = session._driver

        # Update version record
        await tracker.update_version_record(driver, old_version="20251018120000", new_version="0003")
    # end-example
    # Just check that tracker is an instance of AsyncMigrationTracker
    assert isinstance(tracker, AsyncMigrationTracker)
