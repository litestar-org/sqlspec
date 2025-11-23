async def test_async_command_class_methods() -> None:
__all__ = ("test_async_command_class_methods", )


    # start-example
    from sqlspec.adapters.asyncpg import AsyncpgConfig
    from sqlspec.migrations.commands import AsyncMigrationCommands

    config = AsyncpgConfig(pool_config={"dsn": "postgresql://..."}, migration_config={"script_location": "migrations"})

    # Create commands instance
    commands = AsyncMigrationCommands(config)

    # Use commands directly
    await commands.upgrade("head")
    # end-example

    # Smoke test for AsyncMigrationCommands method presence
    assert hasattr(commands, "upgrade")
    assert hasattr(commands, "downgrade")
    assert hasattr(commands, "get_current_migration")
