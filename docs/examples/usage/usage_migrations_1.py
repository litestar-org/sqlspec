__all__ = ("test_async_methods",)


async def test_async_methods() -> None:
    # start-example
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    config = AsyncpgConfig(
        pool_config={"dsn": "postgresql://user:pass@localhost/mydb"},
        migration_config={"enabled": True, "script_location": "migrations"},
    )

    # Apply migrations
    await config.migrate_up("head")
    # Or use the alias
    await config.upgrade("head")

    # Rollback one revision
    await config.migrate_down("-1")
    # Or use the alias
    await config.downgrade("-1")

    # Check current version
    current = await config.get_current_migration(verbose=True)
    print(current)
    # end-example
    # These are just smoke tests for method presence, not actual DB calls
    assert hasattr(config, "migrate_up")
    assert hasattr(config, "upgrade")
    assert hasattr(config, "migrate_down")
    assert hasattr(config, "downgrade")
    assert hasattr(config, "get_current_migration")
    assert hasattr(config, "create_migration")
    assert hasattr(config, "init_migrations")
    assert hasattr(config, "stamp_migration")
    assert hasattr(config, "fix_migrations")
