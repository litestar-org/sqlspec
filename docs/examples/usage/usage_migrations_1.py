# start-example
from sqlspec.adapters.asyncpg import AsyncpgConfig

__all__ = ("test_async_methods",)


config = AsyncpgConfig(
    pool_config={"dsn": "postgresql://user:pass@localhost/mydb"},
    migration_config={"enabled": True, "script_location": "migrations"},
)
# end-example


def test_async_methods() -> None:
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
