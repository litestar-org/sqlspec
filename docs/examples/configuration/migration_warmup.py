"""Warm up migration commands and verify database connectivity before serving traffic."""

# --8<-- [start:example]
from sqlspec.adapters.sqlite import SqliteConfig

conn_cfg = {"database": ":memory:"}
mig_cfg = {"script_location": "migrations"}
config = SqliteConfig(connection_config=conn_cfg, migration_config=mig_cfg)
commands = config.get_migration_commands()

try:
    with config.provide_session() as session:
        value = session.select_value("SELECT 1")
finally:
    config.close_pool()
# --8<-- [end:example]

__all__ = ("main",)


def main() -> int:
    """Initialize migration commands, verify a session query, and close the pool."""
    assert value == 1
    assert config.get_migration_commands() is commands
    return int(value)


if __name__ == "__main__":
    print(main())
