"""Warm up migration commands and verify database connectivity before serving traffic."""

from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("main",)


def main() -> int:
    """Initialize migration commands, verify a session query, and close the pool."""
    config = SqliteConfig(
        connection_config={"database": ":memory:"}, migration_config={"script_location": "migrations"}
    )
    # --8<-- [start:example]
    commands = config.get_migration_commands()

    try:
        with config.provide_session() as session:
            value = session.select_value("SELECT 1")
    finally:
        config.close_pool()
    # --8<-- [end:example]

    assert value == 1
    assert config.get_migration_commands() is commands
    return int(value)


if __name__ == "__main__":
    print(main())
