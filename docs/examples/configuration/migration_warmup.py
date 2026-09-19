def test_migration_warmup() -> None:
    # start-example
    from sqlspec.adapters.sqlite import SqliteConfig

    config = SqliteConfig(
        connection_config={"database": ":memory:"}, migration_config={"script_location": "migrations"}
    )

    # Check custom tracker setup and extension discovery before serving requests.
    commands = config.get_migration_commands()

    try:
        with config.provide_session() as session:
            value = session.select_value("SELECT 1")
    finally:
        config.close_pool()
    # end-example

    assert value == 1
    assert config.get_migration_commands() is commands
