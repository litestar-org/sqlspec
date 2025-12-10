__all__ = ("test_memory_databases",)


def test_memory_databases() -> None:

    # start-example
    from sqlspec.adapters.sqlite import SqliteConfig

    # In-memory database (isolated per connection)
    config = SqliteConfig(connection_config={"database": ":memory:"})
    # end-example
    assert ":memory_" in config.connection_config["database"]

    # Shared memory database
    shared_config = SqliteConfig(connection_config={"database": "file:memdb1?mode=memory&cache=shared", "uri": True})
    assert shared_config.connection_config["database"] == "file:memdb1?mode=memory&cache=shared"
