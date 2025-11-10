def test_memory_databases() -> None:
__all__ = ("test_memory_databases", )


    from sqlspec.adapters.sqlite import SqliteConfig

    # In-memory database (isolated per connection)
    config = SqliteConfig(pool_config={"database": ":memory:"})
    assert ":memory_" in config.pool_config["database"]

    # Shared memory database
    shared_config = SqliteConfig(pool_config={"database": "file:memdb1?mode=memory&cache=shared", "uri": True})
    assert shared_config.pool_config["database"] == "file:memdb1?mode=memory&cache=shared"
