def test_memory_databases() -> None:
    from sqlspec.adapters.sqlite import SqliteConfig
    # In-memory database (isolated per connection)
    config = SqliteConfig(pool_config={"database": ":memory:"})
    assert config.pool_config["database"] == ":memory:"

    # Shared memory database
    shared_config = SqliteConfig(
        pool_config={
            "database": "file:memdb1?mode=memory&cache=shared",
            "uri": True
        }
    )
    assert shared_config.pool_config["database"] == "file:memdb1?mode=memory&cache=shared"

