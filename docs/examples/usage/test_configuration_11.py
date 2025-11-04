def test_thread_local_connections() -> None:
    from sqlspec.adapters.sqlite import SqliteConfig

    config = SqliteConfig(pool_config={"database": "test.db"})
    assert config.pool_config["database"] == "test.db"
