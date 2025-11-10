def test_thread_local_connections() -> None:
__all__ = ("test_thread_local_connections", )


    from sqlspec.adapters.sqlite import SqliteConfig

    config = SqliteConfig(pool_config={"database": "test.db"})
    assert config.pool_config["database"] == "test.db"
