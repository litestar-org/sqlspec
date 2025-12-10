__all__ = ("test_thread_local_connections",)


def test_thread_local_connections() -> None:

    # start-example
    from sqlspec.adapters.sqlite import SqliteConfig

    config = SqliteConfig(connection_config={"database": "test.db"})
    # end-example
    assert config.connection_config["database"] == "test.db"
