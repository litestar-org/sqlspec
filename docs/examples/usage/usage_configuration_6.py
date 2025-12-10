__all__ = ("test_asyncmy_config_setup",)


def test_asyncmy_config_setup() -> None:
    # start-example
    from sqlspec.adapters.asyncmy import AsyncmyConfig

    config = AsyncmyConfig(
        connection_config={
            "host": "localhost",
            "port": 3306,
            "user": "myuser",
            "password": "mypassword",
            "database": "mydb",
            "charset": "utf8mb4",
            "minsize": 1,
            "maxsize": 10,
            "pool_recycle": 3600,
        }
    )
    # end-example
    assert config.connection_config["port"] == 3306
