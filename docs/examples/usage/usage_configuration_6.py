__all__ = ("test_asyncmy_config_setup",)


def test_asyncmy_config_setup() -> None:
    from sqlspec.adapters.asyncmy import AsyncmyConfig

    mysql_port = 3306
    config = AsyncmyConfig(
        pool_config={
            "host": "localhost",
            "port": mysql_port,
            "user": "myuser",
            "password": "mypassword",
            "database": "mydb",
            "charset": "utf8mb4",
            "minsize": 1,
            "maxsize": 10,
            "pool_recycle": 3600,
        }
    )
    assert config.pool_config["port"] == mysql_port
