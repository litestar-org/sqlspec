from sqlspec.adapters.asyncmy import AsyncmyConfig

def test_asyncmy_config_setup():
    config = AsyncmyConfig(
        pool_config={
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
    assert config.pool_config["port"] == 3306

