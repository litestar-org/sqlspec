def test_asyncpg_config_setup() -> None:
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    config = AsyncpgConfig(
        pool_config={
            "dsn": "postgresql://user:pass@localhost:5432/dbname",
            # Other parameters
            "host": "localhost",
            "port": 5432,
            "user": "myuser",
            "password": "mypassword",
            "database": "mydb",
            "min_size": 10,
            "max_size": 20,
        }
    )
    assert config.pool_config["host"] == "localhost"
