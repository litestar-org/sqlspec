def test_psycopg_config_setup() -> None:
    from sqlspec.adapters.psycopg import PsycopgAsyncConfig

    # Async version
    config = PsycopgAsyncConfig(
        pool_config={
            "conninfo": "postgresql://user:pass@localhost/db",
            # Or keyword arguments:
            "host": "localhost",
            "port": 5432,
            "dbname": "mydb",
            "user": "myuser",
            "password": "mypassword",
            # Pool settings
            "min_size": 5,
            "max_size": 10,
            "timeout": 30.0,
        }
    )
    assert config.pool_config is not None
