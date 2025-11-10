def test_psycopg_config_setup() -> None:
__all__ = ("test_psycopg_config_setup", )


    import os

    from sqlspec.adapters.psycopg import PsycopgAsyncConfig

    host = os.getenv("SQLSPEC_USAGE_PG_HOST", "localhost")
    port = int(os.getenv("SQLSPEC_USAGE_PG_PORT", "5432"))
    database = os.getenv("SQLSPEC_USAGE_PG_DATABASE", "db")
    user = os.getenv("SQLSPEC_USAGE_PG_USER", "user")
    password = os.getenv("SQLSPEC_USAGE_PG_PASSWORD", "password")
    dsn = os.getenv("SQLSPEC_USAGE_PG_DSN", f"postgresql://{user}:{password}@{host}:{port}/{database}")

    # Async version
    config = PsycopgAsyncConfig(
        pool_config={
            "conninfo": dsn,
            # Or keyword arguments:
            "host": host,
            "port": port,
            "dbname": database,
            "user": user,
            "password": password,
            # Pool settings
            "min_size": 5,
            "max_size": 10,
            "timeout": 30.0,
        }
    )
    assert config.pool_config is not None
