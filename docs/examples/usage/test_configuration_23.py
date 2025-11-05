"""Test configuration example: Environment-based configuration."""


def test_extension_config() -> None:
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    config = AsyncpgConfig(
        pool_config={"dsn": "postgresql://localhost/db"},
        extension_config={
            "litestar": {
                "connection_key": "db_connection",
                "session_key": "db_session",
                "pool_key": "db_pool",
                "commit_mode": "autocommit",
                "enable_correlation_middleware": True,
            }
        },
    )
    assert config.extension_config == {
        "litestar": {
            "connection_key": "db_connection",
            "session_key": "db_session",
            "pool_key": "db_pool",
            "commit_mode": "autocommit",
            "enable_correlation_middleware": True,
        }
    }
