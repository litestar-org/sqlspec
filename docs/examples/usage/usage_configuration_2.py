__all__ = ("test_sqlite_config_setup",)


def test_sqlite_config_setup() -> None:

    # start-example
    from sqlspec.adapters.sqlite import SqliteConfig

    config = SqliteConfig(
        pool_config={
            "database": "myapp.db",  # Database file path
            "timeout": 5.0,  # Lock timeout in seconds
            "check_same_thread": False,  # Allow multi-thread access
            "cached_statements": 100,  # Statement cache size
            "uri": False,  # Enable URI mode
        }
    )
    # end-example
    assert config.pool_config["database"] == "myapp.db"
