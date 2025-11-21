__all__ = ("test_sqlite_config_setup",)


from pathlib import Path


def test_sqlite_config_setup(tmp_path: Path) -> None:

    # start-example
    from sqlspec.adapters.sqlite import SqliteConfig

    database_file = tmp_path / "myapp.db"
    config = SqliteConfig(
        pool_config={
            "database": database_file.name,  # Database file path
            "timeout": 5.0,  # Lock timeout in seconds
            "check_same_thread": False,  # Allow multi-thread access
            "cached_statements": 100,  # Statement cache size
            "uri": False,  # Enable URI mode
        }
    )
    # end-example
    assert config.pool_config["database"] == "myapp.db"
