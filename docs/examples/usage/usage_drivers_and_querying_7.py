# Test module converted from docs example - code-block 7
"""Minimal smoke test for drivers_and_querying example 7."""

__all__ = ("test_example_7_sync_sqlite",)


def test_example_7_sync_sqlite() -> None:
    # start-example
    from sqlspec.adapters.sqlite import SqliteConfig

    config = SqliteConfig(connection_config={"database": "myapp.db", "timeout": 5.0, "check_same_thread": False})
    assert config.connection_config["database"] == "myapp.db"
    # end-example
