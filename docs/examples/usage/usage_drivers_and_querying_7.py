# Test module converted from docs example - code-block 7
"""Minimal smoke test for drivers_and_querying example 7."""


def test_example_7_sync_sqlite() -> None:
    from sqlspec.adapters.sqlite import SqliteConfig

    config = SqliteConfig(pool_config={"database": "myapp.db", "timeout": 5.0, "check_same_thread": False})
    assert config.pool_config["database"] == "myapp.db"
