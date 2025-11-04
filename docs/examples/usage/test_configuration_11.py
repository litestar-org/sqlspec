from sqlspec.adapters.sqlite import SqliteConfig

def test_thread_local_connections():
    config = SqliteConfig(pool_config={"database": "test.db"})
    assert config.pool_config["database"] == "test.db"

