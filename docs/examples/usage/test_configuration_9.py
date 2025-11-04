def test_pool_lifecycle() -> None:
    from sqlspec import SQLSpec
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    spec = SQLSpec()
    db = spec.add_config(AsyncpgConfig(pool_config={"dsn": "postgresql://localhost/db"}))
    assert db.pool_config["dsn"] == "postgresql://localhost/db"
