def test_duckdb_config_setup() -> None:
__all__ = ("test_duckdb_config_setup", )


    from sqlspec.adapters.duckdb import DuckDBConfig

    in_memory_config = DuckDBConfig()
    assert in_memory_config.pool_config.get("database") == ":memory:shared_db"

    persistent_config = DuckDBConfig(pool_config={"database": "analytics.duckdb", "read_only": False})
    assert persistent_config.pool_config["read_only"] is False
