__all__ = ("test_duckdb_config_setup",)


def test_duckdb_config_setup() -> None:

    # start-example
    from sqlspec.adapters.duckdb import DuckDBConfig

    in_memory_config = DuckDBConfig()
    # end-example
    assert in_memory_config.pool_config.get("database") == ":memory:shared_db"

    persistent_config = DuckDBConfig(pool_config={"database": "analytics.duckdb", "read_only": False})
    assert persistent_config.pool_config["read_only"] is False
