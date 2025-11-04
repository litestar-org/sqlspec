from sqlspec.adapters.duckdb import DuckDBConfig

def test_duckdb_config_setup():
    in_memory_config = DuckDBConfig()
    assert in_memory_config.pool_config == {}

    persistent_config = DuckDBConfig(
        pool_config={
            "database": "analytics.duckdb",
            "read_only": False,
        }
    )
    assert persistent_config.pool_config["read_only"] is False

