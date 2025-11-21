__all__ = ("test_duckdb_config_setup",)


from pathlib import Path


def test_duckdb_config_setup(tmp_path: Path) -> None:

    # start-example
    from sqlspec.adapters.duckdb import DuckDBConfig

    in_memory_config = DuckDBConfig()
    # end-example
    assert in_memory_config.pool_config.get("database") == ":memory:shared_db"

    database_file = tmp_path / "analytics.duckdb"
    persistent_config = DuckDBConfig(pool_config={"database": database_file.name, "read_only": False})
    assert persistent_config.pool_config["read_only"] is False
