# Test module converted from docs example - code-block 10
"""Minimal smoke test for drivers_and_querying example 10."""

from sqlspec.adapters.duckdb import DuckDBConfig


def test_example_10_duckdb_config() -> None:
    DuckDBConfig()
    config_persistent = DuckDBConfig(pool_config={"database": "analytics.duckdb"})
    assert config_persistent.pool_config["database"] == "analytics.duckdb"
