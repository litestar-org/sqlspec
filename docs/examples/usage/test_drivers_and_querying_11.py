# Test module converted from docs example - code-block 11
"""Minimal smoke test for drivers_and_querying example 11."""

from sqlspec.adapters.oracledb import OracleDBConfig


def test_example_11_oracledb_config() -> None:
    config = OracleDBConfig(pool_config={"user": "myuser", "password": "mypassword", "dsn": "localhost:1521/ORCLPDB"})
    assert "dsn" in config.pool_config
