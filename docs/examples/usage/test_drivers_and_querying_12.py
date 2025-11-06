# Test module converted from docs example - code-block 12
"""Minimal smoke test for drivers_and_querying example 12."""

from sqlspec.adapters.bigquery import BigQueryConfig


def test_example_12_bigquery_config() -> None:
    config = BigQueryConfig(pool_config={"project": "my-project", "credentials": None})
    assert config.pool_config["project"] == "my-project"
