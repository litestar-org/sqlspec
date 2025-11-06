# Test module converted from docs example - code-block 4
"""Minimal smoke test for drivers_and_querying example 4."""

from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psycopg import PsycopgSyncConfig


def test_example_4_construct_config(postgres_service: PostgresService) -> None:
    config = PsycopgSyncConfig(
        pool_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        }
    )

    assert config is not None
