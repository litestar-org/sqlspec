# Test module converted from docs example - code-block 1
"""Minimal smoke test for drivers_and_querying example 1."""

import os

from pytest_databases.docker.postgres import PostgresService

__all__ = ("test_importable_1",)


async def test_importable_1(postgres_service: PostgresService) -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgPoolConfig

    # Typical driver usage
    spec = SQLSpec()
    host = os.environ.get("SQLSPEC_USAGE_PG_HOST", "localhost")
    port = int(os.environ.get("SQLSPEC_USAGE_PG_PORT", "5432"))
    user = os.environ.get("SQLSPEC_USAGE_PG_USER", "postgres")
    password = os.environ.get("SQLSPEC_USAGE_PG_PASSWORD", "postgres")
    database = os.environ.get("SQLSPEC_USAGE_PG_DATABASE", "sqlspec")

    db = spec.add_config(
        AsyncpgConfig(
            pool_config=AsyncpgPoolConfig(host=host, port=port, user=user, password=password, database=database)
        )
    )  # Config layer, registers pool
    async with spec.provide_session(db) as session:  # Session layer
        await session.execute("SELECT 1")  # Driver layer
    # end-example
