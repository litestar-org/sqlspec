# Test module converted from docs example - code-block 1
"""Minimal smoke test for drivers_and_querying example 1."""

from pytest_databases.docker.postgres import PostgresService


async def test_importable_1(postgres_service: PostgresService) -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgPoolConfig

    # Typical driver usage
    spec = SQLSpec()
    db = spec.add_config(
        AsyncpgConfig(
            pool_config=AsyncpgPoolConfig(
                host=postgres_service.host,
                port=postgres_service.port,
                user=postgres_service.user,
                password=postgres_service.password,
                database=postgres_service.database,
            )
        )
    )  # Config layer, registers pool
    async with spec.provide_session(db) as session:  # Session layer
        await session.execute("SELECT 1")  # Driver layer
    # end-example
