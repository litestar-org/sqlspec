"""Minimal smoke test for drivers_and_querying example 16."""

__all__ = ("test_example_16_placeholder",)


from pytest_databases.docker.postgres import PostgresService

from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgPoolConfig


async def test_example_16_placeholder(postgres_service: PostgresService) -> None:
    spec = SQLSpec()
    config = spec.add_config(
        AsyncpgConfig(
            pool_config=AsyncpgPoolConfig(
                host=postgres_service.host,
                port=postgres_service.port,
                user=postgres_service.user,
                password=postgres_service.password,
                database=postgres_service.database,
            )
        )
    )
    async with spec.provide_session(config) as session, session.begin():
        _ = session.execute("UPDATE accounts SET balance = balance - 100 WHERE id = ?", 1)
        _ = session.execute("UPDATE accounts SET balance = balance + 100 WHERE id = ?", 2)
    # Auto-commits on success, auto-rollbacks on exception
