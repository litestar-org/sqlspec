"""AsyncPG connection pool configured through SQLSpec."""

import asyncio
import os

from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgPoolConfig

__all__ = ("main",)


DSN = os.getenv("SQLSPEC_ASYNCPG_DSN", "postgresql://postgres:postgres@localhost:5432/postgres")
config = AsyncpgConfig(bind_key="docs_asyncpg", pool_config=AsyncpgPoolConfig(dsn=DSN, min_size=1, max_size=5))


async def main() -> None:
    """Connect to Postgres and return the server version."""
    async with config.provide_session() as session:
        result = await session.execute("SELECT version() AS version")
        row = result.one_or_none()
        if row:
            print({"adapter": "asyncpg", "version": row["version"]})
        else:
            print({"adapter": "asyncpg", "version": "unknown"})


if __name__ == "__main__":
    asyncio.run(main())
