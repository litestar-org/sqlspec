"""Litestar Asyncpg

This example demonstrates how to use a Asyncpg database in a Litestar application.

The example uses the `SQLSpec` extension to create a connection to a Asyncpg database.

The Asyncpg database also demonstrates how to use the plugin loader and `secrets` configuration manager built into SQLSpec.
"""
# /// script
# dependencies = [
#   "sqlspec[psycopg,asyncpg,performance]",
#   "litestar[standard]",
# ]
# ///

from litestar import Litestar, get

from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver, AsyncpgPoolConfig
from sqlspec.extensions.litestar import DatabaseConfig, SQLSpec


@get("/")
async def simple_asyncpg(db_session: AsyncpgDriver) -> dict[str, str]:
    return await db_session.select_one("SELECT 'Hello, world!' AS greeting")


sqlspec = SQLSpec(
    config=[
        DatabaseConfig(
            config=AsyncpgConfig(
                pool_config=AsyncpgPoolConfig(dsn="postgres://app:app@localhost:15432/app", min_size=1, max_size=3),
            ),
            commit_mode="autocommit",
        )
    ]
)
app = Litestar(route_handlers=[simple_asyncpg], plugins=[sqlspec])

if __name__ == "__main__":
    import os

    from litestar.cli import litestar_group

    os.environ["LITESTAR_APP"] = "docs.examples.litestar_asyncpg:app"

    litestar_group()
