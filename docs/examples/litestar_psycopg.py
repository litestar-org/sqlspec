"""Litestar Psycopg

This example demonstrates how to use a Psycopg database in a Litestar application.

The example uses the `SQLSpec` extension to create a connection to a Psycopg database.

The Psycopg database also demonstrates how to use the plugin loader and `secrets` configuration manager built into SQLSpec.
"""
# /// script
# dependencies = [
#   "sqlspec[psycopg]",
#   "litestar[standard]",
# ]
# ///

from litestar import Litestar, get

from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgAsyncDriver, PsycopgAsyncPoolConfig
from sqlspec.extensions.litestar import DatabaseConfig, SQLSpec


@get("/")
async def simple_psycopg(db_session: PsycopgAsyncDriver) -> dict[str, str]:
    return await db_session.select_one("SELECT 'Hello, world!' AS greeting")


sqlspec = SQLSpec(
    config=[
        DatabaseConfig(
            config=PsycopgAsyncConfig(
                pool_config=PsycopgAsyncPoolConfig(
                    conninfo="postgres://app:app@localhost:15432/app", min_size=1, max_size=3
                ),
            ),
            commit_mode="autocommit",
        )
    ],
)
app = Litestar(route_handlers=[simple_psycopg], plugins=[sqlspec])

if __name__ == "__main__":
    import os

    from litestar.cli import litestar_group

    os.environ["LITESTAR_APP"] = "docs.examples.litestar_psycopg:app"

    litestar_group()
