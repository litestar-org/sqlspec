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

from sqlspec import SQLSpec
from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgAsyncDriver
from sqlspec.core.statement import SQL
from sqlspec.extensions.litestar import SQLSpecPlugin


@get("/")
async def simple_psycopg(db_session: PsycopgAsyncDriver) -> dict[str, str]:
    result = await db_session.execute(SQL("SELECT 'Hello, world!' AS greeting"))
    return result.get_first() or {"greeting": "No result found"}


spec = SQLSpec()
db = spec.add_config(
    PsycopgAsyncConfig(
        pool_config={"conninfo": "postgres://app:app@localhost:15432/app", "min_size": 1, "max_size": 3},
        extension_config={"litestar": {"commit_mode": "autocommit"}},
    )
)
plugin = SQLSpecPlugin(sqlspec=spec)
app = Litestar(route_handlers=[simple_psycopg], plugins=[plugin])

if __name__ == "__main__":
    import os

    from litestar.cli import litestar_group

    os.environ["LITESTAR_APP"] = "docs.examples.litestar_psycopg:app"

    litestar_group()
