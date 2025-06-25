"""Litestar Asyncpg

This example demonstrates how to use a Asyncpg database in a Litestar application.

The example uses the `SQLSpec` extension to create a connection to a Asyncpg database.

The Asyncpg database also demonstrates how to use the plugin loader and `secrets` configuration manager built into SQLSpec.
"""
# /// script
# dependencies = [
#   "sqlspec[psycopg,asyncpg,performance] @ git+https://github.com/litestar-org/sqlspec.git@main",
#   "litestar[standard]",
# ]
# ///

from typing import Annotated, Any

from litestar import Litestar, get
from litestar.params import Dependency

from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
from sqlspec.extensions.litestar import DatabaseConfig, SQLSpec, providers
from sqlspec.statement import SQLResult
from sqlspec.statement.filters import FilterTypes


@get("/", dependencies=providers.create_filter_dependencies({"search": "greeting", "search_ignore_case": True}))
async def simple_asyncpg(
    db_session: AsyncpgDriver, filters: Annotated[list[FilterTypes], Dependency(skip_validation=True)]
) -> SQLResult[dict[str, Any]]:
    from sqlspec.statement.sql import SQL

    return await db_session.execute(SQL("SELECT greeting FROM (select 'Hello, world!' as greeting) as t"), *filters)


sqlspec = SQLSpec(
    config=[
        DatabaseConfig(
            config=AsyncpgConfig(dsn="postgres://app:app@localhost:15432/app", min_size=1, max_size=3),
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
