#!/usr/bin/env python3
"""Litestar AsyncPG Integration Example

This example demonstrates how to use SQLSpec with AsyncPG in a Litestar application.
It shows basic database connectivity and SQL execution patterns.

IMPORTANT: This example demonstrates the corrected API usage after fixing the
imports issue that caused "cannot pickle 'module' object" errors. The key fix
was moving the SQL import to the top level instead of inside the route handler.

To run this example, you need a PostgreSQL database running. You can use Docker:
    docker run -d --name postgres-test -e POSTGRES_PASSWORD=test -p 5432:5432 postgres

Then modify the DSN below to match your database configuration.
"""
# /// script
# dependencies = [
#   "sqlspec[asyncpg,performance]",
#   "litestar[standard]",
# ]
# ///

from typing import Any

from litestar import Litestar, get

from sqlspec import SQL
from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver, AsyncpgPoolConfig
from sqlspec.extensions.litestar import DatabaseConfig, SQLSpec


@get("/")
async def hello_world(db_session: AsyncpgDriver) -> dict[str, Any]:
    """Simple endpoint that returns a greeting from the database."""
    result = await db_session.execute(SQL("SELECT 'Hello from AsyncPG!' as greeting"))
    return result.get_first() or {"greeting": "No data returned"}


@get("/version")
async def get_version(db_session: AsyncpgDriver) -> dict[str, Any]:
    """Get PostgreSQL version information."""
    result = await db_session.execute(SQL("SELECT version() as version"))

    if result.data:
        return {"database": "PostgreSQL", "version": result.data[0]["version"][:50] + "..."}
    return {"error": "Could not retrieve version"}


@get("/tables")
async def list_tables(db_session: AsyncpgDriver) -> dict[str, Any]:
    """List all tables in the current database."""
    result = await db_session.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name
        """)

    if result.data:
        tables = [row["table_name"] for row in result.data]
        return {"tables": tables, "count": len(tables)}
    return {"tables": [], "count": 0}


@get("/status")
async def get_status() -> dict[str, str]:
    """Health check endpoint that doesn't require database access."""
    return {"status": "ok", "service": "SQLSpec AsyncPG Example"}


# Configure SQLSpec with AsyncPG
# Note: Modify this DSN to match your database configuration
sqlspec = SQLSpec(
    config=[
        DatabaseConfig(
            config=AsyncpgConfig(
                pool_config=AsyncpgPoolConfig(
                    dsn="postgresql://postgres:postgres@localhost:5433/postgres", min_size=5, max_size=5
                )
            ),
            commit_mode="autocommit",
        )
    ]
)
app = Litestar(route_handlers=[hello_world, get_version, list_tables, get_status], plugins=[sqlspec], debug=True)

if __name__ == "__main__":
    import os

    from litestar.cli import litestar_group

    os.environ["LITESTAR_APP"] = "docs.examples.litestar_asyncpg:app"

    litestar_group()
