"""Litestar Single DB

This example demonstrates how to use a single database in a Litestar application.

This examples hows how to get the raw connection object from the SQLSpec plugin.
"""

# /// script
# dependencies = [
#   "sqlspec[aiosqlite]",
#   "litestar[standard]",
# ]
# ///

from aiosqlite import Connection
from litestar import Litestar, get

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.extensions.litestar import SQLSpec


@get("/")
async def simple_sqlite(db_connection: Connection) -> dict[str, str]:
    """Simple select statement.

    Returns:
        dict[str, str]: The greeting.
    """
    result = await db_connection.execute_fetchall("SELECT 'Hello, world!' AS greeting")
    return {"greeting": result[0][0]}  # type: ignore


sqlspec = SQLSpec(config=AiosqliteConfig())
app = Litestar(route_handlers=[simple_sqlite], plugins=[sqlspec])
