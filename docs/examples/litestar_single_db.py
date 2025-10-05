"""Litestar Single DB

This example demonstrates how to use a single database in a Litestar application.

This examples hows how to get the raw connection object from the SQLSpec plugin.
"""

from aiosqlite import Connection
from litestar import Litestar, get

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.extensions.litestar import SQLSpecPlugin


@get("/")
async def simple_sqlite(db_connection: Connection) -> dict[str, str]:
    """Simple select statement.

    Returns:
        dict[str, str]: The greeting.
    """
    result = await db_connection.execute_fetchall("SELECT 'Hello, world!' AS greeting")
    return {"greeting": next(iter(result))[0]}


sql = SQLSpec()
sql.add_config(AiosqliteConfig())
plugin = SQLSpecPlugin(sqlspec=sql)
app = Litestar(route_handlers=[simple_sqlite], plugins=[plugin])
