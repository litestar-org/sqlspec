"""Litestar Single DB

This example demonstrates how to use a single database in a Litestar application.

This examples hows how to get the raw connection object from the SQLSpec plugin.

Usage:
    litestar --app docs.examples.litestar_single_db:app run --reload
"""
# /// script
# dependencies = [
#   "sqlspec[aiosqlite,litestar]",
#   "rich",
#   "litestar[standard]",
# ]
# requires-python = ">=3.10"
# ///

from aiosqlite import Connection
from litestar import Litestar, get
from rich import print

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


spec = SQLSpec()
db = spec.add_config(AiosqliteConfig())
plugin = SQLSpecPlugin(sqlspec=spec)
app = Litestar(route_handlers=[simple_sqlite], plugins=[plugin])

if __name__ == "__main__":
    print("[cyan]Run with:[/cyan] litestar --app docs.examples.litestar_single_db:app run --reload")
    print("[yellow]Or directly:[/yellow] uv run python docs/examples/litestar_single_db.py")
