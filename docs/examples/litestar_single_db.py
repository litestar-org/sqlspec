from aiosqlite import Connection
from litestar import Litestar, get

from sqlspec.adapters.aiosqlite import Aiosqlite
from sqlspec.extensions.litestar import SQLSpec


@get("/")
async def simple_sqlite(db_connection: Connection) -> dict[str, str]:
    """Simple select statement.

    Returns:
        dict[str, str]: The greeting.
    """
    result = await db_connection.execute_fetchall("SELECT 'Hello, world!' AS greeting")
    return {"greeting": result[0][0]}  # type: ignore


sqlspec = SQLSpec(config=Aiosqlite())
app = Litestar(route_handlers=[simple_sqlite], plugins=[sqlspec])
