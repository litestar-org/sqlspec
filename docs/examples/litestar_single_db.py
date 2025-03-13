from aiosqlite import Connection
from litestar import Litestar, get

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.extensions.litestar import SQLSpec


@get("/")
async def simple_sqlite(db_session: Connection) -> dict[str, str]:
    """Simple select statement.

    Returns:
        dict[str, str]: The greeting.
    """
    result = await db_session.execute_fetchall("SELECT 'Hello, world!' AS greeting")
    return {"greeting": result[0][0]}  # type: ignore  # noqa: PGH003


sqlspec = SQLSpec(config=AiosqliteConfig())
app = Litestar(route_handlers=[simple_sqlite], plugins=[sqlspec])
