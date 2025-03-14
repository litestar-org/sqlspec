from aiosqlite import Connection
from duckdb import DuckDBPyConnection
from litestar import Litestar, get

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.extensions.litestar import DatabaseConfig, SQLSpec


@get("/test", sync_to_thread=True)
def simple_select(etl_session: DuckDBPyConnection) -> dict[str, str]:
    result = etl_session.execute("SELECT 'Hello, world!' AS greeting").fetchall()
    return {"greeting": result[0][0]}


@get("/")
async def simple_sqlite(db_connection: Connection) -> dict[str, str]:
    result = await db_connection.execute_fetchall("SELECT 'Hello, world!' AS greeting")
    return {"greeting": result[0][0]}  # type: ignore  # noqa: PGH003


sqlspec = SQLSpec(
    config=[
        DatabaseConfig(config=AiosqliteConfig(), commit_mode="autocommit"),
        DatabaseConfig(config=DuckDBConfig(), connection_key="etl_session"),
    ],
)
app = Litestar(route_handlers=[simple_sqlite, simple_select], plugins=[sqlspec])
