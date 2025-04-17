from aiosqlite import Connection
from duckdb import DuckDBPyConnection
from litestar import Litestar, get

from sqlspec.adapters.aiosqlite import Aiosqlite
from sqlspec.adapters.duckdb import DuckDB
from sqlspec.extensions.litestar import DatabaseConfig, SQLSpec


@get("/test", sync_to_thread=True)
def simple_select(etl_connection: DuckDBPyConnection) -> dict[str, str]:
    result = etl_connection.execute("SELECT 'Hello, world!' AS greeting").fetchall()
    return {"greeting": result[0][0]}


@get("/")
async def simple_sqlite(db_connection: Connection) -> dict[str, str]:
    result = await db_connection.execute_fetchall("SELECT 'Hello, world!' AS greeting")
    return {"greeting": result[0][0]}  # type: ignore


sqlspec = SQLSpec(
    config=[
        DatabaseConfig(config=Aiosqlite(), commit_mode="autocommit"),
        DatabaseConfig(
            config=DuckDB(
                extensions=[{"name": "vss", "force_install": True}],
                secrets=[{"secret_type": "s3", "name": "s3_secret", "value": {"key_id": "abcd"}}],
            ),
            connection_key="etl_connection",
        ),
    ],
)
app = Litestar(route_handlers=[simple_sqlite, simple_select], plugins=[sqlspec])
