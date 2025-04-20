"""Litestar Multi DB

This example demonstrates how to use multiple databases in a Litestar application.

The example uses the `SQLSpec` extension to create a connection to a SQLite (via `aiosqlite`) and DuckDB database.

The DuckDB database also demonstrates how to use the plugin loader and `secrets` configuration manager built into SQLSpec.
"""
# /// script
# dependencies = [
#   "sqlspec[aiosqlite,duckdb]",
#   "litestar[standard]",
# ]
# ///

from litestar import Litestar, get

from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBDriver
from sqlspec.extensions.litestar import DatabaseConfig, SQLSpec


@get("/test", sync_to_thread=True)
def simple_select(etl_session: DuckDBDriver) -> dict[str, str]:
    result = etl_session.select_one("SELECT 'Hello, ETL world!' AS greeting")
    return {"greeting": result["greeting"]}


@get("/")
async def simple_sqlite(db_session: AiosqliteDriver) -> dict[str, str]:
    return await db_session.select_one("SELECT 'Hello, world!' AS greeting")


sqlspec = SQLSpec(
    config=[
        DatabaseConfig(config=AiosqliteConfig(), commit_mode="autocommit"),
        DatabaseConfig(
            config=DuckDBConfig(
                extensions=[{"name": "vss", "force_install": True}],
                secrets=[{"secret_type": "s3", "name": "s3_secret", "value": {"key_id": "abcd"}}],
            ),
            connection_key="etl_connection",
            session_key="etl_session",
        ),
    ],
)
app = Litestar(route_handlers=[simple_sqlite, simple_select], plugins=[sqlspec])

if __name__ == "__main__":
    import os

    from litestar.cli import litestar_group

    os.environ["LITESTAR_APP"] = "docs.examples.litestar_multi_db:app"

    litestar_group()
