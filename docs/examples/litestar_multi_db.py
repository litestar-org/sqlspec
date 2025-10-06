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

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBDriver
from sqlspec.core.statement import SQL
from sqlspec.extensions.litestar import SQLSpecPlugin


@get("/test", sync_to_thread=True)
def simple_select(etl_session: DuckDBDriver) -> dict[str, str]:
    result = etl_session.execute(SQL("SELECT 'Hello, ETL world!' AS greeting"))
    greeting = result.get_first()
    return {"greeting": greeting["greeting"] if greeting is not None else "hi"}


@get("/")
async def simple_sqlite(db_session: AiosqliteDriver) -> dict[str, str]:
    result = await db_session.execute("SELECT 'Hello, world!' AS greeting")
    greeting = result.get_first()
    return {"greeting": greeting["greeting"] if greeting is not None else "hi"}


spec = SQLSpec()
sqlite_db = spec.add_config(AiosqliteConfig(extension_config={"litestar": {"commit_mode": "autocommit"}}))
duckdb_db = spec.add_config(
    DuckDBConfig(
        driver_features={
            "extensions": [{"name": "vss", "force_install": True}],
            "secrets": [{"secret_type": "s3", "name": "s3_secret", "value": {"key_id": "abcd"}}],
        },
        extension_config={"litestar": {"connection_key": "etl_connection", "session_key": "etl_session"}},
    )
)
plugin = SQLSpecPlugin(sqlspec=spec)
app = Litestar(route_handlers=[simple_sqlite, simple_select], plugins=[plugin])

if __name__ == "__main__":
    import os

    from litestar.cli import litestar_group

    os.environ["LITESTAR_APP"] = "docs.examples.litestar_multi_db:app"

    litestar_group()
