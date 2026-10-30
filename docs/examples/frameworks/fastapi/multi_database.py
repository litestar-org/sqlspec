"""Inject multiple async and sync SQLSpec database sessions into a FastAPI route."""

from typing import Annotated, Any

from fastapi import Depends, FastAPI

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.extensions.fastapi import SQLSpecPlugin

__all__ = ("main",)


def main() -> FastAPI:
    """Create a FastAPI application wired to both primary async and ETL sync databases."""
    spec = SQLSpec()
    spec.add_config(
        AiosqliteConfig(
            connection_config={"database": ":memory:"},
            extension_config={
                "fastapi": {"session_key": "db", "connection_key": "db_connection", "pool_key": "db_pool"}
            },
        )
    )
    spec.add_config(
        SqliteConfig(
            connection_config={"database": ":memory:"},
            extension_config={
                "fastapi": {"session_key": "etl_db", "connection_key": "etl_connection", "pool_key": "etl_pool"}
            },
        )
    )

    app = FastAPI()
    db_plugin = SQLSpecPlugin(spec, app)

    @app.get("/report")
    async def report(
        db: Annotated[AiosqliteDriver, Depends(db_plugin.provide_session("db"))],
        etl_db: Annotated[SqliteDriver, Depends(db_plugin.provide_session("etl_db"))],
    ) -> dict[str, list[dict[str, Any]]]:
        users = await db.select("SELECT 1 AS id, 'Alice' AS name")
        metrics = etl_db.select("SELECT 'metric1' AS name, 100 AS value")
        return {"users": users, "metrics": metrics}

    return app


if __name__ == "__main__":
    print(main())
