from typing import Annotated

import pytest

__all__ = ("test_fastapi_multi_database",)


def test_fastapi_multi_database() -> None:
    pytest.importorskip("fastapi")
    pytest.importorskip("aiosqlite")
    # start-example
    from fastapi import Depends, FastAPI

    from sqlspec import SQLSpec
    from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
    from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
    from sqlspec.extensions.fastapi import SQLSpecPlugin

    sqlspec = SQLSpec()

    # Primary async database
    sqlspec.add_config(
        AiosqliteConfig(
            connection_config={"database": ":memory:"},
            extension_config={
                "starlette": {"session_key": "db", "connection_key": "db_connection", "pool_key": "db_pool"}
            },
        )
    )

    # ETL sync database (e.g., DuckDB pattern)
    sqlspec.add_config(
        SqliteConfig(
            connection_config={"database": ":memory:"},
            extension_config={
                "starlette": {"session_key": "etl_db", "connection_key": "etl_connection", "pool_key": "etl_pool"}
            },
        )
    )

    app = FastAPI()
    db_plugin = SQLSpecPlugin(sqlspec, app)

    @app.get("/report")
    async def report(
        db: Annotated[AiosqliteDriver, Depends(db_plugin.provide_session("db"))],
        etl_db: Annotated[SqliteDriver, Depends(db_plugin.provide_session("etl_db"))],
    ) -> dict[str, list]:
        # Async query to primary database
        users = await db.select("SELECT 1 as id, 'Alice' as name")
        # Sync query to ETL database
        metrics = etl_db.select("SELECT 'metric1' as name, 100 as value")
        return {"users": users, "metrics": metrics}

    # end-example

    assert app is not None
