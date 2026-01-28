from __future__ import annotations

import pytest

__all__ = ("test_starlette_multi_database",)


def test_starlette_multi_database() -> None:
    pytest.importorskip("starlette")
    pytest.importorskip("aiosqlite")
    # start-example
    from starlette.applications import Starlette
    from starlette.requests import Request
    from starlette.responses import JSONResponse
    from starlette.routing import Route

    from sqlspec import SQLSpec
    from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
    from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
    from sqlspec.extensions.starlette import SQLSpecPlugin

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

    # ETL sync database
    sqlspec.add_config(
        SqliteConfig(
            connection_config={"database": ":memory:"},
            extension_config={
                "starlette": {"session_key": "etl_db", "connection_key": "etl_connection", "pool_key": "etl_pool"}
            },
        )
    )

    db_plugin = SQLSpecPlugin(sqlspec)

    async def report(request: Request) -> JSONResponse:
        db: AiosqliteDriver = db_plugin.get_session(request, "db")
        etl_db: SqliteDriver = db_plugin.get_session(request, "etl_db")

        # Async query to primary database
        users = await db.select("SELECT 1 as id, 'Alice' as name")
        # Sync query to ETL database
        metrics = etl_db.select("SELECT 'metric1' as name, 100 as value")
        return JSONResponse({"users": users, "metrics": metrics})

    app = Starlette(routes=[Route("/report", report)])
    db_plugin.init_app(app)
    # end-example

    assert app is not None
