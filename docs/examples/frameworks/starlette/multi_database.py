"""Query both async primary and sync ETL SQLSpec sessions inside a Starlette endpoint."""

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.extensions.starlette import SQLSpecPlugin

__all__ = ("main",)


def main() -> Starlette:
    """Create a Starlette application wired to async and sync SQLite databases."""
    # --8<-- [start:example]
    spec = SQLSpec()
    spec.add_config(
        AiosqliteConfig(
            connection_config={"database": ":memory:"},
            extension_config={
                "starlette": {"session_key": "db", "connection_key": "db_connection", "pool_key": "db_pool"}
            },
        )
    )
    spec.add_config(
        SqliteConfig(
            connection_config={"database": ":memory:"},
            extension_config={
                "starlette": {"session_key": "etl_db", "connection_key": "etl_connection", "pool_key": "etl_pool"}
            },
        )
    )

    db_plugin = SQLSpecPlugin(spec)

    async def report(request: Request) -> JSONResponse:
        db: AiosqliteDriver = db_plugin.get_session(request, "db")
        etl_db: SqliteDriver = db_plugin.get_session(request, "etl_db")
        users = await db.select(
            """
            SELECT 1 AS id, 'Alice' AS name
            """
        )
        metrics = etl_db.select(
            """
            SELECT 'metric1' AS name, 100 AS value
            """
        )
        return JSONResponse({"users": users, "metrics": metrics})

    app = Starlette(routes=[Route("/report", report)])
    db_plugin.init_app(app)
    # --8<-- [end:example]
    return app


if __name__ == "__main__":
    print(main())
