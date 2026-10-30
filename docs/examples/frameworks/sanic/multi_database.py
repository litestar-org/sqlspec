"""Query multiple async and sync SQLSpec database binds inside a Sanic route."""

from sanic import Request, Sanic, response
from sanic.response import HTTPResponse

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.extensions.sanic import SQLSpecPlugin

__all__ = ("main",)


def main() -> Sanic:
    """Create a Sanic application wired to primary async and analytics sync databases."""
    # --8<-- [start:example]
    spec = SQLSpec()
    spec.add_config(
        AiosqliteConfig(
            bind_key="primary",
            connection_config={"database": ":memory:"},
            extension_config={
                "sanic": {
                    "connection_key": "primary_connection",
                    "pool_key": "primary_pool",
                    "session_key": "primary_db",
                }
            },
        )
    )
    spec.add_config(
        SqliteConfig(
            bind_key="analytics",
            connection_config={"database": ":memory:"},
            extension_config={
                "sanic": {
                    "connection_key": "analytics_connection",
                    "pool_key": "analytics_pool",
                    "session_key": "analytics_db",
                }
            },
        )
    )

    db_plugin = SQLSpecPlugin(spec)
    app = Sanic("SQLSpecSanicMultiDatabaseExample")

    @app.get("/report")
    async def report(request: Request) -> HTTPResponse:
        primary: AiosqliteDriver = db_plugin.get_session(request, "primary_db")
        analytics: SqliteDriver = db_plugin.get_session(request, "analytics_db")
        users = await primary.select(
            """
            SELECT 1 AS id, 'Alice' AS name
            """
        )
        metrics = analytics.select(
            """
            SELECT 'active_users' AS name, 100 AS value
            """
        )
        return response.json({"users": users, "metrics": metrics})

    db_plugin.init_app(app)
    # --8<-- [end:example]
    return app


if __name__ == "__main__":
    print(main())
