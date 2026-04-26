import pytest

__all__ = ("test_sanic_multi_database",)


def test_sanic_multi_database() -> None:
    pytest.importorskip("sanic")
    pytest.importorskip("aiosqlite")
    # start-example
    from sanic import Request, Sanic, response
    from sanic.response import HTTPResponse

    from sqlspec import SQLSpec
    from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
    from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
    from sqlspec.extensions.sanic import SQLSpecPlugin

    sqlspec = SQLSpec()
    sqlspec.add_config(
        AiosqliteConfig(
            bind_key="primary",
            connection_config={"database": "primary.db"},
            extension_config={
                "sanic": {
                    "connection_key": "primary_connection",
                    "pool_key": "primary_pool",
                    "session_key": "primary_db",
                }
            },
        )
    )
    sqlspec.add_config(
        SqliteConfig(
            bind_key="analytics",
            connection_config={"database": "analytics.db"},
            extension_config={
                "sanic": {
                    "connection_key": "analytics_connection",
                    "pool_key": "analytics_pool",
                    "session_key": "analytics_db",
                }
            },
        )
    )

    db_plugin = SQLSpecPlugin(sqlspec)
    app = Sanic("SQLSpecSanicMultiDatabaseExample")

    @app.get("/report")
    async def report(request: Request) -> HTTPResponse:
        primary: AiosqliteDriver = db_plugin.get_session(request, "primary_db")
        analytics: SqliteDriver = db_plugin.get_session(request, "analytics_db")
        users = await primary.select("select 1 as id, 'Alice' as name")
        metrics = analytics.select("select 'active_users' as name, 100 as value")
        return response.json({"users": users, "metrics": metrics})

    db_plugin.init_app(app)
    # end-example

    assert app is not None
