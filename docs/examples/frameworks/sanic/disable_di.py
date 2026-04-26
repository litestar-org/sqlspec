import pytest

__all__ = ("test_sanic_disable_di",)


def test_sanic_disable_di() -> None:
    pytest.importorskip("sanic")
    pytest.importorskip("aiosqlite")
    # start-example
    from sanic import Request, Sanic, response
    from sanic.response import HTTPResponse

    from sqlspec import SQLSpec
    from sqlspec.adapters.aiosqlite import AiosqliteConfig
    from sqlspec.extensions.sanic import SQLSpecPlugin

    sqlspec = SQLSpec()
    config = AiosqliteConfig(
        connection_config={"database": "app.db"},
        extension_config={"sanic": {"disable_di": True, "pool_key": "db_pool"}},
    )
    sqlspec.add_config(config)

    db_plugin = SQLSpecPlugin(sqlspec)
    app = Sanic("SQLSpecSanicDisableDIExample")

    @app.get("/health")
    async def health(request: Request) -> HTTPResponse:
        async with config.provide_connection(request.app.ctx.db_pool) as connection:
            db = config.driver_type(connection=connection, statement_config=config.statement_config)
            result = await db.execute("select 1 as ok")
            return response.json(result.one())

    db_plugin.init_app(app)
    # end-example

    assert app is not None
