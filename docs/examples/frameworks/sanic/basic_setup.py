import pytest

__all__ = ("test_sanic_basic_setup",)


def test_sanic_basic_setup() -> None:
    pytest.importorskip("sanic")
    pytest.importorskip("aiosqlite")
    # start-example
    from sanic import Request, Sanic, response
    from sanic.response import HTTPResponse

    from sqlspec import SQLSpec
    from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
    from sqlspec.extensions.sanic import SQLSpecPlugin

    sqlspec = SQLSpec()
    sqlspec.add_config(
        AiosqliteConfig(
            connection_config={"database": ":memory:"},
            extension_config={"sanic": {"commit_mode": "manual", "session_key": "db"}},
        )
    )

    db_plugin = SQLSpecPlugin(sqlspec)
    app = Sanic("SQLSpecSanicBasicExample")

    @app.get("/health")
    async def health(request: Request) -> HTTPResponse:
        db: AiosqliteDriver = db_plugin.get_session(request, "db")
        result = await db.execute("select 1 as ok")
        return response.json(result.one())

    db_plugin.init_app(app)
    # end-example

    assert app is not None
