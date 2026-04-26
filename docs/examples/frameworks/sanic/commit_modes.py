import pytest

__all__ = ("test_sanic_commit_modes",)


def test_sanic_commit_modes() -> None:
    pytest.importorskip("sanic")
    pytest.importorskip("aiosqlite")
    # start-example
    from sanic import Request, Sanic, response
    from sanic.response import HTTPResponse

    from sqlspec import SQLSpec
    from sqlspec.adapters.aiosqlite import AiosqliteConfig
    from sqlspec.extensions.sanic import SQLSpecPlugin

    sqlspec = SQLSpec()
    sqlspec.add_config(
        AiosqliteConfig(
            connection_config={"database": "app.db"},
            extension_config={
                "sanic": {"commit_mode": "autocommit", "extra_rollback_statuses": {409}, "session_key": "db"}
            },
        )
    )

    db_plugin = SQLSpecPlugin(sqlspec)
    app = Sanic("SQLSpecSanicCommitModesExample")

    @app.post("/users")
    async def create_user(request: Request) -> HTTPResponse:
        db = db_plugin.get_session(request, "db")
        await db.execute("insert into users (name) values (:name)", {"name": request.json["name"]})
        return response.json({"created": True}, status=201)

    db_plugin.init_app(app)
    # end-example

    assert app is not None
