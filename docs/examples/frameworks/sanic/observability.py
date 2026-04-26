import pytest

__all__ = ("test_sanic_observability",)


def test_sanic_observability() -> None:
    pytest.importorskip("sanic")
    pytest.importorskip("aiosqlite")
    # start-example
    from sanic import Request, Sanic, response
    from sanic.response import HTTPResponse

    from sqlspec import SQLSpec
    from sqlspec.adapters.aiosqlite import AiosqliteConfig
    from sqlspec.core import StatementConfig
    from sqlspec.extensions.sanic import SQLSpecPlugin

    sqlspec = SQLSpec()
    sqlspec.add_config(
        AiosqliteConfig(
            connection_config={"database": "app.db"},
            statement_config=StatementConfig(enable_sqlcommenter=True),
            extension_config={
                "sanic": {
                    "enable_correlation_middleware": True,
                    "enable_sqlcommenter_middleware": True,
                    "session_key": "db",
                }
            },
        )
    )

    db_plugin = SQLSpecPlugin(sqlspec)
    app = Sanic("SQLSpecSanicObservabilityExample")

    @app.get("/health")
    async def health(request: Request) -> HTTPResponse:
        return response.json({"correlation_id": request.ctx.correlation_id})

    db_plugin.init_app(app)
    # end-example

    assert app is not None
