"""Enable request correlation IDs and SQLCommenter trace context middleware in Sanic."""

from sanic import Request, Sanic, response
from sanic.response import HTTPResponse

from sqlspec import SQLSpec, StatementConfig
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.extensions.sanic import SQLSpecPlugin

__all__ = ("main",)


def main() -> Sanic:
    """Create a Sanic application with correlation and SQLCommenter middleware enabled."""
    spec = SQLSpec()
    spec.add_config(
        AiosqliteConfig(
            connection_config={"database": ":memory:"},
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

    db_plugin = SQLSpecPlugin(spec)
    app = Sanic("SQLSpecSanicObservabilityExample")

    @app.get("/health")
    async def health(request: Request) -> HTTPResponse:
        return response.json({"correlation_id": request.ctx.correlation_id})

    db_plugin.init_app(app)
    return app


if __name__ == "__main__":
    print(main())
