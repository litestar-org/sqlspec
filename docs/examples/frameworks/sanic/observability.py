"""Enable request correlation IDs and SQLCommenter trace context middleware in Sanic."""

# --8<-- [start:example]
from sanic import Request, Sanic, response
from sanic.response import HTTPResponse

from sqlspec import SQLSpec, StatementConfig
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.extensions.sanic import SQLSpecPlugin


def create_app() -> Sanic:
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


# --8<-- [end:example]

__all__ = ("create_app", "main")


def main() -> Sanic:
    """Create and return the observability-enabled Sanic application."""
    return create_app()


if __name__ == "__main__":
    print(main())
