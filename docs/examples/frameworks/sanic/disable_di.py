"""Manage Sanic connection pools directly on app.ctx with per-request DI disabled."""

from sanic import Request, Sanic, response
from sanic.response import HTTPResponse

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.extensions.sanic import SQLSpecPlugin

__all__ = ("main",)


def main() -> Sanic:
    """Create a Sanic application acquiring connections manually from app.ctx.db_pool."""
    spec = SQLSpec()
    config = AiosqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"sanic": {"disable_di": True, "pool_key": "db_pool"}},
    )
    spec.add_config(config)

    db_plugin = SQLSpecPlugin(spec)
    app = Sanic("SQLSpecSanicDisableDIExample")

    @app.get("/health")
    async def health(request: Request) -> HTTPResponse:
        async with config.provide_connection(request.app.ctx.db_pool) as connection:
            db = config.driver_type(connection=connection, statement_config=config.statement_config)
            return response.json(await db.select_one("SELECT 1 AS ok"))

    db_plugin.init_app(app)
    return app


if __name__ == "__main__":
    print(main())
