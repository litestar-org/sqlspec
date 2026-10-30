"""Attach SQLSpecPlugin to a Starlette application and query an async SQLite session."""

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.extensions.starlette import SQLSpecPlugin

__all__ = ("main",)


def main() -> Starlette:
    """Create a Starlette application initialized with SQLSpecPlugin."""
    # --8<-- [start:example]
    spec = SQLSpec()
    spec.add_config(AiosqliteConfig(connection_config={"database": ":memory:"}))
    db_plugin = SQLSpecPlugin(spec)

    async def health(request: Request) -> JSONResponse:
        db: AiosqliteDriver = db_plugin.get_session(request)
        return JSONResponse(await db.select_one("SELECT 1 AS ok"))

    app = Starlette(routes=[Route("/health", health)])
    db_plugin.init_app(app)
    # --8<-- [end:example]
    return app


if __name__ == "__main__":
    print(main())
