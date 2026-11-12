"""Register SQLSpecPlugin with a Sanic application and query an async SQLite session."""

# --8<-- [start:example]
from sanic import Request, Sanic, response
from sanic.response import HTTPResponse

from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.extensions.sanic import SQLSpecPlugin


def create_app() -> Sanic:
    """Create a Sanic application wired with SQLSpecPlugin."""
    app = Sanic("SQLSpecSanicBasicExample")
    db_plugin = SQLSpecPlugin(
        AiosqliteConfig(
            connection_config={"database": ":memory:"},
            extension_config={"sanic": {"commit_mode": "manual", "session_key": "db"}},
        ),
        app,
    )

    @app.get("/health")
    async def health(request: Request) -> HTTPResponse:
        db: AiosqliteDriver = db_plugin.get_session(request, "db")
        return response.json(await db.select_one("SELECT 1 AS ok"))

    return app


# --8<-- [end:example]

__all__ = ("create_app", "main")


def main() -> Sanic:
    """Create and return the configured Sanic application."""
    return create_app()


if __name__ == "__main__":
    print(main())
