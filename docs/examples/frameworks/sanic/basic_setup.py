"""Register SQLSpecPlugin with a Sanic application and query an async SQLite session."""

from sanic import Request, Sanic, response
from sanic.response import HTTPResponse

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.extensions.sanic import SQLSpecPlugin

__all__ = ("main",)


def main() -> Sanic:
    """Create a Sanic application wired with SQLSpecPlugin."""
    spec = SQLSpec()
    spec.add_config(
        AiosqliteConfig(
            connection_config={"database": ":memory:"},
            extension_config={"sanic": {"commit_mode": "manual", "session_key": "db"}},
        )
    )

    db_plugin = SQLSpecPlugin(spec)
    app = Sanic("SQLSpecSanicBasicExample")

    @app.get("/health")
    async def health(request: Request) -> HTTPResponse:
        db: AiosqliteDriver = db_plugin.get_session(request, "db")
        return response.json(await db.select_one("SELECT 1 AS ok"))

    db_plugin.init_app(app)
    return app


if __name__ == "__main__":
    print(main())
