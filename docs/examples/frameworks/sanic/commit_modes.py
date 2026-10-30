"""Configure Sanic autocommit mode with custom HTTP status codes triggering rollback."""

from sanic import Request, Sanic, response
from sanic.response import HTTPResponse

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.extensions.sanic import SQLSpecPlugin

__all__ = ("main",)


def main() -> Sanic:
    """Create a Sanic application rolling back automatically on HTTP 409 responses."""
    # --8<-- [start:example]
    spec = SQLSpec()
    spec.add_config(
        AiosqliteConfig(
            connection_config={"database": ":memory:"},
            extension_config={
                "sanic": {"commit_mode": "autocommit", "extra_rollback_statuses": {409}, "session_key": "db"}
            },
        )
    )

    db_plugin = SQLSpecPlugin(spec)
    app = Sanic("SQLSpecSanicCommitModesExample")

    @app.post("/users")
    async def create_user(request: Request) -> HTTPResponse:
        db = db_plugin.get_session(request, "db")
        await db.execute(
            """
            INSERT INTO users (name)
            VALUES (:name)
            """,
            name=request.json["name"],
        )
        return response.json({"created": True}, status=201)

    db_plugin.init_app(app)
    # --8<-- [end:example]
    return app


if __name__ == "__main__":
    print(main())
