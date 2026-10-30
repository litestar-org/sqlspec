"""Register SQLSpecPlugin with FastAPI and inject an async database session."""

from typing import Annotated, Any

from fastapi import Depends, FastAPI

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.extensions.fastapi import SQLSpecPlugin

__all__ = ("main",)


def main() -> FastAPI:
    """Create a FastAPI application with an injected AiosqliteDriver route."""
    spec = SQLSpec()
    spec.add_config(AiosqliteConfig(connection_config={"database": ":memory:"}))

    app = FastAPI()
    db_ext = SQLSpecPlugin(spec, app)

    @app.get("/teams")
    async def list_teams(db: Annotated[AiosqliteDriver, Depends(db_ext.provide_session())]) -> dict[str, Any]:
        return await db.select_one("SELECT 1 AS ok")

    return app


if __name__ == "__main__":
    print(main())
