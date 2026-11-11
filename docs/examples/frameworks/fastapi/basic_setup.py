"""Register SQLSpecPlugin with FastAPI and inject an async database session."""

# --8<-- [start:example]
from typing import Annotated, Any

from fastapi import Depends, FastAPI

from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.extensions.fastapi import SQLSpecPlugin

app = FastAPI()
db_ext = SQLSpecPlugin(AiosqliteConfig(connection_config={"database": ":memory:"}), app)


@app.get("/teams")
async def list_teams(db: Annotated[AiosqliteDriver, Depends(db_ext.provide_session())]) -> dict[str, Any]:
    return await db.select_one("SELECT 1 AS ok")


# --8<-- [end:example]

__all__ = ("app", "db_ext", "list_teams", "main")


def main() -> FastAPI:
    """Create a FastAPI application with an injected AiosqliteDriver route."""
    return app


if __name__ == "__main__":
    print(main())
