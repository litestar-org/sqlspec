from typing import Annotated, Any

import pytest

__all__ = ("test_fastapi_basic_setup",)


def test_fastapi_basic_setup() -> None:
    pytest.importorskip("fastapi")
    pytest.importorskip("aiosqlite")
    # start-example
    from fastapi import Depends, FastAPI

    from sqlspec import SQLSpec
    from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
    from sqlspec.extensions.fastapi import SQLSpecPlugin

    sqlspec = SQLSpec()
    sqlspec.add_config(AiosqliteConfig(connection_config={"database": ":memory:"}))

    app = FastAPI()
    db_ext = SQLSpecPlugin(sqlspec, app)

    @app.get("/teams")
    async def list_teams(db: Annotated[AiosqliteDriver, Depends(db_ext.provide_session())]) -> dict[str, Any]:
        result = await db.execute("select 1 as ok")
        return result.one()

    # end-example

    assert app is not None
