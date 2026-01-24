from __future__ import annotations

import pytest

__all__ = ("test_starlette_basic_setup",)


def test_starlette_basic_setup() -> None:
    pytest.importorskip("starlette")
    pytest.importorskip("aiosqlite")
    # start-example
    from starlette.applications import Starlette
    from starlette.requests import Request
    from starlette.responses import JSONResponse
    from starlette.routing import Route

    from sqlspec import SQLSpec
    from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
    from sqlspec.extensions.starlette import SQLSpecPlugin

    sqlspec = SQLSpec()
    sqlspec.add_config(AiosqliteConfig(connection_config={"database": ":memory:"}))

    # Create plugin at module level
    db_plugin = SQLSpecPlugin(sqlspec)

    async def health(request: Request) -> JSONResponse:
        db: AiosqliteDriver = db_plugin.get_session(request)
        result = await db.execute("select 1 as ok")
        return JSONResponse(result.one())

    app = Starlette(routes=[Route("/health", health)])
    db_plugin.init_app(app)  # Initialize plugin with app
    # end-example

    assert app is not None
