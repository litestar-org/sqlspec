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
    from sqlspec.adapters.aiosqlite import AiosqliteConfig
    from sqlspec.extensions.starlette import SQLSpecPlugin

    sqlspec = SQLSpec()
    sqlspec.add_config(AiosqliteConfig(connection_config={"database": ":memory:"}))

    async def health(request: Request) -> JSONResponse:
        db = request.app.state.sqlspec.get_session(request)
        result = await db.execute("select 1 as ok")
        return JSONResponse(result.one())

    app = Starlette(routes=[Route("/health", health)])
    app.state.sqlspec = SQLSpecPlugin(sqlspec, app)
    # end-example

    assert app is not None
