"""Tests for SQLCommenter framework middleware."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.extensions.sqlcommenter import SQLCommenterContext

# ── Starlette SQLCommenterMiddleware ──────────────────────────────────────


@pytest.mark.anyio
async def test_starlette_middleware_sets_context() -> None:
    from sqlspec.extensions.starlette.middleware import SQLCommenterMiddleware

    captured_attrs: dict[str, str] | None = None

    async def capture_next(request: object) -> MagicMock:
        nonlocal captured_attrs
        captured_attrs = SQLCommenterContext.get()
        resp = MagicMock()
        resp.status_code = 200
        return resp

    # Build a fake Starlette request with scope
    request = MagicMock()
    request.scope = {"type": "http", "path": "/api/users", "method": "GET"}
    request.url.path = "/api/users"

    app = MagicMock()
    middleware = SQLCommenterMiddleware(app, framework="starlette")
    await middleware.dispatch(request, capture_next)

    assert captured_attrs is not None
    assert captured_attrs["route"] == "/api/users"
    assert captured_attrs["framework"] == "starlette"


@pytest.mark.anyio
async def test_starlette_middleware_cleans_up_context() -> None:
    from sqlspec.extensions.starlette.middleware import SQLCommenterMiddleware

    async def fake_next(request: object) -> MagicMock:
        return MagicMock(status_code=200)

    request = MagicMock()
    request.scope = {"type": "http", "path": "/api/users"}
    request.url.path = "/api/users"

    app = MagicMock()
    middleware = SQLCommenterMiddleware(app, framework="starlette")
    await middleware.dispatch(request, fake_next)

    # Context should be cleaned up after request
    assert SQLCommenterContext.get() is None


@pytest.mark.anyio
async def test_starlette_middleware_restores_previous_context() -> None:
    from sqlspec.extensions.starlette.middleware import SQLCommenterMiddleware

    previous = {"route": "/outer"}
    SQLCommenterContext.set(previous)

    async def fake_next(request: object) -> MagicMock:
        return MagicMock(status_code=200)

    try:
        request = MagicMock()
        request.scope = {"type": "http", "path": "/inner"}
        request.url.path = "/inner"

        app = MagicMock()
        middleware = SQLCommenterMiddleware(app, framework="starlette")
        await middleware.dispatch(request, fake_next)

        # Should restore previous context
        assert SQLCommenterContext.get() is previous
    finally:
        SQLCommenterContext.set(None)


@pytest.mark.anyio
async def test_starlette_middleware_extracts_endpoint() -> None:
    from sqlspec.extensions.starlette.middleware import SQLCommenterMiddleware

    captured_attrs: dict[str, str] | None = None

    async def capture_next(request: object) -> MagicMock:
        nonlocal captured_attrs
        captured_attrs = SQLCommenterContext.get()
        return MagicMock(status_code=200)

    request = MagicMock()
    request.scope = {"type": "http", "path": "/api/users", "endpoint": MagicMock(__name__="list_users")}
    request.url.path = "/api/users"

    app = MagicMock()
    middleware = SQLCommenterMiddleware(app, framework="fastapi")
    await middleware.dispatch(request, capture_next)

    assert captured_attrs is not None
    assert captured_attrs["action"] == "list_users"
    assert captured_attrs["framework"] == "fastapi"


# ── Litestar SQLCommenterMiddleware ───────────────────────────────────────


@pytest.mark.anyio
async def test_litestar_middleware_sets_context() -> None:
    from sqlspec.extensions.litestar.plugin import SQLCommenterMiddleware

    captured_attrs: dict[str, str] | None = None

    async def mock_app(scope: dict, receive: object, send: object) -> None:
        nonlocal captured_attrs
        captured_attrs = SQLCommenterContext.get()

    handler = MagicMock()
    handler.fn.__name__ = "get_users"
    handler.owner = MagicMock()
    handler.owner.__name__ = "UserController"

    scope: dict = {"type": "http", "path": "/api/users", "route_handler": handler}

    middleware = SQLCommenterMiddleware(mock_app)
    await middleware(scope, AsyncMock(), AsyncMock())

    assert captured_attrs is not None
    assert captured_attrs["route"] == "/api/users"
    assert captured_attrs["action"] == "get_users"
    assert captured_attrs["controller"] == "UserController"
    assert captured_attrs["framework"] == "litestar"


@pytest.mark.anyio
async def test_litestar_middleware_cleans_up_context() -> None:
    from sqlspec.extensions.litestar.plugin import SQLCommenterMiddleware

    async def mock_app(scope: dict, receive: object, send: object) -> None:
        pass

    scope: dict = {"type": "http", "path": "/test"}
    middleware = SQLCommenterMiddleware(mock_app)
    await middleware(scope, AsyncMock(), AsyncMock())

    assert SQLCommenterContext.get() is None


@pytest.mark.anyio
async def test_litestar_middleware_passes_non_http() -> None:
    from sqlspec.extensions.litestar.plugin import SQLCommenterMiddleware

    called = False

    async def mock_app(scope: dict, receive: object, send: object) -> None:
        nonlocal called
        called = True

    scope: dict = {"type": "websocket", "path": "/ws"}
    middleware = SQLCommenterMiddleware(mock_app)
    await middleware(scope, AsyncMock(), AsyncMock())

    assert called
    assert SQLCommenterContext.get() is None
