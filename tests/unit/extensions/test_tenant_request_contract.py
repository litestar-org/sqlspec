"""One request tenant contract across Litestar, FastAPI, Starlette, Flask, and Sanic.

Precedence is explicit call arguments, then a scope entered inside the handler, then route
resolution (route bypass, request receipt, extractor), then an ambient scope bound outside the
request. Resolution is lazy, memoized per configuration, and finished before the response ends.
"""

import json
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Annotated, Any, cast
from uuid import uuid4

import pytest

from sqlspec.exceptions import TenantContextError
from sqlspec.utils.tenant import SQLSpecTenantMiddleware, TenantContext, TenantContextPayload, tenant_bypass
from tests.unit.extensions._tenant_configs import (
    AsyncRecordingStrategy,
    SyncRecordingStrategy,
    TenantAiosqliteConfig,
    TenantSqliteConfig,
)

FRAMEWORKS = ["litestar", "fastapi", "starlette", "flask", "sanic"]
ASGI_FRAMEWORKS = ["litestar", "fastapi", "starlette"]


@dataclass
class Harness:
    """A running application plus the observations its routes record."""

    request: Callable[[str, dict[str, str]], tuple[int, Any]]
    checkouts: list[str]
    extractor_calls: list[str] = field(default_factory=list)
    observed: dict[str, Any] = field(default_factory=dict)


def _scope_header(scope: Any, name: str) -> str | None:
    for key, value in scope.get("headers", ()):
        if key.decode("latin-1").lower() == name:
            return str(value.decode("latin-1"))
    return None


def _async_config(extractor: Any, *, tenant: bool = True, framework: str) -> tuple[Any, AsyncRecordingStrategy]:
    strategy = AsyncRecordingStrategy()
    features: dict[str, Any] = {"tenant_strategy": strategy} if tenant else {}
    settings: dict[str, Any] = {"tenant_extractor": extractor} if tenant else {}
    config = TenantAiosqliteConfig(
        connection_config={"database": ":memory:"}, driver_features=features, extension_config={framework: settings}
    )
    return config, strategy


def _asgi_user_middleware(app: Any) -> Any:
    async def middleware(scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] in {"http", "websocket"} and _scope_header(scope, "x-user"):
            scope["user"] = type("User", (), {"is_authenticated": True})()
        await app(scope, receive, send)

    return middleware


def _make_extractor(calls: list[str], read: Callable[[Any], Any], *, asynchronous: bool = False) -> Any:
    """Build an extractor that records calls and maps ``__invalid__`` to an unsupported value."""

    def extract(source: Any) -> Any:
        calls.append("extract")
        value = read(source)
        return 123 if value == "__invalid__" else value

    async def extract_async(source: Any) -> Any:
        return extract(source)

    return extract_async if asynchronous else extract


def _ambient_extractor(scope: Any) -> str | None:
    return _scope_header(scope, "x-ambient")


@contextmanager
def _litestar(tenant: bool, asynchronous: bool) -> Iterator[Harness]:
    from litestar import Litestar, get
    from litestar.background_tasks import BackgroundTask
    from litestar.middleware import DefineMiddleware
    from litestar.params import Dependency
    from litestar.response import Response, Stream
    from litestar.testing import TestClient

    from sqlspec.extensions.litestar import SQLSpecPlugin, set_connection_tenant

    calls: list[str] = []

    extractor = _make_extractor(calls, lambda scope: _scope_header(scope, "x-tenant"), asynchronous=asynchronous)
    config, strategy = _async_config(extractor, tenant=tenant, framework="litestar")
    harness = Harness(request=lambda path, headers: (0, None), checkouts=strategy.checkouts, extractor_calls=calls)

    async def direct() -> None:
        async with config.provide_session() as session:
            await session.select_value("SELECT 1")

    @get("/db")
    async def db(db_session: Any = Dependency(skip_validation=True)) -> dict[str, bool]:
        await db_session.select_value("SELECT 1")
        await direct()
        return {"ok": True}

    @get("/receipt")
    async def receipt(request: Any) -> dict[str, bool]:
        set_connection_tenant(request, "receipt")
        await direct()
        return {"ok": True}

    @get("/bypass")
    @tenant_bypass
    async def bypass() -> dict[str, bool]:
        await direct()
        return {"ok": True}

    @get("/scoped")
    async def scoped() -> dict[str, bool]:
        with TenantContext.scope("handler"):
            await direct()
        return {"ok": True}

    @get("/explicit")
    async def explicit() -> dict[str, bool]:
        with TenantContext.scope("handler"):
            async with config.provide_session(tenant="explicit") as session:
                await session.select_value("SELECT 1")
        return {"ok": True}

    @get("/nodb")
    async def nodb() -> dict[str, Any]:
        return {"binding": type(TenantContext.current_binding()).__name__, "calls": len(calls)}

    @get("/stream")
    async def stream() -> Stream:
        async def body() -> AsyncIterator[bytes]:
            await direct()
            yield b"done"

        return Stream(body())

    @get("/background")
    async def background() -> Response[dict[str, bool]]:
        return Response({"ok": True}, background=BackgroundTask(direct))

    app = Litestar(
        [db, receipt, bypass, scoped, explicit, nodb, stream, background],
        plugins=[SQLSpecPlugin(config)],
        middleware=[
            _asgi_user_middleware,
            DefineMiddleware(cast("Any", SQLSpecTenantMiddleware), extractor=_ambient_extractor),
        ],
        openapi_config=None,
    )
    with TestClient(app=app, raise_server_exceptions=False) as client:

        def request(path: str, headers: dict[str, str]) -> tuple[int, Any]:
            response = client.get(path, headers=headers)
            return response.status_code, response.text

        harness.request = request
        yield harness


def _starlette_family(framework: str, tenant: bool, asynchronous: bool) -> Any:
    @contextmanager
    def build() -> Iterator[Harness]:
        from starlette.background import BackgroundTask
        from starlette.middleware import Middleware
        from starlette.requests import Request
        from starlette.responses import JSONResponse, StreamingResponse
        from starlette.testclient import TestClient

        calls: list[str] = []

        extractor = _make_extractor(calls, lambda scope: _scope_header(scope, "x-tenant"), asynchronous=asynchronous)

        config, strategy = _async_config(extractor, tenant=tenant, framework=framework)
        harness = Harness(request=lambda path, headers: (0, None), checkouts=strategy.checkouts, extractor_calls=calls)

        async def direct() -> None:
            async with config.provide_session() as session:
                await session.select_value("SELECT 1")

        middleware = [
            Middleware(_asgi_user_middleware),
            Middleware(SQLSpecTenantMiddleware, extractor=_ambient_extractor),
        ]
        if framework == "fastapi":
            from fastapi import Depends, FastAPI

            from sqlspec.extensions.fastapi import SQLSpecPlugin as FastAPIPlugin
            from sqlspec.extensions.fastapi import set_request_tenant

            fastapi_app = FastAPI(middleware=middleware)
            plugin: Any = FastAPIPlugin(config, fastapi_app)

            @fastapi_app.get("/db")
            async def fastapi_db(session: Annotated[Any, Depends(plugin.provide_session())]) -> dict[str, bool]:
                await session.select_value("SELECT 1")
                await direct()
                return {"ok": True}

            app: Any = fastapi_app
        else:
            from starlette.applications import Starlette

            from sqlspec.extensions.starlette import SQLSpecPlugin as StarlettePlugin
            from sqlspec.extensions.starlette import set_request_tenant

            app = Starlette(middleware=middleware)
            plugin = StarlettePlugin(config, app)

            async def starlette_db(request: Request) -> JSONResponse:
                session = await plugin.get_session_async(request)
                await session.select_value("SELECT 1")
                await direct()
                return JSONResponse({"ok": True})

            app.add_route("/db", starlette_db)

        async def receipt(request: Request) -> JSONResponse:
            set_request_tenant(request, "receipt")
            await direct()
            return JSONResponse({"ok": True})

        @tenant_bypass
        async def bypass(request: Request) -> JSONResponse:
            await direct()
            return JSONResponse({"ok": True})

        async def scoped(request: Request) -> JSONResponse:
            with TenantContext.scope("handler"):
                await direct()
            return JSONResponse({"ok": True})

        async def explicit(request: Request) -> JSONResponse:
            with TenantContext.scope("handler"):
                async with config.provide_session(tenant="explicit") as session:
                    await session.select_value("SELECT 1")
            return JSONResponse({"ok": True})

        async def nodb(request: Request) -> JSONResponse:
            return JSONResponse({"binding": type(TenantContext.current_binding()).__name__, "calls": len(calls)})

        async def stream(request: Request) -> StreamingResponse:
            async def body() -> AsyncIterator[bytes]:
                await direct()
                yield b"done"

            return StreamingResponse(body())

        async def background(request: Request) -> JSONResponse:
            return JSONResponse({"ok": True}, background=BackgroundTask(direct))

        for path, endpoint in (
            ("/receipt", receipt),
            ("/bypass", bypass),
            ("/scoped", scoped),
            ("/explicit", explicit),
            ("/nodb", nodb),
            ("/stream", stream),
            ("/background", background),
        ):
            app.add_route(path, endpoint)

        with TestClient(app, raise_server_exceptions=False) as client:

            def request(path: str, headers: dict[str, str]) -> tuple[int, Any]:
                response = client.get(path, headers=headers)
                return response.status_code, response.text

            harness.request = request
            yield harness

    return build()


@contextmanager
def _flask(tenant: bool) -> Iterator[Harness]:
    from flask import Flask, Response, g, request, stream_with_context

    from sqlspec.extensions.flask import SQLSpecPlugin, set_request_tenant

    calls: list[str] = []

    extractor = _make_extractor(calls, lambda flask_request: flask_request.headers.get("x-tenant"))

    strategy = SyncRecordingStrategy()
    config = TenantSqliteConfig(
        connection_config={"database": ":memory:"},
        driver_features={"tenant_strategy": strategy} if tenant else {},
        extension_config={"flask": {"tenant_extractor": extractor} if tenant else {}},
    )
    app = Flask(f"tenant_contract_{uuid4().hex}")

    @app.before_request
    def outer_request_state() -> None:
        if request.headers.get("x-user"):
            g.user = type("User", (), {"is_authenticated": True})()
        ambient = request.headers.get("x-ambient")
        g.ambient_token = TenantContext.set(TenantContextPayload(ambient) if ambient else None)

    @app.teardown_appcontext
    def restore_outer_state(_exc: Any) -> None:
        token = g.pop("ambient_token", None)
        if token is not None:
            TenantContext.reset(token)

    plugin = SQLSpecPlugin(config, app)

    def direct() -> None:
        with config.provide_session() as session:
            session.select_value("SELECT 1")

    @app.get("/db")
    def db() -> dict[str, bool]:
        plugin.get_session().select_value("SELECT 1")
        direct()
        return {"ok": True}

    @app.get("/receipt")
    def receipt() -> dict[str, bool]:
        set_request_tenant("receipt")
        direct()
        return {"ok": True}

    @app.get("/bypass")
    @tenant_bypass
    def bypass() -> dict[str, bool]:
        direct()
        return {"ok": True}

    @app.get("/scoped")
    def scoped() -> dict[str, bool]:
        with TenantContext.scope("handler"):
            direct()
        return {"ok": True}

    @app.get("/explicit")
    def explicit() -> dict[str, bool]:
        with TenantContext.scope("handler"), config.provide_session(tenant="explicit") as session:
            session.select_value("SELECT 1")
        return {"ok": True}

    @app.get("/nodb")
    def nodb() -> dict[str, Any]:
        return {"binding": type(TenantContext.current_binding()).__name__, "calls": len(calls)}

    @app.get("/stream")
    def stream() -> Response:
        def body() -> Iterator[bytes]:
            direct()
            yield b"done"

        return Response(stream_with_context(body()))

    client = app.test_client()

    def run(path: str, headers: dict[str, str]) -> tuple[int, Any]:
        response = client.get(path, headers=headers)
        return response.status_code, response.get_data(as_text=True)

    try:
        yield Harness(request=run, checkouts=strategy.checkouts, extractor_calls=calls)
    finally:
        plugin.shutdown()


@contextmanager
def _sanic(tenant: bool, asynchronous: bool) -> Iterator[Harness]:
    from sanic import Sanic, response

    from sqlspec.extensions.sanic import SQLSpecPlugin, set_request_tenant

    calls: list[str] = []

    extractor = _make_extractor(
        calls, lambda sanic_request: sanic_request.headers.get("x-tenant"), asynchronous=asynchronous
    )

    config, strategy = _async_config(extractor, tenant=tenant, framework="sanic")
    app = Sanic(f"TenantContract{uuid4().hex}")
    app.config.USE_UVLOOP = False

    async def outer_request_state(request: Any) -> None:
        if request.headers.get("x-user"):
            request.ctx.user = type("User", (), {"is_authenticated": True})()
        ambient = request.headers.get("x-ambient")
        request.ctx.ambient_token = TenantContext.set(TenantContextPayload(ambient) if ambient else None)

    app.register_middleware(outer_request_state, "request")
    plugin = SQLSpecPlugin(config, app)

    async def direct() -> None:
        async with config.provide_session() as session:
            await session.select_value("SELECT 1")

    async def db(request: Any) -> Any:
        session = await plugin.get_session_async(request)
        await session.select_value("SELECT 1")
        await direct()
        return response.json({"ok": True})

    async def receipt(request: Any) -> Any:
        set_request_tenant(request, "receipt")
        await direct()
        return response.json({"ok": True})

    @tenant_bypass
    async def bypass(request: Any) -> Any:
        await direct()
        return response.json({"ok": True})

    async def scoped(request: Any) -> Any:
        with TenantContext.scope("handler"):
            await direct()
        return response.json({"ok": True})

    async def explicit(request: Any) -> Any:
        with TenantContext.scope("handler"):
            async with config.provide_session(tenant="explicit") as session:
                await session.select_value("SELECT 1")
        return response.json({"ok": True})

    async def nodb(request: Any) -> Any:
        return response.json({"binding": type(TenantContext.current_binding()).__name__, "calls": len(calls)})

    async def stream(request: Any) -> None:
        stream_response = await request.respond()
        await direct()
        await stream_response.send(b"done")
        await stream_response.eof()

    for path, handler in (
        ("/db", db),
        ("/receipt", receipt),
        ("/bypass", bypass),
        ("/scoped", scoped),
        ("/explicit", explicit),
        ("/nodb", nodb),
        ("/stream", stream),
    ):
        app.add_route(handler, path, name=path.strip("/"))

    def run(path: str, headers: dict[str, str]) -> tuple[int, Any]:
        _, result = app.test_client.get(path, headers=headers)
        return result.status, result.text

    try:
        yield Harness(request=run, checkouts=strategy.checkouts, extractor_calls=calls)
    finally:
        Sanic.unregister_app(app)


def _harness(framework: str, *, tenant: bool = True, asynchronous: bool = False) -> Any:
    if framework == "litestar":
        return _litestar(tenant, asynchronous)
    if framework in {"fastapi", "starlette"}:
        return _starlette_family(framework, tenant, asynchronous)
    if framework == "flask":
        return _flask(tenant)
    return _sanic(tenant, asynchronous)


@pytest.mark.parametrize(
    ("path", "expected"),
    [("/db", "ambient"), ("/receipt", "receipt"), ("/bypass", "<bypass>"), ("/db", "extracted")],
    ids=["none", "receipt", "bypass_route", "extractor"],
)
@pytest.mark.parametrize("framework", FRAMEWORKS)
def test_route_resolution_precedes_ambient_scope(framework: str, path: str, expected: str) -> None:
    headers = {"x-ambient": "ambient"}
    if expected != "ambient":
        headers["x-tenant"] = "extracted"
    with _harness(framework) as harness:
        status, _ = harness.request(path, headers)
        assert status == 200
        assert set(harness.checkouts) == {expected}


@pytest.mark.parametrize(("path", "expected"), [("/scoped", "handler"), ("/explicit", "explicit")])
@pytest.mark.parametrize("framework", FRAMEWORKS)
def test_handler_scope_and_explicit_arguments_precede_route_resolution(
    framework: str, path: str, expected: str
) -> None:
    with _harness(framework) as harness:
        status, _ = harness.request(path, {"x-tenant": "extracted", "x-ambient": "ambient"})
        assert status == 200
        assert harness.checkouts == [expected]


@pytest.mark.parametrize(("headers", "status"), [({}, 401), ({"x-user": "1"}, 403)])
@pytest.mark.parametrize("framework", FRAMEWORKS)
def test_missing_tenant_returns_framework_response(framework: str, headers: dict[str, str], status: int) -> None:
    with _harness(framework) as harness:
        assert harness.request("/db", headers)[0] == status
        assert harness.checkouts == []


@pytest.mark.parametrize("framework", FRAMEWORKS)
def test_resolution_is_lazy_and_memoized_per_request(framework: str) -> None:
    with _harness(framework) as harness:
        status, body = harness.request("/nodb", {"x-tenant": "a"})
        assert (status, json.loads(body)) == (200, {"binding": "TenantRequestBinding", "calls": 0})
        assert harness.extractor_calls == ["extract"]
        harness.extractor_calls.clear()
        assert harness.request("/db", {"x-tenant": "a"})[0] == 200
        assert harness.checkouts == ["a", "a"]
        assert harness.extractor_calls == ["extract"]


@pytest.mark.parametrize("framework", FRAMEWORKS)
def test_streaming_body_keeps_request_tenant(framework: str) -> None:
    with _harness(framework) as harness:
        status, body = harness.request("/stream", {"x-tenant": "streamed"})
        assert (status, body) == (200, "done")
        assert harness.checkouts == ["streamed"]


@pytest.mark.parametrize("framework", ASGI_FRAMEWORKS)
def test_background_task_uses_tenant_resolved_at_handoff(framework: str) -> None:
    with _harness(framework) as harness:
        assert harness.request("/background", {"x-tenant": "handoff"})[0] == 200
        assert harness.checkouts == ["handoff"]
        assert harness.extractor_calls == ["extract"]


@pytest.mark.parametrize("framework", FRAMEWORKS)
def test_binding_is_installed_only_for_tenant_configurations(framework: str) -> None:
    with _harness(framework, tenant=False) as harness:
        status, body = harness.request("/nodb", {})
        assert (status, json.loads(body)["binding"]) == (200, "NoneType")


async def test_missing_tenant_outside_a_request_raises() -> None:
    config, _ = _async_config(None, framework="starlette")
    with pytest.raises(TenantContextError):
        async with config.provide_session():
            pass


@pytest.mark.parametrize("framework", ["litestar", "fastapi", "starlette", "sanic"])
def test_async_extractor_resolves_on_first_use(framework: str) -> None:
    with _harness(framework, asynchronous=True) as harness:
        assert harness.request("/db", {"x-tenant": "async"})[0] == 200
        assert harness.checkouts == ["async", "async"]
        assert harness.extractor_calls == ["extract"]


@pytest.mark.parametrize("framework", FRAMEWORKS)
def test_invalid_extractor_result_fails_before_checkout(framework: str) -> None:
    with _harness(framework) as harness:
        assert harness.request("/db", {"x-tenant": "__invalid__"})[0] == 500
        assert harness.checkouts == []
