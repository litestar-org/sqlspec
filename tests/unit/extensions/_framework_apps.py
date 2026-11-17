"""Framework test apps that expose SQLSpec request accessors through one neutral interface."""

import json
from collections.abc import Awaitable, Callable, Generator, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

__all__ = ("FRAMEWORKS", "Access", "Body", "serve")

FRAMEWORKS = ["litestar", "fastapi", "starlette", "sanic"]


@dataclass
class Access:
    """Framework-neutral view of one request's SQLSpec accessors."""

    session: Callable[[], Any]
    connection: Callable[[], Any]
    receipt: Callable[[str], Any]


Body = Callable[[Access, int], Awaitable[Any]]


@contextmanager
def serve(
    framework: str, config: Any, bodies: "dict[str, Body]"
) -> Generator[Callable[[str, int], tuple[int, Any]], None, None]:
    """Serve ``/<name>/<status>`` routes that run ``bodies[name]`` with the framework's accessors.

    Yields:
        A callable issuing one GET request and returning its status code and decoded JSON body.
    """
    if framework == "litestar":
        yield from _serve_litestar(config, bodies)
    elif framework == "sanic":
        yield from _serve_sanic(config, bodies)
    else:
        yield from _serve_starlette(framework, config, bodies)


def _serve_litestar(config: Any, bodies: "dict[str, Body]") -> Iterator[Callable[[str, int], tuple[int, Any]]]:
    from litestar import Litestar, Request, get
    from litestar.response import Response
    from litestar.testing import TestClient

    from sqlspec.extensions.litestar import SQLSpecPlugin, set_request_tenant

    @get("/{name:str}/{status:int}")
    async def route(request: Request[Any, Any, Any]) -> Response[Any]:
        name = request.path_params["name"]
        status = request.path_params["status"]
        access = Access(
            session=lambda: plugin.provide_db_session(config, request.app.state, request.scope),
            connection=lambda: plugin.provide_db_connection(config, request.app.state, request.scope),
            receipt=lambda tenant: set_request_tenant(request, tenant),
        )
        return Response(await bodies[name](access, status), status_code=status)

    plugin = SQLSpecPlugin(config)
    app = Litestar(route_handlers=[route], plugins=[plugin])
    with TestClient(app) as client:

        def run(name: str, status: int) -> tuple[int, Any]:
            response = client.get(f"/{name}/{status}")
            return response.status_code, response.json()

        yield run


def _serve_starlette(
    framework: str, config: Any, bodies: "dict[str, Body]"
) -> Iterator[Callable[[str, int], tuple[int, Any]]]:
    from starlette.requests import Request
    from starlette.responses import JSONResponse
    from starlette.routing import Route
    from starlette.testclient import TestClient

    async def route(request: Request) -> JSONResponse:
        access = Access(
            session=lambda: plugin.get_session(request),
            connection=lambda: plugin.get_connection(request),
            receipt=lambda tenant: set_request_tenant(request, tenant),
        )
        status = int(request.path_params["status"])
        return JSONResponse(await bodies[request.path_params["name"]](access, status), status_code=status)

    routes: list[Any] = [Route("/{name}/{status:int}", route)]
    plugin: Any
    if framework == "fastapi":
        from fastapi import FastAPI

        from sqlspec.extensions.fastapi import SQLSpecPlugin as FastAPIPlugin
        from sqlspec.extensions.fastapi import set_request_tenant

        app: Any = FastAPI(routes=routes)
        plugin = FastAPIPlugin(config, app)
    else:
        from starlette.applications import Starlette

        from sqlspec.extensions.starlette import SQLSpecPlugin, set_request_tenant

        app = Starlette(routes=routes)
        plugin = SQLSpecPlugin(config, app)
    with TestClient(app, raise_server_exceptions=False) as client:

        def run(name: str, status: int) -> tuple[int, Any]:
            response = client.get(f"/{name}/{status}")
            return response.status_code, response.json()

        yield run


def _serve_sanic(config: Any, bodies: "dict[str, Body]") -> Iterator[Callable[[str, int], tuple[int, Any]]]:
    from sanic import Sanic, response

    from sqlspec.extensions.sanic import SQLSpecPlugin, set_request_tenant

    app = Sanic(f"DeferredSession{uuid4().hex}")
    app.config.USE_UVLOOP = False
    plugin = SQLSpecPlugin(config, app)

    async def route(request: Any, name: str, status: int) -> Any:
        access = Access(
            session=lambda: plugin.get_session(request),
            connection=lambda: plugin.get_connection(request),
            receipt=lambda tenant: set_request_tenant(request, tenant),
        )
        return response.json(await bodies[name](access, status), status=status)

    app.add_route(route, "/<name:str>/<status:int>")

    def run(name: str, status: int) -> tuple[int, Any]:
        _, result = app.test_client.get(f"/{name}/{status}")
        return result.status, json.loads(result.text)

    try:
        yield run
    finally:
        Sanic.unregister_app(app)
