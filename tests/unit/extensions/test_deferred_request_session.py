"""Synchronous session accessors on asynchronous configurations across the ASGI frameworks and Flask.

The ASGI plugins return a session that checks out the request connection on its first awaited call,
with the same tenant resolution and commit or rollback handling as the asynchronous accessors. Flask
returns a blocking session whose calls run on the plugin's portal.
"""

import json
import sqlite3
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.exceptions import ImproperConfigurationError
from tests.unit.extensions._tenant_configs import AsyncRecordingStrategy, TenantAiosqliteConfig

FRAMEWORKS = ["litestar", "fastapi", "starlette", "sanic"]


@dataclass
class Access:
    """Framework-neutral view of one request's SQLSpec accessors."""

    session: Callable[[], Any]
    connection: Callable[[], Any]
    receipt: Callable[[str], Any]


Body = Callable[[Access, int], Awaitable[Any]]


def _items_database(tmp_path: Path) -> str:
    database = str(tmp_path / "items.db")
    setup = sqlite3.connect(database)
    setup.execute("CREATE TABLE items (status INTEGER)")
    setup.commit()
    setup.close()
    return database


def _stored(database: str) -> list[int]:
    reader = sqlite3.connect(database)
    try:
        return [row[0] for row in reader.execute("SELECT status FROM items ORDER BY rowid")]
    finally:
        reader.close()


def _counting_config(database: str, framework: str, checkouts: list[int]) -> AiosqliteConfig:
    class CountingConfig(AiosqliteConfig):
        def provide_connection(self, *args: Any, **kwargs: Any) -> Any:
            checkouts.append(1)
            return super().provide_connection(*args, **kwargs)

    return CountingConfig(
        connection_config={"database": database}, extension_config={framework: {"commit_mode": "autocommit"}}
    )


@contextmanager
def _serve(framework: str, config: Any, bodies: "dict[str, Body]") -> Iterator[Callable[[str, int], tuple[int, Any]]]:
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

    from sqlspec.extensions.litestar import SQLSpecPlugin, set_connection_tenant

    @get("/{name:str}/{status:int}")
    async def route(request: Request[Any, Any, Any]) -> Response[Any]:
        name = request.path_params["name"]
        status = request.path_params["status"]
        access = Access(
            session=lambda: plugin.provide_request_session(config, request.app.state, request.scope),
            connection=lambda: plugin.provide_request_connection(config, request.app.state, request.scope),
            receipt=lambda tenant: set_connection_tenant(request, tenant),
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


@pytest.mark.parametrize("framework", FRAMEWORKS)
def test_sync_session_accessor_opens_async_connection_on_first_await(framework: str, tmp_path: Path) -> None:
    database = _items_database(tmp_path)
    checkouts: list[int] = []
    config = _counting_config(database, framework, checkouts)

    async def nodb(access: Access, status: int) -> Any:
        session = access.session()
        return {"dialect": session.dialect, "checkouts": len(checkouts)}

    async def write(access: Access, status: int) -> Any:
        session = access.session()
        before = len(checkouts)
        await session.begin()
        await session.execute("INSERT INTO items VALUES (:status)", {"status": status})
        again = access.session()
        return {
            "before": before,
            "after": len(checkouts),
            "shares_connection": again.connection is session.connection is access.connection(),
        }

    with _serve(framework, config, {"nodb": nodb, "write": write}) as run:
        assert run("nodb", 200) == (200, {"dialect": "sqlite", "checkouts": 0})
        assert checkouts == []
        assert run("write", 201) == (201, {"before": 0, "after": 1, "shares_connection": True})
        assert run("write", 500) == (500, {"before": 1, "after": 2, "shares_connection": True})
    assert _stored(database) == [201]


@pytest.mark.parametrize("framework", FRAMEWORKS)
def test_sync_session_accessor_streams_and_runs_transactions(framework: str, tmp_path: Path) -> None:
    database = _items_database(tmp_path)
    config = _counting_config(database, framework, [])

    async def stream(access: Access, status: int) -> Any:
        session = access.session()
        async with session.transaction():
            await session.execute("INSERT INTO items VALUES (:status)", {"status": 7})
        streamed = [row["status"] async for row in access.session().select_stream("SELECT status FROM items")]
        async with access.session().select_stream("SELECT status FROM items") as rows:
            entered = [row["status"] async for row in rows]
        return {"streamed": streamed, "entered": entered}

    with _serve(framework, config, {"stream": stream}) as run:
        assert run("stream", 200) == (200, {"streamed": [7], "entered": [7]})
    assert _stored(database) == [7]


@pytest.mark.parametrize("framework", FRAMEWORKS)
def test_sync_session_accessor_resolves_tenant_when_connection_opens(framework: str) -> None:
    strategy = AsyncRecordingStrategy()
    config = TenantAiosqliteConfig(
        connection_config={"database": ":memory:"}, driver_features={"tenant_strategy": strategy}
    )

    async def tenant(access: Access, status: int) -> Any:
        session = access.session()
        access.receipt("acme")
        return {"value": await session.select_value("SELECT 1")}

    with _serve(framework, config, {"tenant": tenant}) as run:
        assert run("tenant", 200) == (200, {"value": 1})
    assert strategy.checkouts == ["acme"]


@pytest.mark.parametrize("framework", FRAMEWORKS)
def test_connection_attributes_before_open_name_the_async_accessor(framework: str, tmp_path: Path) -> None:
    checkouts: list[int] = []
    config = _counting_config(_items_database(tmp_path), framework, checkouts)

    async def unopened(access: Access, status: int) -> Any:
        session = access.session()
        errors: list[str] = []
        reads: list[Callable[[], Any]] = [lambda: session.connection, access.connection]
        for read in reads:
            try:
                read()
            except ImproperConfigurationError as exc:
                errors.append(str(exc))
        await session.select_value("SELECT 1")
        return {"errors": errors, "open": session.connection is access.connection(), "checkouts": len(checkouts)}

    with _serve(framework, config, {"unopened": unopened}) as run:
        status, body = run("unopened", 200)
    assert status == 200
    assert body["open"] is True
    assert body["checkouts"] == 1
    session_error, connection_error = body["errors"]
    assert "_session_async(" in session_error
    assert "_connection_async(" in connection_error


def test_flask_session_on_async_configuration_blocks_through_the_portal(tmp_path: Path) -> None:
    from flask import Flask

    from sqlspec.extensions.flask import SQLSpecPlugin, set_request_tenant

    database = _items_database(tmp_path)
    checkouts: list[int] = []
    config = _counting_config(database, "flask", checkouts)
    strategy = AsyncRecordingStrategy()
    tenant_config = TenantAiosqliteConfig(
        connection_config={"database": ":memory:"},
        driver_features={"tenant_strategy": strategy},
        extension_config={"flask": {"session_key": "tenant_db", "connection_key": "tenant_conn", "pool_key": "tp"}},
    )
    app = Flask(__name__)
    plugin = SQLSpecPlugin([config, tenant_config], app)

    @app.get("/nodb")
    def nodb() -> Any:
        return {"checkouts": len(checkouts)}

    @app.get("/write/<int:status>")
    def write(status: int) -> Any:
        session = plugin.get_session()
        session.begin()
        session.execute("INSERT INTO items VALUES (:status)", {"status": status})
        set_request_tenant("acme")
        tenant_value = plugin.get_session("tenant_db").select_value("SELECT 1")
        return {"same": plugin.get_session() is session, "tenant": tenant_value}, status

    @app.get("/stream")
    def stream() -> Any:
        session = plugin.get_session()
        with session.transaction():
            session.execute("INSERT INTO items VALUES (:status)", {"status": 7})
        return {"rows": [row["status"] for row in session.select_stream("SELECT status FROM items")]}

    try:
        client = app.test_client()
        assert client.get("/nodb").json == {"checkouts": 0}
        ok = client.get("/write/201")
        assert ok.status_code == 201
        assert ok.json == {"same": True, "tenant": 1}
        assert client.get("/write/500").status_code == 500
        assert client.get("/stream").json == {"rows": [201, 7]}
    finally:
        plugin.shutdown()
    assert checkouts == [1, 1, 1]
    assert strategy.checkouts == ["acme", "acme"]
    assert _stored(database) == [201, 7]
