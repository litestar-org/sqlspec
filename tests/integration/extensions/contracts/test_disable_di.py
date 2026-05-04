# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false
"""Cross-framework disable-di contract tests."""

import tempfile
from collections.abc import Callable
from typing import Any

import pytest
from flask import Flask, g
from litestar import Litestar, Request, get
from litestar.testing import TestClient as LitestarTestClient
from starlette.applications import Starlette
from starlette.requests import Request as StarletteRequest
from starlette.responses import JSONResponse, Response
from starlette.routing import Route
from starlette.testclient import TestClient as StarletteTestClient

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.base import SQLSpec
from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.extensions.flask import SQLSpecPlugin as FlaskSQLSpecPlugin
from sqlspec.extensions.litestar import SQLSpecPlugin as LitestarSQLSpecPlugin
from sqlspec.extensions.starlette import SQLSpecPlugin as StarletteSQLSpecPlugin

pytestmark = pytest.mark.xdist_group("sqlite")


def _assert_flask_disable_di(database: str) -> None:
    sql = SQLSpec()
    config = SqliteConfig(connection_config={"database": database}, extension_config={"flask": {"disable_di": True}})
    sql.add_config(config)

    app = Flask(__name__)
    FlaskSQLSpecPlugin(sql, app)

    @app.route("/test")
    def flask_route() -> dict[str, Any]:
        pool = config.create_pool()
        with config.provide_connection(pool) as connection:
            session = config.driver_type(connection=connection, statement_config=config.statement_config)
            result = session.execute("SELECT 1 as value")
            data = result.get_first()
            assert data is not None
            config.close_pool()
            return {"value": data["value"]}

    @app.route("/check_g")
    def check_g() -> dict[str, bool]:
        return {"has_connection": hasattr(g, "sqlspec_connection_db_session")}

    with app.test_client() as client:
        response = client.get("/test")
        assert response.status_code == 200
        assert response.json == {"value": 1}

        response = client.get("/check_g")
        assert response.status_code == 200
        assert response.json == {"has_connection": False}


def _assert_starlette_disable_di(database: str) -> None:
    config = AiosqliteConfig(
        connection_config={"database": database}, extension_config={"starlette": {"disable_di": True}}
    )
    sql = SQLSpec()
    sql.add_config(config)
    plugin = StarletteSQLSpecPlugin(sql)

    async def starlette_route(request: StarletteRequest) -> Response:
        pool = await config.create_pool()
        async with config.provide_connection(pool) as connection:
            session = config.driver_type(connection=connection, statement_config=config.statement_config)
            result = await session.execute("SELECT 1 as value")
            data = result.get_first()
            assert data is not None
            await config.close_pool()
            return JSONResponse({"value": data["value"]})

    app = Starlette(routes=[Route("/", starlette_route)])
    plugin.init_app(app)

    with StarletteTestClient(app) as client:
        response = client.get("/")
        assert response.status_code == 200
        assert response.json() == {"value": 1}


def _assert_litestar_disable_di(database: str) -> None:
    config = AiosqliteConfig(
        connection_config={"database": database}, extension_config={"litestar": {"disable_di": True}}
    )
    sql = SQLSpec()
    sql.add_config(config)
    plugin = LitestarSQLSpecPlugin(sqlspec=sql)

    @get("/test")
    async def litestar_route(request: Request) -> dict[str, Any]:
        pool = await config.create_pool()
        async with config.provide_connection(pool) as connection:
            session = config.driver_type(connection=connection, statement_config=config.statement_config)
            result = await session.execute("SELECT 1 as value")
            data = result.get_first()
            assert data is not None
            await config.close_pool()
            return {"value": data["value"]}

    app = Litestar(route_handlers=[litestar_route], plugins=[plugin])

    with LitestarTestClient(app=app) as client:
        response = client.get("/test")
        assert response.status_code == 200
        assert response.json() == {"value": 1}


def _assert_flask_default_di(database: str) -> None:
    sql = SQLSpec()
    config = SqliteConfig(connection_config={"database": database}, extension_config={"flask": {"session_key": "db"}})
    sql.add_config(config)

    app = Flask(__name__)
    plugin = FlaskSQLSpecPlugin(sql, app)

    @app.route("/test")
    def flask_route() -> dict[str, Any]:
        session = plugin.get_session("db")
        result = session.execute("SELECT 1 as value")
        data = result.get_first()
        assert data is not None
        return {"value": data["value"]}

    with app.test_client() as client:
        response = client.get("/test")
        assert response.status_code == 200
        assert response.json == {"value": 1}


def _assert_starlette_default_di(database: str) -> None:
    config = AiosqliteConfig(
        connection_config={"database": database}, extension_config={"starlette": {"session_key": "db"}}
    )
    sql = SQLSpec()
    sql.add_config(config)
    plugin = StarletteSQLSpecPlugin(sql)

    async def starlette_route(request: StarletteRequest) -> Response:
        session = plugin.get_session(request, "db")
        result = await session.execute("SELECT 1 as value")
        data = result.get_first()
        assert data is not None
        return JSONResponse({"value": data["value"]})

    app = Starlette(routes=[Route("/", starlette_route)])
    plugin.init_app(app)

    with StarletteTestClient(app) as client:
        response = client.get("/")
        assert response.status_code == 200
        assert response.json() == {"value": 1}


def _assert_litestar_default_di(database: str) -> None:
    config = AiosqliteConfig(
        connection_config={"database": database}, extension_config={"litestar": {"session_key": "db"}}
    )
    sql = SQLSpec()
    sql.add_config(config)
    plugin = LitestarSQLSpecPlugin(sqlspec=sql)

    @get("/test")
    async def litestar_route(db: AsyncDriverAdapterBase) -> dict[str, Any]:
        result = await db.execute("SELECT 1 as value")
        data = result.get_first()
        assert data is not None
        return {"value": data["value"]}

    app = Litestar(route_handlers=[litestar_route], plugins=[plugin])

    with LitestarTestClient(app=app) as client:
        response = client.get("/test")
        assert response.status_code == 200
        assert response.json() == {"value": 1}


@pytest.mark.parametrize(
    "assertion",
    [
        pytest.param(_assert_flask_disable_di, id="flask"),
        pytest.param(_assert_starlette_disable_di, id="starlette"),
        pytest.param(_assert_litestar_disable_di, id="litestar"),
    ],
)
def test_disable_di_disables_framework_injection(assertion: Callable[[str], None]) -> None:
    """Framework integrations allow manual sessions when dependency injection is disabled."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        assertion(tmp.name)


@pytest.mark.parametrize(
    "assertion",
    [
        pytest.param(_assert_flask_default_di, id="flask"),
        pytest.param(_assert_starlette_default_di, id="starlette"),
        pytest.param(_assert_litestar_default_di, id="litestar"),
    ],
)
def test_default_framework_injection_enabled(assertion: Callable[[str], None]) -> None:
    """Framework integrations enable dependency injection by default."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        assertion(tmp.name)
