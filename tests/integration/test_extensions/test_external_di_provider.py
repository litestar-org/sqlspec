"""Integration tests for disable_di flag across all framework extensions."""

import tempfile

import pytest
from flask import Flask, g
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route
from starlette.testclient import TestClient

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.base import SQLSpec
from sqlspec.extensions.flask import SQLSpecPlugin as FlaskPlugin
from sqlspec.extensions.starlette import SQLSpecPlugin as StarlettePlugin

pytestmark = pytest.mark.xdist_group("sqlite")


def test_starlette_disable_di_disables_middleware() -> None:
    """Test that disable_di disables middleware in Starlette extension."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(
            pool_config={"database": tmp.name}, extension_config={"starlette": {"disable_di": True}}
        )
        sql.add_config(config)
        db_ext = StarlettePlugin(sql)

        async def test_route(request: Request) -> Response:
            pool = await config.create_pool()
            async with config.provide_connection(pool) as connection:
                session = config.driver_type(connection=connection, statement_config=config.statement_config)
                result = await session.execute("SELECT 1 as value")
                data = result.get_first()
                assert data is not None
                await config.close_pool()
                return JSONResponse({"value": data["value"]})

        app = Starlette(routes=[Route("/", test_route)])
        db_ext.init_app(app)

        with TestClient(app) as client:
            response = client.get("/")
            assert response.status_code == 200
            assert response.json() == {"value": 1}


def test_flask_disable_di_disables_hooks() -> None:
    """Test that disable_di disables request hooks in Flask extension."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = SqliteConfig(pool_config={"database": tmp.name}, extension_config={"flask": {"disable_di": True}})
        sql.add_config(config)

        app = Flask(__name__)
        FlaskPlugin(sql, app)

        @app.route("/test")
        def test_route():
            pool = config.create_pool()
            with config.provide_connection(pool) as connection:
                session = config.driver_type(connection=connection, statement_config=config.statement_config)
                result = session.execute("SELECT 1 as value")
                data = result.get_first()
                assert data is not None
                config.close_pool()
                return {"value": data["value"]}

        @app.route("/check_g")
        def check_g():
            return {"has_connection": hasattr(g, "sqlspec_connection_db_session")}

        with app.test_client() as client:
            response = client.get("/test")
            assert response.status_code == 200
            response_json = response.json
            assert response_json is not None
            assert response_json == {"value": 1}

            response = client.get("/check_g")
            assert response.status_code == 200
            response_json = response.json
            assert response_json is not None
            assert response_json == {"has_connection": False}


def test_starlette_default_di_provider_enabled() -> None:
    """Test that default behavior has disable_di=False."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(
            pool_config={"database": tmp.name}, extension_config={"starlette": {"session_key": "db"}}
        )
        sql.add_config(config)
        db_ext = StarlettePlugin(sql)

        async def test_route(request: Request) -> Response:
            session = db_ext.get_session(request, "db")
            result = await session.execute("SELECT 1 as value")
            data = result.get_first()
            return JSONResponse({"value": data["value"]})

        app = Starlette(routes=[Route("/", test_route)])
        db_ext.init_app(app)

        with TestClient(app) as client:
            response = client.get("/")
            assert response.status_code == 200
            assert response.json() == {"value": 1}


def test_flask_default_di_provider_enabled() -> None:
    """Test that default behavior has disable_di=False."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = SqliteConfig(pool_config={"database": tmp.name}, extension_config={"flask": {"session_key": "db"}})
        sql.add_config(config)

        app = Flask(__name__)
        plugin = FlaskPlugin(sql, app)

        @app.route("/test")
        def test_route():
            session = plugin.get_session("db")
            result = session.execute("SELECT 1 as value")
            data = result.get_first()
            return {"value": data["value"]}

        with app.test_client() as client:
            response = client.get("/test")
            assert response.status_code == 200
            response_json = response.json
            assert response_json is not None
            assert response_json == {"value": 1}
