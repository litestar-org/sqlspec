"""Integration tests for disable_di flag in Starlette extension."""

import tempfile

import pytest
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route
from starlette.testclient import TestClient

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.base import SQLSpec
from sqlspec.extensions.starlette import SQLSpecPlugin

pytestmark = pytest.mark.xdist_group("sqlite")


def test_starlette_disable_di_disables_middleware() -> None:
    """Test that disable_di disables middleware in Starlette extension."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(
            connection_config={"database": tmp.name}, extension_config={"starlette": {"disable_di": True}}
        )
        sql.add_config(config)
        db_ext = SQLSpecPlugin(sql)

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


def test_starlette_default_di_enabled() -> None:
    """Test that default behavior has disable_di=False."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(
            connection_config={"database": tmp.name}, extension_config={"starlette": {"session_key": "db"}}
        )
        sql.add_config(config)
        db_ext = SQLSpecPlugin(sql)

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
