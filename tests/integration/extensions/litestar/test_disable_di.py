"""Integration tests for disable_di flag in Litestar extension."""

import tempfile

import pytest
from litestar import Litestar, Request, get
from litestar.testing import TestClient

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.base import SQLSpec
from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.extensions.litestar import SQLSpecPlugin

pytestmark = pytest.mark.xdist_group("sqlite")


def test_litestar_disable_di_disables_providers() -> None:
    """Test that disable_di disables dependency providers in Litestar extension."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(
            connection_config={"database": tmp.name}, extension_config={"litestar": {"disable_di": True}}
        )
        sql.add_config(config)
        plugin = SQLSpecPlugin(sqlspec=sql)

        @get("/test")
        async def test_route(request: Request) -> dict:
            pool = await config.create_pool()
            async with config.provide_connection(pool) as connection:
                session = config.driver_type(connection=connection, statement_config=config.statement_config)
                result = await session.execute("SELECT 1 as value")
                data = result.get_first()
                assert data is not None
                await config.close_pool()
                return {"value": data["value"]}

        app = Litestar(route_handlers=[test_route], plugins=[plugin])

        with TestClient(app=app) as client:
            response = client.get("/test")
            assert response.status_code == 200
            assert response.json() == {"value": 1}


def test_litestar_default_di_enabled() -> None:
    """Test that default behavior has disable_di=False."""

    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(
            connection_config={"database": tmp.name}, extension_config={"litestar": {"session_key": "db"}}
        )
        sql.add_config(config)
        plugin = SQLSpecPlugin(sqlspec=sql)

        @get("/test")
        async def test_route(db: AsyncDriverAdapterBase) -> dict:
            result = await db.execute("SELECT 1 as value")
            data = result.get_first()
            assert data is not None
            return {"value": data["value"]}

        app = Litestar(route_handlers=[test_route], plugins=[plugin])

        with TestClient(app=app) as client:
            response = client.get("/test")
            assert response.status_code == 200
            assert response.json() == {"value": 1}
