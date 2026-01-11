"""Integration tests for Starlette extension with real database."""

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


def test_starlette_basic_query() -> None:
    """Test basic query execution through Starlette extension."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(
            connection_config={"database": tmp.name},
            extension_config={"starlette": {"commit_mode": "manual", "session_key": "db"}},
        )
        sql.add_config(config)
        db_ext = SQLSpecPlugin(sql)

        async def homepage(request: Request) -> Response:
            session = db_ext.get_session(request)
            result = await session.execute("SELECT 1 as value")
            data = result.get_first()
            return JSONResponse({"value": data["value"]})

        app = Starlette(routes=[Route("/", homepage)])
        db_ext.init_app(app)

        with TestClient(app) as client:
            response = client.get("/")
            assert response.status_code == 200
            assert response.json() == {"value": 1}


def test_starlette_manual_commit_mode() -> None:
    """Test manual commit mode requires explicit transaction handling."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(
            connection_config={"database": tmp.name},
            extension_config={"starlette": {"commit_mode": "manual", "session_key": "db"}},
        )
        sql.add_config(config)
        db_ext = SQLSpecPlugin(sql)

        async def create_table(request: Request) -> Response:
            session = db_ext.get_session(request)
            await session.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
            await session.execute("INSERT INTO test (name) VALUES (:name)", {"name": "Alice"})
            connection = db_ext.get_connection(request)
            await connection.commit()
            return JSONResponse({"created": True})

        async def get_data(request: Request) -> Response:
            session = db_ext.get_session(request)
            result = await session.execute("SELECT * FROM test")
            rows = result.all()
            return JSONResponse({"count": len(rows)})

        app = Starlette(routes=[Route("/create", create_table), Route("/data", get_data)])
        db_ext.init_app(app)

        with TestClient(app) as client:
            response = client.get("/create")
            assert response.status_code == 200

            response = client.get("/data")
            assert response.status_code == 200
            assert response.json() == {"count": 1}


def test_starlette_autocommit_mode() -> None:
    """Test autocommit mode automatically commits on success."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(
            connection_config={"database": tmp.name},
            extension_config={"starlette": {"commit_mode": "autocommit", "session_key": "db"}},
        )
        sql.add_config(config)
        db_ext = SQLSpecPlugin(sql)

        async def create_table(request: Request) -> Response:
            session = db_ext.get_session(request)
            await session.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
            await session.execute("INSERT INTO test (name) VALUES (:name)", {"name": "Bob"})
            return JSONResponse({"created": True})

        async def get_data(request: Request) -> Response:
            session = db_ext.get_session(request)
            result = await session.execute("SELECT * FROM test")
            rows = result.all()
            return JSONResponse({"count": len(rows)})

        app = Starlette(routes=[Route("/create", create_table), Route("/data", get_data)])
        db_ext.init_app(app)

        with TestClient(app) as client:
            response = client.get("/create")
            assert response.status_code == 200

            response = client.get("/data")
            assert response.status_code == 200
            assert response.json() == {"count": 1}


def test_starlette_autocommit_rolls_back_on_error() -> None:
    """Test autocommit mode rolls back on error status."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(
            connection_config={"database": tmp.name},
            extension_config={"starlette": {"commit_mode": "autocommit", "session_key": "db"}},
        )
        sql.add_config(config)
        db_ext = SQLSpecPlugin(sql)

        async def create_table(request: Request) -> Response:
            session = db_ext.get_session(request)
            await session.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
            await session.execute("INSERT INTO test (name) VALUES (:name)", {"name": "Charlie"})
            return JSONResponse({"error": "Failed"}, status_code=500)

        async def get_data(request: Request) -> Response:
            session = db_ext.get_session(request)
            try:
                result = await session.execute("SELECT * FROM test")
                rows = result.all()
                return JSONResponse({"count": len(rows)})
            except Exception:
                return JSONResponse({"count": 0})

        app = Starlette(routes=[Route("/create", create_table), Route("/data", get_data)])
        db_ext.init_app(app)

        with TestClient(app) as client:
            response = client.get("/create")
            assert response.status_code == 500

            response = client.get("/data")
            assert response.status_code == 200
            assert response.json() == {"count": 0}


def test_starlette_session_caching() -> None:
    """Test session caching within single request."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(
            connection_config={"database": tmp.name},
            extension_config={"starlette": {"commit_mode": "manual", "session_key": "db"}},
        )
        sql.add_config(config)
        db_ext = SQLSpecPlugin(sql)

        async def check_session_caching(request: Request) -> Response:
            session1 = db_ext.get_session(request)
            session2 = db_ext.get_session(request)
            return JSONResponse({"same_session": session1 is session2})

        app = Starlette(routes=[Route("/", check_session_caching)])
        db_ext.init_app(app)

        with TestClient(app) as client:
            response = client.get("/")
            assert response.status_code == 200
            assert response.json() == {"same_session": True}


def test_starlette_connection_pool_lifecycle() -> None:
    """Test connection pool is created during lifespan."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(
            connection_config={"database": tmp.name},
            extension_config={"starlette": {"commit_mode": "manual", "session_key": "db"}},
        )
        sql.add_config(config)
        db_ext = SQLSpecPlugin(sql)

        async def test_query(request: Request) -> Response:
            session = db_ext.get_session(request)
            result = await session.execute("SELECT 1 as value")
            return JSONResponse({"value": result.get_first()["value"]})

        app = Starlette(routes=[Route("/", test_query)])
        db_ext.init_app(app)

        with TestClient(app) as client:
            response = client.get("/")
            assert response.status_code == 200
            assert hasattr(app.state, "db_pool")


def test_starlette_default_session_key() -> None:
    """Test default session key matches explicit 'db_session'."""

    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(connection_config={"database": tmp.name}, extension_config={"starlette": {}})
        sql.add_config(config)
        db_ext = SQLSpecPlugin(sql)

        async def check_default(request: Request) -> Response:
            session_default = db_ext.get_session(request)
            session_named = db_ext.get_session(request, "db_session")
            return JSONResponse({"same_session": session_default is session_named})

        app = Starlette(routes=[Route("/", check_default)])
        db_ext.init_app(app)

        with TestClient(app) as client:
            response = client.get("/")
            assert response.status_code == 200
            assert response.json() == {"same_session": True}
