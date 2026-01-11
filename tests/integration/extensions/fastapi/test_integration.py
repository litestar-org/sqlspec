"""Integration tests for FastAPI extension with real database."""

import tempfile
from typing import Annotated, Any

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteConnection, AiosqliteDriver
from sqlspec.base import SQLSpec
from sqlspec.extensions.fastapi import SQLSpecPlugin

pytestmark = pytest.mark.xdist_group("sqlite")


def test_fastapi_dependency_injection() -> None:
    """Test FastAPI dependency injection with session_dependency."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(
            connection_config={"database": tmp.name},
            extension_config={"starlette": {"commit_mode": "manual", "session_key": "db"}},
        )
        sql.add_config(config)

        app = FastAPI()
        db_ext = SQLSpecPlugin(sql, app=app)

        @app.get("/query")
        async def query_endpoint(
            db: Annotated[AiosqliteDriver, Depends(db_ext.provide_session(config))],
        ) -> dict[str, Any]:
            result = await db.select_value("SELECT 1 as value")
            return {"value": result}

        with TestClient(app) as client:
            response = client.get("/query")
            assert response.status_code == 200
            assert response.json() == {"value": 1}


def test_fastapi_connection_dependency() -> None:
    """Test FastAPI connection_dependency for raw connection access."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(
            connection_config={"database": tmp.name},
            extension_config={"starlette": {"commit_mode": "manual", "session_key": "db"}},
        )
        sql.add_config(config)

        app = FastAPI()
        db_ext = SQLSpecPlugin(sql, app=app)

        @app.get("/raw")
        async def raw_endpoint(
            conn: Annotated[AiosqliteConnection, Depends(db_ext.provide_connection(config))],
        ) -> dict[str, Any]:
            cursor = await conn.cursor()
            await cursor.execute("SELECT 42 as answer")
            row = await cursor.fetchone()
            await cursor.close()
            return {"answer": row[0] if row else "No Data"}

        with TestClient(app) as client:
            response = client.get("/raw")
            assert response.status_code == 200
            assert response.json() == {"answer": 42}


def test_fastapi_manual_commit() -> None:
    """Test FastAPI with manual commit mode."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(
            connection_config={"database": tmp.name},
            extension_config={"starlette": {"commit_mode": "manual", "session_key": "db"}},
        )
        sql.add_config(config)

        app = FastAPI()
        db_ext = SQLSpecPlugin(sql, app=app)

        @app.post("/create")
        async def create_table(
            db: Annotated[AiosqliteDriver, Depends(db_ext.provide_session(config))],
            conn: Annotated[AiosqliteConnection, Depends(db_ext.provide_connection(config))],
        ) -> dict[str, Any]:
            await db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
            await db.execute("INSERT INTO test (name) VALUES (:name)", {"name": "FastAPI"})
            await conn.commit()
            return {"created": True}

        @app.get("/data")
        async def get_data(db: Annotated[AiosqliteDriver, Depends(db_ext.provide_session(config))]) -> dict[str, Any]:
            result = await db.execute("SELECT * FROM test")
            rows = result.all()
            return {"count": len(rows), "rows": rows}

        with TestClient(app) as client:
            response = client.post("/create")
            assert response.status_code == 200

            response = client.get("/data")
            assert response.status_code == 200
            assert response.json()["count"] == 1
            assert response.json()["rows"][0]["name"] == "FastAPI"


def test_fastapi_autocommit_mode() -> None:
    """Test FastAPI with autocommit mode."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(
            connection_config={"database": tmp.name},
            extension_config={"starlette": {"commit_mode": "autocommit", "session_key": "db"}},
        )
        sql.add_config(config)

        app = FastAPI()
        db_ext = SQLSpecPlugin(sql, app=app)

        @app.post("/create")
        async def create_table(
            db: Annotated[AiosqliteDriver, Depends(db_ext.provide_session(config))],
        ) -> dict[str, Any]:
            await db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
            await db.execute("INSERT INTO test (name) VALUES (:name)", {"name": "AutoCommit"})
            return {"created": True}

        @app.get("/data")
        async def get_data(db: Annotated[AiosqliteDriver, Depends(db_ext.provide_session(config))]) -> dict[str, Any]:
            result = await db.execute("SELECT * FROM test")
            rows = result.all()
            return {"count": len(rows)}

        with TestClient(app) as client:
            response = client.post("/create")
            assert response.status_code == 200

            response = client.get("/data")
            assert response.status_code == 200
            assert response.json() == {"count": 1}


def test_fastapi_session_caching_across_dependencies() -> None:
    """Test session is cached across multiple dependencies in same request."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(
            connection_config={"database": tmp.name},
            extension_config={"starlette": {"commit_mode": "manual", "session_key": "db"}},
        )
        sql.add_config(config)

        app = FastAPI()
        db_ext = SQLSpecPlugin(sql, app=app)

        @app.get("/check")
        async def check_caching(
            db1: Annotated[AiosqliteDriver, Depends(db_ext.provide_session(config))],
            db2: Annotated[AiosqliteDriver, Depends(db_ext.provide_session(config))],
        ) -> dict[str, Any]:
            return {"same_session": db1 is db2}

        with TestClient(app) as client:
            response = client.get("/check")
            assert response.status_code == 200
            assert response.json() == {"same_session": True}


def test_fastapi_complex_route_with_multiple_queries() -> None:
    """Test FastAPI route with multiple queries using same session."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(
            connection_config={"database": tmp.name},
            extension_config={"starlette": {"commit_mode": "manual", "session_key": "db"}},
        )
        sql.add_config(config)

        app = FastAPI()
        db_ext = SQLSpecPlugin(sql, app=app)

        @app.post("/setup")
        async def setup(
            db: Annotated[AiosqliteDriver, Depends(db_ext.provide_session(config))],
            conn: Annotated[Any, Depends(db_ext.provide_connection(config))],
        ) -> dict[str, Any]:
            await db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
            await db.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT)")

            await db.execute("INSERT INTO users (name) VALUES (:name)", {"name": "Alice"})
            await db.execute("INSERT INTO users (name) VALUES (:name)", {"name": "Bob"})

            await db.execute(
                "INSERT INTO posts (user_id, title) VALUES (:user_id, :title)", {"user_id": 1, "title": "Post 1"}
            )
            await db.execute(
                "INSERT INTO posts (user_id, title) VALUES (:user_id, :title)", {"user_id": 1, "title": "Post 2"}
            )

            await conn.commit()
            return {"setup": True}

        @app.get("/stats")
        async def get_stats(db: Annotated[AiosqliteDriver, Depends(db_ext.provide_session(config))]) -> dict[str, Any]:
            users = await db.select_value("SELECT COUNT(*) as count FROM users")
            posts = await db.select_value("SELECT COUNT(*) as count FROM posts")

            user_posts = await db.select(
                """
                SELECT u.name, COUNT(p.id) as post_count
                FROM users u
                LEFT JOIN posts p ON u.id = p.user_id
                GROUP BY u.id, u.name
                """
            )

            return {"total_users": users, "total_posts": posts, "user_posts": user_posts}

        with TestClient(app) as client:
            response = client.post("/setup")
            assert response.status_code == 200

            response = client.get("/stats")
            assert response.status_code == 200
            data = response.json()
            assert data["total_users"] == 2
            assert data["total_posts"] == 2
            assert len(data["user_posts"]) == 2


def test_fastapi_inherits_starlette_behavior() -> None:
    """Test FastAPI extension behaves identically to Starlette for basic operations."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(
            connection_config={"database": tmp.name},
            extension_config={"starlette": {"commit_mode": "manual", "session_key": "db"}},
        )
        sql.add_config(config)

        app = FastAPI()
        db_ext = SQLSpecPlugin(sql, app=app)

        @app.get("/test")
        async def test_endpoint(
            db: Annotated[AiosqliteDriver, Depends(db_ext.provide_session(config))],
        ) -> dict[str, Any]:
            result = await db.select_one("SELECT 1 as value")
            return {"value": result["value"]}

        with TestClient(app) as client:
            response = client.get("/test")
            assert response.status_code == 200
            assert response.json() == {"value": 1}

            assert hasattr(app.state, "db_pool")


def test_fastapi_default_session_key() -> None:
    """Test FastAPI defaults to the shared 'db_session' key."""

    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(connection_config={"database": tmp.name}, extension_config={"starlette": {}})
        sql.add_config(config)

        app = FastAPI()
        db_ext = SQLSpecPlugin(sql, app=app)

        @app.get("/default-key")
        async def default_key(
            default_db: Annotated[AiosqliteDriver, Depends(db_ext.provide_session())],
            named_db: Annotated[AiosqliteDriver, Depends(db_ext.provide_session("db_session"))],
        ) -> dict[str, bool]:
            return {"same_session": default_db is named_db}

        with TestClient(app) as client:
            response = client.get("/default-key")
            assert response.status_code == 200
            assert response.json() == {"same_session": True}
