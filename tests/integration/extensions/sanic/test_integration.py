"""Integration tests for Sanic extension with real database requests."""

import tempfile

import pytest
from sanic import Request, Sanic, response

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.base import SQLSpec
from sqlspec.extensions.sanic import SQLSpecPlugin
from sqlspec.utils.correlation import CorrelationContext

pytestmark = pytest.mark.xdist_group("sqlite")


def test_sanic_basic_query() -> None:
    """Sanic extension should execute a basic async SQLite query."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sqlspec = SQLSpec()
        config = AiosqliteConfig(
            connection_config={"database": tmp.name},
            extension_config={"sanic": {"commit_mode": "manual", "session_key": "db"}},
        )
        sqlspec.add_config(config)
        plugin = SQLSpecPlugin(sqlspec)
        app = Sanic("SQLSpecSanicBasic")

        @app.get("/")
        async def handler(request: Request):
            db = plugin.get_session(request, "db")
            result = await db.execute("SELECT 1 as value")
            return response.json({"value": result.get_first()["value"]})

        plugin.init_app(app)

        _, sanic_response = app.test_client.get("/")

        assert sanic_response.status == 200
        assert sanic_response.json == {"value": 1}


def test_sanic_autocommit_commits_success_and_rolls_back_error_status() -> None:
    """Autocommit should commit 2xx responses and rollback error responses."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sqlspec = SQLSpec()
        config = AiosqliteConfig(
            connection_config={"database": tmp.name},
            extension_config={"sanic": {"commit_mode": "autocommit", "session_key": "db"}},
        )
        sqlspec.add_config(config)
        plugin = SQLSpecPlugin(sqlspec)
        app = Sanic("SQLSpecSanicAutocommit")

        @app.post("/setup")
        async def setup(request: Request):
            db = plugin.get_session(request, "db")
            await db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
            await db.execute("INSERT INTO test (name) VALUES (:name)", {"name": "committed"})
            return response.json({"created": True})

        @app.post("/insert-error")
        async def insert_error(request: Request):
            db = plugin.get_session(request, "db")
            await db.execute("INSERT INTO test (name) VALUES (:name)", {"name": "rolled-back"})
            return response.json({"error": "failed"}, status=500)

        @app.get("/data")
        async def data(request: Request):
            db = plugin.get_session(request, "db")
            result = await db.execute("SELECT name FROM test ORDER BY id")
            return response.json({"names": [row["name"] for row in result.all()]})

        plugin.init_app(app)

        _, sanic_response = app.test_client.post("/setup")
        assert sanic_response.status == 200

        _, sanic_response = app.test_client.post("/insert-error")
        assert sanic_response.status == 500

        _, sanic_response = app.test_client.get("/data")
        assert sanic_response.status == 200
        assert sanic_response.json == {"names": ["committed"]}


def test_sanic_autocommit_rolls_back_on_exception_response() -> None:
    """Exceptions should produce rollback through Sanic's error response path."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sqlspec = SQLSpec()
        config = AiosqliteConfig(
            connection_config={"database": tmp.name},
            extension_config={"sanic": {"commit_mode": "autocommit", "session_key": "db"}},
        )
        sqlspec.add_config(config)
        plugin = SQLSpecPlugin(sqlspec)
        app = Sanic("SQLSpecSanicExceptionRollback")

        @app.post("/setup")
        async def setup(request: Request):
            db = plugin.get_session(request, "db")
            await db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
            await db.execute("INSERT INTO test (name) VALUES (:name)", {"name": "committed"})
            return response.json({"created": True})

        @app.post("/explode")
        async def explode(request: Request):
            db = plugin.get_session(request, "db")
            await db.execute("INSERT INTO test (name) VALUES (:name)", {"name": "rolled-back"})
            msg = "request failed"
            raise RuntimeError(msg)

        @app.get("/data")
        async def data(request: Request):
            db = plugin.get_session(request, "db")
            result = await db.execute("SELECT name FROM test ORDER BY id")
            return response.json({"names": [row["name"] for row in result.all()]})

        plugin.init_app(app)

        _, sanic_response = app.test_client.post("/setup")
        assert sanic_response.status == 200

        _, sanic_response = app.test_client.post("/explode", debug=False)
        assert sanic_response.status == 500

        _, sanic_response = app.test_client.get("/data")
        assert sanic_response.status == 200
        assert sanic_response.json == {"names": ["committed"]}


def test_sanic_sync_sqlite_autocommit_commit_and_rollback() -> None:
    """Sync SQLite should work through Sanic request middleware."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sqlspec = SQLSpec()
        config = SqliteConfig(
            connection_config={"database": tmp.name},
            extension_config={"sanic": {"commit_mode": "autocommit", "session_key": "db"}},
        )
        sqlspec.add_config(config)
        plugin = SQLSpecPlugin(sqlspec)
        app = Sanic("SQLSpecSanicSyncSqlite")

        @app.post("/setup")
        async def setup(request: Request):
            db = plugin.get_session(request, "db")
            db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
            db.execute("INSERT INTO test (name) VALUES (?)", ("committed",))
            return response.json({"created": True})

        @app.post("/insert-error")
        async def insert_error(request: Request):
            db = plugin.get_session(request, "db")
            db.execute("INSERT INTO test (name) VALUES (?)", ("rolled-back",))
            return response.json({"error": "failed"}, status=500)

        @app.get("/data")
        async def data(request: Request):
            db = plugin.get_session(request, "db")
            rows = db.execute("SELECT name FROM test ORDER BY id").all()
            return response.json({"names": [row["name"] for row in rows]})

        plugin.init_app(app)

        _, sanic_response = app.test_client.post("/setup")
        assert sanic_response.status == 200

        _, sanic_response = app.test_client.post("/insert-error")
        assert sanic_response.status == 500

        _, sanic_response = app.test_client.get("/data")
        assert sanic_response.status == 200
        assert sanic_response.json == {"names": ["committed"]}


def test_sanic_multi_database_sessions() -> None:
    """Sanic plugin should support multiple configured databases."""
    with (
        tempfile.NamedTemporaryFile(suffix=".db", delete=True) as users_tmp,
        tempfile.NamedTemporaryFile(suffix=".db", delete=True) as products_tmp,
    ):
        sqlspec = SQLSpec()
        users_config = AiosqliteConfig(
            bind_key="users",
            connection_config={"database": users_tmp.name},
            extension_config={
                "sanic": {
                    "commit_mode": "autocommit",
                    "connection_key": "users_connection",
                    "pool_key": "users_pool",
                    "session_key": "users_db",
                }
            },
        )
        products_config = AiosqliteConfig(
            bind_key="products",
            connection_config={"database": products_tmp.name},
            extension_config={
                "sanic": {
                    "commit_mode": "autocommit",
                    "connection_key": "products_connection",
                    "pool_key": "products_pool",
                    "session_key": "products_db",
                }
            },
        )
        sqlspec.add_config(users_config)
        sqlspec.add_config(products_config)
        plugin = SQLSpecPlugin(sqlspec)
        app = Sanic("SQLSpecSanicMultiDatabase")

        @app.post("/setup")
        async def setup(request: Request):
            users_db = plugin.get_session(request, "users_db")
            products_db = plugin.get_session(request, "products_db")
            await users_db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
            await products_db.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT)")
            await users_db.execute("INSERT INTO users (name) VALUES (:name)", {"name": "Alice"})
            await products_db.execute("INSERT INTO products (name) VALUES (:name)", {"name": "Widget"})
            return response.json({"created": True})

        @app.get("/counts")
        async def counts(request: Request):
            users_db = plugin.get_session(request, "users_db")
            products_db = plugin.get_session(request, "products_db")
            users_count = await users_db.select_value("SELECT COUNT(*) FROM users")
            products_count = await products_db.select_value("SELECT COUNT(*) FROM products")
            return response.json({"users": users_count, "products": products_count})

        plugin.init_app(app)

        _, sanic_response = app.test_client.post("/setup")
        assert sanic_response.status == 200

        _, sanic_response = app.test_client.get("/counts")
        assert sanic_response.status == 200
        assert sanic_response.json == {"users": 1, "products": 1}


def test_sanic_session_caching() -> None:
    """Sanic plugin should cache sessions within one request."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sqlspec = SQLSpec()
        config = AiosqliteConfig(
            connection_config={"database": tmp.name},
            extension_config={"sanic": {"commit_mode": "manual", "session_key": "db"}},
        )
        sqlspec.add_config(config)
        plugin = SQLSpecPlugin(sqlspec)
        app = Sanic("SQLSpecSanicSessionCaching")

        @app.get("/")
        async def handler(request: Request):
            session_one = plugin.get_session(request, "db")
            session_two = plugin.get_session(request, "db")
            return response.json({"same_session": session_one is session_two})

        plugin.init_app(app)

        _, sanic_response = app.test_client.get("/")

        assert sanic_response.status == 200
        assert sanic_response.json == {"same_session": True}


def test_sanic_disable_di_preserves_pool_lifecycle() -> None:
    """disable_di should skip request management while preserving app.ctx pools."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sqlspec = SQLSpec()
        config = AiosqliteConfig(
            connection_config={"database": tmp.name},
            extension_config={"sanic": {"disable_di": True, "pool_key": "manual_pool"}},
        )
        sqlspec.add_config(config)
        plugin = SQLSpecPlugin(sqlspec)
        app = Sanic("SQLSpecSanicDisableDI")

        @app.get("/")
        async def handler(request: Request):
            pool = request.app.ctx.manual_pool
            async with config.provide_connection(pool) as connection:
                db = config.driver_type(connection=connection, statement_config=config.statement_config)
                result = await db.execute("SELECT 1 as value")
                data = result.get_first()
                assert data is not None
                return response.json({"value": data["value"]})

        plugin.init_app(app)

        _, sanic_response = app.test_client.get("/")

        assert sanic_response.status == 200
        assert sanic_response.json == {"value": 1}


def test_sanic_correlation_header_round_trip() -> None:
    """Sanic correlation middleware should propagate request correlation IDs."""
    sqlspec = SQLSpec()
    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"sanic": {"disable_di": True, "enable_correlation_middleware": True}},
    )
    sqlspec.add_config(config)
    plugin = SQLSpecPlugin(sqlspec)
    app = Sanic("SQLSpecSanicCorrelation")
    seen_context: list[str | None] = []

    @app.get("/")
    async def handler(request: Request):
        seen_context.append(CorrelationContext.get())
        return response.json({"correlation_id": request.ctx.correlation_id})

    plugin.init_app(app)

    _, sanic_response = app.test_client.get("/", headers={"x-request-id": "sanic-cid"})

    assert sanic_response.status == 200
    assert sanic_response.json == {"correlation_id": "sanic-cid"}
    assert sanic_response.headers["X-Correlation-ID"] == "sanic-cid"
    assert seen_context == ["sanic-cid"]
    assert CorrelationContext.get() is None
