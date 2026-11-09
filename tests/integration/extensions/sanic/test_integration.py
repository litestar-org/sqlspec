"""Integration tests for Sanic extension with real database requests."""

import os
import tempfile
from typing import TYPE_CHECKING, Any

import pytest

os.environ.setdefault("SANIC_DEPRECATION_FILTER", "ignore")

from sanic import Request, Sanic, response

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.base import SQLSpec
from sqlspec.extensions.sanic import SQLSpecPlugin
from sqlspec.utils.correlation import CorrelationContext

if TYPE_CHECKING:
    from pytest_databases.docker.postgres import PostgresService

pytestmark = pytest.mark.xdist_group("sqlite")


def _create_sanic_app(name: str) -> Sanic:
    app = Sanic(name)
    app.config.USE_UVLOOP = False
    return app


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
        app = _create_sanic_app("SQLSpecSanicBasic")

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
        app = _create_sanic_app("SQLSpecSanicAutocommit")

        @app.post("/setup")
        async def setup(request: Request):
            db = plugin.get_session(request, "db")
            await db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
            await db.execute("INSERT INTO test (name) VALUES (:name)", name="committed")
            return response.json({"created": True})

        @app.post("/insert-error")
        async def insert_error(request: Request):
            db = plugin.get_session(request, "db")
            await db.execute("INSERT INTO test (name) VALUES (:name)", name="rolled-back")
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
        app = _create_sanic_app("SQLSpecSanicExceptionRollback")

        @app.post("/setup")
        async def setup(request: Request):
            db = plugin.get_session(request, "db")
            await db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
            await db.execute("INSERT INTO test (name) VALUES (:name)", name="committed")
            return response.json({"created": True})

        @app.post("/explode")
        async def explode(request: Request):
            db = plugin.get_session(request, "db")
            await db.execute("INSERT INTO test (name) VALUES (:name)", name="rolled-back")
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
        app = _create_sanic_app("SQLSpecSanicSyncSqlite")

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
        app = _create_sanic_app("SQLSpecSanicMultiDatabase")

        @app.post("/setup")
        async def setup(request: Request):
            users_db = plugin.get_session(request, "users_db")
            products_db = plugin.get_session(request, "products_db")
            await users_db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
            await products_db.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT)")
            await users_db.execute("INSERT INTO users (name) VALUES (:name)", name="Alice")
            await products_db.execute("INSERT INTO products (name) VALUES (:name)", name="Widget")
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
        app = _create_sanic_app("SQLSpecSanicSessionCaching")

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
        app = _create_sanic_app("SQLSpecSanicDisableDI")

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
    app = _create_sanic_app("SQLSpecSanicCorrelation")
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


def test_sanic_sqlite_manual_rolls_back_on_raise() -> None:
    """Sanic manual mode with SQLite should roll back uncommitted work when handler raises."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sqlspec = SQLSpec()
        config = SqliteConfig(
            connection_config={"database": tmp.name},
            extension_config={"sanic": {"commit_mode": "manual", "session_key": "db"}},
        )
        sqlspec.add_config(config)
        plugin = SQLSpecPlugin(sqlspec)
        app = _create_sanic_app("SQLSpecSanicManualSqliteRollback")

        @app.post("/setup")
        async def setup(request: Request):
            db = plugin.get_session(request, "db")
            db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
            db.execute("INSERT INTO test (name) VALUES (?)", ("initial",))
            db.commit()
            return response.json({"created": True})

        @app.post("/insert-raise")
        async def insert_raise(request: Request):
            db = plugin.get_session(request, "db")
            db.execute("INSERT INTO test (name) VALUES (?)", ("failing",))
            raise RuntimeError("handler failed")

        @app.get("/data")
        async def data(request: Request):
            db = plugin.get_session(request, "db")
            rows = db.execute("SELECT name FROM test ORDER BY id").all()
            return response.json({"names": [row["name"] for row in rows]})

        plugin.init_app(app)

        _, res_setup = app.test_client.post("/setup")
        assert res_setup.status == 200

        _, res_raise = app.test_client.post("/insert-raise", debug=False)
        assert res_raise.status == 500

        _, res_data = app.test_client.get("/data")
        assert res_data.status == 200
        assert res_data.json == {"names": ["initial"]}


def test_sanic_sqlite_manual_commits_on_clean_return() -> None:
    """Sanic manual mode with SQLite should commit uncommitted work on clean 200 return."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sqlspec = SQLSpec()
        config = SqliteConfig(
            connection_config={"database": tmp.name},
            extension_config={"sanic": {"commit_mode": "manual", "session_key": "db"}},
        )
        sqlspec.add_config(config)
        plugin = SQLSpecPlugin(sqlspec)
        app = _create_sanic_app("SQLSpecSanicManualSqliteCommit")

        @app.post("/setup")
        async def setup(request: Request):
            db = plugin.get_session(request, "db")
            db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
            db.commit()
            return response.json({"created": True})

        @app.post("/insert-clean")
        async def insert_clean(request: Request):
            db = plugin.get_session(request, "db")
            db.execute("INSERT INTO test (name) VALUES (?)", ("clean_row",))
            return response.json({"success": True})

        @app.get("/data")
        async def data(request: Request):
            db = plugin.get_session(request, "db")
            rows = db.execute("SELECT name FROM test ORDER BY id").all()
            return response.json({"names": [row["name"] for row in rows]})

        plugin.init_app(app)

        _, res_setup = app.test_client.post("/setup")
        assert res_setup.status == 200

        _, res_clean = app.test_client.post("/insert-clean")
        assert res_clean.status == 200

        _, res_data = app.test_client.get("/data")
        assert res_data.status == 200
        assert res_data.json == {"names": ["clean_row"]}


def test_sanic_psycopg_manual_rolls_back_on_raise(postgres_service: "PostgresService") -> None:
    """Sanic manual mode with psycopg should roll back on exception and next request succeeds."""
    pytest.importorskip("psycopg")
    pytest.importorskip("psycopg_pool")
    import psycopg

    from sqlspec.adapters.psycopg import PsycopgAsyncConfig
    from tests.integration.fixtures.postgres import _postgres_conninfo

    conninfo = _postgres_conninfo(postgres_service)
    with psycopg.connect(conninfo, autocommit=True) as p_conn:
        with p_conn.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS sanic_psycopg_test (name VARCHAR(64) NOT NULL);")
            cur.execute("DELETE FROM sanic_psycopg_test;")

    sqlspec = SQLSpec()
    config = PsycopgAsyncConfig(
        connection_config={"conninfo": conninfo, "min_size": 1, "max_size": 4},
        extension_config={"sanic": {"commit_mode": "manual", "session_key": "db"}},
    )
    sqlspec.add_config(config)
    plugin = SQLSpecPlugin(sqlspec)
    app = _create_sanic_app("SQLSpecSanicPsycopgManual")

    @app.post("/insert-raise")
    async def insert_raise(request: Request):
        db = plugin.get_session(request, "db")
        await db.execute("INSERT INTO sanic_psycopg_test (name) VALUES (%s)", ("failing",))
        raise RuntimeError("handler boom")

    @app.post("/insert-ok")
    async def insert_ok(request: Request):
        db = plugin.get_session(request, "db")
        await db.execute("INSERT INTO sanic_psycopg_test (name) VALUES (%s)", ("succeeding",))
        await db.commit()
        return response.json({"status": "ok"})

    plugin.init_app(app)

    _, res_raise = app.test_client.post("/insert-raise", debug=False)
    assert res_raise.status == 500

    _, res_ok = app.test_client.post("/insert-ok")
    assert res_ok.status == 200

    with psycopg.connect(conninfo, autocommit=True) as p_conn:
        with p_conn.cursor() as cur:
            cur.execute("SELECT name FROM sanic_psycopg_test ORDER BY name;")
            rows = [row[0] for row in cur.fetchall()]

    assert rows == ["succeeding"]


def test_sanic_autocommit_and_manual_roll_back_when_exception_handler_returns_200() -> None:
    """Mapped exceptions returning HTTP 200 must roll back in both autocommit and manual modes."""

    class FallbackError(Exception):
        pass

    for mode in ("autocommit", "manual"):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
            sqlspec = SQLSpec()
            config = AiosqliteConfig(
                connection_config={"database": tmp.name},
                extension_config={"sanic": {"commit_mode": mode, "session_key": "db"}},
            )
            sqlspec.add_config(config)
            plugin = SQLSpecPlugin(sqlspec)
            app = _create_sanic_app(f"SQLSpecSanicMapped200_{mode}")

            @app.exception(FallbackError)
            async def handle_fallback(_request: Request, _exc: FallbackError):
                return response.json({"fallback": True}, status=200)

            @app.post("/setup")
            async def setup(request: Request):
                db = plugin.get_session(request, "db")
                await db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
                await db.execute("INSERT INTO test (name) VALUES (:name)", name="baseline")
                if mode == "manual":
                    await db.commit()
                return response.json({"created": True})

            @app.post("/partial-fail")
            async def partial_fail(request: Request):
                db = plugin.get_session(request, "db")
                await db.execute("INSERT INTO test (name) VALUES (:name)", name="leaked_partial")
                raise FallbackError("abort after write")

            @app.get("/data")
            async def data(request: Request):
                db = plugin.get_session(request, "db")
                result = await db.execute("SELECT name FROM test ORDER BY id")
                return response.json({"names": [row["name"] for row in result.all()]})

            plugin.init_app(app)

            _, res_setup = app.test_client.post("/setup")
            assert res_setup.status == 200

            _, res_partial = app.test_client.post("/partial-fail")
            assert res_partial.status == 200
            assert res_partial.json == {"fallback": True}

            _, res_data = app.test_client.get("/data")
            assert res_data.status == 200
            assert res_data.json == {"names": ["baseline"]}


def test_sanic_abrupt_disconnect_cancellation_rolls_back_and_releases_pool() -> None:
    """Cancelled request with closed transport rolls back aiosqlite and releases single-slot pool."""
    import asyncio
    import contextlib
    import importlib

    reusable_client_cls: Any = importlib.import_module("sanic_testing.reusable").ReusableClient

    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sqlspec = SQLSpec()
        config = AiosqliteConfig(
            connection_config={"database": tmp.name, "pool_size": 1, "min_size": 1, "connect_timeout": 0.5},
            extension_config={"sanic": {"commit_mode": "manual", "session_key": "db"}},
        )
        sqlspec.add_config(config)
        plugin = SQLSpecPlugin(sqlspec)
        app = _create_sanic_app("SQLSpecSanicDisconnectCancel")

        @app.post("/setup")
        async def setup(request: Request):
            db = plugin.get_session(request, "db")
            await db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
            await db.execute("INSERT INTO test (name) VALUES (:name)", name="initial")
            await db.commit()
            return response.json({"created": True})

        @app.post("/disconnect-cancel")
        async def disconnect_cancel(request: Request):
            db = plugin.get_session(request, "db")
            await db.execute("INSERT INTO test (name) VALUES (:name)", name="aborted_row")
            protocol: Any = request.transport.get_protocol()
            request.transport.close()
            protocol.transport = None
            raise asyncio.CancelledError()

        @app.get("/data")
        async def data(request: Request):
            db = plugin.get_session(request, "db")
            result = await db.execute("SELECT name FROM test ORDER BY id")
            return response.json({"names": [row["name"] for row in result.all()]})

        plugin.init_app(app)

        client: Any = reusable_client_cls(app)
        with client:
            _, res_setup = client.post("/setup")
            assert res_setup.status == 200

            with contextlib.suppress(Exception):
                client.post("/disconnect-cancel")

            _, res_data = client.get("/data")
            assert res_data.status == 200
            assert res_data.json == {"names": ["initial"]}


def test_sanic_autocommit_deferred_fk_violation_forwards_commit_error_without_double_commit() -> None:
    """Deferred FK violation during Sanic autocommit COMMIT rolls back once without a second COMMIT on exit."""
    import importlib
    import sqlite3

    reusable_client_cls: Any = importlib.import_module("sanic_testing.reusable").ReusableClient

    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        with sqlite3.connect(tmp.name) as setup_conn:
            setup_conn.execute("PRAGMA foreign_keys = ON")
            setup_conn.execute("CREATE TABLE parents (id INTEGER PRIMARY KEY)")
            setup_conn.execute(
                "CREATE TABLE children ("
                "id INTEGER PRIMARY KEY, "
                "parent_id INTEGER NOT NULL REFERENCES parents(id) DEFERRABLE INITIALLY DEFERRED"
                ")"
            )

        traced_statements: list[str] = []
        sqlspec = SQLSpec()
        config = AiosqliteConfig(
            connection_config={
                "database": tmp.name,
                "pool_size": 1,
                "min_size": 1,
                "connect_timeout": 0.5,
                "enable_foreign_keys": True,
            },
            driver_features={"trace_callback": traced_statements.append},
            extension_config={"sanic": {"commit_mode": "autocommit", "session_key": "db"}},
        )
        sqlspec.add_config(config)
        plugin = SQLSpecPlugin(sqlspec)
        app = _create_sanic_app("SQLSpecSanicDeferredFKCommit")

        @app.post("/violate")
        async def violate(request: Request):
            db = plugin.get_session(request, "db")
            await db.execute("INSERT INTO children (id, parent_id) VALUES (:id, :parent_id)", id=1, parent_id=999)
            return response.json({"status": "deferred_until_commit"})

        @app.post("/valid")
        async def valid(request: Request):
            db = plugin.get_session(request, "db")
            await db.execute("INSERT INTO parents (id) VALUES (:id)", id=10)
            await db.execute("INSERT INTO children (id, parent_id) VALUES (:id, :parent_id)", id=2, parent_id=10)
            return response.json({"status": "ok"})

        @app.get("/children")
        async def children(request: Request):
            db = plugin.get_session(request, "db")
            result = await db.execute("SELECT id, parent_id FROM children ORDER BY id")
            return response.json({"rows": result.all()})

        plugin.init_app(app)

        client: Any = reusable_client_cls(app)
        with client:
            traced_statements.clear()
            client.post("/violate")
            violate_sql = [stmt.strip().upper() for stmt in traced_statements]

            _, res_valid = client.post("/valid")
            _, res_children = client.get("/children")

        assert violate_sql.count("COMMIT") == 1
        assert "ROLLBACK" in violate_sql
        assert res_valid.status == 200
        assert res_children.status == 200
        assert res_children.json == {"rows": [{"id": 2, "parent_id": 10}]}
