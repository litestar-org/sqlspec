"""Integration tests for Flask extension."""

from typing import Any

import pytest
from flask import Flask

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.extensions.flask import SQLSpecPlugin

pytestmark = pytest.mark.xdist_group("sqlite")


def test_flask_manual_mode_sync_sqlite() -> None:
    """Test Flask extension with manual commit mode and sync SQLite."""
    sqlspec = SQLSpec()
    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"flask": {"commit_mode": "manual", "session_key": "db"}},
    )
    sqlspec.add_config(config)

    app = Flask(__name__)
    plugin = SQLSpecPlugin(sqlspec, app)

    @app.route("/create")
    def create_table() -> dict[str, bool]:
        db = plugin.get_session()
        db.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT)")
        db.execute("INSERT INTO test (name) VALUES (?)", ("Alice",))
        conn = plugin.get_connection()
        conn.commit()
        return {"created": True}

    @app.route("/query")
    def query_table() -> dict[str, Any]:
        db = plugin.get_session()
        result = db.execute("SELECT * FROM test")
        rows = result.all()
        return {"count": len(rows), "rows": [dict(r) for r in rows]}

    with app.test_client() as client:
        response = client.get("/create")
        assert response.status_code == 200
        assert response.json == {"created": True}

        response = client.get("/query")
        assert response.status_code == 200
        data = response.json
        assert data is not None
        assert data["count"] == 1
        assert data["rows"][0]["name"] == "Alice"


def test_flask_autocommit_mode_sync_sqlite() -> None:
    """Test Flask extension with autocommit mode and sync SQLite."""
    sqlspec = SQLSpec()
    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"flask": {"commit_mode": "autocommit", "session_key": "db"}},
    )
    sqlspec.add_config(config)

    app = Flask(__name__)
    plugin = SQLSpecPlugin(sqlspec, app)

    @app.route("/create")
    def create_table() -> tuple[dict[str, bool], int]:
        db = plugin.get_session()
        db.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT)")
        db.execute("INSERT INTO test (name) VALUES (?)", ("Bob",))
        return {"created": True}, 201

    @app.route("/query")
    def query_table() -> dict[str, Any]:
        db = plugin.get_session()
        result = db.execute("SELECT * FROM test")
        rows = result.all()
        return {"count": len(rows), "rows": [dict(r) for r in rows]}

    with app.test_client() as client:
        response = client.get("/create")
        assert response.status_code == 201

        response = client.get("/query")
        assert response.status_code == 200
        data = response.json
        assert data is not None
        assert data["count"] == 1
        assert data["rows"][0]["name"] == "Bob"


def test_flask_autocommit_rollback_on_error() -> None:
    """Test Flask extension autocommit rolls back on error status."""
    sqlspec = SQLSpec()
    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"flask": {"commit_mode": "autocommit", "session_key": "db"}},
    )
    sqlspec.add_config(config)

    app = Flask(__name__)
    plugin = SQLSpecPlugin(sqlspec, app)

    @app.route("/setup")
    def setup() -> dict[str, bool]:
        db = plugin.get_session()
        db.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT)")
        return {"created": True}

    @app.route("/insert-error")
    def insert_error() -> tuple[dict[str, str], int]:
        db = plugin.get_session()
        db.execute("INSERT INTO test (name) VALUES (?)", ("Charlie",))
        return {"error": "something went wrong"}, 400

    @app.route("/query")
    def query_table() -> dict[str, int]:
        db = plugin.get_session()
        result = db.execute("SELECT * FROM test")
        rows = result.all()
        return {"count": len(rows)}

    with app.test_client() as client:
        client.get("/setup")

        client.get("/insert-error")

        response = client.get("/query")
        data = response.json
        assert data is not None
        assert data["count"] == 0


def test_flask_autocommit_include_redirect() -> None:
    """Test Flask extension autocommit_include_redirect commits on 3xx."""
    sqlspec = SQLSpec()
    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"flask": {"commit_mode": "autocommit_include_redirect", "session_key": "db"}},
    )
    sqlspec.add_config(config)

    app = Flask(__name__)
    plugin = SQLSpecPlugin(sqlspec, app)

    @app.route("/create")
    def create_table() -> Any:
        from flask import redirect

        db = plugin.get_session()
        db.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT)")
        db.execute("INSERT INTO test (name) VALUES (?)", ("Diana",))
        return redirect("/query")

    @app.route("/query")
    def query_table() -> dict[str, Any]:
        db = plugin.get_session()
        result = db.execute("SELECT * FROM test")
        rows = result.all()
        return {"count": len(rows), "rows": [dict(r) for r in rows]}

    with app.test_client() as client:
        response = client.get("/create", follow_redirects=False)
        assert response.status_code in (301, 302, 303)

        response = client.get("/query")
        data = response.json
        assert data is not None
        assert data["count"] == 1
        assert data["rows"][0]["name"] == "Diana"


def test_flask_multi_database() -> None:
    """Test Flask extension with multiple databases (different adapters)."""
    from sqlspec.adapters.duckdb import DuckDBConfig

    sqlspec = SQLSpec()

    sqlite_config = SqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"flask": {"commit_mode": "autocommit", "session_key": "sqlite_db"}},
    )

    duckdb_config = DuckDBConfig(
        connection_config={"database": ":memory:"},
        extension_config={"flask": {"commit_mode": "autocommit", "session_key": "duckdb_db"}},
    )

    sqlspec.add_config(sqlite_config)
    sqlspec.add_config(duckdb_config)

    app = Flask(__name__)
    plugin = SQLSpecPlugin(sqlspec, app)

    @app.route("/setup")
    def setup() -> dict[str, bool]:
        sqlite_db = plugin.get_session(key="sqlite_db")
        sqlite_db.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)")
        sqlite_db.execute("INSERT INTO users (id, name) VALUES (?, ?)", (1, "Alice"))

        duckdb_db = plugin.get_session(key="duckdb_db")
        duckdb_db.execute("CREATE TABLE IF NOT EXISTS events (id INTEGER, event TEXT)")
        duckdb_db.execute("INSERT INTO events (id, event) VALUES (?, ?)", (1, "login"))

        return {"setup": True}

    @app.route("/query")
    def query() -> dict[str, Any]:
        sqlite_db = plugin.get_session(key="sqlite_db")
        users = sqlite_db.execute("SELECT COUNT(*) as count FROM users").scalar()

        duckdb_db = plugin.get_session(key="duckdb_db")
        events = duckdb_db.execute("SELECT COUNT(*) as count FROM events").scalar()

        return {"users": users, "events": events}

    with app.test_client() as client:
        client.get("/setup")

        response = client.get("/query")
        data = response.json
        assert data is not None
        assert data["users"] == 1
        assert data["events"] == 1


def test_flask_session_caching() -> None:
    """Test that sessions are cached per request."""
    sqlspec = SQLSpec()
    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"flask": {"commit_mode": "manual", "session_key": "db"}},
    )
    sqlspec.add_config(config)

    app = Flask(__name__)
    plugin = SQLSpecPlugin(sqlspec, app)

    @app.route("/test")
    def test_caching() -> dict[str, bool]:
        session1 = plugin.get_session()
        session2 = plugin.get_session()
        assert session1 is session2
        return {"cached": True}

    with app.test_client() as client:
        response = client.get("/test")
        assert response.json == {"cached": True}


def test_flask_default_session_key() -> None:
    """Test default session key resolves to 'db_session'."""

    sqlspec = SQLSpec()
    config = SqliteConfig(connection_config={"database": ":memory:"})
    sqlspec.add_config(config)

    app = Flask(__name__)
    plugin = SQLSpecPlugin(sqlspec, app)

    @app.route("/default-key")
    def default_key() -> dict[str, bool]:
        session_default = plugin.get_session()
        session_named = plugin.get_session("db_session")
        return {"same_session": session_default is session_named}

    with app.test_client() as client:
        response = client.get("/default-key")
        assert response.status_code == 200
        assert response.json == {"same_session": True}


@pytest.mark.skip(
    reason="Async adapters in sync Flask routes require wrapping every driver call - experimental feature"
)
def test_flask_async_adapter_via_portal() -> None:
    """Test Flask extension with async adapter via portal."""
    pytest.importorskip("aiosqlite")

    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    sqlspec = SQLSpec()
    config = AiosqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"flask": {"commit_mode": "autocommit", "session_key": "db"}},
    )
    sqlspec.add_config(config)

    app = Flask(__name__)
    plugin = SQLSpecPlugin(sqlspec, app)

    @app.route("/create")
    def create_table() -> dict[str, bool]:
        db = plugin.get_session()
        db.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT)")
        db.execute("INSERT INTO test (name) VALUES (?)", ("Eve",))
        return {"created": True}

    @app.route("/query")
    def query_table() -> dict[str, Any]:
        db = plugin.get_session()
        result = db.execute("SELECT * FROM test")
        rows = result.all()
        return {"count": len(rows), "rows": [dict(r) for r in rows]}

    with app.test_client() as client:
        response = client.get("/create")
        assert response.status_code == 200

        response = client.get("/query")
        data = response.json
        assert data is not None
        assert data["count"] == 1
        assert data["rows"][0]["name"] == "Eve"
