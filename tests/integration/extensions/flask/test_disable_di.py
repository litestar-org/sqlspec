"""Integration tests for disable_di flag in Flask extension."""

import tempfile

import pytest
from flask import Flask, g

from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.base import SQLSpec
from sqlspec.extensions.flask import SQLSpecPlugin

pytestmark = pytest.mark.xdist_group("sqlite")


def test_flask_disable_di_disables_hooks() -> None:
    """Test that disable_di disables request hooks in Flask extension."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = SqliteConfig(
            connection_config={"database": tmp.name}, extension_config={"flask": {"disable_di": True}}
        )
        sql.add_config(config)

        app = Flask(__name__)
        SQLSpecPlugin(sql, app)

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


def test_flask_default_di_enabled() -> None:
    """Test that default behavior has disable_di=False."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = SqliteConfig(
            connection_config={"database": tmp.name}, extension_config={"flask": {"session_key": "db"}}
        )
        sql.add_config(config)

        app = Flask(__name__)
        plugin = SQLSpecPlugin(sql, app)

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
