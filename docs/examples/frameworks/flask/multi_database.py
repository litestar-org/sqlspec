from __future__ import annotations

import pytest

__all__ = ("test_flask_multi_database",)


def test_flask_multi_database() -> None:
    pytest.importorskip("flask")
    # start-example
    from flask import Flask

    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
    from sqlspec.extensions.flask import SQLSpecPlugin

    sqlspec = SQLSpec()

    # Primary database
    sqlspec.add_config(
        SqliteConfig(
            connection_config={"database": ":memory:"},
            extension_config={"flask": {"session_key": "db", "connection_key": "db_connection", "pool_key": "db_pool"}},
        )
    )

    # ETL database with custom keys
    sqlspec.add_config(
        SqliteConfig(
            connection_config={"database": ":memory:"},
            extension_config={
                "flask": {"session_key": "etl_db", "connection_key": "etl_connection", "pool_key": "etl_pool"}
            },
        )
    )

    plugin = SQLSpecPlugin(sqlspec)

    def create_app() -> Flask:
        app = Flask(__name__)
        plugin.init_app(app)

        @app.get("/report")
        def report() -> dict[str, list]:
            db: SqliteDriver = plugin.get_session("db")
            etl_db: SqliteDriver = plugin.get_session("etl_db")

            users = db.select("SELECT 1 as id, 'Alice' as name")
            metrics = etl_db.select("SELECT 'metric1' as name, 100 as value")
            return {"users": users, "metrics": metrics}

        return app

    app = create_app()
    # end-example

    assert app is not None
