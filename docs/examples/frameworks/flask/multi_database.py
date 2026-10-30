"""Access multiple named SQLSpec database sessions inside a Flask route."""

from typing import Any

from flask import Flask

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.extensions.flask import SQLSpecPlugin

__all__ = ("app", "create_app", "main")


# --8<-- [start:example]
def create_app(plugin: SQLSpecPlugin) -> Flask:
    """Create a Flask application exposing a multi-database report endpoint."""
    app = Flask(__name__)
    plugin.init_app(app)

    @app.get("/report")
    def report() -> dict[str, list[dict[str, Any]]]:
        db: SqliteDriver = plugin.get_session("db")
        etl_db: SqliteDriver = plugin.get_session("etl_db")
        users = db.select(
            """
            SELECT 1 AS id, 'Alice' AS name
            """
        )
        metrics = etl_db.select(
            """
            SELECT 'metric1' AS name, 100 AS value
            """
        )
        return {"users": users, "metrics": metrics}

    return app


spec = SQLSpec()
spec.add_config(
    SqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"flask": {"session_key": "db", "connection_key": "db_connection", "pool_key": "db_pool"}},
    )
)
spec.add_config(
    SqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={
            "flask": {"session_key": "etl_db", "connection_key": "etl_connection", "pool_key": "etl_pool"}
        },
    )
)
plugin = SQLSpecPlugin(spec)
app = create_app(plugin)
# --8<-- [end:example]


def main() -> Flask:
    """Configure primary and ETL SQLite binds for Flask and return the app."""
    return app


if __name__ == "__main__":
    print(main())
