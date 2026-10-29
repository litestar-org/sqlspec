"""Configure SQLSpecPlugin with a Flask application factory."""

from typing import Any

from flask import Flask

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.extensions.flask import SQLSpecPlugin

__all__ = ("create_app", "main")


def create_app(plugin: SQLSpecPlugin) -> Flask:
    """Create a Flask application initialized with SQLSpecPlugin."""
    app = Flask(__name__)
    plugin.init_app(app)

    @app.get("/health")
    def health() -> dict[str, Any]:
        db: SqliteDriver = plugin.get_session()
        return db.select_one("SELECT 1 AS ok")

    return app


def main() -> Flask:
    """Build the SQLSpec plugin and return the configured Flask application."""
    spec = SQLSpec()
    spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))
    plugin = SQLSpecPlugin(spec)
    return create_app(plugin)


if __name__ == "__main__":
    print(main())
