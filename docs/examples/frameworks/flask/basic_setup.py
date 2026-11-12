"""Configure SQLSpecPlugin with a Flask application factory."""

# --8<-- [start:example]
from typing import Any

from flask import Flask

from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.extensions.flask import SQLSpecPlugin


def create_app(plugin: SQLSpecPlugin) -> Flask:
    """Create a Flask application initialized with SQLSpecPlugin."""
    app = Flask(__name__)
    plugin.init_app(app)

    @app.get("/health")
    def health() -> dict[str, Any]:
        db: SqliteDriver = plugin.get_session()
        return db.select_one("SELECT 1 AS ok")

    return app


plugin = SQLSpecPlugin(SqliteConfig(connection_config={"database": ":memory:"}))
app = create_app(plugin)
# --8<-- [end:example]

__all__ = ("app", "create_app", "main", "plugin")


def main() -> Flask:
    """Build the SQLSpec plugin and return the configured Flask application."""
    return app


if __name__ == "__main__":
    print(main())
