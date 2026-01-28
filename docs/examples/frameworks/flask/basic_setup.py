from __future__ import annotations

import pytest

__all__ = ("test_flask_basic_setup",)


def test_flask_basic_setup() -> None:
    pytest.importorskip("flask")
    # start-example
    from flask import Flask

    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
    from sqlspec.extensions.flask import SQLSpecPlugin

    # Create SQLSpec and plugin at module level
    sqlspec = SQLSpec()
    sqlspec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))
    plugin = SQLSpecPlugin(sqlspec)

    def create_app() -> Flask:
        """Application factory pattern."""
        app = Flask(__name__)
        plugin.init_app(app)

        @app.get("/health")
        def health() -> dict[str, int]:
            db: SqliteDriver = plugin.get_session()
            result = db.execute("select 1 as ok")
            return result.one()

        return app

    app = create_app()
    # end-example

    assert plugin is not None
    assert app is not None
