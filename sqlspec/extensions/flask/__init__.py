"""Flask extension for SQLSpec.

Provides request-scoped session management, automatic transaction handling,
and async adapter support via portal pattern.

Example:
    from flask import Flask
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.extensions.flask import SQLSpecPlugin

    sqlspec = SQLSpec()
    config = SqliteConfig(
        connection_config={"database": "app.db"},
        extension_config={
            "flask": {
                "commit_mode": "autocommit",
                "session_key": "db"
            }
        }
    )
    sqlspec.add_config(config)

    app = Flask(__name__)
    plugin = SQLSpecPlugin(sqlspec, app)

    @app.route("/users")
    def list_users():
        db = plugin.get_session()
        result = db.execute("SELECT * FROM users")
        return {"users": result.all()}
"""

from sqlspec.extensions.flask._state import FlaskConfigState
from sqlspec.extensions.flask.extension import SQLSpecPlugin

__all__ = ("FlaskConfigState", "SQLSpecPlugin")
