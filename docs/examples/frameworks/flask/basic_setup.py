from __future__ import annotations

from typing import Any

import pytest

__all__ = ("test_flask_basic_setup",)


def test_flask_basic_setup() -> None:
    pytest.importorskip("flask")
    # start-example
    from flask import Flask

    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.extensions.flask import SQLSpecPlugin

    sqlspec = SQLSpec()
    sqlspec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    app = Flask(__name__)
    plugin = SQLSpecPlugin(sqlspec, app)

    @app.get("/health")
    def health() -> Any:
        session = plugin.get_session()
        result = session.execute("select 1 as ok")
        return result.one()

    # end-example

    assert plugin is not None
