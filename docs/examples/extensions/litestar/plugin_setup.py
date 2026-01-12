from __future__ import annotations

import pytest

__all__ = ("test_litestar_plugin_setup",)


def test_litestar_plugin_setup() -> None:
    pytest.importorskip("litestar")
    # start-example
    from litestar import Litestar

    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.extensions.litestar import SQLSpecPlugin

    sqlspec = SQLSpec()
    sqlspec.add_config(
        SqliteConfig(
            connection_config={"database": ":memory:"}, extension_config={"litestar": {"session_key": "db_session"}}
        )
    )

    app = Litestar(plugins=[SQLSpecPlugin(sqlspec=sqlspec)])
    # end-example

    assert app is not None
