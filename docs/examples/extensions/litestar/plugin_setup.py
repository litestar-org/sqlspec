from __future__ import annotations

import pytest

__all__ = ("test_litestar_plugin_setup",)


def test_litestar_plugin_setup() -> None:
    pytest.importorskip("litestar")
    # start-example
    from litestar import Litestar, get

    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
    from sqlspec.extensions.litestar import SQLSpecPlugin

    sqlspec = SQLSpec()
    sqlspec.add_config(
        SqliteConfig(
            connection_config={"database": ":memory:"}, extension_config={"litestar": {"session_key": "db_session"}}
        )
    )

    @get("/health")
    def health_check(db_session: SqliteDriver) -> dict[str, str]:
        result = db_session.execute("SELECT 'ok' as status")
        return result.one()

    app = Litestar(route_handlers=[health_check], plugins=[SQLSpecPlugin(sqlspec=sqlspec)])
    # end-example

    assert app is not None
