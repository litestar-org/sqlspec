from __future__ import annotations

from typing import Any

import pytest

__all__ = ("test_litestar_dependency_injection",)


def test_litestar_dependency_injection() -> None:
    pytest.importorskip("litestar")
    # start-example
    from litestar import Litestar, get

    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.extensions.litestar import SQLSpecPlugin

    sqlspec = SQLSpec()
    analytics = sqlspec.add_config(
        SqliteConfig(
            connection_config={"database": ":memory:"},
            extension_config={"litestar": {"session_key": "analytics_session"}},
        )
    )

    @get("/analytics")
    def analytics_view(analytics_session: Any) -> Any:
        result = analytics_session.execute("select 42 as metric")
        return result.one()

    app = Litestar(route_handlers=[analytics_view], plugins=[SQLSpecPlugin(sqlspec=sqlspec)])
    # end-example

    assert analytics.name == "default"
    assert app is not None
