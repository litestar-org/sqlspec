from __future__ import annotations

from typing import Any

import pytest

__all__ = ("test_litestar_basic_setup",)


def test_litestar_basic_setup() -> None:
    pytest.importorskip("litestar")
    # start-example
    from litestar import Litestar, get

    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.extensions.litestar import SQLSpecPlugin

    sqlspec = SQLSpec()
    sqlspec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    @get("/health")
    def health(db_session: Any) -> Any:
        result = db_session.execute("select 1 as ok")
        return result.one()

    app = Litestar(route_handlers=[health], plugins=[SQLSpecPlugin(sqlspec=sqlspec)])
    # end-example

    assert app is not None
