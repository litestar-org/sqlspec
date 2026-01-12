from __future__ import annotations

import pytest

__all__ = ("test_litestar_multiple_databases",)


def test_litestar_multiple_databases() -> None:
    pytest.importorskip("litestar")
    # start-example
    from litestar import Litestar

    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.extensions.litestar import SQLSpecPlugin

    sqlspec = SQLSpec()
    sqlspec.add_config(SqliteConfig(connection_config={"database": ":memory:"}), name="primary")
    sqlspec.add_config(SqliteConfig(connection_config={"database": ":memory:"}), name="analytics")

    app = Litestar(plugins=[SQLSpecPlugin(sqlspec=sqlspec)])
    # end-example

    assert app is not None
