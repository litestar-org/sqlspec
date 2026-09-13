from __future__ import annotations

import pytest

__all__ = ("test_litestar_dependency_keys",)


def test_litestar_dependency_keys() -> None:
    pytest.importorskip("litestar")
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.extensions.litestar import SQLSpecPlugin

    sqlspec = SQLSpec()
    sqlspec.add_config(
        SqliteConfig(
            bind_key="analytics",
            connection_config={"database": ":memory:"},
            extension_config={"litestar": {"session_key": "analytics"}},
        )
    )
    sqlspec.add_config(
        SqliteConfig(
            bind_key="primary",
            connection_config={"database": ":memory:"},
            extension_config={"litestar": {"session_key": "primary"}},
        )
    )
    plugin = SQLSpecPlugin(sqlspec=sqlspec)
    # end-example

    assert plugin.get_config("analytics").bind_key == "analytics"
    assert plugin.get_config("primary").bind_key == "primary"
