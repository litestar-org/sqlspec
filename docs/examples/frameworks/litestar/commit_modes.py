from __future__ import annotations

import pytest

__all__ = ("test_litestar_commit_modes",)


def test_litestar_commit_modes() -> None:
    pytest.importorskip("litestar")
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.extensions.litestar import SQLSpecPlugin

    sqlspec = SQLSpec()
    sqlspec.add_config(
        SqliteConfig(
            connection_config={"database": ":memory:"}, extension_config={"litestar": {"commit_mode": "autocommit"}}
        )
    )
    sqlspec.add_config(
        SqliteConfig(
            bind_key="manual",
            connection_config={"database": ":memory:"},
            extension_config={"litestar": {"commit_mode": "manual"}},
        )
    )

    plugin = SQLSpecPlugin(sqlspec=sqlspec)
    # end-example

    assert plugin is not None
