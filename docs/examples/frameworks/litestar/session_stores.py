from __future__ import annotations

import pytest

__all__ = ("test_litestar_session_stores",)


def test_litestar_session_stores() -> None:
    pytest.importorskip("litestar")
    pytest.importorskip("aiosqlite")
    # start-example
    from sqlspec.adapters.aiosqlite import AiosqliteConfig
    from sqlspec.adapters.aiosqlite.litestar import AiosqliteStore

    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    store = AiosqliteStore(config)
    # end-example

    assert store is not None
