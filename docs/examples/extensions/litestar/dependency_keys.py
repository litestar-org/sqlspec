from __future__ import annotations

import pytest

__all__ = ("test_litestar_dependency_keys",)


def test_litestar_dependency_keys() -> None:
    pytest.importorskip("litestar")
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    sqlspec = SQLSpec()
    sqlspec.add_config(
        SqliteConfig(
            connection_config={"database": ":memory:"}, extension_config={"litestar": {"session_key": "analytics"}}
        ),
        name="analytics",
    )
    sqlspec.add_config(
        SqliteConfig(
            connection_config={"database": ":memory:"}, extension_config={"litestar": {"session_key": "primary"}}
        ),
        name="primary",
    )
    # end-example

    assert set(sqlspec.configs.keys()) == {"analytics", "primary"}
