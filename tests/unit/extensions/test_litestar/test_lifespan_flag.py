"""Tests for the Litestar ``manage_lifespan`` setting and its interaction with ``disable_di``."""

from typing import Any
from unittest.mock import MagicMock

import pytest
from litestar.config.app import AppConfig

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.adapters.aiosqlite.litestar import AiosqliteStore
from sqlspec.base import SQLSpec
from sqlspec.extensions.litestar.plugin import SQLSpecPlugin


def _init_app(*litestar_settings: "dict[str, Any]") -> AppConfig:
    sqlspec = SQLSpec()
    for index, settings in enumerate(litestar_settings):
        sqlspec.add_config(
            AiosqliteConfig(
                connection_config={"database": ":memory:"},
                bind_key=f"db{index}",
                extension_config={
                    "litestar": {
                        "session_key": f"db_session_{index}",
                        "connection_key": f"db_connection_{index}",
                        "pool_key": f"db_pool_{index}",
                        **settings,
                    }
                },
            )
        )
    app_config = AppConfig()
    SQLSpecPlugin(sqlspec=sqlspec).on_app_init(app_config)
    return app_config


async def _managed_pool_keys(app_config: AppConfig) -> "set[str]":
    keys: set[str] = set()
    for lifespan in app_config.lifespan:
        app = MagicMock()
        app.state = {}
        app.logger = None
        async with lifespan(app):  # type: ignore[operator]
            keys.update(app.state)
    return keys


def test_disable_di_manage_lifespan_registers_lifespan_only() -> None:
    app_config = _init_app({"disable_di": True, "manage_lifespan": True})
    assert len(app_config.lifespan) == 1
    assert app_config.dependencies == {}
    assert app_config.before_send == []


def test_defaults_unchanged() -> None:
    disabled = _init_app({"disable_di": True})
    assert disabled.lifespan == []
    assert disabled.dependencies == {}

    default = _init_app({})
    assert len(default.lifespan) == 1
    assert set(default.dependencies) == {"db_session_0", "db_connection_0", "db_pool_0"}
    assert len(default.before_send) == 1


def test_manage_lifespan_false_with_di() -> None:
    app_config = _init_app({"disable_di": False, "manage_lifespan": False})
    assert app_config.lifespan == []
    assert set(app_config.dependencies) == {"db_session_0", "db_connection_0", "db_pool_0"}
    assert len(app_config.before_send) == 1


@pytest.mark.anyio
async def test_manage_lifespan_applies_per_config() -> None:
    app_config = _init_app({"disable_di": True, "manage_lifespan": True}, {"manage_lifespan": False}, {})
    assert await _managed_pool_keys(app_config) == {"db_pool_0", "db_pool_2"}
    assert set(app_config.dependencies) == {
        "db_session_1",
        "db_connection_1",
        "db_pool_1",
        "db_session_2",
        "db_connection_2",
        "db_pool_2",
    }


def test_store_accepts_manage_lifespan() -> None:
    config = AiosqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"litestar": {"disable_di": True, "manage_lifespan": True}},
    )
    assert AiosqliteStore(config).config is config
