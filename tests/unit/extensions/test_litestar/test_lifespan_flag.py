"""Tests for the Litestar ``manage_lifespan`` setting and its interaction with ``disable_di``."""

from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import MagicMock

import pytest
from litestar import get
from litestar.config.app import AppConfig
from litestar.di import NamedDependency, Provide
from litestar.testing import create_test_client

from sqlspec.adapters.aiosqlite import AiosqliteDriver
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


def test_disable_di_manage_lifespan_serves_requests_with_user_provider() -> None:
    config = AiosqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"litestar": {"disable_di": True, "manage_lifespan": True}},
    )
    sqlspec = SQLSpec()
    sqlspec.add_config(config)

    async def provide_session() -> "AsyncIterator[AiosqliteDriver]":
        async with config.provide_session() as session:
            yield session  # noqa: ASYNC119

    @get("/value")
    async def read_value(session: NamedDependency[AiosqliteDriver]) -> "dict[str, Any]":
        return {"value": await session.select_value("SELECT 1"), "pool_started": config.connection_instance is not None}

    with create_test_client(
        route_handlers=[read_value],
        plugins=[SQLSpecPlugin(sqlspec=sqlspec)],
        dependencies={"session": Provide(provide_session)},
    ) as client:
        assert client.app.state.get("db_pool") is not None
        response = client.get("/value")

    assert response.status_code == 200
    assert response.json() == {"value": 1, "pool_started": True}
    assert config.connection_instance is None
