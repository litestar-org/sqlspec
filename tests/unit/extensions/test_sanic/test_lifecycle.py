"""Tests for Sanic extension lifecycle listeners."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

pytest.importorskip("sanic")

from sanic import Sanic

from sqlspec.extensions.sanic import SQLSpecPlugin
from sqlspec.extensions.sanic._utils import has_context_value


async def test_startup_creates_pool_on_app_context() -> None:
    """before_server_start should create configured pools on app.ctx."""
    pool = object()
    config = _make_config(pool=pool)
    plugin = SQLSpecPlugin(MagicMock(configs={"default": config}))
    app = SimpleNamespace(ctx=SimpleNamespace())

    await plugin._before_server_start(app)  # pyright: ignore[reportPrivateUsage]

    assert app.ctx.db_pool is pool
    config.create_pool.assert_awaited_once_with()


async def test_startup_is_idempotent_for_existing_pool() -> None:
    """before_server_start should not recreate a pool already present on app.ctx."""
    pool = object()
    config = _make_config(pool=pool)
    plugin = SQLSpecPlugin(MagicMock(configs={"default": config}))
    app = SimpleNamespace(ctx=SimpleNamespace(db_pool=pool))

    await plugin._before_server_start(app)  # pyright: ignore[reportPrivateUsage]

    assert app.ctx.db_pool is pool
    config.create_pool.assert_not_awaited()


async def test_shutdown_closes_pool_and_removes_app_context_value() -> None:
    """after_server_stop should close configured pools and remove app.ctx storage."""
    pool = object()
    config = _make_config(pool=pool)
    plugin = SQLSpecPlugin(MagicMock(configs={"default": config}))
    app = SimpleNamespace(ctx=SimpleNamespace(db_pool=pool))

    await plugin._after_server_stop(app)  # pyright: ignore[reportPrivateUsage]

    config.close_pool.assert_awaited_once_with()
    assert not has_context_value(app.ctx, "db_pool")


async def test_lifecycle_wraps_sync_pool_hooks() -> None:
    """Lifecycle listeners should support sync adapter pool hooks."""
    pool = object()
    config = _make_config(pool=pool)
    config.create_pool = MagicMock(return_value=pool)
    config.close_pool = MagicMock()
    plugin = SQLSpecPlugin(MagicMock(configs={"default": config}))
    app = SimpleNamespace(ctx=SimpleNamespace())

    await plugin._before_server_start(app)  # pyright: ignore[reportPrivateUsage]
    await plugin._after_server_stop(app)  # pyright: ignore[reportPrivateUsage]

    config.create_pool.assert_called_once_with()
    config.close_pool.assert_called_once_with()
    assert not has_context_value(app.ctx, "db_pool")


def test_init_app_registers_server_lifecycle_listeners() -> None:
    """init_app should register Sanic startup and shutdown listeners."""
    app = Sanic(f"SQLSpecLifecycle{uuid4().hex}")
    plugin = SQLSpecPlugin(MagicMock(configs={}))

    plugin.init_app(app)

    assert "server.init.before" in app.signal_router.name_index
    assert "server.shutdown.after" in app.signal_router.name_index


def _make_config(*, pool: object) -> MagicMock:
    config = MagicMock()
    config.supports_connection_pooling = True
    config.extension_config = {"sanic": {}}
    config.create_pool = AsyncMock(return_value=pool)
    config.close_pool = AsyncMock()
    return config
