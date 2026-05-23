"""Verify ``on_pool_destroying`` lifecycle hook fires before pool teardown."""

from typing import Any, cast

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.config import ExtensionConfigs


def test_sync_close_pool_invokes_on_pool_destroying_before_close() -> None:
    """SyncDatabaseConfig.close_pool drains on_pool_destroying hooks first."""

    events: list[str] = []

    def _hook(_context: dict[str, Any]) -> None:
        events.append("destroying")

    config = SqliteConfig(connection_config={"database": ":memory:"})
    config.get_observability_runtime().register_lifecycle_hook("on_pool_destroying", _hook)

    config.create_pool()
    assert config.connection_instance is not None

    config.close_pool()

    assert events == ["destroying"]
    assert config.connection_instance is None


def test_sync_runtime_register_hook_works_without_existing_pool() -> None:
    """Registration must be safe even before create_pool runs."""

    events: list[str] = []
    config = SqliteConfig(connection_config={"database": ":memory:"})

    def _hook(_context: dict[str, Any]) -> None:
        events.append("hit")

    config.get_observability_runtime().register_lifecycle_hook("on_pool_destroying", _hook)

    # No pool to act on, but close_pool must not crash and must skip the hook
    # (connection_instance is None -> close helper passes through without invoking).
    config.close_pool()

    assert events == []


@pytest.mark.anyio
async def test_async_close_pool_awaits_on_pool_destroying_coroutine() -> None:
    """AsyncDatabaseConfig.close_pool awaits awaitable hooks before draining the pool."""

    events: list[str] = []

    async def _hook(_context: dict[str, Any]) -> None:
        events.append("destroying")

    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    config.get_observability_runtime().register_lifecycle_hook("on_pool_destroying", cast(Any, _hook))

    await config.create_pool()
    assert config.connection_instance is not None

    await config.close_pool()

    assert events == ["destroying"]
    assert config.connection_instance is None


@pytest.mark.anyio
async def test_async_close_pool_supports_sync_hooks_mixed_with_async() -> None:
    """Async close_pool tolerates sync hooks alongside async ones."""

    events: list[str] = []

    def _sync_hook(_context: dict[str, Any]) -> None:
        events.append("sync")

    async def _async_hook(_context: dict[str, Any]) -> None:
        events.append("async")

    extension_config = cast("ExtensionConfigs", {})
    config = AiosqliteConfig(connection_config={"database": ":memory:"}, extension_config=extension_config)
    runtime = config.get_observability_runtime()
    runtime.register_lifecycle_hook("on_pool_destroying", _sync_hook)
    runtime.register_lifecycle_hook("on_pool_destroying", cast(Any, _async_hook))

    await config.create_pool()
    await config.close_pool()

    assert events == ["sync", "async"]
